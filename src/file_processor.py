import json
import logging
import multiprocessing
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pendulum
from pydantic import ValidationError
from sqlalchemy import MetaData, Table, insert, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from src.db import (
    calculate_batch_size,
    create_duplicate_sql,
    create_grain_validation_sql,
    create_merge_sql,
    create_row_hash,
    create_stage_table,
    create_tables,
    get_delete_dlq_sql,
    get_table_columns,
)
from src.exceptions import (
    FILE_ERROR_EXCEPTIONS,
    AuditFailedError,
    GrainValidationError,
    ValidationThresholdExceededError,
)
from src.notifications import send_failure_notification
from src.readers.base_reader import BaseReader
from src.readers.reader_factory import ReaderFactory
from src.retry import get_error_location, retry
from src.settings import config
from src.sources.base import DataSource, FileLoadLog
from src.sources.systems.master import MASTER_REGISTRY
from src.utils import (
    create_field_mapping,
    create_reverse_field_mapping,
    extract_failed_field_names,
    extract_validation_error_message,
    get_field_alias,
)

logger = logging.getLogger(__name__)


class FileProcessor:
    def __init__(self):
        self.reader_factory = ReaderFactory()
        self.engine = create_tables()
        self.Session = sessionmaker[Session](bind=self.engine)
        self.thread_pool = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())
        self._metadata = MetaData()
        self._file_load_log: Optional[Table] = None
        self._file_load_dlq: Optional[Table] = None

    def _get_file_load_log(self) -> Table:
        if self._file_load_log is None:
            self._metadata.reflect(bind=self.engine, only=["file_load_log"])
            self._file_load_log = Table(
                "file_load_log", self._metadata, autoload_with=self.engine
            )
        return self._file_load_log

    def _get_file_load_dlq(self) -> Table:
        if self._file_load_dlq is None:
            self._metadata.reflect(bind=self.engine, only=["file_load_dlq"])
            self._file_load_dlq = Table(
                "file_load_dlq", self._metadata, autoload_with=self.engine
            )
        return self._file_load_dlq

    @retry()
    def _log_start(self, file_name: str, started_at) -> int:
        log = self._get_file_load_log()
        stmt = insert(log).values(file_name=file_name, started_at=started_at)
        with self.engine.begin() as conn:
            res = conn.execute(stmt)
            return int(res.inserted_primary_key[0])

    @retry()
    def _log_update(self, log: FileLoadLog) -> None:
        log_table = self._get_file_load_log()
        vals = log.model_dump(
            exclude_unset=True, exclude={"id", "file_name", "started_at"}
        )
        stmt = update(log_table).where(log_table.c.id == log.id).values(**vals)
        with self.engine.begin() as conn:
            conn.execute(stmt)

    def _get_reader(self, file_path: Path) -> BaseReader:
        source = MASTER_REGISTRY.find_source_for_file(file_path.name)
        if not source:
            logger.warning(f"No source configuration found for file: {file_path.name}")
            return None

        reader = self.reader_factory.create_reader(file_path, source=source)
        return reader

    @retry()
    def _check_duplicate_file(self, source: DataSource, source_filename: str) -> bool:
        with self.Session() as session:
            try:
                check_sql = text(
                    f"SELECT CASE WHEN EXISTS(SELECT 1 FROM {source.table_name} WHERE source_filename = :filename) THEN 1 ELSE 0 END"
                )
                result = session.execute(
                    check_sql, {"filename": source_filename}
                ).scalar()
                return bool(result)
            except Exception as e:
                logger.warning(
                    f"Error checking for duplicate file {source_filename} in {source.table_name}: {e}"
                )
                return False

    def _copy_to_archive(
        self, file_path: Path, archive_path: Path, log: FileLoadLog
    ) -> None:
        """Copy file to archive directory with logging."""
        archive_file_path = archive_path / file_path.name
        try:
            log.archive_copy_started_at = pendulum.now()
            shutil.copyfile(file_path, archive_file_path)
            log.archive_copy_ended_at = pendulum.now()
            log.archive_copy_success = True
            logger.info(
                f"[log_id={log.id}] Copied {file_path.name} to archive: {archive_file_path}"
            )
        except Exception as e:
            logger.error(
                f"[log_id={log.id}] Failed to copy {file_path.name} to archive: {e}"
            )
            raise

    def _move_to_duplicates(self, file_path: Path, duplicates_path: Path) -> None:
        """Move a duplicate file to the duplicates directory."""
        duplicates_path.mkdir(parents=True, exist_ok=True)
        destination = duplicates_path / file_path.name

        if destination.exists():
            timestamp = pendulum.now().format("YYYYMMDD_HHmmss")
            stem = file_path.stem
            suffix = file_path.suffix
            destination = duplicates_path / f"{stem}_{timestamp}{suffix}"

        shutil.move(str(file_path), str(destination))
        logger.info(
            f"Moved duplicate file {file_path.name} to duplicates directory: {destination}"
        )

    def _process_file(
        self, file_path: Path, archive_path: Path, reader: BaseReader, log: FileLoadLog
    ) -> Iterator[Dict[str, Any]]:
        self._copy_to_archive(file_path, archive_path, log)

        log.processing_started_at = pendulum.now()
        records_processed = 0
        validation_errors = 0
        sample_validation_errors = []
        result = tuple()

        field_mapping = create_field_mapping(reader)
        reverse_field_mapping = create_reverse_field_mapping(reader)

        for index, record in enumerate(reader, 1):
            passed = True
            try:
                validated_record = reader.source.source_model.model_validate(record)
                record = validated_record.model_dump(mode="json")
            except ValidationError as e:
                validation_errors += 1
                records_processed += 1
                logger.debug(
                    f"[log_id={log.id}] Validation failed for row {index} for file {file_path.name}: {e}"
                )
                error_details = (
                    e.errors() if hasattr(e, "errors") else [{"msg": str(e)}]
                )
                failed_field_names = extract_failed_field_names(
                    error_details, reader.source.grain
                )

                # Filter record to only include failed fields and grain fields
                record = {
                    alias_key: value
                    for alias_key, value in record.items()
                    if (field_name := field_mapping.get(alias_key.lower()))
                    and field_name in failed_field_names
                }

                if len(sample_validation_errors) <= 5:
                    sample_validation_errors.append(
                        {
                            "file_row_number": index,
                            "validation_error": extract_validation_error_message(
                                error_details, reverse_field_mapping
                            ),
                            "record": record,
                        }
                    )
                passed = False

            if passed:
                # Rename alias keys to column names and trim unneeded columns
                record = {
                    field_mapping[k.lower()]: v
                    for k, v in record.items()
                    if k.lower() in field_mapping
                }

                record["etl_row_hash"] = create_row_hash(record)
                record["source_filename"] = file_path.name
                record["file_load_log_id"] = log.id
                result = (record, True)
            else:
                record = {
                    "file_record_data": self._serialize_json_for_dlq_table(record),
                    "validation_errors": self._serialize_json_for_dlq_table(
                        extract_validation_error_message(
                            error_details, reverse_field_mapping
                        )
                    ),
                    "file_row_number": index,
                    "source_filename": file_path.name,
                    "file_load_log_id": log.id,
                    "target_table_name": reader.source.table_name,
                    "failed_at": pendulum.now(),
                }
                result = (record, False)

            yield result
            records_processed += 1

        log.records_processed = records_processed
        log.validation_errors = validation_errors

        # Check validation error threshold (per file configuration)
        if records_processed > 0 and validation_errors > 0:
            error_rate = validation_errors / records_processed
            threshold = reader.source.validation_error_threshold
            if error_rate > threshold:
                error_msg = (
                    f"Validation error rate ({error_rate:.2%}) exceeds threshold "
                    f"({threshold:.2%}). "
                    f"Total Records Processed: {records_processed}, "
                    f"Failed Records: {validation_errors}. "
                    f"Sample validation errors: {sample_validation_errors}"
                )
                raise ValidationThresholdExceededError(error_msg)

        log.processing_ended_at = pendulum.now()
        log.processing_success = True

    def _load_records(
        self,
        results: Iterator[tuple[Dict[str, Any], bool]],
        reader: BaseReader,
        log: FileLoadLog,
    ) -> FileLoadLog:
        log.stage_load_started_at = pendulum.now()
        source_filename = reader.file_path.name
        target_table_name = reader.source.table_name

        stage_table_name = create_stage_table(
            self.engine, reader.source, source_filename, log
        )

        # If not SQL Server, uses configured batch size
        batch_size = calculate_batch_size(reader.source)
        try:
            stage_batch = []
            failed_batch = []
            records_stage_loaded = 0
            records_dlq_loaded = 0

            try:
                for record, passed in results:
                    if passed:
                        stage_batch.append(record)

                        if len(stage_batch) >= batch_size:
                            self._insert_batch(stage_batch, stage_table_name)
                            records_stage_loaded += len(stage_batch)
                            stage_batch = []
                            # Log progress every 100k records for large files, or every batch for smaller files
                            if (
                                records_stage_loaded % 100000 == 0
                                or records_stage_loaded < 100000
                            ):
                                logger.info(
                                    f"[log_id={log.id}] Loaded {records_stage_loaded:,} records so far into stage table {stage_table_name} from {source_filename}..."
                                )

                    if not passed:
                        logger.debug(
                            f"[log_id={log.id}] Record failed validation, adding to DLQ batch. Row: {record.get('file_row_number', 'unknown')}, Batch size: {len(failed_batch) + 1}"
                        )
                        failed_batch.append(record)
                        if len(failed_batch) >= batch_size:
                            logger.info(
                                f"[log_id={log.id}] DLQ batch size reached ({batch_size}), calling _insert_dlq_records"
                            )
                            self._insert_dlq_records(failed_batch, log)
                            records_dlq_loaded += len(failed_batch)
                            failed_batch = []
                            if (
                                records_dlq_loaded % 100000 == 0
                                or records_dlq_loaded < 100000
                            ):
                                logger.info(
                                    f"[log_id={log.id}] Loaded {records_dlq_loaded:,} records so far into DLQ from {source_filename}..."
                                )
            finally:  # Insert remaining records in final batch
                if stage_batch:
                    self._insert_batch(stage_batch, stage_table_name)
                    records_stage_loaded += len(stage_batch)
                if failed_batch:
                    logger.info(
                        f"[log_id={log.id}] Flushing final DLQ batch with {len(failed_batch)} records"
                    )
                    self._insert_dlq_records(failed_batch, log)
                    records_dlq_loaded += len(failed_batch)
                else:
                    logger.debug(f"[log_id={log.id}] No failed records to flush to DLQ")

            logger.info(
                f"[log_id={log.id}] Successfully loaded {records_stage_loaded} records into stage table {stage_table_name} and {records_dlq_loaded} records into DLQ"
            )
            log.stage_load_ended_at = pendulum.now()
            log.records_stage_loaded = records_stage_loaded
            log.stage_load_success = True

            log = self._audit_data(
                stage_table_name, source_filename, reader.source, log
            )

            log = self._merge_stage_to_target(
                stage_table_name, target_table_name, reader.source, source_filename, log
            )

            # Only delete DLQ records if this is a reprocessing run (existing DLQ records from previous run)
            self._delete_dlq_records_if_reprocessing(source_filename, log)

            return log

        finally:
            reader.file_path.unlink()
            logger.info(f"[log_id={log.id}] Deleted {source_filename}")
            self._drop_stage_table(stage_table_name, log)

    @retry()
    def _insert_batch(self, batch: list[Dict[str, Any]], table_name: str):
        columns = list[str](batch[0].keys())

        placeholders = ", ".join([f":{col}" for col in columns])
        insert_sql = (
            f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        )

        with self.Session() as session:
            try:
                session.execute(text(insert_sql), batch)
                session.commit()
                logger.debug(f"Inserted {len(batch)} records into {table_name}")
            except Exception as e:
                session.rollback()
                logger.error(f"Failed to insert batch into {table_name}: {e}")
                raise

    @retry()
    def _validate_grain(
        self, source: DataSource, stage_table_name: str, source_filename: str
    ):
        with self.Session() as session:
            grain_sql = create_grain_validation_sql(source)
            grain_sql = grain_sql.format(table=stage_table_name)
            result = session.execute(text(grain_sql)).fetchone()
            if result._mapping["grain_unique"] == 0:
                duplicate_sql = create_duplicate_sql(source)
                duplicate_sql = duplicate_sql.format(table=stage_table_name)
                duplicate_records = session.execute(text(duplicate_sql)).fetchall()

                grain_aliases = [
                    get_field_alias(source, grain_field) for grain_field in source.grain
                ]
                error_msg_parts = [
                    f"Grain values are not unique for file: {source_filename}",
                    f"Table: {stage_table_name}",
                    f"Grain columns (file column names): {', '.join(grain_aliases)}",
                    "Example duplicate grain violations:",
                ]
                for record in duplicate_records:
                    record_dict = dict(record._mapping)
                    aliased_record = {
                        get_field_alias(source, grain_field): record_dict[grain_field]
                        for grain_field in source.grain
                    }
                    aliased_record["duplicate_count"] = record_dict["duplicate_count"]
                    record_str = ", ".join(
                        f"{k}: {v}" for k, v in aliased_record.items()
                    )
                    error_msg_parts.append(f"  - {record_str}")

                error_msg = "\n".join(error_msg_parts)
                raise GrainValidationError(error_msg)
        return True

    @retry()
    def _audit_data(
        self,
        stage_table_name: str,
        source_filename: str,
        source: DataSource,
        log: FileLoadLog,
    ) -> FileLoadLog:
        log.audit_started_at = pendulum.now()

        self._validate_grain(source, stage_table_name, source_filename)

        # If no custom audit_query is provided, only grain validation runs
        if source.audit_query is None:
            log.audit_ended_at = pendulum.now()
            log.audit_success = True
            return log

        with self.Session() as session:
            audit_sql = text(source.audit_query.format(table=stage_table_name).strip())

            result = session.execute(audit_sql).fetchone()
            column_names = list(result._mapping.keys())

            # Check each CASE statement result (1 = success, 0 = failure)
            failed_audits = []
            for audit_name in column_names:
                value = result._mapping[audit_name]
                if value == 0:
                    failed_audits.append(audit_name)

            if failed_audits:
                log.audit_ended_at = pendulum.now()
                log.audit_success = False

                error_msg_parts = [
                    f"Audit checks failed for file: {source_filename}",
                    f"Table: {stage_table_name}",
                    f"Failed audits: {', '.join(failed_audits)}",
                ]

                error_msg = "\n".join(error_msg_parts)
                raise AuditFailedError(error_msg)
            log.audit_ended_at = pendulum.now()
            log.audit_success = True
            return log

    @retry()
    def _merge_stage_to_target(
        self,
        stage_table_name: str,
        target_table_name: str,
        source: DataSource,
        source_filename: str,
        log: FileLoadLog,
    ) -> FileLoadLog:
        with self.Session() as session:
            try:
                log.merge_started_at = pendulum.now()
                columns = [
                    col.name
                    for col in get_table_columns(source, include_timestamps=False)
                ]

                join_condition = " AND ".join(
                    [f"target.{col} = stage.{col}" for col in source.grain]
                )

                now_iso = pendulum.now().to_iso8601_string()

                update_columns = [col for col in columns if col not in source.grain]

                # Get Estimated Target Inserts and Updates
                # EXISTS is more performant than NOT EXISTS
                insert_sql = text(f"""
                SELECT 
                COUNT(*) 
                FROM {stage_table_name} AS stage
                WHERE EXISTS (
                    SELECT 1 
                    FROM {target_table_name} AS target
                    WHERE {join_condition}
                )""")
                existing_records = session.execute(insert_sql).scalar()
                log.target_inserts = log.records_stage_loaded - existing_records

                update_sql = text(f"""
                SELECT 
                COUNT(*) 
                FROM {stage_table_name} AS stage
                WHERE EXISTS (
                    SELECT 1 
                    FROM {target_table_name} AS target
                    WHERE {join_condition}
                    AND stage.etl_row_hash != target.etl_row_hash
                ) 
                """)
                new_updates = session.execute(update_sql).scalar()
                log.target_updates = new_updates

                merge_sql = text(
                    create_merge_sql(
                        stage_table_name=stage_table_name,
                        target_table_name=target_table_name,
                        join_condition=join_condition,
                        columns=columns,
                        update_columns=update_columns,
                        grain=source.grain,
                        now_iso=now_iso,
                    )
                )

                session.execute(merge_sql)
                session.commit()
                log.merge_ended_at = pendulum.now()
                log.merge_success = True
                logger.info(
                    f"[log_id={log.id}] Successfully performed merge from {stage_table_name} to {target_table_name}: {log.target_inserts} inserts, {log.target_updates} updates"
                )
                return log

            except Exception as e:
                session.rollback()
                logger.error(
                    f"[log_id={log.id}] Failed to merge {stage_table_name} to {target_table_name}: {e}"
                )
                raise

    def _delete_dlq_records_if_reprocessing(self, file_name: str, log: FileLoadLog):
        with self.Session() as session:
            dlq_table = self._get_file_load_dlq()
            # Check if there are DLQ records from a previous processing run (log_id < current)
            existing_dlq = session.execute(
                select(dlq_table.c.id)
                .where(
                    dlq_table.c.source_filename == file_name,
                    dlq_table.c.file_load_log_id < log.id,
                )
                .limit(1)
            ).first()

            if existing_dlq:
                # This is a reprocessing run - delete all DLQ records for this file
                self._delete_dlq_records(file_name, log)
            else:
                logger.debug(
                    f"[log_id={log.id}] No previous DLQ records found for {file_name}, skipping deletion (first time processing)"
                )

    @retry()
    def _delete_dlq_records(self, file_name: str, log: FileLoadLog):
        with self.Session() as session:
            delete_sql = text(get_delete_dlq_sql())
            total_deleted = 0

            try:
                while True:
                    result = session.execute(
                        delete_sql, {"file_name": file_name, "limit": config.BATCH_SIZE}
                    )
                    session.commit()

                    if result.rowcount == 0:
                        break

                    total_deleted += result.rowcount

                logger.info(
                    f"[log_id={log.id}] Deleted total of {total_deleted} DLQ record(s) for file: {file_name}"
                )
            except Exception as e:
                session.rollback()
                logger.error(
                    f"[log_id={log.id}] Failed to delete DLQ records for file: {file_name}: {e}"
                )
                raise

    @retry()
    def _drop_stage_table(self, stage_table_name: str, log: FileLoadLog):
        with self.Session() as session:
            try:
                drop_sql = text(f"DROP TABLE IF EXISTS {stage_table_name}")
                session.execute(drop_sql)
                session.commit()
                logger.info(
                    f"[log_id={log.id}] Dropped stage table: {stage_table_name}"
                )
            except Exception as e:
                session.rollback()
                logger.warning(
                    f"[log_id={log.id}] Failed to drop stage table {stage_table_name}: {e}"
                )

    def _serialize_json_for_dlq_table(self, data: Any) -> Any:
        drivername = config.DRIVERNAME

        if drivername == "mssql":
            json_str = json.dumps(data, ensure_ascii=False)
            if len(json_str) > 4000:
                json_str = json_str[:3997] + "..."  # Leave room for "..."
                logger.warning(
                    f"JSON data truncated to 4000 chars for SQL Server compatibility"
                )
            return json_str
        elif drivername == "sqlite":
            return json.dumps(data, ensure_ascii=False)
        else:
            return data

    @retry()
    def _insert_dlq_records(
        self, failed_records: List[Dict[str, Any]], log: FileLoadLog
    ) -> None:
        if not failed_records:
            logger.warning(
                f"[log_id={log.id}] _insert_dlq_records called with empty list"
            )
            return

        logger.info(
            f"[log_id={log.id}] _insert_dlq_records called with {len(failed_records)} records"
        )
        logger.debug(
            f"[log_id={log.id}] First failed record keys: {list(failed_records[0].keys()) if failed_records else 'N/A'}"
        )
        logger.debug(
            f"[log_id={log.id}] First failed record structure: {failed_records[0] if failed_records else 'N/A'}"
        )

        dlq_table = self._get_file_load_dlq()

        with self.Session() as session:
            try:
                stmt = insert(dlq_table).values(failed_records)
                session.execute(stmt)
                session.commit()
                logger.info(
                    f"[log_id={log.id}] Successfully inserted {len(failed_records)} failed records into DLQ"
                )
            except Exception as e:
                session.rollback()
                logger.error(
                    f"[log_id={log.id}] Failed to insert records into DLQ: {e}"
                )
                raise

    def _process_file_batch(self, batch: List[str], archive_path: Path) -> list[dict]:
        results: list[dict] = []
        duplicates_path = config.DUPLICATE_FILES_PATH
        log = None

        for file_path_str in batch:
            file_path = Path(file_path_str)
            try:
                reader = self._get_reader(file_path)
                if not reader:
                    logger.warning(
                        f"[log_id=N/A] No reader found for file: {file_path.name}"
                    )
                    continue

                file_name = reader.file_path.name
                log = FileLoadLog(
                    file_name=file_name,
                    started_at=pendulum.now(),
                )
                log.id = self._log_start(log.file_name, log.started_at)
                if self._check_duplicate_file(reader.source, file_name):
                    logger.warning(
                        f"[log_id={log.id}] File {file_name} has already been processed - moving to duplicates directory"
                    )
                    self._move_to_duplicates(file_path, duplicates_path)
                    log.duplicate_skipped = True
                    logger.info(
                        f"[log_id={log.id}] Successfully moved duplicate file {file_name} to duplicates directory"
                    )
                    if reader.source.notification_emails:
                        error_message = (
                            f"The file {file_name} has already been processed and has been moved to the duplicates directory.\n\n"
                            f"To reprocess this file:\n"
                            f"1. Existing records need to be removed from the target table where source_filename = '{file_name}'\n"
                            f"2. Move the file from the duplicates directory back to the processing directory"
                        )
                        send_failure_notification(
                            file_name=file_name,
                            error_type="Duplicate File Detected",
                            error_message=error_message,
                            log_id=log.id,
                            recipient_emails=reader.source.notification_emails,
                        )
                    self._log_update(log)
                    continue
                try:
                    # Process file and get iterator (which may store failed records in log)
                    records_iterator = self._process_file(
                        file_path, archive_path, reader, log
                    )
                    log = self._load_records(records_iterator, reader, log)
                    log.success = True

                    # Insert failed records into DLQ if DLQ is enabled and there are failures
                    if hasattr(log, "_dlq_failed_records") and log._dlq_failed_records:
                        self._insert_dlq_records(log._dlq_failed_records, reader, log)
                        # Clean up the temporary attribute
                        delattr(log, "_dlq_failed_records")
                finally:
                    log.ended_at = pendulum.now()
                    log.success = False if log.success is None else log.success
                    self._log_update(log)

                results.append(log.model_dump(include={"id", "file_name", "success"}))
            except tuple(FILE_ERROR_EXCEPTIONS) as e:
                logger.error(f"[log_id={log.id}] Failed to process {file_path}: {e}")

                if reader.source.notification_emails:
                    send_failure_notification(
                        file_name=file_path.name,
                        error_type=e.error_type,
                        error_message=str(e),
                        log_id=log.id,
                        recipient_emails=reader.source.notification_emails,
                    )

                log.ended_at = pendulum.now()
                log.success = False
                log.error_type = e.error_type
                self._log_update(log)
                results.append(
                    {
                        "id": log.id,
                        "file_name": file_path.name,
                        "success": False,
                        "error_type": e.error_type,
                        "error_message": str(e),
                        "error_location": get_error_location(e),
                    }
                )
            except Exception as e:
                log_id = log.id if log else "N/A"
                logger.error(f"[log_id={log_id}] Failed to process {file_path}: {e}")

                if log:
                    log.ended_at = pendulum.now()
                    log.success = False
                    log.error_type = type(e).__name__
                    self._log_update(log)
                results.append(
                    {
                        "id": log.id if log else None,
                        "file_name": file_path.name,
                        "success": False,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "error_location": get_error_location(e),
                    }
                )
        return results

    def process_files_parallel(
        self, file_paths: List[str], archive_path: Path
    ) -> list[dict]:
        """Process multiple files in parallel using thread pool."""

        # Divide files evenly among threads
        # Example: 4 threads, 17 files
        # files_per_thread = 17 // 4 = 4
        # remainder = 17 % 4 = 1
        # Thread 0: 4 + 1 = 5 files (gets extra)
        # Thread 1: 4 + 0 = 4 files
        # Thread 2: 4 + 0 = 4 files
        # Thread 3: 4 + 0 = 4 files
        num_threads = self.thread_pool._max_workers
        files_per_thread = len(file_paths) // num_threads
        remainder = len(file_paths) % num_threads

        # Create file batches for each thread
        file_batches = []
        start = 0
        for index in range(num_threads):
            batch_size = files_per_thread + (1 if index < remainder else 0)
            end = start + batch_size
            batch = file_paths[start:end]
            if batch:  # Only add non-empty batches
                file_batches.append(batch)
            start = end

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            batch_futures = [
                executor.submit(self._process_file_batch, batch, archive_path)
                for batch in file_batches
            ]
            batch_results = [future.result() for future in batch_futures]

        all_results = []
        for batch_result in batch_results:
            all_results.extend(batch_result)

        successful = sum(1 for r in all_results if r.get("success"))
        failed = len(all_results) - successful

        logger.info(
            f"Processed {len(file_paths)} files: {successful} successful, {failed} failed"
        )
        return all_results

    def __del__(self):
        if hasattr(self, "thread_pool"):
            self.thread_pool.shutdown(wait=True)
