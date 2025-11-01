import logging
import multiprocessing
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pendulum
from pydantic import ValidationError
from sqlalchemy import MetaData, Table, insert, text, update
from sqlalchemy.orm import Session, sessionmaker

from src.db import (
    create_row_hash,
    create_stage_table,
    create_tables,
    get_table_columns,
)
from src.exceptions import (
    FILE_ERROR_EXCEPTIONS,
    AuditFailedError,
    ValidationThresholdExceededError,
)
from src.notifications import send_failure_notification
from src.readers.base_reader import BaseReader
from src.readers.reader_factory import ReaderFactory
from src.retry import get_error_location, retry
from src.settings import config
from src.sources.base import DataSource, FileLoadLog
from src.sources.systems.master import MASTER_REGISTRY

logger = logging.getLogger(__name__)


class FileProcessor:
    def __init__(self):
        self.reader_factory = ReaderFactory()
        self.engine = create_tables()
        self.Session = sessionmaker[Session](bind=self.engine)
        self.thread_pool = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())
        self._metadata = MetaData()
        self._file_load_log: Optional[Table] = None

    def _get_file_load_log(self) -> Table:
        if self._file_load_log is None:
            self._metadata.reflect(bind=self.engine, only=["file_load_log"])
            self._file_load_log = Table(
                "file_load_log", self._metadata, autoload_with=self.engine
            )
        return self._file_load_log

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
                    f"SELECT EXISTS(SELECT 1 FROM {source.table_name} WHERE source_filename = :filename)"
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

    def _create_field_mapping(self, reader: BaseReader) -> Dict[str, str]:
        field_mapping = {}
        for field_name, field_info in reader.source.source_model.model_fields.items():
            if field_info.alias:
                field_mapping[field_info.alias.lower()] = field_name
            else:
                field_mapping[field_name.lower()] = field_name
        return field_mapping

    def _process_file(
        self, file_path: Path, archive_path: Path, reader: BaseReader, log: FileLoadLog
    ) -> Iterator[Dict[str, Any]]:
        self._copy_to_archive(file_path, archive_path, log)

        log.processing_started_at = pendulum.now()
        records_processed = 0
        validation_errors = 0
        sample_validation_errors = []

        field_mapping = self._create_field_mapping(reader)

        for index, record in enumerate(reader, 1):
            try:
                reader.source.source_model.model_validate(record)
            except ValidationError as e:
                validation_errors += 1
                records_processed += 1
                logger.warning(
                    f"[log_id={log.id}] Validation failed for row {index} for file {file_path.name}: {e}"
                )
                if len(sample_validation_errors) <= 5:
                    record["row_number"] = (
                        index  # Add Row Number to sample for debugging
                    )
                    record["validation_error"] = (
                        e  # Add Validation Error to sample for debugging
                    )
                    sample_validation_errors.append(record)
                continue

            # Rename alias keys to column names and trim unneeded columns
            record = {
                field_mapping[k.lower()]: v
                for k, v in record.items()
                if k.lower() in field_mapping
            }

            record["etl_row_hash"] = create_row_hash(record)
            record["source_filename"] = file_path.name
            record["file_load_log_id"] = log.id

            yield record
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
                    f"Sample errors: {sample_validation_errors}"
                )
                raise ValidationThresholdExceededError(error_msg)

        log.processing_ended_at = pendulum.now()
        log.processing_success = True
        return log

    @retry()
    def _load_records(
        self, records: Iterator[Dict[str, Any]], reader: BaseReader, log: FileLoadLog
    ) -> FileLoadLog:
        log.stage_load_started_at = pendulum.now()
        source_filename = reader.file_path.name
        target_table_name = reader.source.table_name

        stage_table_name = create_stage_table(
            self.engine, reader.source, source_filename, log
        )

        # If not SQL Server, uses configured batch size
        batch_size = self._calculate_batch_size(reader.source)

        try:
            batch = []
            records_stage_loaded = 0

            for record in records:
                batch.append(record)

                if len(batch) >= batch_size:
                    self._insert_batch(batch, stage_table_name)
                    records_stage_loaded += len(batch)
                    batch = []
                    # Log progress every 100k records for large files, or every batch for smaller files
                    if (
                        records_stage_loaded % 100000 == 0
                        or records_stage_loaded < 100000
                    ):
                        logger.info(
                            f"[log_id={log.id}] Loaded {records_stage_loaded:,} records so far into stage table {stage_table_name} from {source_filename}..."
                        )

            # Insert remaining records in final batch
            if batch:
                self._insert_batch(batch, stage_table_name)
                records_stage_loaded += len(batch)

            logger.info(
                f"[log_id={log.id}] Successfully loaded {records_stage_loaded} records into stage table {stage_table_name}"
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

            return log

        finally:
            reader.file_path.unlink()
            logger.info(f"[log_id={log.id}] Deleted {source_filename}")
            self._drop_stage_table(stage_table_name, log)

    def _calculate_batch_size(self, source) -> int:
        """If SQL Server, calculate batch size based on 1000 values per INSERT limit."""
        database_url = str(self.engine.url)
        if "mssql" in database_url.lower():
            # SQL Server has 1000 values per INSERT limit
            max_values = 1000
            column_count = (
                len(source.source_model.model_fields) + 2
            )  # +2 for ETL metadata columns (etl_row_hash, source_filename)
            # Calculate max rows: (max_values / columns_per_row) - 1 for safety margin
            max_rows = (max_values // column_count) - 1
            return max(1, min(max_rows, config.BATCH_SIZE))

        # For other databases, use configured batch size
        return config.BATCH_SIZE

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
    def _audit_data(
        self,
        stage_table_name: str,
        source_filename: str,
        source: DataSource,
        log: FileLoadLog,
    ) -> FileLoadLog:
        log.audit_started_at = pendulum.now()
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

                grain_related_audits = [
                    audit for audit in failed_audits if "grain_unique" in audit.lower()
                ]

                if grain_related_audits and source.grain:
                    grain_aliases = []
                    for grain_field in source.grain:
                        field_info = source.source_model.model_fields.get(grain_field)
                        if field_info:
                            alias = (
                                field_info.alias if field_info.alias else grain_field
                            )
                            grain_aliases.append(alias)
                        else:
                            grain_aliases.append(grain_field)

                    error_msg_parts.append(
                        f"Grain columns (file column names): {', '.join(grain_aliases)}"
                    )

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
                update_set = ", ".join(
                    [f"{col} = stage.{col}" for col in update_columns]
                )
                update_set += f", etl_updated_at = '{now_iso}'"

                insert_columns = ", ".join(columns) + ", etl_created_at"
                insert_values = ", ".join([f"stage.{col}" for col in columns])
                insert_values += f", '{now_iso}'"

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

                merge_sql = text(f"""
                    MERGE INTO {target_table_name} AS target
                    USING {stage_table_name} AS stage
                    ON {join_condition}
                    WHEN MATCHED AND stage.etl_row_hash != target.etl_row_hash THEN
                        UPDATE SET {update_set}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_columns})
                        VALUES ({insert_values})
                """)

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
                    log = self._load_records(
                        self._process_file(file_path, archive_path, reader, log),
                        reader,
                        log,
                    )
                    log.success = True
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
