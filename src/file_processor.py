import logging
import multiprocessing
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterator, List

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
from src.exceptions import AuditFailedError, ValidationThresholdExceededError
from src.readers.base_reader import BaseReader
from src.readers.reader_factory import ReaderFactory
from src.retry import retry
from src.settings import config
from src.sources.base import DataSource, FileLoadLog
from src.sources.systems.master import MASTER_REGISTRY

logger = logging.getLogger(__name__)


class FileProcessor:
    def __init__(self):
        self.reader_factory = ReaderFactory()
        self.engine = create_tables(config.DATABASE_URL)
        self.Session = sessionmaker[Session](bind=self.engine)
        self.thread_pool = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())
        self._metadata = MetaData()
        self._file_load_log: Table | None = None

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

    def _create_field_mapping(self, reader: BaseReader) -> Dict[str, str]:
        field_mapping = {}
        for field_name, field_info in reader.source.source_model.model_fields.items():
            if field_info.alias:
                field_mapping[field_info.alias] = field_name
            else:
                field_mapping[field_name] = field_name
        return field_mapping

    def _process_file(
        self, file_path: str, archive_path: str, reader: BaseReader, log: FileLoadLog
    ) -> Iterator[Dict[str, Any]]:
        log.processing_started_at = pendulum.now()
        try:
            file_path = Path(file_path)
            archive_path = Path(archive_path)
        except Exception as e:
            raise ValueError(
                f"Invalid file path: {file_path} or archive path: {archive_path}: {e}"
            )

        archive_file_path = archive_path / file_path.name
        try:
            shutil.copyfile(file_path, archive_file_path)
            logger.info(
                f"[log_id={log.id}] Copied {file_path.name} to archive: {archive_file_path}"
            )
        except Exception as e:
            logger.error(
                f"[log_id={log.id}] Failed to copy {file_path.name} to archive: {e}"
            )
            raise

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
                field_mapping[k]: v for k, v in record.items() if k in field_mapping
            }

            record["etl_row_hash"] = create_row_hash(record)
            record["source_filename"] = file_path.name

            yield record
            records_processed += 1

        log.records_processed = records_processed
        log.validation_errors = validation_errors

        # Check validation error threshold (per file configuration)
        if records_processed > 0 and validation_errors > 0:
            error_rate = validation_errors / records_processed
            threshold = reader.source.validation_error_threshold
            if error_rate > threshold:
                raise ValidationThresholdExceededError(
                    f"Validation error rate ({error_rate:.2%}) exceeds threshold "
                    f"({threshold:.2%}). "
                    f"Sample errors: {sample_validation_errors}"
                )

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
            logger.info(
                f"[log_id={log.id}] Deleted {source_filename} after successful load"
            )
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
                raise AuditFailedError(
                    f"Audit checks failed for file: {source_filename} table: {stage_table_name} audits: {failed_audits}"
                )
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
                # Check if this filename has already been processed
                check_sql = text(
                    f"SELECT EXISTS(SELECT 1 FROM {target_table_name} WHERE source_filename = :filename)"
                )
                result = session.execute(
                    check_sql, {"filename": source_filename}
                ).scalar()

                if result:
                    log.merge_skipped = True
                    logger.warning(
                        f"[log_id={log.id}] File {source_filename} already processed, skipping merge"
                    )
                    return log

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
                log.merge_skipped = False
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

    def _process_file_batch(self, batch: List[str], archive_path: str) -> list[dict]:
        results: list[dict] = []
        for file_path in batch:
            try:
                reader = self._get_reader(Path(file_path))
                if not reader:
                    logger.warning(
                        f"[log_id=N/A] No reader found for file: {Path(file_path).name}"
                    )
                    continue

                log = FileLoadLog(
                    file_name=Path(file_path).name,
                    started_at=pendulum.now(),
                )
                log.id = self._log_start(log.file_name, log.started_at)
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
            except Exception as e:
                log_id = log.id if log else "N/A"
                logger.error(f"[log_id={log_id}] Failed to process {file_path}: {e}")
                if log:
                    log.ended_at = pendulum.now()
                    log.success = False
                    self._log_update(log)
                    results.append(
                        log.model_dump(include={"id", "file_name", "success"})
                    )
        return results

    def process_files_parallel(
        self, file_paths: List[str], archive_path: str
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
