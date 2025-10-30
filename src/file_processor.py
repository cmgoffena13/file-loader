import logging
import multiprocessing
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterator, List

import pendulum
from pydantic import BaseModel, ValidationError
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from src.db import (
    create_row_hash,
    create_stage_table,
    create_tables,
    get_table_columns,
)
from src.exceptions import AuditFailedError
from src.readers.base_reader import BaseReader
from src.readers.reader_factory import ReaderFactory
from src.settings import config
from src.sources.base import DataSource
from src.sources.systems.master import MASTER_REGISTRY

logger = logging.getLogger(__name__)


class FileProcessingResult(BaseModel):
    file_name: str
    success: bool


class FileProcessor:
    def __init__(self):
        self.reader_factory = ReaderFactory()
        self.engine = create_tables(config.DATABASE_URL)
        self.Session = sessionmaker[Session](bind=self.engine)

        cpu_count = multiprocessing.cpu_count()
        self.thread_pool = ThreadPoolExecutor(max_workers=cpu_count)

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
        self, file_path: str, archive_path: str, reader: BaseReader
    ) -> Iterator[Dict[str, Any]]:
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
            logger.info(f"Copied {file_path.name} to archive: {archive_file_path}")
        except Exception as e:
            logger.error(f"Failed to copy {file_path.name} to archive: {e}")
            raise

        record_count = 0
        error_count = 0

        field_mapping = self._create_field_mapping(reader)

        for i, record in enumerate(reader, 1):
            try:
                reader.source.source_model.model_validate(record)
            except ValidationError as e:
                error_count += 1
                record_count += 1
                logger.warning(f"Validation failed for row {i}: {e}")
                continue

            # Rename alias keys to column names and trim unneeded columns
            record = {
                field_mapping[k]: v for k, v in record.items() if k in field_mapping
            }

            record["etl_row_hash"] = create_row_hash(record)
            record["source_filename"] = file_path.name

            yield record
            record_count += 1

    def _load_records(self, records: Iterator[Dict[str, Any]], reader: BaseReader):
        """Load records into stage table, audit, then merge to target table."""
        source_filename = reader.file_path.name
        target_table_name = reader.source.table_name

        # Create stage table
        stage_table_name = create_stage_table(
            self.engine, reader.source, source_filename
        )

        try:
            batch = []
            total_loaded = 0

            for record in records:
                batch.append(record)

                if len(batch) >= config.BATCH_SIZE:
                    self._insert_batch(batch, stage_table_name)
                    total_loaded += len(batch)
                    batch = []
                    logger.info(
                        f"Loaded {total_loaded} records so far into stage from {source_filename}..."
                    )

            # Insert remaining records in final batch
            if batch:
                self._insert_batch(batch, stage_table_name)
                total_loaded += len(batch)

            logger.info(
                f"Successfully loaded {total_loaded} records into stage table {stage_table_name}"
            )

            self._audit_data(stage_table_name, source_filename, reader.source)

            self._merge_stage_to_target(
                stage_table_name, target_table_name, reader.source
            )

            logger.info(
                f"Successfully performed merged from {stage_table_name} to {target_table_name}"
            )

            return total_loaded

        finally:
            reader.file_path.unlink()
            logger.info(f"Deleted {source_filename} after successful load")
            self._drop_stage_table(stage_table_name)

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

    def _audit_data(
        self, stage_table_name: str, source_filename: str, source: DataSource
    ):
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
                raise AuditFailedError(
                    f"Audit checks failed for file: {source_filename} table: {stage_table_name} audits: {failed_audits}"
                )

    def _merge_stage_to_target(
        self, stage_table_name: str, target_table_name: str, source: DataSource
    ):
        with self.Session() as session:
            try:
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

                merge_sql = text(f"""
                    MERGE INTO {target_table_name} AS target
                    USING {stage_table_name} AS stage
                    ON {join_condition}
                    WHEN MATCHED THEN
                        UPDATE SET {update_set}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_columns})
                        VALUES ({insert_values})
                """)

                session.execute(merge_sql)
                session.commit()
                logger.info(
                    f"Merged records from {stage_table_name} to {target_table_name}"
                )

            except Exception as e:
                session.rollback()
                logger.error(
                    f"Failed to merge {stage_table_name} to {target_table_name}: {e}"
                )
                raise

    def _drop_stage_table(self, stage_table_name: str):
        """Drop the stage table after successful merge."""
        with self.Session() as session:
            try:
                drop_sql = text(f"DROP TABLE IF EXISTS {stage_table_name}")
                session.execute(drop_sql)
                session.commit()
                logger.info(f"Dropped stage table: {stage_table_name}")
            except Exception as e:
                session.rollback()
                logger.warning(f"Failed to drop stage table {stage_table_name}: {e}")

    def process_files_parallel(
        self, file_paths: List[str], archive_path: str
    ) -> List[FileProcessingResult]:
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

        def process_file_batch(batch: List[str]):
            results = []
            for file_path in batch:
                try:
                    # Get reader first to pass to iterators
                    reader = self._get_reader(Path(file_path))
                    if not reader:
                        logger.warning(
                            f"No reader found for file: {Path(file_path).name}"
                        )
                        continue

                    # Chain iterators to process file and load records (creates a stream)
                    self._load_records(
                        self._process_file(file_path, archive_path, reader), reader
                    )

                    # Append success result to the list after completion if no exception was raised
                    results.append(
                        FileProcessingResult(
                            file_name=Path(file_path).name, success=True
                        )
                    )
                except AuditFailedError as e:
                    logger.error(f"Audit failed for {file_path}: {e}")
                    results.append(
                        FileProcessingResult(
                            file_name=Path(file_path).name, success=False
                        )
                    )
                except Exception as e:
                    logger.error(f"Failed to process {file_path}: {e}")
                    results.append(
                        FileProcessingResult(
                            file_name=Path(file_path).name, success=False
                        )
                    )
            return results

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            batch_futures = [
                executor.submit(process_file_batch, batch) for batch in file_batches
            ]
            batch_results = [future.result() for future in batch_futures]

        all_results = []
        for batch_result in batch_results:
            all_results.extend(batch_result)

        successful = sum(1 for r in all_results if r.success)
        failed = len(all_results) - successful

        logger.info(
            f"Processed {len(file_paths)} files: {successful} successful, {failed} failed"
        )
        return all_results

    def __del__(self):
        if hasattr(self, "thread_pool"):
            self.thread_pool.shutdown(wait=True)
