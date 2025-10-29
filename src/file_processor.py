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

from src.db import create_row_hash, create_tables
from src.readers.base_reader import BaseReader
from src.readers.reader_factory import ReaderFactory
from src.settings import config
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
        now = pendulum.now().to_iso8601_string()

        field_mapping = self._create_field_mapping(reader)

        for i, record in enumerate(reader, 1):
            try:
                reader.source.source_model.model_validate(record)
            except ValidationError as e:
                error_count += 1
                record_count += 1
                logger.warning(f"Validation failed for row {i}: {e}")
                continue

            # Rename alias keys to column names
            record = {field_mapping[k]: v for k, v in record.items()}

            record["etl_row_hash"] = create_row_hash(record)
            record["source_filename"] = file_path.name
            record["etl_created_at"] = now

            yield record
            record_count += 1

    def _load_records(self, records: Iterator[Dict[str, Any]], reader: BaseReader):
        batch = []
        total_loaded = 0
        table_name = reader.source.table_name

        for record in records:
            batch.append(record)

            if len(batch) >= config.BATCH_SIZE:
                self._insert_batch(batch, table_name)
                total_loaded += len(batch)
                batch = []
                logger.info(
                    f"Loaded {total_loaded} records so far from {reader.file_path.name}..."
                )

        # Insert remaining records in final batch
        if batch:
            self._insert_batch(batch, table_name)
            total_loaded += len(batch)

        logger.info(
            f"Successfully loaded {total_loaded} records into table {table_name} from {reader.file_path.name}"
        )

        # Delete the original file after successful load (already archived)
        reader.file_path.unlink()
        logger.info(f"Deleted {reader.file_path.name} after successful load")

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
