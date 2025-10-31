import tempfile

import pytest
from sqlalchemy.orm import Session, sessionmaker

from src.exceptions import MissingColumnsError, MissingHeaderError
from src.file_processor import FileProcessor
from src.readers.csv_reader import CSVReader
from src.sources.systems.master import MASTER_REGISTRY
from src.tests.fixtures.source_configs import TEST_SALES


def test_csv_missing_header_raises_error(csv_missing_header):
    """Test that MissingHeaderError is raised when CSV has no header."""
    reader = CSVReader(
        file_path=csv_missing_header,
        source=TEST_SALES,
        delimiter=",",
        encoding="utf-8",
        skip_rows=0,
    )

    with pytest.raises(MissingHeaderError) as exc_info:
        list(reader.read())

    assert "No headers found" in str(exc_info.value)


def test_csv_blank_string_header_raises_error(csv_blank_header):
    """Test that MissingHeaderError is raised when CSV has blank/whitespace headers."""
    reader = CSVReader(
        file_path=csv_blank_header,
        source=TEST_SALES,
        delimiter=",",
        encoding="utf-8",
        skip_rows=0,
    )

    with pytest.raises(MissingHeaderError) as exc_info:
        list(reader.read())

    assert "Whitespace-only headers" in str(exc_info.value)


def test_csv_missing_columns_raises_error(csv_missing_columns):
    """Test that MissingColumnsError is raised when required columns are missing."""
    reader = CSVReader(
        file_path=csv_missing_columns,
        source=TEST_SALES,
        delimiter=",",
        encoding="utf-8",
        skip_rows=0,
    )

    with pytest.raises(MissingColumnsError) as exc_info:
        list(reader.read())

    error_msg = str(exc_info.value)
    assert "Missing required fields" in error_msg
    assert "Required fields:" in error_msg
    assert "Missing fields:" in error_msg


def test_csv_valid_file_reads_successfully(test_csv_file):
    """Test that a valid CSV file reads successfully."""
    reader = CSVReader(
        file_path=test_csv_file,
        source=TEST_SALES,
        delimiter=",",
        encoding="utf-8",
        skip_rows=0,
    )

    records = list(reader.read())
    assert len(records) == 2
    assert records[0]["transaction_id"] == "TXN001"
    assert records[1]["transaction_id"] == "TXN002"


def test_csv_duplicate_grain_fails_audit(csv_duplicate_grain, temp_sqlite_db):
    """Test that duplicate grain values trigger AuditFailedError in SQLite."""
    # Create a temporary archive directory
    with tempfile.TemporaryDirectory() as archive_dir:
        MASTER_REGISTRY.sources = [TEST_SALES]

        processor = FileProcessor()
        # Override the engine with our test database
        processor.engine = temp_sqlite_db
        processor.Session = sessionmaker[Session](bind=temp_sqlite_db)

        # Process file - should fail during audit
        results = processor.process_files_parallel(
            [str(csv_duplicate_grain)], archive_dir
        )

        # Verify that processing failed
        assert len(results) == 1
        assert results[0]["success"] is False
