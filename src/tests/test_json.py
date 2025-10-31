import tempfile

import pytest
from sqlalchemy.orm import Session, sessionmaker

from src.exceptions import MissingColumnsError
from src.file_processor import FileProcessor
from src.readers.json_reader import JSONReader
from src.sources.systems.master import MASTER_REGISTRY
from src.tests.fixtures.source_configs import TEST_FINANCIAL


def test_json_missing_fields_raises_error(json_missing_fields):
    """Test that MissingColumnsError is raised when JSON is missing required fields."""
    reader = JSONReader(
        file_path=json_missing_fields,
        source=TEST_FINANCIAL,
        array_path="entries.item",
        skip_rows=0,
    )

    with pytest.raises(MissingColumnsError) as exc_info:
        list(reader.read())

    error_msg = str(exc_info.value)
    assert "Missing required fields" in error_msg
    assert "Required fields:" in error_msg
    assert "Missing fields:" in error_msg


def test_json_valid_file_reads_successfully(test_json_file):
    """Test that a valid JSON file reads successfully."""
    reader = JSONReader(
        file_path=test_json_file,
        source=TEST_FINANCIAL,
        array_path="entries.item",
        skip_rows=0,
    )

    records = list(reader.read())
    assert len(records) == 2
    assert records[0]["entry_id"] == 1
    assert records[1]["entry_id"] == 2


def test_json_duplicate_grain_fails_audit(json_duplicate_grain, temp_sqlite_db):
    """Test that duplicate grain values trigger AuditFailedError in SQLite."""
    # Create a temporary archive directory
    with tempfile.TemporaryDirectory() as archive_dir:
        MASTER_REGISTRY.sources = [TEST_FINANCIAL]

        processor = FileProcessor()
        # Override the engine with our test database
        processor.engine = temp_sqlite_db
        processor.Session = sessionmaker[Session](bind=temp_sqlite_db)

        # Process file - should fail during audit
        results = processor.process_files_parallel(
            [str(json_duplicate_grain)], archive_dir
        )

        # Verify that processing failed
        assert len(results) == 1
        assert results[0]["success"] is False
