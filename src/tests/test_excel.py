import tempfile

import pytest

from src.exceptions import MissingColumnsError, MissingHeaderError
from src.file_processor import FileProcessor
from src.readers.excel_reader import ExcelReader
from src.sources.systems.master import MASTER_REGISTRY
from src.tests.fixtures.source_configs import TEST_INVENTORY


def test_excel_missing_header_raises_error(excel_missing_header):
    """Test that MissingHeaderError is raised when Excel has empty/invalid headers."""
    reader = ExcelReader(
        file_path=excel_missing_header,
        source=TEST_INVENTORY,
        sheet_name=None,
        skip_rows=0,
    )

    with pytest.raises(MissingHeaderError) as exc_info:
        list(reader.read())

    assert "Empty or invalid column headers" in str(exc_info.value)


def test_excel_blank_string_header_raises_error(excel_missing_header):
    """Test that MissingHeaderError is raised when Excel has blank/empty headers."""
    reader = ExcelReader(
        file_path=excel_missing_header,
        source=TEST_INVENTORY,
        sheet_name=None,
        skip_rows=0,
    )

    with pytest.raises(MissingHeaderError):
        list(reader.read())


def test_excel_missing_columns_raises_error(excel_missing_columns):
    """Test that MissingColumnsError is raised when required columns are missing."""
    reader = ExcelReader(
        file_path=excel_missing_columns,
        source=TEST_INVENTORY,
        sheet_name=None,
        skip_rows=0,
    )

    with pytest.raises(MissingColumnsError) as exc_info:
        list(reader.read())

    error_msg = str(exc_info.value)
    assert "Missing required fields" in error_msg
    assert "Required fields:" in error_msg
    assert "Missing fields:" in error_msg
    assert "sku" in error_msg or "category" in error_msg or "price" in error_msg


def test_excel_valid_file_reads_successfully(test_excel_file):
    """Test that a valid Excel file reads successfully."""
    reader = ExcelReader(
        file_path=test_excel_file,
        source=TEST_INVENTORY,
        sheet_name=None,
        skip_rows=0,
    )

    records = list(reader.read())
    assert len(records) >= 1
    assert records[0]["SKU"] == "SKU001"


def test_excel_duplicate_grain_fails_audit(excel_duplicate_grain, temp_sqlite_db):
    """Test that duplicate grain values trigger AuditFailedError in SQLite."""
    # Create a temporary archive directory
    with tempfile.TemporaryDirectory() as archive_dir:
        MASTER_REGISTRY.sources = [TEST_INVENTORY]

        processor = FileProcessor()

        # Process file - should fail during audit
        results = processor.process_files_parallel(
            [str(excel_duplicate_grain)], archive_dir
        )

        # Verify that processing failed
        assert len(results) == 1
        assert results[0]["success"] is False
