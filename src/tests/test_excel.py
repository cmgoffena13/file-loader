import tempfile

import pendulum
import pyexcel
import pytest

from src.exceptions import MissingColumnsError, MissingHeaderError
from src.file_processor import FileProcessor
from src.readers.excel_reader import ExcelReader
from src.sources.systems.master import MASTER_REGISTRY
from src.tests.fixtures.source_configs import (
    TEST_DATE_CONVERSION,
    TEST_INVENTORY,
)


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


@pytest.fixture
def excel_date_conversion_file(temp_directory):
    """Create an Excel file with serial date numbers to test conversion."""
    file_path = temp_directory / "dates_test.xlsx"

    # Excel serial numbers (calculated from epoch 1899-12-30):
    # 45306 = 2024-01-15 (integer date)
    # 45306.5 = 2024-01-15 12:00:00 (float date with time)
    # 45307 = 2024-01-16 (integer date)
    # 45307.75 = 2024-01-16 18:00:00 (float date with time)
    data = [
        ["id", "name", "Birth Date", "Created At", "quantity"],
        ["ID001", "John Doe", 45306, 45306.5, 100],  # Integer date, float datetime
        ["ID002", "Jane Smith", 45307, 45307.75, 200],  # Integer date, float datetime
    ]

    pyexcel.save_as(array=data, dest_file_name=str(file_path), name_columns_by_row=0)

    yield file_path

    # Teardown
    if file_path.exists():
        file_path.unlink()


def test_excel_date_conversion(excel_date_conversion_file):
    """Test that Excel serial date numbers are converted to pendulum Date/DateTime objects."""
    reader = ExcelReader(
        file_path=excel_date_conversion_file,
        source=TEST_DATE_CONVERSION,
        sheet_name=None,
        skip_rows=0,
    )

    records = list(reader.read())
    assert len(records) == 2

    # First record
    record1 = records[0]
    assert record1["id"] == "ID001"
    assert record1["name"] == "John Doe"
    assert record1["quantity"] == 100  # Non-date field should remain as int

    # Check birth_date (Date field) - should be converted from integer 45306
    assert isinstance(record1["Birth Date"], pendulum.Date)
    assert record1["Birth Date"] == pendulum.date(2024, 1, 15)

    # Check created_at (DateTime field) - should be converted from float 45306.5
    assert isinstance(record1["Created At"], pendulum.DateTime)
    assert record1["Created At"] == pendulum.datetime(2024, 1, 15, 12, 0, 0)

    # Second record
    record2 = records[1]
    assert record2["id"] == "ID002"
    assert record2["name"] == "Jane Smith"
    assert record2["quantity"] == 200  # Non-date field should remain as int

    # Check birth_date (Date field) - should be converted from integer 45307
    assert isinstance(record2["Birth Date"], pendulum.Date)
    assert record2["Birth Date"] == pendulum.date(2024, 1, 16)

    # Check created_at (DateTime field) - should be converted from float 45307.75
    assert isinstance(record2["Created At"], pendulum.DateTime)
    assert record2["Created At"] == pendulum.datetime(2024, 1, 16, 18, 0, 0)
