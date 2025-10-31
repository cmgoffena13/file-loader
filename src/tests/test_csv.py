import pytest

from src.exceptions import MissingColumnsError, MissingHeaderError
from src.readers.csv_reader import CSVReader
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


def test_csv_duplicate_grain_violation(csv_duplicate_grain):
    """Test that duplicate grain values can be detected (audit will fail, but reader should process)."""
    reader = CSVReader(
        file_path=csv_duplicate_grain,
        source=TEST_SALES,
        delimiter=",",
        encoding="utf-8",
        skip_rows=0,
    )

    records = list(reader.read())
    assert len(records) == 2
    assert records[0]["transaction_id"] == "TXN001"
    assert records[1]["transaction_id"] == "TXN001"


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
