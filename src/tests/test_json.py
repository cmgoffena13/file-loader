import pytest

from src.exceptions import MissingColumnsError
from src.readers.json_reader import JSONReader
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


def test_json_duplicate_grain_violation(json_duplicate_grain):
    """Test that duplicate grain values can be detected (audit will fail, but reader should process)."""
    reader = JSONReader(
        file_path=json_duplicate_grain,
        source=TEST_FINANCIAL,
        array_path="entries.item",
        skip_rows=0,
    )

    records = list(reader.read())
    assert len(records) == 2
    assert records[0]["entry_id"] == 1
    assert records[1]["entry_id"] == 1


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
