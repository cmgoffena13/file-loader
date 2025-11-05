import gzip
import json

import pytest

from src.readers.json_reader import JSONReader
from src.tests.fixtures.source_configs import TEST_FINANCIAL


@pytest.fixture
def test_json_gzip_file(temp_directory):
    """Create a valid gzipped JSON test file."""
    file_path = temp_directory / "financial_2024.json.gz"

    # Create JSON content matching TEST_FINANCIAL structure
    json_data = {
        "entries": {
            "item": [
                {
                    "entry_id": 1,
                    "account_code": "ACC001",
                    "account_name": "Cash",
                    "debit_amount": 1000.00,
                    "credit_amount": None,
                    "description": "Payment received",
                    "transaction_date": "2024-01-15",
                    "reference_number": "REF001",
                },
                {
                    "entry_id": 2,
                    "account_code": "ACC002",
                    "account_name": "Revenue",
                    "debit_amount": None,
                    "credit_amount": 1000.00,
                    "description": "Sale made",
                    "transaction_date": "2024-01-15",
                    "reference_number": "REF001",
                },
            ]
        }
    }

    # Write JSON content to gzip file
    with gzip.open(file_path, "wt", encoding="utf-8") as gz_file:
        json.dump(json_data, gz_file)

    yield file_path

    # Teardown
    if file_path.exists():
        file_path.unlink()


def test_json_gzip_valid_file_reads_successfully(test_json_gzip_file):
    """Test that a valid gzipped JSON file reads successfully."""
    reader = JSONReader(
        file_path=test_json_gzip_file,
        source=TEST_FINANCIAL,
        array_path="entries.item",
        skip_rows=0,
    )

    # Verify the file is detected as gzipped
    assert reader.is_gzipped is True

    records = list(reader.read())
    assert len(records) == 2
    assert records[0]["entry_id"] == 1
    assert records[1]["entry_id"] == 2
    assert records[0]["account_code"] == "ACC001"
    assert records[1]["account_code"] == "ACC002"


def test_json_gzip_streaming_works(test_json_gzip_file):
    """Test that gzipped JSON files can be read in a streaming fashion."""
    reader = JSONReader(
        file_path=test_json_gzip_file,
        source=TEST_FINANCIAL,
        array_path="entries.item",
        skip_rows=0,
    )

    # Read records one at a time (streaming)
    record_count = 0
    for record in reader.read():
        record_count += 1
        assert "entry_id" in record
        # Should be able to process records incrementally
        if record_count == 1:
            assert record["entry_id"] == 1
        elif record_count == 2:
            assert record["entry_id"] == 2

    assert record_count == 2
