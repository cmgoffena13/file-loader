import csv
import gzip

import pytest

from src.readers.csv_reader import CSVReader
from src.tests.fixtures.source_configs import TEST_SALES


@pytest.fixture
def test_csv_gzip_file(temp_directory):
    """Create a valid gzipped CSV test file."""
    file_path = temp_directory / "sales_2024.csv.gz"

    # Create CSV content in memory first
    csv_content = []
    csv_content.append(
        [
            "transaction_id",
            "customer_id",
            "product_sku",
            "quantity",
            "unit_price",
            "total_amount",
            "sale_date",
            "sales_rep",
        ]
    )
    csv_content.append(
        [
            "TXN001",
            "CUST001",
            "SKU001",
            "2",
            "10.50",
            "21.00",
            "2024-01-15",
            "John Doe",
        ]
    )
    csv_content.append(
        [
            "TXN002",
            "CUST002",
            "SKU002",
            "1",
            "25.00",
            "25.00",
            "2024-01-16",
            "Jane Smith",
        ]
    )

    # Write CSV content to gzip file
    with gzip.open(file_path, "wt", encoding="utf-8", newline="") as gz_file:
        writer = csv.writer(gz_file)
        writer.writerows(csv_content)

    yield file_path

    # Teardown
    if file_path.exists():
        file_path.unlink()


def test_csv_gzip_valid_file_reads_successfully(test_csv_gzip_file):
    """Test that a valid gzipped CSV file reads successfully."""
    reader = CSVReader(
        file_path=test_csv_gzip_file,
        source=TEST_SALES,
        delimiter=",",
        encoding="utf-8",
        skip_rows=0,
    )

    # Verify the file is detected as gzipped
    assert reader.is_gzipped is True

    records = list(reader.read())
    assert len(records) == 2
    assert records[0]["transaction_id"] == "TXN001"
    assert records[1]["transaction_id"] == "TXN002"
    assert records[0]["customer_id"] == "CUST001"
    assert records[1]["customer_id"] == "CUST002"


def test_csv_gzip_streaming_works(test_csv_gzip_file):
    """Test that gzipped CSV files can be read in a streaming fashion."""
    reader = CSVReader(
        file_path=test_csv_gzip_file,
        source=TEST_SALES,
        delimiter=",",
        encoding="utf-8",
        skip_rows=0,
    )

    # Read records one at a time (streaming)
    record_count = 0
    for record in reader.read():
        record_count += 1
        assert "transaction_id" in record
        # Should be able to process records incrementally
        if record_count == 1:
            assert record["transaction_id"] == "TXN001"
        elif record_count == 2:
            assert record["transaction_id"] == "TXN002"

    assert record_count == 2
