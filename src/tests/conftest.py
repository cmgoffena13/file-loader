import os
from unittest.mock import patch

# Needs to happen before local imports
os.environ["ENV_STATE"] = "test"

import csv
import json
from pathlib import Path

import pendulum
import pyexcel
import pytest
from sqlalchemy import MetaData

from src.db import create_tables
from src.settings import config
from src.sources.systems.master import MASTER_REGISTRY
from src.tests.fixtures.source_configs import TEST_FINANCIAL, TEST_INVENTORY, TEST_SALES


@pytest.fixture
def temp_directory(tmp_path):
    return Path(tmp_path)


@pytest.fixture
def test_csv_file(temp_directory):
    """Create a valid CSV test file."""
    file_path = temp_directory / "sales_2024.csv"

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
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
        writer.writerow(
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
        writer.writerow(
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

    yield file_path

    # Teardown
    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def csv_missing_columns(temp_directory):
    """Create a CSV file with missing required columns."""
    file_path = temp_directory / "sales_missing_columns.csv"

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "transaction_id",
                "customer_id",
                # Missing: product_sku, quantity, unit_price, total_amount, sale_date, sales_rep
            ]
        )
        writer.writerow(["TXN001", "CUST001"])

    yield file_path

    # Teardown
    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def temp_sqlite_db(tmp_path):
    """Create a temporary SQLite database for integration tests."""
    db_path = tmp_path / "test.db"
    database_url = f"sqlite:///{db_path}"

    # Temporarily replace MASTER_REGISTRY with test sources for table creation
    original_sources = MASTER_REGISTRY.sources.copy()
    MASTER_REGISTRY.sources = [TEST_SALES, TEST_INVENTORY, TEST_FINANCIAL]

    engine = None
    try:
        # Set DATABASE_URL so FileProcessor uses the test database
        config.DATABASE_URL = database_url
        engine = create_tables()
        yield engine
    finally:
        # Restore original sources
        MASTER_REGISTRY.sources = original_sources
        # Cleanup - drop all tables if engine was created
        if engine is not None:
            metadata = MetaData()
            metadata.reflect(bind=engine)
            metadata.drop_all(bind=engine)


@pytest.fixture(autouse=True)
def mock_sqlite_merge():
    """Mock merge operation for SQLite tests - just sets timestamps without actually merging."""

    def sqlite_merge(
        self, stage_table_name, target_table_name, source, source_filename, log
    ):
        """No-op merge for tests - just sets merge timestamps."""
        log.merge_started_at = pendulum.now()
        log.target_inserts = log.records_stage_loaded or 0
        log.target_updates = 0
        log.merge_ended_at = pendulum.now()
        log.merge_success = True
        return log

    with patch("src.file_processor.FileProcessor._merge_stage_to_target", sqlite_merge):
        yield


@pytest.fixture
def csv_missing_header(temp_directory):
    """Create a CSV file with no header (empty file to trigger first check)."""
    file_path = temp_directory / "sales_no_header.csv"

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        # Empty file - no headers, no data
        pass

    yield file_path

    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def csv_blank_header(temp_directory):
    """Create a CSV file with blank/whitespace headers."""
    file_path = temp_directory / "sales_blank_header.csv"

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["", "", "", "", "", "", "", ""])  # Blank headers
        writer.writerow(
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

    yield file_path

    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def csv_duplicate_grain(temp_directory):
    """Create a CSV file with duplicate grain values (should fail audit)."""
    file_path = temp_directory / "sales_duplicate_grain.csv"

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
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
        # Duplicate transaction_id
        writer.writerow(
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
        writer.writerow(
            [
                "TXN001",  # Duplicate
                "CUST002",
                "SKU002",
                "1",
                "25.00",
                "25.00",
                "2024-01-16",
                "Jane Smith",
            ]
        )

    yield file_path

    # Teardown
    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def test_excel_file(temp_directory):
    """Create a valid Excel test file."""
    file_path = temp_directory / "inventory_2024.xlsx"

    data = [
        [
            "SKU",
            "Product Name",
            "Category",
            "Price",
            "Stock Qty",
            "Supplier",
            "Last Updated",
        ],
        [
            "SKU001",
            "Widget A",
            "Electronics",
            "10.50",
            "100",
            "Supplier A",
            "2024-01-15T10:00:00",
        ],
        [
            "SKU002",
            "Widget B",
            "Electronics",
            "25.00",
            "50",
            "Supplier B",
            "2024-01-16T10:00:00",
        ],
    ]

    pyexcel.save_as(array=data, dest_file_name=str(file_path), name_columns_by_row=0)

    yield file_path

    # Teardown
    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def excel_missing_columns(temp_directory):
    """Create an Excel file with missing required columns."""
    file_path = temp_directory / "inventory_missing_columns.xlsx"

    data = [
        ["SKU", "Product Name"],
        # Missing: Category, Price, Stock Qty, Supplier, Last Updated
        ["SKU001", "Widget A"],
    ]

    pyexcel.save_as(array=data, dest_file_name=str(file_path), name_columns_by_row=0)

    yield file_path

    # Teardown
    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def excel_missing_header(temp_directory):
    """Create an Excel file with invalid/empty headers."""
    file_path = temp_directory / "inventory_no_header.xlsx"

    data = [
        ["", "", "", "", "", "", ""],  # Empty headers
        [
            "SKU001",
            "Widget A",
            "Electronics",
            "10.50",
            "100",
            "Supplier A",
            "2024-01-15T10:00:00",
        ],
    ]

    pyexcel.save_as(array=data, dest_file_name=str(file_path), name_columns_by_row=0)

    yield file_path

    # Teardown
    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def excel_duplicate_grain(temp_directory):
    """Create an Excel file with duplicate grain values (should fail audit)."""
    file_path = temp_directory / "inventory_duplicate_grain.xlsx"

    data = [
        [
            "SKU",
            "Product Name",
            "Category",
            "Price",
            "Stock Qty",
            "Supplier",
            "Last Updated",
        ],
        [
            "SKU001",
            "Widget A",
            "Electronics",
            "10.50",
            "100",
            "Supplier A",
            "2024-01-15T10:00:00",
        ],
        [
            "SKU001",
            "Widget B",
            "Electronics",
            "25.00",
            "50",
            "Supplier B",
            "2024-01-16T10:00:00",
        ],  # Duplicate SKU
    ]

    pyexcel.save_as(array=data, dest_file_name=str(file_path), name_columns_by_row=0)

    yield file_path

    # Teardown
    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def test_json_file(temp_directory):
    """Create a valid JSON test file."""
    file_path = temp_directory / "ledger_2024.json"

    data = {
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

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    yield file_path

    # Teardown
    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def json_missing_fields(temp_directory):
    """Create a JSON file with missing required fields."""
    file_path = temp_directory / "ledger_missing_fields.json"

    data = {
        "entries": {
            "item": [
                {
                    "entry_id": 1,
                    "account_code": "ACC001",
                    # Missing: account_name, debit_amount, credit_amount, description, transaction_date, reference_number
                },
            ]
        }
    }

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    yield file_path

    # Teardown
    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def json_duplicate_grain(temp_directory):
    """Create a JSON file with duplicate grain values (should fail audit)."""
    file_path = temp_directory / "ledger_duplicate_grain.json"

    data = {
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
                    "entry_id": 1,  # Duplicate entry_id
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

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    yield file_path

    # Teardown
    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def csv_validation_errors(temp_directory):
    """Create a CSV file with validation errors (invalid data types, etc.)."""
    file_path = temp_directory / "sales_validation_errors.csv"

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
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
        # Invalid quantity (should be int, got string)
        writer.writerow(
            [
                "TXN001",
                "CUST001",
                "SKU001",
                "not_a_number",  # Invalid
                "10.50",
                "21.00",
                "2024-01-15",
                "John Doe",
            ]
        )
        # Invalid date format
        writer.writerow(
            [
                "TXN002",
                "CUST002",
                "SKU002",
                "1",
                "25.00",
                "25.00",
                "invalid_date",  # Invalid
                "Jane Smith",
            ]
        )

    yield file_path

    # Teardown
    if file_path.exists():
        file_path.unlink()
