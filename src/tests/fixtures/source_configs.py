from typing import Optional

from pydantic import Field
from pydantic_extra_types.pendulum_dt import Date, DateTime

from src.sources.base import CSVSource, ExcelSource, JSONSource, TableModel


class TestTransaction(TableModel):
    transaction_id: str
    customer_id: str
    product_sku: str
    quantity: int
    unit_price: float
    total_amount: float
    sale_date: Date
    sales_rep: str


TEST_SALES = CSVSource(
    file_pattern="sales_*.csv",
    source_model=TestTransaction,
    table_name="transactions",
    grain=["transaction_id"],
    delimiter=",",
    encoding="utf-8",
    skip_rows=0,
)

TEST_SALES_WITH_DLQ = CSVSource(
    file_pattern="sales_*.csv",
    source_model=TestTransaction,
    table_name="transactions",
    grain=["transaction_id"],
    delimiter=",",
    encoding="utf-8",
    skip_rows=0,
    validation_error_threshold=1.0,  # Allow 100% error rate to capture all errors in DLQ
)


class TestProduct(TableModel):
    sku: str = Field(alias="SKU")
    name: str = Field(alias="Product Name")
    category: str = Field(alias="Category")
    price: float = Field(alias="Price")
    stock_quantity: int = Field(alias="Stock Qty")
    supplier: str = Field(alias="Supplier")
    last_updated: DateTime = Field(alias="Last Updated")


TEST_INVENTORY = ExcelSource(
    file_pattern="inventory_*.xlsx",
    source_model=TestProduct,
    table_name="products",
    grain=["sku"],
    sheet_name=None,
    skip_rows=0,
)


class TestLedgerEntry(TableModel):
    entry_id: int
    account_code: str
    account_name: str
    debit_amount: Optional[float]
    credit_amount: Optional[float]
    description: str
    transaction_date: Date
    reference_number: str


TEST_FINANCIAL = JSONSource(
    file_pattern="ledger_*.json",
    source_model=TestLedgerEntry,
    table_name="ledger_entries",
    grain=["entry_id"],
    array_path="entries.item",
    skip_rows=0,
)


class TestDateConversion(TableModel):
    id: str
    name: str
    birth_date: Date = Field(alias="Birth Date")
    created_at: DateTime = Field(alias="Created At")
    quantity: int  # Non-date field to ensure it's not converted


TEST_DATE_CONVERSION = ExcelSource(
    file_pattern="dates_*.xlsx",
    source_model=TestDateConversion,
    table_name="date_test",
    grain=["id"],
    sheet_name=None,
    skip_rows=0,
)
