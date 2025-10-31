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
    audit_query="""
        SELECT CASE WHEN COUNT(transaction_id) = COUNT(*) THEN 1 ELSE 0 END AS grain_unique
        FROM {table}
    """,
    delimiter=",",
    encoding="utf-8",
    skip_rows=0,
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
    audit_query="""
        SELECT CASE WHEN COUNT(sku) = COUNT(*) THEN 1 ELSE 0 END AS grain_unique
        FROM {table}
    """,
    sheet_name=None,
    skip_rows=0,
)


class TestLedgerEntry(TableModel):
    entry_id: int
    account_code: str
    account_name: str
    debit_amount: float | None
    credit_amount: float | None
    description: str
    transaction_date: Date
    reference_number: str


TEST_FINANCIAL = JSONSource(
    file_pattern="ledger_*.json",
    source_model=TestLedgerEntry,
    table_name="ledger_entries",
    grain=["entry_id"],
    audit_query="""
        SELECT CASE WHEN COUNT(entry_id) = COUNT(*) THEN 1 ELSE 0 END AS grain_unique
        FROM {table}
    """,
    array_path="entries.item",
    skip_rows=0,
)
