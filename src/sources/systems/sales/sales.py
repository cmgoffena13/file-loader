from pydantic_extra_types.pendulum_dt import Date

from src.sources.base import CSVSource, TableModel


class Transaction(TableModel):
    transaction_id: str
    customer_id: str
    product_sku: str
    quantity: int
    unit_price: float
    total_amount: float
    sale_date: Date
    sales_rep: str


SALES = CSVSource(
    file_pattern="sales_*.csv",
    source_model=Transaction,
    table_name="transactions",
    delimiter=",",
    encoding="utf-8",
    skip_rows=1,
)
