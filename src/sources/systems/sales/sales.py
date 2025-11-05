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
    grain=["transaction_id"],
    audit_query="""
        SELECT 
        CASE WHEN SUM(CASE WHEN total_amount > 0 THEN 1 ELSE 0 END) = COUNT(*) THEN 1 ELSE 0 END AS total_amount_positive,
        CASE WHEN SUM(CASE WHEN unit_price > 0 THEN 1 ELSE 0 END) = COUNT(*) THEN 1 ELSE 0 END AS unit_price_positive
        FROM {table}
    """,
    delimiter=",",
    encoding="utf-8",
    skip_rows=0,
)
