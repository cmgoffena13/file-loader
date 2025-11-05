from pydantic import Field
from pydantic_extra_types.pendulum_dt import DateTime

from src.sources.base import ExcelSource, TableModel


class Product(TableModel):
    sku: str = Field(alias="SKU", max_length=100)
    name: str = Field(alias="Product Name", max_length=100)
    category: str = Field(alias="Category", max_length=100)
    price: float = Field(alias="Price")
    stock_quantity: int = Field(alias="Stock Qty")
    supplier: str = Field(alias="Supplier", max_length=100)
    last_updated: DateTime = Field(alias="Last Updated")


INVENTORY = ExcelSource(
    file_pattern="inventory_*.xlsx",
    source_model=Product,
    table_name="products",
    grain=["sku"],
    audit_query="""
        SELECT 
        CASE WHEN SUM(CASE WHEN price > 0 THEN 1 ELSE 0 END) = COUNT(*) THEN 1 ELSE 0 END AS price_positive
        FROM {table}
    """,
    sheet_name="Products",
    skip_rows=1,
)
