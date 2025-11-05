from typing import Optional

from pydantic import Field
from pydantic_extra_types.pendulum_dt import Date

from src.sources.base import JSONSource, TableModel


class LedgerEntry(TableModel):
    entry_id: int  # nested key: "entries.item.Entry.ID"
    account_code: str = Field(max_length=100)
    account_name: str = Field(max_length=100)
    debit_amount: Optional[float]
    credit_amount: Optional[float]
    description: str = Field(max_length=500)
    transaction_date: Date
    reference_number: str = Field(max_length=100)


FINANCIAL = JSONSource(
    file_pattern="ledger_*.json",
    source_model=LedgerEntry,
    table_name="ledger_entries",
    grain=["entry_id"],
    array_path="entries.item",
    skip_rows=0,
)
