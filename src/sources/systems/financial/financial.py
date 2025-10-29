from typing import Optional

from pydantic_extra_types.pendulum_dt import Date

from src.sources.base import JSONSource, TableModel


class LedgerEntry(TableModel):
    entry_id: int
    account_code: str
    account_name: str
    debit_amount: Optional[float]
    credit_amount: Optional[float]
    description: str
    transaction_date: Date
    reference_number: str


FINANCIAL = JSONSource(
    file_pattern="ledger_*.json",
    source_model=LedgerEntry,
    table_name="ledger_entries",
    array_path="entries.item",
    skip_rows=0,
)
