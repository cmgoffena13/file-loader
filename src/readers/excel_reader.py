from pathlib import Path
from typing import Any, Dict, Iterator

import pyexcel

from src.readers.base_reader import BaseReader
from src.sources.base import ExcelSource


class ExcelReader(BaseReader):
    def __init__(self, file_path: Path, source, sheet_name: str, skip_rows: int):
        super().__init__(file_path, source)
        self.sheet_name = sheet_name
        self.skip_rows = skip_rows

    def read(self) -> Iterator[Dict[str, Any]]:
        records = pyexcel.iget_records(
            file_name=str(self.file_path),
            sheet_name=self.sheet_name,
            name_columns_by_row=0,
        )

        try:
            first_record = next(records)
        except StopIteration:
            raise ValueError(f"No data found in Excel file: {self.file_path}")

        if not any(isinstance(key, str) and key.strip() for key in first_record.keys()):
            raise ValueError(
                f"Empty or invalid column headers in Excel file: {self.file_path}"
            )

        actual_headers = set(first_record.keys())
        self._validate_fields(actual_headers)

        if self.skip_rows <= 0:
            yield first_record

        for i, record in enumerate(records, start=1):
            if i < self.skip_rows:
                continue
            yield record

    @classmethod
    def matches_source_type(cls, source_type) -> bool:
        return source_type == ExcelSource
