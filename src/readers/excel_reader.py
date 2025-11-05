from pathlib import Path
from typing import Any, Dict, Iterator

import pyexcel

from src.exceptions import MissingHeaderError
from src.readers.base_reader import BaseReader
from src.sources.base import ExcelSource


class ExcelReader(BaseReader):
    def __init__(self, file_path: Path, source, sheet_name: str, skip_rows: int):
        super().__init__(file_path, source)
        self.sheet_name = sheet_name
        self.skip_rows = skip_rows

    @property
    def starting_row_number(self) -> int:
        """Excel: Row 1 = header (name_columns_by_row=0), so starting row = 2 + skip_rows."""
        return 2 + self.skip_rows

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

        actual_headers = set(first_record.keys())

        # Check if headers are empty/whitespace OR all look like default pyexcel column names (e.g., '', '-1', '-2')
        # These are created when headers are missing or empty
        no_valid_headers = not any(
            isinstance(key, str) and key.strip() for key in actual_headers
        )
        all_default_names = len(actual_headers) > 0 and all(
            (
                not key
                or not str(key).strip()
                or (isinstance(key, str) and str(key).strip().lstrip("-").isdigit())
            )
            for key in actual_headers
        )

        if no_valid_headers or all_default_names:
            raise MissingHeaderError(
                f"Empty or invalid column headers in Excel file: {self.file_path}"
            )

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
