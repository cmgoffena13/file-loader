from pathlib import Path
from typing import Any, Dict, Iterator, get_args, get_origin

import pendulum
import pyexcel
from pydantic_extra_types.pendulum_dt import Date, DateTime

from src.exceptions import MissingHeaderError
from src.readers.base_reader import BaseReader
from src.sources.base import ExcelSource


class ExcelReader(BaseReader):
    # Excel epoch: 1899-12-30 (Excel's epoch with 1900 leap year bug)
    # Serial number 1 = 1900-01-01
    _EXCEL_EPOCH = pendulum.datetime(1899, 12, 30)

    def __init__(self, file_path: Path, source, sheet_name: str, skip_rows: int):
        super().__init__(file_path, source)
        self.sheet_name = sheet_name
        self.skip_rows = skip_rows

    @property
    def starting_row_number(self) -> int:
        """Excel: Row 1 = header (name_columns_by_row=0), so starting row = 2 + skip_rows."""
        return 2 + self.skip_rows

    def _build_date_field_mapping(self) -> Dict[str, type]:
        """Build mapping of column names (aliases) to Date/DateTime field types."""
        date_field_mapping = {}
        for field_name, field_info in self.source.source_model.model_fields.items():
            field_type = field_info.annotation
            origin = get_origin(field_type)
            if origin is not None:  # It's Optional or Union
                args = get_args(field_type)
                field_type = args[0] if args else field_type

            if field_type in (Date, DateTime):
                # Map both the field name and alias (if exists) to the field type
                date_field_mapping[field_name.lower()] = field_type
                if field_info.alias:
                    date_field_mapping[field_info.alias.lower()] = field_type
        return date_field_mapping

    def _convert_excel_dates(
        self, record: Dict[str, Any], date_field_mapping: Dict[str, type]
    ) -> Dict[str, Any]:
        """Convert Excel serial date numbers to datetime objects in the record.

        Excel stores dates as serial numbers (days since 1899-12-30, because Excel
        incorrectly treats 1900 as a leap year). Serial number 1 = 1900-01-01.

        Only converts numeric values in fields that are Date or DateTime in the source model.
        """
        converted = {}
        for key, value in record.items():
            key_lower = key.lower()
            # Only convert if the field is Date/DateTime in the source model AND value is numeric
            if key_lower in date_field_mapping and isinstance(value, (int, float)):
                days = int(value)
                fractional = value - days

                # Convert to datetime using pendulum's add() method
                dt = self._EXCEL_EPOCH.add(days=days)

                # Add time component if there's a fractional part (time of day)
                if fractional > 0:
                    seconds = int(fractional * 86400)  # 86400 seconds in a day
                    dt = dt.add(seconds=seconds)

                if date_field_mapping[key_lower] == Date:
                    converted[key] = dt.date()
                else:
                    converted[key] = dt
            else:
                converted[key] = value
        return converted

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

        # Build mapping of column names (aliases) to field types for date conversion
        date_field_mapping = self._build_date_field_mapping()

        if self.skip_rows <= 0:
            yield self._convert_excel_dates(first_record, date_field_mapping)

        for i, record in enumerate(records, start=1):
            if i < self.skip_rows:
                continue
            yield self._convert_excel_dates(record, date_field_mapping)

    @classmethod
    def matches_source_type(cls, source_type) -> bool:
        return source_type == ExcelSource
