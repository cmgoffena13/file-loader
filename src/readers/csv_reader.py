import csv
from pathlib import Path
from typing import Any, Dict, Iterator

from src.exceptions import MissingHeaderError
from src.readers.base_reader import BaseReader
from src.sources.base import CSVSource


class CSVReader(BaseReader):
    def __init__(
        self, file_path: Path, source, delimiter: str, encoding: str, skip_rows: int
    ):
        super().__init__(file_path, source)
        self.delimiter = delimiter
        self.encoding = encoding
        self.skip_rows = skip_rows

    def read(self) -> Iterator[Dict[str, Any]]:
        with open(self.file_path, "r", encoding=self.encoding, newline="") as csvfile:
            reader = csv.DictReader(csvfile, delimiter=self.delimiter)

            # Check if headers exist
            if not reader.fieldnames:
                raise MissingHeaderError(
                    f"No headers found in CSV file: {self.file_path}"
                )

            # Check if headers are just whitespace
            if not any(
                fieldname and fieldname.strip() for fieldname in reader.fieldnames
            ):
                raise MissingHeaderError(
                    f"Whitespace-only headers in CSV file: {self.file_path}"
                )

            actual_headers = set[str](reader.fieldnames)
            self._validate_fields(actual_headers)

            for i, row in enumerate(reader):
                if i < self.skip_rows:
                    continue
                yield row

    @classmethod
    def matches_source_type(cls, source_type) -> bool:
        return source_type == CSVSource
