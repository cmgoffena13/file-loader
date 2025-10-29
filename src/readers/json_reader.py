from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterator

import ijson

from src.readers.base_reader import BaseReader
from src.sources.base import JSONSource


class JSONReader(BaseReader):
    def __init__(self, file_path: Path, source, array_path: str, skip_rows: int):
        super().__init__(file_path, source)
        self.array_path = array_path
        self.skip_rows = skip_rows

    def _convert_decimals_to_float(self, value: Any) -> Any:
        """Convert Decimal values to float for database compatibility."""
        if isinstance(value, Decimal):
            return float(value)
        return value

    def read(self) -> Iterator[Dict[str, Any]]:
        with open(self.file_path, "rb") as file:
            objects = ijson.items(file, self.array_path)

            try:
                first_obj = next(objects)
            except StopIteration:
                raise ValueError(f"No data found in JSON file: {self.file_path}")

            # Handle case where first_obj is a list (from array_path)
            if isinstance(first_obj, list):
                if not first_obj:
                    raise ValueError(f"No data found in JSON file: {self.file_path}")
                first_obj = first_obj[0]

            flattened_first = self._flatten_dict(first_obj)
            actual_fields = set(flattened_first.keys())
            self._validate_fields(actual_fields)

            # Yield first object if not skipped
            if self.skip_rows <= 0:
                yield flattened_first

            for i, obj in enumerate(objects, start=1):
                if i < self.skip_rows:
                    continue
                # Handle case where obj is a list
                if isinstance(obj, list):
                    if not obj:
                        continue
                    obj = obj[0]
                yield self._flatten_dict(obj)

    def _flatten_dict(
        self, dictionary: Dict[str, Any], parent_key: str = "", sep: str = "_"
    ) -> Dict[str, Any]:
        items = []
        for k, v in dictionary.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Handle lists by converting to string or flattening if they contain dicts
                if v and isinstance(v[0], dict):
                    # If list contains dicts, flatten each dict with index
                    for i, item in enumerate(v):
                        if isinstance(item, dict):
                            items.extend(
                                self._flatten_dict(
                                    item, f"{new_key}{sep}{i}", sep=sep
                                ).items()
                            )
                        else:
                            items.append(
                                (
                                    f"{new_key}{sep}{i}",
                                    self._convert_decimals_to_float(item),
                                )
                            )
                else:
                    items.append((new_key, str(self._convert_decimals_to_float(v))))
            else:
                items.append((new_key, self._convert_decimals_to_float(v)))
        return dict(items)

    @classmethod
    def matches_source_type(cls, source_type) -> bool:
        return source_type == JSONSource
