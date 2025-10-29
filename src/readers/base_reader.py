from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterator

from src.sources.base import DataSource


class BaseReader(ABC):
    def __init__(self, file_path: Path, source: DataSource):
        self.file_path = file_path
        self.source = source
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")

    def _validate_fields(self, actual_fields: set[str]) -> None:
        expected_fields = set[str](
            field.alias if field.alias else name
            for name, field in self.source.source_model.model_fields.items()
        )
        missing_fields = expected_fields - actual_fields

        if missing_fields:
            raise ValueError(
                f"Missing required fields in {self.file_path.suffix.upper()} file {self.file_path}: {sorted(missing_fields)}"
            )

    @abstractmethod
    def read(self) -> Iterator[Dict[str, Any]]:
        pass

    @classmethod
    @abstractmethod
    def matches_source_type(cls, source_type) -> bool:
        pass

    def __iter__(self):
        return self.read()
