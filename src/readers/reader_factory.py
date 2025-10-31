from pathlib import Path

from src.readers.base_reader import BaseReader
from src.readers.csv_reader import CSVReader
from src.readers.excel_reader import ExcelReader
from src.readers.json_reader import JSONReader
from src.sources.base import DataSource


class ReaderFactory:
    _readers = {
        ".csv": CSVReader,
        ".xlsx": ExcelReader,
        ".xls": ExcelReader,
        ".json": JSONReader,
    }

    @classmethod
    def create_reader(cls, file_path: Path, source: DataSource, **kwargs) -> BaseReader:
        extension = file_path.suffix.lower()

        if extension not in cls._readers:
            supported_extensions = ", ".join(cls._readers.keys())
            raise ValueError(
                f"Unsupported file extension: {extension}. "
                f"Supported extensions: {supported_extensions}"
            )

        reader_class = cls._readers[extension]

        if not reader_class.matches_source_type(type(source)):
            raise ValueError(
                f"File extension {extension} expects {reader_class.__name__} source, got {type(source).__name__}"
            )

        # Extract reader-specific config from source
        reader_kwargs = source.model_dump(
            exclude={
                "file_pattern",
                "source_model",
                "table_name",
                "grain",
                "audit_query",
                "validation_error_threshold",
            }
        )
        reader_kwargs.update(kwargs)  # Allow override of any kwargs

        return reader_class(file_path, source, **reader_kwargs)

    @classmethod
    def get_supported_extensions(cls) -> list[str]:
        return list[str](cls._readers.keys())
