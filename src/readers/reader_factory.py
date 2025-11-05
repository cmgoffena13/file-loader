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
        ".csv.gz": CSVReader,
        ".json.gz": JSONReader,
    }

    @classmethod
    def _get_extension(cls, file_path: Path) -> str:
        """Get the file extension, checking for compressed variants first."""
        suffixes = file_path.suffixes
        # Check for compressed extension first (e.g., .csv.gz)
        if len(suffixes) >= 2:
            combined = "".join(suffixes[-2:]).lower()
            if combined in cls._readers:
                return combined
        # Fall back to single extension
        return file_path.suffix.lower()

    @classmethod
    def create_reader(cls, file_path: Path, source: DataSource, **kwargs) -> BaseReader:
        extension = cls._get_extension(file_path)

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
            include={"delimiter", "encoding", "skip_rows", "sheet_name", "array_path"}
        )
        reader_kwargs.update(kwargs)  # Allow override of any kwargs

        return reader_class(file_path, source, **reader_kwargs)

    @classmethod
    def get_supported_extensions(cls) -> list[str]:
        return list[str](cls._readers.keys())
