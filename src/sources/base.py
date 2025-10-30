from pathlib import Path
from typing import Optional, Type

from pydantic import BaseModel, Field


class TableModel(BaseModel):
    model_config = {"populate_by_name": True}


class DataSource(BaseModel):
    file_pattern: str
    source_model: Type[TableModel]
    table_name: str
    grain: list[str]
    audit_query: str

    def matches_file(self, file_path: str) -> bool:
        return Path(file_path).match(self.file_pattern)


class CSVSource(DataSource):
    delimiter: str = Field(default=",")
    encoding: str = Field(default="utf-8")
    skip_rows: int = Field(default=0)


class ExcelSource(DataSource):
    sheet_name: Optional[str] = None
    skip_rows: int = Field(default=0)


class JSONSource(DataSource):
    array_path: str = Field(default="item")
    skip_rows: int = Field(default=0)
