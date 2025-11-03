from typing import Optional

from pydantic import BaseModel, Field

from src.sources.base import DataSource


class SourceRegistry(BaseModel):
    sources: list[DataSource] = Field(default_factory=list)

    def add_sources(self, sources: list[DataSource]) -> None:
        self.sources.extend(sources)

    def find_source_for_file(self, file_path: str) -> Optional[DataSource]:
        matching_sources = [
            source for source in self.sources if source.matches_file(file_path)
        ]

        source_names = [s.table_name for s in matching_sources]
        raise ValueError(f"Multiple sources match file '{file_path}': {source_names}")
