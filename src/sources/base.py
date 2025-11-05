from pathlib import Path
from typing import Optional, Type

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_extra_types.pendulum_dt import DateTime


class FileLoadLog(BaseModel):
    id: Optional[int] = None
    file_name: str
    started_at: DateTime
    duplicate_skipped: Optional[bool] = None
    archive_copy_started_at: Optional[DateTime] = None
    archive_copy_ended_at: Optional[DateTime] = None
    archive_copy_success: Optional[bool] = None
    processing_started_at: Optional[DateTime] = None
    processing_ended_at: Optional[DateTime] = None
    processing_success: Optional[bool] = None
    stage_load_started_at: Optional[DateTime] = None
    stage_load_ended_at: Optional[DateTime] = None
    stage_load_success: Optional[bool] = None
    audit_started_at: Optional[DateTime] = None
    audit_ended_at: Optional[DateTime] = None
    audit_success: Optional[bool] = None
    merge_started_at: Optional[DateTime] = None
    merge_ended_at: Optional[DateTime] = None
    merge_success: Optional[bool] = None
    ended_at: Optional[DateTime] = None
    records_processed: Optional[int] = None
    validation_errors: Optional[int] = None
    records_stage_loaded: Optional[int] = None
    target_inserts: Optional[int] = None
    target_updates: Optional[int] = None
    success: Optional[bool] = None
    error_type: Optional[str] = None


class TableModel(BaseModel):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)


class DataSource(BaseModel):
    file_pattern: str
    source_model: Type[TableModel]
    table_name: str
    grain: list[str]
    audit_query: str
    validation_error_threshold: float = Field(default=0.0)
    notification_emails: Optional[list[str]] = Field(
        default=None
    )  # List of email addresses to notify on failures

    @model_validator(mode="after")
    def validate_grain_fields(self):
        """Validate that all grain columns are fields in the source model."""
        model_fields = set(self.source_model.model_fields.keys())
        invalid_grain = [g for g in self.grain if g not in model_fields]
        if invalid_grain:
            raise ValueError(
                f"Grain columns {invalid_grain} are not fields in {self.source_model.__name__}. "
                f"Available fields: {sorted(model_fields)}"
            )
        return self

    def matches_file(self, file_path: str) -> bool:
        return Path(file_path.lower()).match(self.file_pattern.lower())


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
