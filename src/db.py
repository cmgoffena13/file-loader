import logging
import re
from decimal import Decimal
from pathlib import Path
from typing import Dict, Union, get_args, get_origin

import xxhash
from pydantic_extra_types.pendulum_dt import Date, DateTime
from sqlalchemy import (
    Column,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Table,
    create_engine,
)
from sqlalchemy import Date as SQLDate
from sqlalchemy import DateTime as SQLDateTime

from src.sources.systems.master import MASTER_REGISTRY

logger = logging.getLogger(__name__)

TYPE_MAPPING = {
    str: String,
    int: Integer,
    float: Numeric,
    Decimal: Numeric,
    DateTime: SQLDateTime,
    Date: SQLDate,
}


def _get_column_type(field_type):
    # Handle Optional types (Union[Type, None])
    if get_origin(field_type) is Union and type(None) in get_args(field_type):
        # Extract the non-None type from Optional[Type]
        inner_types = [t for t in get_args(field_type) if t is not type(None)]
        if len(inner_types) == 1:
            field_type = inner_types[0]

    if field_type in TYPE_MAPPING:
        return TYPE_MAPPING[field_type]

    raise ValueError(f"Unsupported field type {field_type}")


def get_table_columns(source, include_timestamps: bool = True) -> list[Column]:
    columns = []

    for field_name, field_info in source.source_model.model_fields.items():
        field_type = field_info.annotation
        column_name = field_name

        is_nullable = (
            not field_info.is_required()
            or field_info.default is not None
            or field_info.default_factory is not None
        )

        sqlalchemy_type = _get_column_type(field_type)
        columns.append(Column(column_name, sqlalchemy_type, nullable=is_nullable))

    columns.extend(
        [
            Column("etl_row_hash", LargeBinary(32), nullable=False),
            Column("source_filename", String, nullable=False),
        ]
    )

    if include_timestamps:
        columns.append(Column("etl_created_at", SQLDateTime, nullable=False))
        columns.append(Column("etl_updated_at", SQLDateTime, nullable=True))

    return columns


def create_tables(database_url: str):
    engine = create_engine(database_url)
    metadata = MetaData()
    tables = []

    for source in MASTER_REGISTRY.sources:
        columns = get_table_columns(source, include_timestamps=True)
        if len(source.grain) > 3:
            logger.warning(
                f"Source {source.table_name} has more than 3 grain columns. Inefficient primary key."
            )
        primary_key = PrimaryKeyConstraint(*source.grain)
        source_filename_index = Index(
            f"idx_{source.table_name}_source_filename", "source_filename"
        )
        table = Table(
            source.table_name, metadata, *columns, primary_key, source_filename_index
        )
        tables.append(table)

    metadata.create_all(engine, tables=tables)
    return engine


def create_row_hash(record: Dict[str, str]) -> bytes:
    string_items = {
        key: str(value) if value is not None else "" for key, value in record.items()
    }
    sorted_items = sorted(string_items.items())
    data_string = "|".join(v for _, v in sorted_items)
    return xxhash.xxh32(data_string.encode("utf-8")).digest()


def sanitize_table_name(filename: str) -> str:
    name = Path(filename).stem
    # Replace invalid characters with underscore
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    # Ensure it starts with letter
    if not name[0].isalpha():
        name = f"t_{name}"
    return name


def create_stage_table(engine, source, source_filename: str) -> str:
    sanitized_name = sanitize_table_name(source_filename)
    stage_table_name = f"stage_{sanitized_name}"

    metadata = MetaData()
    columns = get_table_columns(source, include_timestamps=False)

    stage_table = Table(stage_table_name, metadata, *columns)
    metadata.drop_all(engine, tables=[stage_table])
    metadata.create_all(engine, tables=[stage_table])
    logger.info(f"Created stage table: {stage_table_name}")

    return stage_table_name
