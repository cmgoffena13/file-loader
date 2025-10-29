import logging
from decimal import Decimal
from typing import Any, Dict, Union, get_args, get_origin

import xxhash
from pydantic_extra_types.pendulum_dt import Date, DateTime
from sqlalchemy import (
    BINARY,
    Column,
    Float,
    Integer,
    MetaData,
    Numeric,
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


def create_tables(database_url: str):
    engine = create_engine(database_url)
    metadata = MetaData()
    tables = []

    for source in MASTER_REGISTRY.sources:
        columns = []

        for field_name, field_info in source.source_model.model_fields.items():
            field_type = field_info.annotation
            column_name = field_name

            # Check if field is nullable (Optional types or has default)
            is_nullable = (
                not field_info.is_required()  # Optional fields
                or field_info.default is not None  # Has default value
                or field_info.default_factory is not None  # Has default factory
            )

            sqlalchemy_type = _get_column_type(field_type)
            columns.append(Column(column_name, sqlalchemy_type, nullable=is_nullable))

        # Add ETL metadata columns
        columns.extend(
            [
                Column("etl_row_hash", BINARY(32)),
                Column("_source_file", String),
                Column("etl_created_at", SQLDateTime),
            ]
        )

        table = Table(source.table_name, metadata, *columns)
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
