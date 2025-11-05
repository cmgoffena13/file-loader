import json
import logging
import re
from decimal import Decimal
from pathlib import Path
from typing import Dict, Union, get_args, get_origin

import xxhash
from pydantic_extra_types.pendulum_dt import Date, DateTime
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Engine,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    create_engine,
    text,
)
from sqlalchemy import Date as SQLDate
from sqlalchemy import DateTime as SQLDateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine

from src.settings import config, get_database_config
from src.sources.base import DataSource, FileLoadLog
from src.sources.systems.master import MASTER_REGISTRY

logger = logging.getLogger(__name__)

TYPE_MAPPING = {
    str: String,
    int: Integer,
    float: Numeric,
    bool: Boolean,
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

    # SQLite requires Integer for auto-increment primary keys and foreign keys
    id_column_type = Integer if config.DRIVERNAME == "sqlite" else BigInteger

    columns.extend(
        [
            Column("etl_row_hash", LargeBinary(32), nullable=False),
            Column("source_filename", String, nullable=False),
            Column("file_load_log_id", id_column_type, nullable=False),
        ]
    )

    if include_timestamps:
        columns.append(Column("etl_created_at", SQLDateTime, nullable=False))
        columns.append(Column("etl_updated_at", SQLDateTime, nullable=True))

    return columns


def _get_json_column_type(engine: Engine):
    drivername = config.DRIVERNAME

    json_column_mapping = {
        "postgresql": JSONB,
        "mysql": JSON,
        "mssql": String(4000),
        "sqlite": Text,
    }

    for dialect_key, column_type in json_column_mapping.items():
        if dialect_key == drivername:
            return column_type

    logger.warning(
        f"Unknown database dialect '{drivername}', defaulting to JSON: {engine.url}"
    )
    return JSON


def create_merge_sql(
    stage_table_name: str,
    target_table_name: str,
    join_condition: str,
    columns: list[str],
    update_columns: list[str],
    grain: list[str],
    now_iso: str,
) -> str:
    drivername = config.DRIVERNAME
    insert_columns = ", ".join(columns) + ", etl_created_at"
    select_columns = ", ".join([f"stage.{col}" for col in columns]) + f", '{now_iso}'"

    if drivername == "mysql":
        # MySQL uses INSERT ... ON DUPLICATE KEY UPDATE
        update_on_duplicate_parts = []
        for col in update_columns:
            update_on_duplicate_parts.append(f"{col} = stage.{col}")
        # Only update etl_updated_at if data actually changed
        update_on_duplicate_parts.append(
            f"etl_updated_at = IF(stage.etl_row_hash != {target_table_name}.etl_row_hash, '{now_iso}', {target_table_name}.etl_updated_at)"
        )
        update_on_duplicate = ", ".join(update_on_duplicate_parts)

        merge_sql = f"""
            INSERT INTO {target_table_name} ({insert_columns})
            SELECT {select_columns}
            FROM {stage_table_name} AS stage
            ON DUPLICATE KEY UPDATE
                {update_on_duplicate}
        """
    elif drivername == "sqlite":
        # SQLite uses INSERT ... ON CONFLICT ... DO UPDATE
        # Note: Must include WHERE clause to resolve parser ambiguity with ON CONFLICT
        conflict_columns = ", ".join(grain)
        update_set_parts = []
        for col in update_columns:
            update_set_parts.append(f"{col} = excluded.{col}")
        # Only update etl_updated_at if data actually changed
        # In SQLite ON CONFLICT DO UPDATE, use unqualified column names for existing table values
        update_set_parts.append(
            f"etl_updated_at = CASE WHEN excluded.etl_row_hash != etl_row_hash THEN excluded.etl_updated_at ELSE etl_updated_at END"
        )
        update_set = ", ".join(update_set_parts)

        merge_sql = f"""
            INSERT INTO {target_table_name} ({insert_columns})
            SELECT {select_columns}
            FROM {stage_table_name} AS stage
            WHERE 1=1
            ON CONFLICT({conflict_columns})
            DO UPDATE SET
                {update_set}
        """
    else:
        # PostgreSQL and SQL Server use MERGE INTO
        update_set = ", ".join([f"{col} = stage.{col}" for col in update_columns])
        update_set += f", etl_updated_at = '{now_iso}'"
        insert_values = ", ".join([f"stage.{col}" for col in columns])
        insert_values += f", '{now_iso}'"

        merge_sql = f"""
            MERGE INTO {target_table_name} AS target
            USING {stage_table_name} AS stage
            ON {join_condition}
            WHEN MATCHED AND stage.etl_row_hash != target.etl_row_hash THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
        """

    return merge_sql


def calculate_batch_size(source: DataSource) -> int:
    """If SQL Server, calculate batch size based on 1000 values per INSERT limit."""
    drivername = config.DRIVERNAME
    if "mssql" in drivername:
        # SQL Server has 1000 values per INSERT limit
        max_values = 1000
        column_count = (
            len(source.source_model.model_fields) + 2
        )  # +2 for ETL metadata columns (etl_row_hash, source_filename)
        # Calculate max rows: (max_values / columns_per_row) - 1 for safety margin
        max_rows = (max_values // column_count) - 1
        return max(1, min(max_rows, config.BATCH_SIZE))

    # For other databases, use configured batch size
    return config.BATCH_SIZE


def create_tables() -> Engine:
    db_config = get_database_config()
    engine_kwargs = {
        "url": db_config["sqlalchemy.url"],
        "echo": db_config["sqlalchemy.echo"],
        "future": db_config["sqlalchemy.future"],
        "connect_args": db_config.get("sqlalchemy.connect_args", {}),
        "pool_size": db_config.get("sqlalchemy.pool_size", 20),
    }
    if "sqlalchemy.max_overflow" in db_config:
        engine_kwargs["max_overflow"] = db_config["sqlalchemy.max_overflow"]
    if "sqlalchemy.pool_timeout" in db_config:
        engine_kwargs["pool_timeout"] = db_config["sqlalchemy.pool_timeout"]

    engine = create_engine(**engine_kwargs)

    metadata = MetaData()
    tables = []

    for source in MASTER_REGISTRY.sources:
        columns = get_table_columns(source, include_timestamps=True)
        if len(source.grain) > 3:
            logger.warning(
                f"Source {source.table_name} has more than 3 grain columns. Inefficient primary key."
            )
        primary_key = PrimaryKeyConstraint(*source.grain)
        table = Table(
            source.table_name,
            metadata,
            *columns,
            primary_key,
        )
        # define index separately, bound to table column
        Index(f"idx_{source.table_name}_source_filename", table.c.source_filename)
        tables.append(table)

    # SQLite requires Integer for auto-increment primary keys
    id_column_type = Integer if config.DRIVERNAME == "sqlite" else BigInteger

    file_load_log = Table(
        "file_load_log",
        metadata,
        Column("id", id_column_type, primary_key=True, autoincrement=True),
        Column("file_name", String, nullable=False),
        Column("started_at", SQLDateTime, nullable=False),
        Column("duplicate_skipped", Boolean, nullable=True),
        # archive copy phase
        Column("archive_copy_started_at", SQLDateTime, nullable=True),
        Column("archive_copy_ended_at", SQLDateTime, nullable=True),
        Column("archive_copy_success", Boolean, nullable=True),
        # processing phase
        Column("processing_started_at", SQLDateTime, nullable=True),
        Column("processing_ended_at", SQLDateTime, nullable=True),
        Column("processing_success", Boolean, nullable=True),
        # stage load phase
        Column("stage_load_started_at", SQLDateTime, nullable=True),
        Column("stage_load_ended_at", SQLDateTime, nullable=True),
        Column("stage_load_success", Boolean, nullable=True),
        # audit phase
        Column("audit_started_at", SQLDateTime, nullable=True),
        Column("audit_ended_at", SQLDateTime, nullable=True),
        Column("audit_success", Boolean, nullable=True),
        # merge phase
        Column("merge_started_at", SQLDateTime, nullable=True),
        Column("merge_ended_at", SQLDateTime, nullable=True),
        Column("merge_success", Boolean, nullable=True),
        # summary
        Column("ended_at", SQLDateTime, nullable=True),
        Column("records_processed", Integer, nullable=True),
        Column("validation_errors", Integer, nullable=True),
        Column("records_stage_loaded", Integer, nullable=True),
        Column("target_inserts", Integer, nullable=True),
        Column("target_updates", Integer, nullable=True),
        Column("success", Boolean, nullable=True),
        Column("error_type", String(50), nullable=True),
    )
    Index("idx_file_load_log_file_name", file_load_log.c.file_name)
    tables.append(file_load_log)

    # Dead Letter Queue table for validation failures
    # Use appropriate JSON column type based on database backend
    json_column_type = _get_json_column_type(engine)

    file_load_dlq = Table(
        "file_load_dlq",
        metadata,
        Column("id", id_column_type, primary_key=True, autoincrement=True),
        Column("source_filename", String, nullable=False),
        Column("file_row_number", Integer, nullable=False),
        Column("file_record_data", json_column_type, nullable=False),
        Column("validation_errors", json_column_type, nullable=False),
        Column(
            "file_load_log_id",
            id_column_type,
            ForeignKey("file_load_log.id"),
            nullable=False,
        ),
        Column("target_table_name", String, nullable=False),
        Column("failed_at", SQLDateTime, nullable=False),
    )
    Index("idx_dlq_file_load_log_id", file_load_dlq.c.file_load_log_id)
    Index(
        "idx_dlq_source_filename", file_load_dlq.c.source_filename, file_load_dlq.c.id
    )
    tables.append(file_load_dlq)
    metadata.drop_all(engine, tables=[file_load_dlq])
    metadata.create_all(engine, tables=tables)
    return engine


def get_delete_dlq_sql() -> str:
    drivername = config.DRIVERNAME

    if drivername == "mssql":
        delete_sql = (
            "DELETE TOP(:limit) FROM file_load_dlq WHERE source_filename = :file_name"
        )
    elif drivername in ["postgresql", "sqlite"]:
        # PostgreSQL, Sqlite doesn't support LIMIT in DELETE, use subquery with id
        delete_sql = """
            DELETE FROM file_load_dlq 
            WHERE id IN (
                SELECT id FROM file_load_dlq 
                WHERE source_filename = :file_name 
                LIMIT :limit
            )
        """
    else:
        # MySQL supports LIMIT directly
        delete_sql = (
            "DELETE FROM file_load_dlq WHERE source_filename = :file_name LIMIT :limit"
        )

    return delete_sql


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


def create_stage_table(engine, source, source_filename: str, log: FileLoadLog) -> str:
    sanitized_name = sanitize_table_name(source_filename)
    stage_table_name = f"stage_{sanitized_name}"

    metadata = MetaData()
    columns = get_table_columns(source, include_timestamps=False)

    stage_table = Table(stage_table_name, metadata, *columns)
    metadata.drop_all(engine, tables=[stage_table])
    metadata.create_all(engine, tables=[stage_table])
    logger.info(f"[log_id={log.id}] Created stage table: {stage_table_name}")

    return stage_table_name
