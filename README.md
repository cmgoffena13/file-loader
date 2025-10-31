# FileLoader

An ETL framework for processing CSV, Excel, and JSON files with memory efficient processing, validation, staging, auditing, and database loading.

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
  - [Configuration](#configuration)
    - [Email Notifications](#email-notifications-optional)
    - [Slack Notifications](#slack-notifications-optional)
- [How It Works](#how-it-works)
  - [Initialization](#initialization)
  - [File Processing Pipeline](#file-processing-pipeline)
    - [Failure Notifications](#failure-notifications)
  - [Detailed Logging](#detailed-logging)
- [How to Add a New Source](#how-to-add-a-new-source)
  - [Step 1: Create the System Directory (if new system)](#step-1-create-the-system-directory-if-new-system)
  - [Step 2: Create the Source File](#step-2-create-the-source-file)
  - [Step 3: Register the Source](#step-3-register-the-source)
  - [Required Fields](#required-fields)
  - [Optional Fields](#optional-fields)
  - [Format-Specific Fields](#format-specific-fields)
- [How to Add a New Reader](#how-to-add-a-new-reader)
  - [Step 1: Create the Reader Class](#step-1-create-the-reader-class)
  - [Required Methods](#required-methods)
  - [Step 2: Create Source Configuration](#step-2-create-source-configuration)
  - [Step 3: Register the Reader](#step-3-register-the-reader)
  - [Step 4: Use the Reader](#step-4-use-the-reader)


## Features

## Scalable

- **Multiple File Formats**: Supports CSV, Excel (`.xlsx`, `.xls`), and JSON files
- **Memory Efficient**: Uses iterative reading to handle large files without loading everything into memory
- **Parallel Processing**: Processes multiple files concurrently using thread pools
- **Flexible Database Support**: PostgreSQL, MySQL, and SQL Server Compatability
- **Proper Indexing**: Table indexing strategy that supports high data volumes
- **Portable/Flexible**: Dockerized deployment option for containerized execution or native installation using UV

## Reliable

- **Data Validation**: Pydantic model validation for each record
- **Write-Audit-Publish Pattern**: 
  - Writes data into a staging table
  - Audits the staging data
  - Publishes to target tables
- **Audit Framework**: Configurable audit queries to ensure data quality
- **Retry Logic**: Automatic retry with exponential backoff for database operations to handle transient failures
- **Error Isolation**: Errors in one file do not stop processing of other files - each file is processed independently with errors logged to `file_load_log` table and optional notification firing
- **Notifications**: 
  - Email notifications to business stakeholders for file-based issues:
    - No Header detected
    - Missing required columns/fields
    - Record validation error threshold exceeded
    - Dataset audits failed
  - Slack notifications to Data Team for internal processing errors (code bugs, database failures) with detailed debugging information
- **File Management**: Automatic archiving and deletion after successful processing to keep directory clean

## Maintainable

- **Type-Safe Configuration**: Schema-validated configuration using Pydantic models ensures correct setup and prevents configuration errors
- **Centralized Registry**: Single source of truth for all data source configurations via MASTER_REGISTRY - all file mappings and processing rules accessible in one place
- **Extensible Factory Pattern**: Uses a factory pattern with abstract base classes, making it easy to add new file format readers (e.g., `.txt`, `.parquet`)
- **Automatic Table Creation**: Database tables & indexes are automatically generated from Pydantic model schemas - no manual DDL required
- **Test Suite**: Comprehensive test coverage with pytest, fixtures, and isolated test configurations

## Quick Start

The project utilizes `uv`

1. sync the packages using uv or the Make command
```bash
uv sync 
OR
make install
```
2. Install the pre-commit hooks (you might need to run `source .venv/bin/activate` if your uv environment is not being recognized)
```bash
pre-commit install --install-hooks
```

### Configuration

Set environment variables (Add the appropriate env prefix (DEV, TEST, PROD) - Ex. DEV_DATABASE_URL):

**Required:**
- `DATABASE_URL`: Database connection string (where to load the files)
- `DIRECTORY_PATH`: Directory to watch for files
- `ARCHIVE_PATH`: Directory to archive processed files
- `BATCH_SIZE`: Number of records per batch insert (default: 10000)

### Email Notifications (Optional)
- `SMTP_HOST`: SMTP server hostname
- `SMTP_PORT`: SMTP server port (default: 587)
- `SMTP_USER`: SMTP username for authentication
- `SMTP_PASSWORD`: SMTP password for authentication
- `FROM_EMAIL`: Email address to send notifications from
- `DATA_TEAM_EMAIL`: Data team email address (always CC'd on failure notifications)

### Slack Notifications (Optional)
- `SLACK_WEBHOOK_URL`: Slack webhook URL for internal processing errors (code-based issues, not file problems)

## How It Works

### Initialization

- **Automatic Table Creation**: On startup, automatically creates (IF NOT EXISTS) all target tables and the `file_load_log` table based on source configurations and Pydantic model schemas

### File Processing Pipeline

The system uses **parallel processing** with threads to handle multiple files concurrently. **Each file is processed independently** - errors in one file are logged but do not affect processing of other files:

1. **File Discovery**: Scans the designated directory (`DIRECTORY_PATH`) for supported file types (CSV, Excel, JSON)

2. **Archive First**: Immediately copies each file to the archive directory before any processing begins (preserves original for recovery)

3. **Pattern Matching**: Uses pattern matching to match file names against source configurations to determine processing rules

4. **Missing Header Detection**: Checks for required headers/fields in the file (CSV/Excel) or validates field presence (JSON). Errors immediately if no header.

5. **Dynamic Column Mapping**: Maps column names using Pydantic field aliases - supports flexible column naming in source files

6. **Column Pruning**: Automatically removes unnecessary columns that aren't defined in the source model

7. **Missing Column Detection**: Errors immediately if any required columns/fields are missing from the file

8. **Iterative Row Processing**: Processes rows iteratively using generators for memory efficiency - handles large files without loading everything into memory

9. **Record Validation**: Each record is validated against the Pydantic model schema. Records that fail validation are logged but **do not** get inserted into the staging table. A `validation_error_threshold` can be configured per source file - if the error rate (validation_errors / records_processed) exceeds the threshold, processing stops and the file is marked as failed. Default validation_error_threshold is zero.

10. **Staging Table Creation**: Automatically creates a unique staging table (`stage_{filename}`) for each file, enabling parallel processing of multiple files targeting the same destination table

11. **Chunked Inserts**: Inserts records into the staging table in configurable batches (`BATCH_SIZE`) for memory efficiency

12. **Data Auditing**: Executes configured audit queries on the staging table (e.g., grain uniqueness checks). If any audit fails, the merge step is skipped and the process is marked as failed

13. **Duplicate File Detection**: Checks if a file has already been processed by querying the target table. If found, skips the merge step and logs a warning

14. **MERGE Operation**: Merges staging data into the target table based on grain columns, handling inserts and updates appropriately

15. **Cleanup**: Drops the staging table and deletes the original file from the directory. The archived copy remains for recovery if needed (simply move from archive back to directory to reprocess). If bad data got into the table, then DELETE out of the target table where `source_filename = {file_name}` and then reprocess.

16. **Failure Notifications** {#failure-notifications}: 
    - **Email**: If `notification_emails` is configured for a source, email notifications are automatically sent to business owners when files fail (validation threshold exceeded, audit failures, missing headers/columns). The data team (configured via `DATA_TEAM_EMAIL`) is always CC'd. Notifications include error details, log_id for reference, and sample validation errors when applicable.
    - **Slack**: Internal processing errors (code bugs, database connection failures, system exceptions) are automatically sent to Slack if `SLACK_WEBHOOK_URL` is configured. These are separate from file-related issues and include system information.

### Detailed Logging

The `file_load_log` table automatically tracks detailed metrics for every file processing run:

- **Processing Phase**: Records processed count, validation errors count, start/end timestamps
- **Staging Phase**: Records loaded into staging table count, start/end timestamps
- **Audit Phase**: Audit success/failure status, start/end timestamps  
- **Merge Phase**: Records inserted/updated in target table counts, merge success/skip status, start/end timestamps
- **Overall Status**: Success/failure status for the entire file processing run, start/end timestamps

All metrics are logged automatically throughout the process, providing complete visibility into each stage of the ETL pipeline.

## How to Add a New Source

Adding a new data source involves creating a Pydantic model and source configuration, then registering it in the master registry.

The system will automatically:
- Create the database table on startup
- Match files using the `file_pattern`
- Validate and load data according to your configuration

### Step 1: Create the System Directory (if new system)

Create a new directory under `src/sources/systems/{system_name}/`:

```bash
mkdir -p src/sources/systems/{system_name}
```

### Step 2: Create the Source File

Create `{source_file}.py` in the new directory with:

1. **Pydantic Model**: Define your table schema by extending `TableModel`:
   ```python
   from src.sources.base import CSVSource, TableModel
   from pydantic_extra_types.pendulum_dt import Date
   from pydantic import Field
   
   class YourModel(TableModel):
       field1: str = Field(alias="Column Name")  # Use alias if column names differ
       field2: int
       field3: Date
   ```

2. **Source Configuration**: Create a source instance (CSVSource, ExcelSource, or JSONSource):
   ```python
   YOUR_SOURCE = CSVSource(
       file_pattern="files_*.csv",           # Wildcard pattern to match files
       source_model=YourModel,                # Pydantic model defined above
       table_name="your_table",               # Database table name
       grain=["field1", "field2"],            # Unique key columns (for MERGE)
       audit_query="""                        # SQL audit query (must return 1=success, 0=failure)
           SELECT CASE WHEN COUNT(field1) = COUNT(*) THEN 1 ELSE 0 END AS grain_unique
           FROM {table}
       """,
       validation_error_threshold=0.05,      # Optional: % errors allowed (default: 0.0)
       delimiter=",",                         # CSV-specific
       encoding="utf-8",                      # CSV-specific
       skip_rows=0,                           # Rows to skip at start
   )
   ```

### Step 3: Register the Source

Import and register your source in `src/sources/systems/master.py`:

```python
from src.sources.systems.{system_name}.{system_name} import YOUR_SOURCE

MASTER_REGISTRY.add_sources([YOUR_SOURCE])
```

### Required Fields

All sources require:
- `file_pattern`: Wildcard pattern (e.g., `"sales_*.csv"`) to match file names
- `source_model`: Pydantic model class extending `TableModel`
- `table_name`: Database table name where data will be loaded
- `grain`: List of column names that form the unique key (used for MERGE operations)
- `audit_query`: SQL query with `{table}` placeholder that returns CASE statements (1=success, 0=failure)
- `validation_error_threshold`: Float (default: 0.0) - maximum allowed error rate

### Optional Fields

- `notification_emails`: List of email addresses (e.g., `["owner@company.com", "team@company.com"]`) to notify when files fail. If configured, notifications are sent for:
  - Validation threshold exceeded (too many validation errors)
  - Audit failures (data quality checks failed)
  - General processing errors
  - The data team (configured via `DATA_TEAM_EMAIL` setting) is always CC'd for visibility

### Format-Specific Fields

**CSVSource**:
- `delimiter`: Field delimiter (default: ",")
- `encoding`: File encoding (default: "utf-8")
- `skip_rows`: Number of rows to skip (default: 0)

**ExcelSource**:
- `sheet_name`: Sheet name (optional, uses first sheet if None)
- `skip_rows`: Number of rows to skip (default: 0)

**JSONSource**:
- `array_path`: JSONPath to array items (default: "item")
- `skip_rows`: Number of items to skip (default: 0)

## How to Add a New Reader

Adding support for a new file format (e.g., `.txt`, `.parquet`, `.xml`) involves creating a reader class and a source configuration, then registering it in the factory.

### Step 1: Create the Reader Class

Create a new file `src/readers/{format}_reader.py` extending `BaseReader`:

```python
from pathlib import Path
from typing import Any, Dict, Iterator

from src.readers.base_reader import BaseReader
from src.sources.base import TXTSource  # Your custom source class


class TXTReader(BaseReader):
    def __init__(self, file_path: Path, source: TXTSource, delimiter: str, skip_rows: int):
        super().__init__(file_path, source)
        self.delimiter = delimiter  # From Source Config
        self.skip_rows = skip_rows  # From Source Config

    def read(self) -> Iterator[Dict[str, Any]]:
        """Read file iteratively, yielding dict records."""
        # Validate headers/fields using self._validate_fields(actual_fields)
        # Yield records as dictionaries
        pass

    @classmethod
    def matches_source_type(cls, source_type) -> bool:
        """Return True if source_type matches this reader's source class."""
        return source_type == TXTSource
```

### Required Methods

**`__init__`**:
- Must accept `file_path: Path` and `source: DataSource`
- Must call `super().__init__(file_path, source)`
- Accept any reader-specific parameters needed

**`read()`**:
- Must return `Iterator[Dict[str, Any]]`
- Should validate fields using `self._validate_fields(actual_fields)` where `actual_fields` is a `set[str]` of field names
- Should yield records as dictionaries where keys match Pydantic model field names or aliases
- Should respect `self.skip_rows` if applicable

**`matches_source_type()`**:
- Classmethod that returns `True` if the source type matches this reader
- Used by `ReaderFactory` to validate source/reader compatibility

### Step 2: Create Source Configuration

Create a new source class in `src/sources/base.py` extending `DataSource` with reader-specific configuration fields:

```python
class TXTSource(DataSource):
    delimiter: str = Field(default="|")
    skip_rows: int = Field(default=0)
```

### Step 3: Register the Reader

Add your reader to `src/readers/reader_factory.py`:

1. **Import the reader**:
   ```python
   from src.readers.txt_reader import TXTReader
   ```

2. **Add to `_readers` dictionary**:
   ```python
   _readers = {
       ".csv": CSVReader,
       ".xlsx": ExcelReader,
       ".xls": ExcelReader,
       ".json": JSONReader,
       ".txt": TXTReader,  # Add your reader
   }
   ```

3. **Update `include` set** (if you added new source fields):
   ```python
   reader_kwargs = source.model_dump(
       include={"delimiter", "encoding", "skip_rows", "sheet_name", "array_path", "your_new_field"}
   )
   ```

### Step 4: Use the Reader

Once registered, create a source configuration using your new source type and the system will automatically use your reader for matching file extensions.