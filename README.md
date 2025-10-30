# File Loader

A Python-based ETL tool for processing CSV, Excel, and JSON files with memory efficient processing, validation, staging, auditing, and database loading.

## Features

- **Multiple File Formats**: Supports CSV, Excel (`.xlsx`, `.xls`), and JSON files
- **Memory Efficient**: Uses iterative reading to handle large files without loading everything into memory
- **Data Validation**: Pydantic model validation for each record
- **Staging Pattern**: Loads data into stage tables, audits it, then merges to target tables
- **Parallel Processing**: Processes multiple files concurrently using thread pools
- **Database Support**: PostgreSQL, MySQL, and SQL Server Compatability
- **Audit Framework**: Configurable audit queries to ensure data quality
- **File Management**: Automatic archiving and deletion after successful processing

## Quick Start

### Configuration

Set environment variables:

- `DATABASE_URL`: Database connection string (where to load the files)
- `DIRECTORY_PATH`: Directory to watch for files
- `ARCHIVE_PATH`: Directory to archive processed files
- `BATCH_SIZE`: Number of records per batch insert (default: 10000)

## How It Works

### Initialization

- **Automatic Table Creation**: On startup, automatically creates (IF NOT EXISTS) all target tables and the `file_load_log` table based on source configurations and Pydantic model schemas

### File Processing Pipeline

The system uses **parallel processing** with threads to handle multiple files concurrently:

1. **File Discovery**: Scans the designated directory (`DIRECTORY_PATH`) for supported file types (CSV, Excel, JSON)

2. **Archive First**: Immediately copies each file to the archive directory before any processing begins (preserves original for recovery)

3. **Pattern Matching**: Uses pattern matching to match file names against source configurations to determine processing rules

4. **Header Validation**: Checks for required headers/fields in the file (CSV/Excel) or validates field presence (JSON)

5. **Dynamic Column Mapping**: Maps column names using Pydantic field aliases - supports flexible column naming in source files

6. **Column Pruning**: Automatically removes unnecessary columns that aren't defined in the source model

7. **Missing Column Detection**: Errors immediately if any required columns/fields are missing from the file

8. **Iterative Row Processing**: Processes rows iteratively using generators for memory efficiency - handles large files without loading everything into memory

9. **Record Validation**: Each record is validated against the Pydantic model schema. Records that fail validation are logged but **do not** get inserted into the staging table

10. **Staging Table Creation**: Automatically creates a unique staging table (`stage_{filename}`) for each file, enabling parallel processing of multiple files targeting the same destination table

11. **Chunked Inserts**: Inserts records into the staging table in configurable batches (`BATCH_SIZE`) for memory efficiency

12. **Data Auditing**: Executes configured audit queries on the staging table (e.g., grain uniqueness checks). If any audit fails, the merge step is skipped and the process is marked as failed

13. **Duplicate File Detection**: Checks if a file has already been processed by querying the target table. If found, skips the merge step and logs a warning

14. **MERGE Operation**: Merges staging data into the target table based on grain columns, handling inserts and updates appropriately

15. **Cleanup**: Drops the staging table and deletes the original file from the directory. The archived copy remains for recovery if needed (simply move from archive back to directory to reprocess). If bad data got in, then DELETE out of the target table where `source_filename = {file_name}` and then reprocess.

### Detailed Logging

The `file_load_log` table automatically tracks detailed metrics for every file processing run:

- **Processing Phase**: Records processed count, validation errors count, timestamps
- **Staging Phase**: Records loaded into staging table count, timestamps
- **Audit Phase**: Audit success/failure status, timestamps  
- **Merge Phase**: Records inserted/updated in target table counts, merge success/skip status, timestamps
- **Overall Status**: Success/failure status for the entire file processing run, timestamps

All metrics are logged automatically throughout the process, providing complete visibility into each stage of the ETL pipeline.