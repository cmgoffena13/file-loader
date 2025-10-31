import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pendulum
import pytest
from sqlalchemy import text

from src.exceptions import MissingColumnsError, MissingHeaderError
from src.file_processor import FileProcessor
from src.readers.csv_reader import CSVReader
from src.settings import config
from src.sources.systems.master import MASTER_REGISTRY
from src.tests.fixtures.source_configs import TEST_SALES


def test_csv_missing_header_raises_error(csv_missing_header):
    """Test that MissingHeaderError is raised when CSV has no header."""
    reader = CSVReader(
        file_path=csv_missing_header,
        source=TEST_SALES,
        delimiter=",",
        encoding="utf-8",
        skip_rows=0,
    )

    with pytest.raises(MissingHeaderError) as exc_info:
        list(reader.read())

    assert "No headers found" in str(exc_info.value)


def test_csv_blank_string_header_raises_error(csv_blank_header):
    """Test that MissingHeaderError is raised when CSV has blank/whitespace headers."""
    reader = CSVReader(
        file_path=csv_blank_header,
        source=TEST_SALES,
        delimiter=",",
        encoding="utf-8",
        skip_rows=0,
    )

    with pytest.raises(MissingHeaderError) as exc_info:
        list(reader.read())

    assert "Whitespace-only headers" in str(exc_info.value)


def test_csv_missing_columns_raises_error(csv_missing_columns):
    """Test that MissingColumnsError is raised when required columns are missing."""
    reader = CSVReader(
        file_path=csv_missing_columns,
        source=TEST_SALES,
        delimiter=",",
        encoding="utf-8",
        skip_rows=0,
    )

    with pytest.raises(MissingColumnsError) as exc_info:
        list(reader.read())

    error_msg = str(exc_info.value)
    assert "Missing required fields" in error_msg
    assert "Required fields:" in error_msg
    assert "Missing fields:" in error_msg


def test_csv_valid_file_reads_successfully(test_csv_file):
    """Test that a valid CSV file reads successfully."""
    reader = CSVReader(
        file_path=test_csv_file,
        source=TEST_SALES,
        delimiter=",",
        encoding="utf-8",
        skip_rows=0,
    )

    records = list(reader.read())
    assert len(records) == 2
    assert records[0]["transaction_id"] == "TXN001"
    assert records[1]["transaction_id"] == "TXN002"


def test_csv_duplicate_grain_fails_audit(csv_duplicate_grain, temp_sqlite_db):
    """Test that duplicate grain values trigger AuditFailedError in SQLite."""
    # Create a temporary archive directory
    with tempfile.TemporaryDirectory() as archive_dir:
        MASTER_REGISTRY.sources = [TEST_SALES]

        processor = FileProcessor()

        # Process file - should fail during audit
        results = processor.process_files_parallel(
            [str(csv_duplicate_grain)], archive_dir
        )

        # Verify that processing failed
        assert len(results) == 1
        assert results[0]["success"] is False


def test_csv_duplicate_file_moved_to_duplicates(test_csv_file, temp_sqlite_db):
    """Test that duplicate files are detected and moved to duplicates directory."""
    # Set up directories
    with tempfile.TemporaryDirectory() as archive_dir:
        duplicates_dir = Path(tempfile.gettempdir()) / "test_duplicates"
        duplicates_dir.mkdir(exist_ok=True)

        # Override only DUPLICATE_FILES_PATH since it's used by the duplicate check
        original_duplicates = config.DUPLICATE_FILES_PATH
        try:
            config.DUPLICATE_FILES_PATH = duplicates_dir

            MASTER_REGISTRY.sources = [TEST_SALES]

            processor = FileProcessor()

            # First processing - should succeed
            file_path_str = str(test_csv_file)
            archive_path_obj = Path(archive_dir)
            results = processor.process_files_parallel(
                [file_path_str], archive_path_obj
            )

            print(f"DEBUG: results = {results}")
            assert len(results) == 1
            assert results[0]["success"] is True

            # Since merge is mocked, manually insert a record to simulate duplicate detection
            # This allows _check_duplicate_file to find it on the second processing
            with processor.Session() as session:
                session.execute(
                    text(f"""
                        INSERT INTO transactions 
                        (transaction_id, customer_id, product_sku, quantity, unit_price, 
                         total_amount, sale_date, sales_rep, etl_row_hash, source_filename, 
                         file_load_log_id, etl_created_at)
                        VALUES 
                        ('TEST001', 'CUST001', 'SKU001', 1, 10.0, 10.0, '2024-01-01', 'TEST', 
                         X'0000000000000000000000000000000000000000000000000000000000000000',
                         :filename, :log_id, :created_at)
                    """),
                    {
                        "filename": test_csv_file.name,
                        "log_id": results[0]["id"],
                        "created_at": pendulum.now().to_iso8601_string(),
                    },
                )
                session.commit()

            # Find the archived file
            archive_path = Path(archive_dir)
            archived_files = list(archive_path.glob(test_csv_file.name))
            assert len(archived_files) == 1, "File should have been archived"

            source_file_copy = Path(test_csv_file.parent) / test_csv_file.name
            shutil.copy(archived_files[0], source_file_copy)

            # Temporarily add notification_emails to TEST_SALES to test notification
            original_emails = TEST_SALES.notification_emails
            TEST_SALES.notification_emails = ["test@example.com"]

            # Second processing - should detect duplicate and move to duplicates
            with patch(
                "src.file_processor.send_failure_notification"
            ) as mock_notification:
                results = processor.process_files_parallel(
                    [str(source_file_copy)], archive_path
                )

                # Should not process (no results because file was moved before logging)
                # The file should be in duplicates directory
                duplicate_files = list(duplicates_dir.glob(test_csv_file.name))
                assert len(duplicate_files) == 1
                assert duplicate_files[0].name == test_csv_file.name

                # Verify email notification was sent
                assert mock_notification.called
                call_args = mock_notification.call_args
                assert call_args[1]["file_name"] == test_csv_file.name
                assert call_args[1]["error_type"] == "Duplicate File Detected"
                assert "has already been processed" in call_args[1]["error_message"]
                assert call_args[1]["recipient_emails"] == ["test@example.com"]

                # Verify original file is gone from source location
                assert not source_file_copy.exists()

            # Restore original notification_emails
            TEST_SALES.notification_emails = original_emails

        finally:
            config.DUPLICATE_FILES_PATH = original_duplicates
            # Cleanup duplicates directory
            if duplicates_dir.exists():
                shutil.rmtree(duplicates_dir)
