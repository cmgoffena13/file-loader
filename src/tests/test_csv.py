import csv
import json
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import MetaData, Table, select

from src.exceptions import MissingColumnsError, MissingHeaderError
from src.file_processor import FileProcessor
from src.readers.csv_reader import CSVReader
from src.settings import config
from src.sources.systems.master import MASTER_REGISTRY
from src.tests.fixtures.source_configs import TEST_SALES, TEST_SALES_WITH_DLQ


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

            # First processing should have merged records into target table with source_filename
            # No need for manual insert - merge handled it

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


def test_csv_dead_letter_queue_stores_validation_errors(
    csv_mixed_valid_invalid, temp_sqlite_db
):
    """Test that validation errors are stored in the dead letter queue."""

    # Create a temporary archive directory
    with tempfile.TemporaryDirectory() as archive_dir:
        MASTER_REGISTRY.sources = [TEST_SALES_WITH_DLQ]

        processor = FileProcessor()

        # Process file with mixed valid/invalid records
        archive_path_obj = Path(archive_dir)
        results = processor.process_files_parallel(
            [str(csv_mixed_valid_invalid)], archive_path_obj
        )

        # Verify that processing succeeded (file was processed)
        assert len(results) == 1
        assert results[0]["success"] is True

        log_id = results[0]["id"]

        # Verify valid records are in the target table
        with processor.Session() as session:
            # Reflect the transactions table
            metadata = MetaData()
            metadata.reflect(bind=processor.engine, only=["transactions"])
            transactions_table = Table(
                "transactions", metadata, autoload_with=processor.engine
            )

            # Check transactions table for valid records
            valid_records = session.execute(
                select(transactions_table).where(
                    transactions_table.c.source_filename == "sales_mixed.csv"
                )
            ).fetchall()

            # Should have 2 valid records (TXN001 and TXN003)
            assert len(valid_records) == 2
            transaction_ids = {row[0] for row in valid_records}
            assert "TXN001" in transaction_ids
            assert "TXN003" in transaction_ids

            # Verify invalid records are in the DLQ table
            dlq_table = processor._get_file_load_dlq()
            dlq_records = session.execute(
                select(dlq_table).where(
                    dlq_table.c.file_load_log_id == log_id,
                    dlq_table.c.source_filename == "sales_mixed.csv",
                )
            ).fetchall()

            # Should have 2 invalid records (TXN002 and TXN004)
            assert len(dlq_records) == 2

            # Verify DLQ record structure and content
            dlq_row_numbers = {row.file_row_number for row in dlq_records}
            assert 2 in dlq_row_numbers  # TXN002 (invalid quantity)
            assert 4 in dlq_row_numbers  # TXN004 (invalid date)

            for dlq_record in dlq_records:
                # Verify required fields are present
                assert dlq_record.file_record_data is not None
                assert dlq_record.validation_errors is not None
                assert dlq_record.file_row_number > 0
                assert dlq_record.source_filename == "sales_mixed.csv"
                assert dlq_record.file_load_log_id == log_id
                assert dlq_record.target_table_name == "transactions"
                assert dlq_record.failed_at is not None

                # Verify file_record_data contains the original data
                # For SQLite, file_record_data is stored as TEXT (JSON string)
                raw_data = json.loads(dlq_record.file_record_data)
                assert "transaction_id" in raw_data

                # Row 2 should have TXN002, Row 4 should have TXN004
                if dlq_record.file_row_number == 2:
                    assert raw_data["transaction_id"] == "TXN002"
                    assert "not_a_number" in str(raw_data.get("quantity", ""))
                elif dlq_record.file_row_number == 4:
                    assert raw_data["transaction_id"] == "TXN004"
                    assert "invalid_date" in str(raw_data.get("sale_date", ""))

            # Verify no records with TXN002 or TXN004 in transactions table
            invalid_in_target = session.execute(
                select(transactions_table).where(
                    transactions_table.c.transaction_id.in_(["TXN002", "TXN004"]),
                    transactions_table.c.source_filename == "sales_mixed.csv",
                )
            ).fetchall()
            assert len(invalid_in_target) == 0, (
                "Invalid records should not be in target table"
            )


def test_csv_dead_letter_queue_deletes_records_after_reprocessing(
    csv_mixed_valid_invalid, temp_sqlite_db, temp_directory
):
    """Test that DLQ records are deleted after successful reprocessing of a file."""
    # Create a temporary archive directory
    with tempfile.TemporaryDirectory() as archive_dir:
        MASTER_REGISTRY.sources = [TEST_SALES_WITH_DLQ]

        processor = FileProcessor()
        archive_path_obj = Path(archive_dir)

        # First processing: file with validation errors
        results = processor.process_files_parallel(
            [str(csv_mixed_valid_invalid)], archive_path_obj
        )

        assert len(results) == 1
        assert results[0]["success"] is True
        first_log_id = results[0]["id"]

        # Verify DLQ records exist from first run
        with processor.Session() as session:
            dlq_table = processor._get_file_load_dlq()
            first_dlq_records = session.execute(
                select(dlq_table).where(
                    dlq_table.c.source_filename == "sales_mixed.csv"
                )
            ).fetchall()

            # Should have 2 DLQ records from first run
            assert len(first_dlq_records) == 2, (
                "Should have 2 DLQ records from first processing"
            )

        # Create a corrected version of the file with all valid records
        corrected_file = temp_directory / "sales_mixed.csv"
        with open(corrected_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "transaction_id",
                    "customer_id",
                    "product_sku",
                    "quantity",
                    "unit_price",
                    "total_amount",
                    "sale_date",
                    "sales_rep",
                ]
            )
            # All valid records (fixing the invalid ones)
            writer.writerow(
                [
                    "TXN001",
                    "CUST001",
                    "SKU001",
                    "2",
                    "10.50",
                    "21.00",
                    "2024-01-15",
                    "John Doe",
                ]
            )
            writer.writerow(
                [
                    "TXN002",
                    "CUST002",
                    "SKU002",
                    "2",
                    "25.00",
                    "50.00",
                    "2024-01-16",
                    "Jane Smith",
                ]
            )  # Fixed quantity
            writer.writerow(
                [
                    "TXN003",
                    "CUST003",
                    "SKU003",
                    "3",
                    "15.00",
                    "45.00",
                    "2024-01-17",
                    "Bob Johnson",
                ]
            )
            writer.writerow(
                [
                    "TXN004",
                    "CUST004",
                    "SKU004",
                    "1",
                    "30.00",
                    "30.00",
                    "2024-01-18",
                    "Alice Brown",
                ]
            )  # Fixed date

        # Delete target records to allow reprocessing (simulating fixing data and reprocessing)
        with processor.Session() as session:
            metadata = MetaData()
            metadata.reflect(bind=processor.engine, only=["transactions"])
            transactions_table = Table(
                "transactions", metadata, autoload_with=processor.engine
            )
            delete_stmt = transactions_table.delete().where(
                transactions_table.c.source_filename == "sales_mixed.csv"
            )
            session.execute(delete_stmt)
            session.commit()

        # Second processing: reprocess with corrected file
        results = processor.process_files_parallel(
            [str(corrected_file)], archive_path_obj
        )

        assert len(results) == 1
        assert results[0]["success"] is True
        second_log_id = results[0]["id"]

        # Verify DLQ records are deleted after successful reprocessing
        with processor.Session() as session:
            dlq_table = processor._get_file_load_dlq()
            remaining_dlq_records = session.execute(
                select(dlq_table).where(
                    dlq_table.c.source_filename == "sales_mixed.csv"
                )
            ).fetchall()

            # DLQ records should be deleted after successful merge
            assert len(remaining_dlq_records) == 0, (
                "DLQ records should be deleted after successful reprocessing"
            )

            # Verify all records are now in the target table
            metadata = MetaData()
            metadata.reflect(bind=processor.engine, only=["transactions"])
            transactions_table = Table(
                "transactions", metadata, autoload_with=processor.engine
            )

            all_records = session.execute(
                select(transactions_table).where(
                    transactions_table.c.source_filename == "sales_mixed.csv"
                )
            ).fetchall()

            # Should have all 4 records (including the previously invalid ones)
            assert len(all_records) == 4
            transaction_ids = {row[0] for row in all_records}
            assert "TXN001" in transaction_ids
            assert "TXN002" in transaction_ids  # Now valid
            assert "TXN003" in transaction_ids
            assert "TXN004" in transaction_ids  # Now valid
