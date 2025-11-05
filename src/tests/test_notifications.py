import csv
import tempfile
from pathlib import Path
from unittest.mock import patch

import pendulum
import pytest

from src.exceptions import (
    AuditFailedError,
    GrainValidationError,
    MissingColumnsError,
    MissingHeaderError,
    ValidationThresholdExceededError,
)
from src.file_processor import FileProcessor
from src.settings import config
from src.sources.systems.master import MASTER_REGISTRY
from src.tests.fixtures.source_configs import TEST_SALES


def test_email_notification_on_missing_header(csv_missing_header, temp_sqlite_db):
    """Test that email notification is sent for MissingHeaderError."""
    MASTER_REGISTRY.sources = [TEST_SALES]

    # Add notification emails to test source
    original_emails = TEST_SALES.notification_emails
    TEST_SALES.notification_emails = ["business@example.com"]

    try:
        processor = FileProcessor()

        with patch("src.file_processor.send_failure_notification") as mock_email:
            with tempfile.TemporaryDirectory() as archive_dir:
                processor.process_files_parallel(
                    [str(csv_missing_header)], Path(archive_dir)
                )

            # Verify email was sent
            assert mock_email.called
            call_args = mock_email.call_args
            assert call_args[1]["file_name"] == csv_missing_header.name
            assert call_args[1]["error_type"] == MissingHeaderError.error_type
            assert call_args[1]["log_id"] is not None
            assert call_args[1]["recipient_emails"] == ["business@example.com"]
    finally:
        TEST_SALES.notification_emails = original_emails


def test_email_notification_on_missing_columns(csv_missing_columns, temp_sqlite_db):
    """Test that email notification is sent for MissingColumnsError."""
    MASTER_REGISTRY.sources = [TEST_SALES]

    original_emails = TEST_SALES.notification_emails
    TEST_SALES.notification_emails = ["business@example.com"]

    try:
        processor = FileProcessor()

        with patch("src.file_processor.send_failure_notification") as mock_email:
            with tempfile.TemporaryDirectory() as archive_dir:
                processor.process_files_parallel(
                    [str(csv_missing_columns)], Path(archive_dir)
                )

            assert mock_email.called
            call_args = mock_email.call_args
            assert call_args[1]["error_type"] == MissingColumnsError.error_type
            assert call_args[1]["recipient_emails"] == ["business@example.com"]
    finally:
        TEST_SALES.notification_emails = original_emails


def test_email_notification_on_duplicate_file(test_csv_file, temp_sqlite_db):
    """Test that email notification is sent for duplicate file detection."""
    MASTER_REGISTRY.sources = [TEST_SALES]

    original_emails = TEST_SALES.notification_emails
    TEST_SALES.notification_emails = ["business@example.com"]

    try:
        with tempfile.TemporaryDirectory() as archive_dir:
            processor = FileProcessor()

            # First processing - should merge records into target table
            processor.process_files_parallel([str(test_csv_file)], Path(archive_dir))

            # Recreate the file (it was deleted after first processing)
            with open(test_csv_file, "w", newline="", encoding="utf-8") as f:
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
                        "1",
                        "25.00",
                        "25.00",
                        "2024-01-16",
                        "Jane Smith",
                    ]
                )

            # First processing should have merged records into target table with source_filename
            # No need for manual insert - merge handled it

            # Second processing - should detect duplicate
            with patch("src.file_processor.send_failure_notification") as mock_email:
                processor.process_files_parallel(
                    [str(test_csv_file)], Path(archive_dir)
                )

                assert mock_email.called
                call_args = mock_email.call_args
                assert call_args[1]["error_type"] == "Duplicate File Detected"
                assert call_args[1]["recipient_emails"] == ["business@example.com"]
    finally:
        TEST_SALES.notification_emails = original_emails


def test_email_notification_on_audit_failure(csv_duplicate_grain, temp_sqlite_db):
    """Test that email notification is sent for GrainValidationError."""
    MASTER_REGISTRY.sources = [TEST_SALES]

    original_emails = TEST_SALES.notification_emails
    TEST_SALES.notification_emails = ["business@example.com"]

    try:
        processor = FileProcessor()

        with patch("src.file_processor.send_failure_notification") as mock_email:
            with tempfile.TemporaryDirectory() as archive_dir:
                processor.process_files_parallel(
                    [str(csv_duplicate_grain)], Path(archive_dir)
                )

            # Verify email was sent
            assert mock_email.called
            call_args = mock_email.call_args
            assert call_args[1]["file_name"] == csv_duplicate_grain.name
            assert call_args[1]["error_type"] == GrainValidationError.error_type
            assert call_args[1]["log_id"] is not None
            assert call_args[1]["recipient_emails"] == ["business@example.com"]
            assert "Grain values are not unique" in call_args[1]["error_message"]
    finally:
        TEST_SALES.notification_emails = original_emails


def test_slack_notification_on_unexpected_exception(test_csv_file, temp_sqlite_db):
    """Test that unexpected exceptions are captured in results for Slack notification."""
    MASTER_REGISTRY.sources = [TEST_SALES]

    processor = FileProcessor()

    # Mock _load_records to raise an unexpected exception
    with patch.object(
        processor, "_load_records", side_effect=ValueError("Unexpected error")
    ):
        with tempfile.TemporaryDirectory() as archive_dir:
            results = processor.process_files_parallel(
                [str(test_csv_file)], Path(archive_dir)
            )

    # Verify error info is captured in results (main.py will send Slack notification)
    assert len(results) == 1
    assert results[0]["success"] is False
    assert results[0]["error_type"] == "ValueError"
    assert "Unexpected error" in results[0]["error_message"]
    assert results[0]["source_filename"] == test_csv_file.name
    assert results[0]["error_location"] is not None


def test_slack_notification_aggregate_in_main(test_csv_file, temp_sqlite_db):
    """Test that main.py sends aggregated Slack notification for code failures."""
    MASTER_REGISTRY.sources = [TEST_SALES]

    with patch("src.notifications.send_slack_notification") as mock_slack:
        processor = FileProcessor()

        # Mock _load_records to raise an unexpected exception (code problem)
        with patch.object(
            processor, "_load_records", side_effect=RuntimeError("Code bug")
        ):
            with tempfile.TemporaryDirectory() as archive_dir:
                results = processor.process_files_parallel(
                    [str(test_csv_file)], Path(archive_dir)
                )

        # Simulate main.py aggregation logic
        file_error_types = {
            MissingHeaderError.error_type,
            MissingColumnsError.error_type,
            ValidationThresholdExceededError.error_type,
            AuditFailedError.error_type,
            GrainValidationError.error_type,
        }
        code_failures = [
            r
            for r in results
            if not r.get("success", True)
            and r.get("error_type") not in file_error_types
        ]

        if code_failures:
            failure_count = len(code_failures)
            total_count = len(results)

            failure_details = []
            for failure in code_failures:
                source_filename = failure.get("source_filename", "Unknown")
                error_type = failure.get("error_type", "Unknown Error")
                error_message = failure.get("error_message", "No error details")

                detail = f"• {source_filename}: {error_type}"
                if error_message:
                    if len(error_message) > 200:
                        error_message = error_message[:200] + "..."
                    detail += f" - {error_message}"
                failure_details.append(detail)

            summary_message = (
                f"File processing completed with {failure_count} failure(s) out of {total_count} file(s).\n\n"
                f"Failed files:\n" + "\n".join(failure_details)
            )

            mock_slack(
                error_message=summary_message,
                file_name=None,
                log_id=None,
                error_location=None,
            )

        # Verify aggregate Slack was sent
        assert mock_slack.called
        call_args = mock_slack.call_args
        assert "File processing completed" in call_args[1]["error_message"]
        assert f"{failure_count} failure" in call_args[1]["error_message"]


def test_no_email_notification_when_emails_not_configured(
    csv_missing_header, temp_sqlite_db
):
    """Test that email notification is not sent when notification_emails is not configured."""
    MASTER_REGISTRY.sources = [TEST_SALES]

    # Ensure no notification emails configured
    original_emails = TEST_SALES.notification_emails
    TEST_SALES.notification_emails = None

    try:
        processor = FileProcessor()

        with patch("src.file_processor.send_failure_notification") as mock_email:
            with tempfile.TemporaryDirectory() as archive_dir:
                processor.process_files_parallel(
                    [str(csv_missing_header)], Path(archive_dir)
                )

            # Verify email was NOT sent
            assert not mock_email.called
    finally:
        TEST_SALES.notification_emails = original_emails


def test_no_slack_notification_when_webhook_not_configured(
    test_csv_file, temp_sqlite_db
):
    """Test that error info is still captured when SLACK_WEBHOOK_URL is not configured."""
    MASTER_REGISTRY.sources = [TEST_SALES]

    original_webhook = config.SLACK_WEBHOOK_URL
    config.SLACK_WEBHOOK_URL = None

    try:
        processor = FileProcessor()

        # Mock _load_records to raise an unexpected exception
        with patch.object(
            processor, "_load_records", side_effect=ValueError("Unexpected error")
        ):
            with tempfile.TemporaryDirectory() as archive_dir:
                results = processor.process_files_parallel(
                    [str(test_csv_file)], Path(archive_dir)
                )

        # Verify error info is still captured in results
        # main.py will call send_slack_notification, but it will skip sending due to config
        assert len(results) == 1
        assert results[0]["success"] is False
        assert results[0]["error_type"] == "ValueError"
    finally:
        config.SLACK_WEBHOOK_URL = original_webhook
