import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any, Dict, Optional

import pendulum
from slack_sdk.webhook import WebhookClient

from src.retry import retry
from src.settings import config

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    INFO = "ℹ️"
    WARNING = "⚠️"
    ERROR = "❌"
    CRITICAL = "🚨"
    SUCCESS = "✅"


def _create_slack_message(
    level: AlertLevel,
    title: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
) -> str:
    timestamp = pendulum.now("UTC").format("YYYY-MM-DD HH:mm:ss z")

    formatted_message = [
        f"{level.value} *{level.name}*",
        f"*{title}*",
        f"*Timestamp:* {timestamp}",
        f"*Message:* {message}",
    ]

    if details:
        detail_lines = []
        for key, value in details.items():
            detail_lines.append(f"• *{key}:* {value}")
        if detail_lines:
            formatted_message.append("\n*Details:*")
            formatted_message.extend(detail_lines)

    return "\n".join(formatted_message)


def send_failure_notification(
    file_name: str,
    error_type: str,
    error_message: str,
    log_id: Optional[int] = None,
    recipient_emails: Optional[list[str]] = None,
    additional_details: Optional[str] = None,
) -> None:
    """Send email notification for file processing failures.

    Args:
        file_name: Name of the file that failed
        error_type: Type of error (e.g., "Validation Threshold Exceeded", "Audit Failed")
        error_message: Detailed error message
        log_id: Log ID for reference
        recipient_emails: List of email addresses from source config
        additional_details: Additional context to include in email
    """
    if not config.FROM_EMAIL:
        logger.warning("FROM_EMAIL not configured, skipping email notification")
        return

    recipients = []
    if recipient_emails:
        recipients.extend(recipient_emails)

    cc_recipients = []
    if config.DATA_TEAM_EMAIL:
        cc_recipients.append(config.DATA_TEAM_EMAIL)

    if not recipients and not cc_recipients:
        logger.warning(
            f"No email recipients configured for file {file_name}, skipping notification"
        )
        return

    if not recipients:
        logger.warning(
            f"No email recipients configured for file {file_name}, skipping notification"
        )
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"FileLoader Failed: {file_name} - {error_type}"
    msg["From"] = config.FROM_EMAIL

    msg["To"] = ", ".join(recipients)
    if cc_recipients:
        msg["Cc"] = ", ".join(cc_recipients)

    body_text = f"""
File Processing Failure Notification

File: {file_name}
Error Type: {error_type}
Log ID: {log_id if log_id else "N/A"}

Error Details:
{error_message}
"""

    if additional_details:
        body_text += f"\nAdditional Information:\n{additional_details}"

    body_text += f"\n\Data Team can reference log_id={log_id} for more details."

    msg.attach(MIMEText(body_text, "plain"))

    @retry()
    def _send_email():
        if not config.SMTP_HOST:
            logger.warning("SMTP_HOST not configured, skipping email notification")
            return

        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT) as server:
            if config.SMTP_USER and config.SMTP_PASSWORD:
                server.starttls()
                server.login(config.SMTP_USER, config.SMTP_PASSWORD)

            all_recipients = recipients + cc_recipients
            server.sendmail(config.FROM_EMAIL, all_recipients, msg.as_string())
            logger.info(
                f"Sent failure notification email for {file_name} to {len(all_recipients)} recipient(s)"
            )

    try:
        _send_email()
    except Exception as e:
        logger.error(
            f"Failed to send notification email for {file_name} after retries: {e}"
        )


def send_slack_notification(
    error_message: str,
    file_name: Optional[str] = None,
    log_id: Optional[int] = None,
    error_location: Optional[str] = None,
) -> None:
    """Send Slack notification for internal processing errors (code-based issues).

    Args:
        error_message: The error message or exception details
        file_name: Name of the file being processed (if applicable)
        log_id: Log ID for reference
        error_location: File and line number where error occurred (e.g., "file_processor.py:123")
    """
    if not config.SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not configured, skipping Slack notification")
        return

    details: Dict[str, Any] = {}
    if file_name:
        details["File"] = file_name
    if log_id:
        details["Log ID"] = log_id
    if error_location:
        details["Location"] = error_location

    formatted_message = _create_slack_message(
        level=AlertLevel.ERROR,
        title="FileLoader - Internal Processing Error",
        message=error_message,
        details=details if details else None,
    )

    @retry()
    def _send_slack():
        webhook = WebhookClient(config.SLACK_WEBHOOK_URL)
        response = webhook.send(text=formatted_message)
        if response.status_code == 200:
            logger.info("Sent Slack notification for internal processing error")
        else:
            raise Exception(
                f"Slack webhook returned status {response.status_code}: {response.body}"
            )

    try:
        _send_slack()
    except Exception as e:
        logger.error(f"Failed to send Slack notification after retries: {e}")
