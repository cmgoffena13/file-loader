import logging

from src.exceptions import FILE_ERROR_EXCEPTIONS
from src.notifications import send_slack_notification
from src.retry import get_error_location
from src.settings import config
from src.utils import process_directory

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper()),
    format="%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s",
)

# Suppress noisy package loggers
logging.getLogger("pyexcel").setLevel(logging.WARNING)
logging.getLogger("pyexcel_io").setLevel(logging.WARNING)
logging.getLogger("pyexcel.internal").setLevel(logging.WARNING)

# Prevent SQLAlchemy logger from propagating to root (prevents duplicate query logs)
logging.getLogger("sqlalchemy.engine").propagate = False


def main():
    try:
        results = process_directory()
        # File-specific errors (MissingHeaderError, etc.) are emailed to business stakeholders
        # Code problems (unexpected exceptions) should go to Slack
        file_error_types = {exc.error_type for exc in FILE_ERROR_EXCEPTIONS}
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
                file_name = failure.get("file_name", "Unknown")
                error_type = failure.get("error_type", "Unknown Error")
                error_message = failure.get("error_message", "No error details")
                log_id = failure.get("id")

                detail = f"• {file_name}"
                if log_id:
                    detail += f" (log_id: {log_id})"
                detail += f": {error_type}"
                if error_message:
                    if len(error_message) > 200:
                        error_message = error_message[:200] + "..."
                    detail += f" - {error_message}"
                failure_details.append(detail)

            summary_message = (
                f"File processing completed with {failure_count} failure(s) out of {total_count} file(s).\n\n"
                f"Failed files:\n" + "\n".join(failure_details)
            )

            send_slack_notification(
                error_message=summary_message,
                file_name=None,
                log_id=None,
                error_location=None,
            )

    except Exception as e:
        send_slack_notification(
            error_message=str(e),
            file_name=None,
            log_id=None,
            error_location=get_error_location(e),
        )


if __name__ == "__main__":
    main()
