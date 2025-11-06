import logging
from logging.config import dictConfig

import logfire
from logfire import LogfireLoggingHandler

from src.settings import config

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    # Build handlers based on whether Logfire is configured

    if config.LOGFIRE_TOKEN:
        logfire.configure(
            token=config.LOGFIRE_TOKEN,
            service_name="fileloader",
            console=config.LOGFIRE_CONSOLE,
        )

    handlers = {
        "default": {
            "class": "rich.logging.RichHandler",
            "level": config.LOG_LEVEL,
            "formatter": "console",
            "show_path": False,
        }
    }

    loggers = {
        "src": {
            "handlers": ["default"],
            "level": config.LOG_LEVEL,
            "propagate": False,
        }
    }

    # Add Logfire handlers if token is provided
    if config.LOGFIRE_TOKEN:
        handlers["logfire_src"] = {
            "class": LogfireLoggingHandler,
            "level": config.LOG_LEVEL,
        }
        handlers["logfire_sql"] = {
            "class": LogfireLoggingHandler,
            "level": config.LOG_LEVEL,
        }
        loggers["src"]["handlers"].append("logfire_src")

        # NOTE: Uncomment this if you want to log SQLAlchemy engine logs to Logfire
        # loggers["sqlalchemy.engine"] = {
        #     "handlers": ["logfire_sql"],
        #     "level": config.LOG_LEVEL,
        #     "propagate": False,
        # }

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "console": {
                    "class": "logging.Formatter",
                    "datefmt": "%Y-%m-%dT%H:%M:%S",
                    "format": "%(name)s:%(lineno)d - %(message)s",
                }
            },
            "handlers": handlers,
            "loggers": loggers,
        }
    )

    # Suppress noisy package loggers
    logging.getLogger("pyexcel").setLevel(logging.WARNING)
    logging.getLogger("pyexcel_io").setLevel(logging.WARNING)
    logging.getLogger("pyexcel.internal").setLevel(logging.WARNING)
    logger.info("Logging Configuration Successful")
