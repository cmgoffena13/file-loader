import logging
from logging.config import dictConfig

import logfire
from logfire import LogfireLoggingHandler

from src.settings import DevConfig, config

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    if config.LOGFIRE_TOKEN:
        logfire.configure(token=config.LOGFIRE_TOKEN, service_name="fileloader")

    # Build handlers based on whether Logfire is configured
    handlers = {
        "default": {
            "class": "rich.logging.RichHandler",
            "level": "DEBUG",
            "formatter": "console",
            "show_path": False,
        }
    }

    loggers = {
        "src": {
            "handlers": ["default"],
            "level": "DEBUG" if isinstance(config, DevConfig) else "INFO",
            "propagate": False,
        }
    }

    # Add Logfire handlers if token is provided
    if config.LOGFIRE_TOKEN:
        handlers["logfire_src"] = {
            "class": LogfireLoggingHandler,
            "level": "DEBUG" if isinstance(config, DevConfig) else "INFO",
        }
        handlers["logfire_sql"] = {
            "class": LogfireLoggingHandler,
            "level": "INFO",
        }
        loggers["src"]["handlers"].append("logfire_src")
        loggers["sqlalchemy.engine"] = {
            "handlers": ["logfire_sql"],
            "level": "INFO",
            "propagate": False,
        }

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
