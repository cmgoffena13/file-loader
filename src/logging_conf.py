from logging.config import dictConfig

from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from src.settings import ProdConfig, config


def setup_logging():
    # Setup OpenTelemetry tracing
    tracer_provider = TracerProvider()
    trace.set_tracer_provider(tracer_provider)

    # Setup OpenTelemetry logging
    logger_provider = LoggerProvider()
    set_logger_provider(logger_provider)

    if isinstance(config, ProdConfig):
        # Setup OpenTelemetry tracing exporter
        trace_exporter = OTLPSpanExporter(
            endpoint=config.OPEN_TELEMETRY_TRACE_ENDPOINT,
            headers={"Authorization": config.OPEN_TELEMETRY_AUTHORIZATION_TOKEN},
        )
        trace_processor = BatchSpanProcessor(trace_exporter)
        tracer_provider.add_span_processor(trace_processor)

        # Setup OpenTelemetry logging exporter
        log_exporter = OTLPLogExporter(
            endpoint=config.OPEN_TELEMETRY_LOG_ENDPOINT,
            headers={"Authorization": config.OPEN_TELEMETRY_AUTHORIZATION_TOKEN},
        )
        log_processor = BatchLogRecordProcessor(log_exporter)
        logger_provider.add_log_record_processor(log_processor)

        handlers = {
            "default": {
                "class": "rich.logging.RichHandler",
                "level": config.LOG_LEVEL,
                "formatter": "console",
                "show_path": False,
            },
            "otel": {
                "()": LoggingHandler,
                "level": config.LOG_LEVEL,
                "logger_provider": logger_provider,
            },
        }
    else:
        handlers = {
            "default": {
                "class": "rich.logging.RichHandler",
                "level": config.LOG_LEVEL,
                "formatter": "console",
                "show_path": False,
            },
        }

    formatters = {
        "console": {
            "class": "logging.Formatter",
            "datefmt": "%Y-%m-%dT%H:%M:%S",
            "format": "%(name)s:%(lineno)d - %(message)s",
        }
    }

    # Declare src logger as the root logger
    # Any other loggers will be children of src and inherit the settings
    loggers = {
        "src": {
            "level": config.LOG_LEVEL,
            "handlers": list(handlers.keys()),
            "propagate": False,
        }
    }

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": formatters,
            "handlers": handlers,
            "loggers": loggers,
        }
    )
