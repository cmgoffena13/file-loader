import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pythonnet import load

logger = logging.getLogger(__name__)
SUPPORTED_DATABASE_DRIVERS = {
    "postgresql": "postgresql",
    "mysql": "mysql",
    "mssql": "mssql",
    "sqlite": "sqlite",
}


class BaseConfig(BaseSettings):
    ENV_STATE: Optional[str] = None

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class GlobalConfig(BaseConfig):
    DATABASE_URL: str
    DIRECTORY_PATH: Path
    ARCHIVE_PATH: Path
    DUPLICATE_FILES_PATH: Path

    BATCH_SIZE: int = 10000
    LOG_LEVEL: str = "INFO"

    @property
    def DRIVERNAME(self) -> str:
        for drivername, dialect in SUPPORTED_DATABASE_DRIVERS.items():
            if drivername in self.DATABASE_URL.lower():
                return dialect.lower()
        raise ValueError(
            f"Unsupported database driver in DATABASE_URL: {self.DATABASE_URL}"
        )

    # Email notification settings
    SMTP_HOST: Optional[str] = None
    SMTP_PORT: Optional[int] = 587
    SMTP_USER: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    FROM_EMAIL: Optional[str] = None
    DATA_TEAM_EMAIL: Optional[str] = None  # Always CC'd on failure notifications
    # Slack notification settings
    SLACK_WEBHOOK_URL: Optional[str] = None

    @field_validator(
        "DIRECTORY_PATH", "ARCHIVE_PATH", "DUPLICATE_FILES_PATH", mode="before"
    )
    @classmethod
    def convert_path(cls, v):
        if isinstance(v, Path):
            return v
        return Path(v) if v else v

    OTEL_PYTHON_LOG_CORRELATION: Optional[bool] = None
    OPEN_TELEMETRY_LOG_ENDPOINT: Optional[str] = None
    OPEN_TELEMETRY_TRACE_ENDPOINT: Optional[str] = None
    OPEN_TELEMETRY_AUTHORIZATION_TOKEN: Optional[str] = None

    SQL_SERVER_SQLBULKCOPY_FLAG: bool = False


class DevConfig(GlobalConfig):
    DIRECTORY_PATH: Path = Path("src/tests/test_data")
    ARCHIVE_PATH: Path = Path("src/tests/archive_data")
    DUPLICATE_FILES_PATH: Path = Path("src/tests/duplicate_files_data")
    LOG_LEVEL: str = "DEBUG"
    OTEL_PYTHON_LOG_CORRELATION: bool = False

    model_config = SettingsConfigDict(env_prefix="DEV_")


class TestConfig(GlobalConfig):
    DATABASE_URL: str = "sqlite:///:memory:"
    DIRECTORY_PATH: Path = Path("src/tests/test_data")
    ARCHIVE_PATH: Path = Path("src/tests/archive_data")
    DUPLICATE_FILES_PATH: Path = Path("src/tests/duplicate_files_data")
    BATCH_SIZE: int = 100
    OTEL_PYTHON_LOG_CORRELATION: bool = False

    model_config = SettingsConfigDict(env_prefix="TEST_")


class ProdConfig(GlobalConfig):
    LOG_LEVEL: Optional[str] = "WARNING"
    OTEL_PYTHON_LOG_CORRELATION: bool = True

    model_config = SettingsConfigDict(env_prefix="PROD_")


@lru_cache()
def get_config(env_state: str):
    if not env_state:
        raise ValueError("ENV_STATE is not set. Possible values are: DEV, TEST, PROD")
    env_state = env_state.lower()
    configs = {"dev": DevConfig, "prod": ProdConfig, "test": TestConfig}
    return configs[env_state]()


config = get_config(BaseConfig().ENV_STATE)


def _initialize_dotnet_runtime():
    """Initialize .NET runtime once at startup if using SQL Server and bulk copy is enabled."""
    if config.DRIVERNAME == "mssql" and config.SQL_SERVER_SQLBULKCOPY_FLAG:
        try:
            runtime = os.environ.get("PYTHONNET_RUNTIME", "coreclr")
            load(runtime)
            logger.debug(f"Initialized .NET runtime: {runtime}")
        except Exception as e:
            # Log but don't fail - sqlserver.py will handle the error when actually used
            logger.warning(f"Failed to initialize .NET runtime at startup: {e}")


# Initialize .NET runtime at module load if needed
_initialize_dotnet_runtime()


def get_database_config():
    env_state = BaseConfig().ENV_STATE
    db_config = get_config(env_state)

    is_sqlite = db_config.DATABASE_URL.startswith("sqlite")

    config_dict = {
        "sqlalchemy.url": db_config.DATABASE_URL,
        "sqlalchemy.echo": False,
        "sqlalchemy.future": True,
    }

    if is_sqlite:
        config_dict["sqlalchemy.connect_args"] = {"check_same_thread": False}
        config_dict["sqlalchemy.pool_size"] = 1
    else:
        config_dict["sqlalchemy.pool_size"] = 20
        config_dict["sqlalchemy.max_overflow"] = 10
        config_dict["sqlalchemy.pool_timeout"] = 30

    return config_dict
