from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    # Email notification settings
    SMTP_HOST: Optional[str] = None
    SMTP_PORT: Optional[int] = 587
    SMTP_USER: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    FROM_EMAIL: Optional[str] = None
    DATA_TEAM_EMAIL: Optional[str] = None  # Always CC'd on failure notifications
    # Slack notification settings
    SLACK_WEBHOOK_URL: Optional[str] = (
        None  # Slack webhook URL for internal processing errors
    )

    @field_validator(
        "DIRECTORY_PATH", "ARCHIVE_PATH", "DUPLICATE_FILES_PATH", mode="before"
    )
    @classmethod
    def convert_path(cls, v):
        if isinstance(v, Path):
            return v
        return Path(v) if v else v


class DevConfig(GlobalConfig):
    DATABASE_URL: str = (
        "postgresql+psycopg://fileloader:fileloader@localhost:5432/fileloader"
    )
    DIRECTORY_PATH: Path = Path("src/tests/test_data")
    ARCHIVE_PATH: Path = Path("src/tests/archive_data")
    DUPLICATE_FILES_PATH: Path = Path("src/tests/duplicate_files_data")
    LOG_LEVEL: str = "DEBUG"

    model_config = SettingsConfigDict(env_prefix="DEV_")


class TestConfig(GlobalConfig):
    DATABASE_URL: str = "sqlite:///:memory:"
    DIRECTORY_PATH: Path = Path("src/tests/test_data")
    ARCHIVE_PATH: Path = Path("src/tests/archive_data")
    DUPLICATE_FILES_PATH: Path = Path("src/tests/duplicate_files_data")
    BATCH_SIZE: int = 100

    model_config = SettingsConfigDict(env_prefix="TEST_")


class ProdConfig(GlobalConfig):
    LOG_LEVEL: Optional[str] = "WARNING"
    model_config = SettingsConfigDict(env_prefix="PROD_")


@lru_cache()
def get_config(env_state: str):
    configs = {"dev": DevConfig, "prod": ProdConfig, "test": TestConfig}
    return configs[env_state]()


config = get_config(BaseConfig().ENV_STATE)


def get_database_config():
    env_state = BaseConfig().ENV_STATE
    db_config = get_config(env_state)

    is_sqlite = db_config.DATABASE_URL.startswith("sqlite")

    config_dict = {
        "sqlalchemy.url": db_config.DATABASE_URL,
        "sqlalchemy.echo": True if isinstance(config, DevConfig) else False,
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
