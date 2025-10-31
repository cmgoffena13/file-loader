from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseConfig(BaseSettings):
    ENV_STATE: Optional[str] = None

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class GlobalConfig(BaseConfig):
    DATABASE_URL: str
    DIRECTORY_PATH: str
    ARCHIVE_PATH: str
    DUPLICATE_FILES_PATH: str
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


class DevConfig(GlobalConfig):
    DATABASE_URL: str = (
        "postgresql+psycopg://fileloader:fileloader@localhost:5432/fileloader"
    )
    DIRECTORY_PATH: str = "src/tests/test_data"
    ARCHIVE_PATH: str = "src/tests/archive_data"
    DUPLICATE_FILES_PATH: str = "src/tests/duplicate_files_data"
    LOG_LEVEL: str = "DEBUG"

    model_config = SettingsConfigDict(env_prefix="DEV_")


class TestConfig(GlobalConfig):
    DATABASE_URL: str = "sqlite:///:memory:"
    DIRECTORY_PATH: str = "src/tests/test_data"
    ARCHIVE_PATH: str = "src/tests/archive_data"
    DUPLICATE_FILES_PATH: str = "src/tests/duplicate_files_data"
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

    config_dict = {
        "sqlalchemy.url": db_config.DATABASE_URL,
        "sqlalchemy.echo": True if isinstance(config, DevConfig) else False,
        "sqlalchemy.future": True,
        "sqlalchemy.pool_size": 20,
        "sqlalchemy.max_overflow": 10,
        "sqlalchemy.pool_timeout": 30,
    }

    # Add database-specific connect args
    if db_config.DATABASE_URL.startswith("sqlite"):
        config_dict["sqlalchemy.connect_args"] = {"check_same_thread": False}

    return config_dict
