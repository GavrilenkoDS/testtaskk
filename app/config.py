from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    api_key: str = Field(min_length=16)
    database_url: str = Field(min_length=1)
    rabbitmq_url: str = Field(min_length=1)
    retry_base_seconds: float = Field(gt=0)
    outbox_poll_seconds: float = Field(gt=0)
    webhook_timeout_seconds: float = Field(gt=0)
    publish_timeout_seconds: float = Field(gt=0)
    database_requeue_delay_seconds: float = Field(gt=0)


class SmokeSettings(BaseSettings):
    model_config = Settings.model_config

    api_key: str = Field(min_length=16)
    smoke_api_url: str = Field(min_length=1)
    smoke_webhook_url: str = Field(min_length=1)
    smoke_rabbit_url: str = Field(min_length=1)
    smoke_callback_url: str = Field(min_length=1)
    rabbitmq_default_user: str = Field(min_length=1)
    rabbitmq_default_pass: str = Field(min_length=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
