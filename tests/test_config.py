import secrets

import pytest
from pydantic import ValidationError

from app.config import Settings


@pytest.fixture
def clean_environment(monkeypatch):
    for field in Settings.model_fields:
        monkeypatch.delenv(field.upper(), raising=False)


def test_missing_configuration_fails_without_fallback_credentials(clean_environment):
    with pytest.raises(ValidationError) as error:
        Settings(_env_file=None)
    missing = {item["loc"][0] for item in error.value.errors()}
    assert {"api_key", "database_url", "rabbitmq_url"} <= missing


def test_environment_overrides_env_file(clean_environment, monkeypatch):
    key = secrets.token_urlsafe(24)
    monkeypatch.setenv("API_KEY", key)
    settings = Settings(_env_file=".env.example")
    assert settings.api_key == key
    assert settings.database_url.startswith("postgresql+asyncpg://")
    assert "${" not in settings.database_url
    assert "${" not in settings.rabbitmq_url


def test_invalid_environment_timeout_is_rejected(clean_environment, monkeypatch):
    monkeypatch.setenv("WEBHOOK_TIMEOUT_SECONDS", "0")
    with pytest.raises(ValidationError) as error:
        Settings(_env_file=".env.example")
    assert any(item["loc"] == ("webhook_timeout_seconds",) for item in error.value.errors())
