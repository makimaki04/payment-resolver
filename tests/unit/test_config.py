import pytest
from pydantic import ValidationError

from payment_resolver.modules.config import Settings


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    for key in Settings.model_fields:
        monkeypatch.delenv(key, raising=False)


def config_file(tmp_path):
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "API_KEY=test-only-key\n"
        "DATABASE_URL=postgresql+asyncpg://u:p@localhost/test\n"
        "RABBITMQ_URL=amqp://u:p@localhost/test\n"
        'WEBHOOK_ALLOWED_ORIGINS=["HTTP://Receiver:80"]\n'
        "RETRY_BASE_SECONDS=7\n"
        "POSTGRES_USER=bootstrap\nRABBITMQ_DEFAULT_USER=bootstrap\n",
        encoding="utf-8",
    )
    return dotenv


def test_explicit_then_environment_then_dotenv_priority(tmp_path, monkeypatch):
    dotenv = config_file(tmp_path)
    assert Settings(_env_file=dotenv).RETRY_BASE_SECONDS == 7
    monkeypatch.setenv("RETRY_BASE_SECONDS", "9")
    assert Settings(_env_file=dotenv).RETRY_BASE_SECONDS == 9
    explicit = Settings(_env_file=dotenv, RETRY_BASE_SECONDS=11)
    assert explicit.RETRY_BASE_SECONDS == 11
    assert explicit.WEBHOOK_ALLOWED_ORIGINS == ["http://receiver/"]


def test_all_required_settings_can_come_from_environment_without_dotenv(monkeypatch):
    for key, value in {
        "API_KEY": "environment-only-key",
        "DATABASE_URL": "postgresql+asyncpg://localhost/test",
        "RABBITMQ_URL": "amqp://localhost/test",
        "WEBHOOK_ALLOWED_ORIGINS": '["http://receiver"]',
    }.items():
        monkeypatch.setenv(key, value)
    settings = Settings(_env_file=None)
    assert settings.API_KEY.get_secret_value() == "environment-only-key"
    assert settings.RETRY_BASE_SECONDS == 5


def test_missing_key_refuses_startup_without_exposing_other_inputs(tmp_path):
    marker = "private-password-never-in-error"
    with pytest.raises(ValidationError) as error:
        Settings(
            _env_file=None,
            DATABASE_URL=f"postgresql+asyncpg://u:{marker}@host/test",
            RABBITMQ_URL="amqp://localhost",
            WEBHOOK_ALLOWED_ORIGINS=["http://receiver"],
        )
    assert "API_KEY" in str(error.value)
    assert marker not in str(error.value)


@pytest.mark.parametrize(
    "overrides",
    [
        {"API_KEY": ""},
        {"API_KEY": "ключ"},
        {"WEBHOOK_ALLOWED_ORIGINS": []},
        {"WEBHOOK_ALLOWED_ORIGINS": ["http://receiver/path"]},
        {"WEBHOOK_ALLOWED_ORIGINS": ["http://receiver?query"]},
        {"WEBHOOK_ALLOWED_ORIGINS": ["http://user@receiver"]},
        {"RETRY_BASE_SECONDS": 0},
        {"RETRY_BASE_SECONDS": float("inf")},
    ],
)
def test_invalid_settings_fail_before_resource_creation(tmp_path, overrides):
    with pytest.raises(ValidationError):
        Settings(_env_file=config_file(tmp_path), **overrides)
