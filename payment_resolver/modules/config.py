import logging
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from payment_resolver.core.payments.urls import normalize_origin

AppEnvironment = Literal["development", "production"]
LogFormat = Literal["text", "json"]
LogLevelName = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
        hide_input_in_errors=True,
    )

    API_KEY: SecretStr
    DATABASE_URL: str = Field(min_length=1, repr=False)
    RABBITMQ_URL: str = Field(min_length=1, repr=False)
    WEBHOOK_ALLOWED_ORIGINS: list[str] = Field(min_length=1)
    RETRY_BASE_SECONDS: float = Field(default=5, gt=0, allow_inf_nan=False)
    APP_ENV: AppEnvironment = "development"
    LOG_LEVEL: LogLevelName | None = None
    LOG_FORMAT: LogFormat | None = None

    @field_validator("API_KEY")
    @classmethod
    def validate_key(cls, value: SecretStr) -> SecretStr:
        key = value.get_secret_value()
        if not key or not key.isascii():
            raise ValueError("API_KEY must be nonempty ASCII")
        return value

    @field_validator("WEBHOOK_ALLOWED_ORIGINS")
    @classmethod
    def validate_origins(cls, value: list[str]) -> list[str]:
        return [normalize_origin(origin) for origin in value]

    @field_validator("APP_ENV", "LOG_FORMAT", mode="before")
    @classmethod
    def normalize_choice(cls, value: object) -> object:
        return value.strip().lower() if isinstance(value, str) else value

    @field_validator("LOG_LEVEL", mode="before")
    @classmethod
    def normalize_level(cls, value: object) -> object:
        return value.strip().upper() if isinstance(value, str) else value

    @property
    def log_level(self) -> int:
        return logging.getLevelNamesMapping()[self.LOG_LEVEL or "INFO"]

    @property
    def log_format(self) -> LogFormat:
        if self.LOG_FORMAT is not None:
            return self.LOG_FORMAT
        return "json" if self.APP_ENV == "production" else "text"
