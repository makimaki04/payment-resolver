import io
import json
import logging
from contextlib import redirect_stderr

import pytest
from pydantic import ValidationError

from payment_resolver.modules.config import Settings
from payment_resolver.modules.logging import configure_logging, log_extra

REQUIRED = {
    "API_KEY": "test-only-key",
    "DATABASE_URL": "postgresql+asyncpg://u:p@localhost/test",
    "RABBITMQ_URL": "amqp://u:p@localhost/test",
    "WEBHOOK_ALLOWED_ORIGINS": ["http://receiver"],
}
QUIET_LOGGERS = (
    "httpx",
    "httpcore",
    "faststream",
    "aio_pika",
    "aiormq",
    "sqlalchemy",
    "sqlalchemy.engine",
    "asyncpg",
)
SERVER_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    for key in Settings.model_fields:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def logging_state():
    root = logging.getLogger()
    handlers = root.handlers[:]
    level = root.level
    names = set(QUIET_LOGGERS) | set(SERVER_LOGGERS) | {"payment_resolver.test"}
    names.update(name for name in logging.root.manager.loggerDict if name.startswith("faststream."))
    loggers = {name: logging.getLogger(name) for name in names}
    saved = {
        name: (logger.level, logger.handlers[:], logger.propagate)
        for name, logger in loggers.items()
    }
    yield
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        if handler not in handlers:
            handler.close()
    root.handlers[:] = handlers
    root.setLevel(level)
    for name, (saved_level, saved_handlers, saved_propagation) in saved.items():
        logger = logging.getLogger(name)
        logger.setLevel(saved_level)
        logger.handlers[:] = saved_handlers
        logger.propagate = saved_propagation


def make_settings(**overrides):
    return Settings(_env_file=None, **REQUIRED, **overrides)


def configure_and_capture(**overrides):
    stream = io.StringIO()
    with redirect_stderr(stream):
        configure_logging(make_settings(**overrides))
    return stream


def test_development_defaults_to_text_and_info():
    settings = make_settings()
    assert settings.APP_ENV == "development"
    assert settings.log_level == logging.INFO
    assert settings.log_format == "text"


def test_production_derives_json_and_explicit_values_override():
    production = make_settings(APP_ENV="production")
    assert production.log_format == "json"
    assert production.log_level == logging.INFO

    overridden = make_settings(APP_ENV="PRODUCTION", LOG_FORMAT="TEXT", LOG_LEVEL="debug")
    assert overridden.log_format == "text"
    assert overridden.log_level == logging.DEBUG


def test_environment_values_are_normalized(monkeypatch):
    monkeypatch.setenv("APP_ENV", "PRODUCTION")
    monkeypatch.setenv("LOG_FORMAT", "JSON")
    monkeypatch.setenv("LOG_LEVEL", "warning")
    settings = Settings(_env_file=None, **REQUIRED)
    assert settings.APP_ENV == "production"
    assert settings.LOG_FORMAT == "json"
    assert settings.LOG_LEVEL == "WARNING"


@pytest.mark.parametrize(
    "overrides",
    [
        {"APP_ENV": "staging"},
        {"LOG_LEVEL": "verbose"},
        {"LOG_FORMAT": "xml"},
    ],
)
def test_invalid_logging_settings_are_rejected(overrides):
    with pytest.raises(ValidationError):
        make_settings(**overrides)


def test_production_json_contains_only_safe_allowlisted_context(logging_state):
    stream = configure_and_capture(APP_ENV="production")
    logger = logging.getLogger("payment_resolver.test")

    logger.info(
        "Value that must not be serialized",
        extra=log_extra(
            "payment.financial_result",
            payment_id="payment-id",
            event_id="event-id",
            attempt=2,
            status="succeeded",
            error_code="gateway_error",
            http_status=200,
            component="inbox",
        )
        | {
            "description": "SECRET-DESCRIPTION",
            "metadata": {"key": "SECRET-METADATA"},
            "api_key": "SECRET-API-KEY",
            "webhook_url": "https://user:pass@example.test/hook?token=SECRET-QUERY",
        },
    )

    line = stream.getvalue().strip()
    payload = json.loads(line)
    assert payload["event"] == "payment.financial_result"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "payment_resolver.test"
    assert payload["time"].endswith("Z")
    assert payload["payment_id"] == "payment-id"
    assert payload["event_id"] == "event-id"
    assert payload["attempt"] == 2
    assert payload["status"] == "succeeded"
    assert payload["error_code"] == "gateway_error"
    assert payload["http_status"] == 200
    assert payload["component"] == "inbox"
    assert set(payload) == {
        "time",
        "level",
        "logger",
        "event",
        "payment_id",
        "event_id",
        "attempt",
        "status",
        "error_code",
        "http_status",
        "component",
    }
    for secret in (
        "Value that must not be serialized",
        "SECRET-DESCRIPTION",
        "SECRET-METADATA",
        "SECRET-API-KEY",
        "SECRET-QUERY",
    ):
        assert secret not in line


def test_text_format_includes_safe_context(logging_state):
    stream = configure_and_capture()
    logging.getLogger("payment_resolver.test").info(
        "Payment accepted",
        extra=log_extra("payment.accepted", payment_id="payment-id"),
    )

    output = stream.getvalue()
    assert "INFO" in output
    assert "event=payment.accepted" in output
    assert "payment_id=payment-id" in output


def test_exception_message_is_not_exposed_in_json(logging_state):
    stream = configure_and_capture(APP_ENV="production")
    secret = "SECRET-EXCEPTION-VALUE"
    try:
        raise ValueError(f"{secret} at https://user:pass@example.test/hook?token={secret}")
    except ValueError as exc:
        logging.getLogger("payment_resolver.test").error(
            "Unexpected failure",
            extra=log_extra(
                "payment.processing_error",
                payment_id="payment-id",
                error_code="processing_error",
            ),
            exc_info=exc,
        )

    line = stream.getvalue().strip()
    payload = json.loads(line)
    assert payload["event"] == "payment.processing_error"
    assert payload["exception_type"] == "ValueError"
    assert secret not in line
    assert "https://" not in line
    assert "Traceback" not in line


def test_text_exception_report_has_type_and_frames_without_message(logging_state):
    stream = configure_and_capture()
    secret = "SECRET-TEXT-EXCEPTION"
    try:
        raise RuntimeError(f"{secret} https://user:pass@example.test/path?q={secret}")
    except RuntimeError as exc:
        logging.getLogger("payment_resolver.test").error(
            "Unexpected failure",
            extra=log_extra("payment.processing_error"),
            exc_info=exc,
        )

    output = stream.getvalue()
    assert "RuntimeError" in output
    assert ".py:" in output
    assert secret not in output
    assert "https://" not in output


def test_third_party_messages_are_not_promoted_to_events(logging_state):
    stream = configure_and_capture(APP_ENV="production")
    logging.getLogger("third.party").error(
        "connect amqp://user:password@broker:5672/vhost failed api_key=SECRET-THIRD-PARTY"
    )

    line = stream.getvalue().strip()
    payload = json.loads(line)
    assert payload["event"] == "third_party_log"
    assert payload["logger"] == "third.party"
    assert "SECRET-THIRD-PARTY" not in line
    assert "password" not in line


def test_httpx_and_faststream_info_is_quiet_while_errors_survive(logging_state):
    legacy = io.StringIO()
    faststream_logger = logging.getLogger("faststream")
    faststream_logger.addHandler(logging.StreamHandler(legacy))
    faststream_logger.propagate = False
    stream = configure_and_capture(APP_ENV="production", LOG_LEVEL="DEBUG")
    names = ("httpx", "faststream", "sqlalchemy.engine")
    for name in names:
        logger = logging.getLogger(name)
        logger.info("UNWANTED per-request message")
        logger.error("Operational failure")
    output = stream.getvalue()
    records = [json.loads(line) for line in output.splitlines()]
    assert "UNWANTED" not in output
    assert {record["logger"] for record in records} == set(names)
    assert all(record["level"] == "ERROR" for record in records)
    assert legacy.getvalue() == ""


def test_application_debug_level_is_honoured(logging_state):
    stream = configure_and_capture(LOG_LEVEL="DEBUG")
    logging.getLogger("payment_resolver.test").debug("diagnostic", extra=log_extra("payment.debug"))
    assert "payment.debug" in stream.getvalue()


def test_reconfiguration_does_not_duplicate_output(logging_state):
    stream = io.StringIO()
    with redirect_stderr(stream):
        configure_logging(make_settings(APP_ENV="production"))
        configure_logging(make_settings(APP_ENV="development"))
        logging.getLogger("payment_resolver.test").info(
            "Payment accepted",
            extra=log_extra("payment.once"),
        )
    lines = stream.getvalue().splitlines()
    assert len(lines) == 1
    assert "event=payment.once" in lines[0]


def test_json_event_is_single_line_and_bounded(logging_state):
    stream = configure_and_capture(APP_ENV="production")
    logging.getLogger("payment_resolver.test").info(
        "static message",
        extra=log_extra("bad\nevent " + "x" * 300),
    )
    line = stream.getvalue().strip()
    assert "\n" not in line
    payload = json.loads(line)
    assert len(payload["event"]) <= 128
    assert "\n" not in payload["event"]


def test_uvicorn_errors_use_safe_formatter_without_legacy_output(logging_state):
    legacy = io.StringIO()
    logger = logging.getLogger("uvicorn.error")
    logger.addHandler(logging.StreamHandler(legacy))
    logger.propagate = False
    stream = configure_and_capture(APP_ENV="production")
    logger.info("UNWANTED access-like line")
    try:
        raise RuntimeError("SECRET-URL https://user:password@example.test/?token=secret")
    except RuntimeError:
        logger.exception("UNSAFE exception text")
    assert legacy.getvalue() == ""
    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert len(records) == 1
    assert records[0]["logger"] == "uvicorn.error"
    assert records[0]["exception_type"] == "RuntimeError"
    assert "SECRET-URL" not in stream.getvalue()
    assert "password" not in stream.getvalue()


def test_cyclic_exception_cause_does_not_break_text_logging(logging_state):
    stream = configure_and_capture()
    error = RuntimeError("SECRET cycle message")
    error.__cause__ = error
    logging.getLogger("payment_resolver.test").error(
        "Processing failed",
        extra=log_extra("payment.processing_error"),
        exc_info=(RuntimeError, error, None),
    )
    output = stream.getvalue()
    assert output.count("RuntimeError") <= 2
    assert "RuntimeError" in output
    assert "event=payment.processing_error" in output
    assert "SECRET cycle message" not in output
