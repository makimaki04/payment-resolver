"""Structured stderr logging for the API and consumer."""

import json
import logging
import re
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any

from payment_resolver.modules.config import Settings

APP_LOGGER_NAME = "payment_resolver"
_EVENT_LIMIT = 128
_MESSAGE_LIMIT = 512
_ID_LIMIT = 128
_CODE_LIMIT = 64
_MAX_EXCEPTION_FRAMES = 8

_CONTEXT_FIELDS = (
    "payment_id",
    "event_id",
    "attempt",
    "status",
    "error_code",
    "http_status",
    "component",
    "exception_type",
)

_CONTROL_CHARACTERS = re.compile(r"[\x00-\x1f\x7f]+")
_URL_PATTERN = re.compile(r"\b[a-zA-Z][a-zA-Z0-9+.\-]*://[^\s]+")
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|access[_-]?token|token|secret|password|passwd|"
    r"authorization|idempotency[_-]?key|body_base64)\b\s*[:=]\s*\S+"
)
_LONG_OPAQUE_VALUE = re.compile(r"\b[A-Za-z0-9+/]{48,}={0,2}\b")

# These libraries may emit a record per request/message at INFO. WARNING keeps
# their errors and is applied both here and to broker creation.
_QUIET_LOGGERS = (
    "httpx",
    "httpcore",
    "faststream",
    "aio_pika",
    "aiormq",
    "sqlalchemy",
    "sqlalchemy.engine",
    "asyncpg",
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
)

ExceptionInfo = (
    tuple[type[BaseException], BaseException, TracebackType | None] | tuple[None, None, None]
)


def configure_logging(settings: Settings) -> None:
    level = settings.log_level
    formatter: logging.Formatter
    if settings.log_format == "json":
        formatter = JsonFormatter()
    else:
        formatter = TextFormatter()

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    for old_handler in list(root.handlers):
        root.removeHandler(old_handler)
    root.addHandler(handler)
    root.setLevel(level)

    _quiet_third_party_loggers(level)


def _quiet_third_party_loggers(level: int) -> None:
    minimum = max(level, logging.WARNING)
    names = set(_QUIET_LOGGERS)
    names.update(name for name in logging.root.manager.loggerDict if name.startswith("faststream."))
    # FastStream and Uvicorn install handlers before the composition roots run.
    # Preserve errors, but route them through the safe process-wide formatter.
    for name in names:
        library_logger = logging.getLogger(name)
        library_logger.handlers.clear()
        library_logger.propagate = True
        library_logger.setLevel(minimum)


def log_extra(
    event: str,
    *,
    payment_id: str | None = None,
    event_id: str | None = None,
    attempt: int | None = None,
    status: str | None = None,
    error_code: str | None = None,
    http_status: int | None = None,
    component: str | None = None,
    exception_type: str | None = None,
) -> dict[str, object]:
    """Collect event fields; formatters filter and sanitize their values."""
    extra: dict[str, object] = {"event": event}
    if payment_id is not None:
        extra["payment_id"] = payment_id
    if event_id is not None:
        extra["event_id"] = event_id
    if attempt is not None:
        extra["attempt"] = attempt
    if status is not None:
        extra["status"] = status
    if error_code is not None:
        extra["error_code"] = error_code
    if http_status is not None:
        extra["http_status"] = http_status
    if component is not None:
        extra["component"] = component
    if exception_type is not None:
        extra["exception_type"] = exception_type
    return extra


class TextFormatter(logging.Formatter):
    """Format one text line with a timestamp, event and bounded context."""

    def __init__(self) -> None:
        super().__init__(datefmt="%Y-%m-%dT%H:%M:%S%z")

    def format(self, record: logging.LogRecord) -> str:
        if _is_application_logger(record.name):
            message = _sanitize(record.getMessage(), _MESSAGE_LIMIT)
        else:
            message = "third_party_log"
        prefix = f"{self.formatTime(record, self.datefmt)} {record.levelname} {record.name}"
        parts = [f"{prefix} {message}"]
        context = _structured_context(record)
        if context:
            parts.append(" ".join(f"{key}={value}" for key, value in context.items()))
        if record.exc_info:
            parts.append(self.formatException(record.exc_info))
        return " ".join(parts)

    def formatException(self, ei: ExceptionInfo) -> str:
        return _safe_exception_text(ei)


class JsonFormatter(logging.Formatter):
    """Production format: one JSON object with an allowlisted field set."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "time": _utc_time(record),
            "level": record.levelname,
            "logger": record.name,
            "event": _event_name(record),
        }
        payload.update(_structured_context(record))
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _is_application_logger(name: str) -> bool:
    return name == APP_LOGGER_NAME or name.startswith(f"{APP_LOGGER_NAME}.")


def _event_name(record: logging.LogRecord) -> str:
    explicit = getattr(record, "event", None)
    if isinstance(explicit, str) and explicit.strip():
        return _sanitize(explicit, _EVENT_LIMIT)

    message = _sanitize(record.getMessage(), _EVENT_LIMIT)
    if _is_application_logger(record.name):
        return message or "log"
    # Third-party messages are not trusted to be free of URLs or payloads.
    return "third_party_log"


def _structured_context(record: logging.LogRecord) -> dict[str, str | int]:
    context: dict[str, str | int] = {"event": _event_name(record)}
    for field in _CONTEXT_FIELDS:
        value = getattr(record, field, None)
        if value is None:
            continue

        if field in ("attempt", "http_status"):
            if isinstance(value, int) and not isinstance(value, bool):
                if field == "attempt" and value >= 0:
                    context[field] = value
                elif field == "http_status" and 100 <= value <= 599:
                    context[field] = value
            continue

        if not isinstance(value, str):
            continue
        if field in ("payment_id", "event_id"):
            limit = _ID_LIMIT
        elif field == "error_code":
            limit = _CODE_LIMIT
        else:
            limit = _EVENT_LIMIT
        sanitized = _sanitize(value, limit)
        if sanitized:
            context[field] = sanitized

    derived_exception = _exception_name(record.exc_info)
    if derived_exception is not None:
        context["exception_type"] = derived_exception
    return context


def _exception_name(exc_info: ExceptionInfo | None) -> str | None:
    if not exc_info:
        return None
    error = exc_info[1]
    return type(error).__name__ if error is not None else None


def _safe_exception_text(exc_info: ExceptionInfo) -> str:
    error = exc_info[1]
    if error is None:
        return ""

    names = [type(error).__name__]
    cause = error.__cause__
    seen = {id(error)}
    while cause is not None and id(cause) not in seen and len(names) < _MAX_EXCEPTION_FRAMES:
        seen.add(id(cause))
        names.append(type(cause).__name__)
        cause = cause.__cause__

    frames = traceback.extract_tb(exc_info[2])[-_MAX_EXCEPTION_FRAMES:] if exc_info[2] else []
    locations = ", ".join(
        f"{Path(frame.filename).name}:{frame.lineno} in {frame.name}" for frame in frames
    )
    summary = " caused by ".join(names)
    return f"{summary} at {locations}" if locations else summary


def _utc_time(record: logging.LogRecord) -> str:
    value = datetime.fromtimestamp(record.created, tz=UTC)
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _sanitize(value: str, limit: int) -> str:
    text = _CONTROL_CHARACTERS.sub(" ", value)
    text = _URL_PATTERN.sub("[url]", text)
    text = _SECRET_ASSIGNMENT.sub(r"\1=[redacted]", text)
    text = _LONG_OPAQUE_VALUE.sub("[redacted]", text)
    return " ".join(text.split())[:limit]
