# Logging research and decisions

The API and consumer share one stdlib-only logging configuration: safe structured
business events, third-party per-request and per-message logs that are quiet by
default, and error diagnostics left on.

## Primary-source findings

### Python standard library

- `logging.getLogger(__name__)` is the intended module pattern; module loggers
  propagate records to ancestor/root handlers, so one root handler can serve the
  application while preserving logger names for diagnosis. `Logger.propagate =
  False` stops propagation at that logger.
  [Python logging reference](https://docs.python.org/3/library/logging.html),
  [Logging Cookbook: logging in multiple modules](https://docs.python.org/3/howto/logging-cookbook.html#using-logging-in-multiple-modules).
- `extra=` injects user-defined attributes into the `LogRecord`; a formatter can
  read them. This is the supported way to attach `event`, `payment_id`,
  `event_id`, `attempt`, `status`, `error_code`, `http_status`, and
  `component` without changing the message.
  [Python logging reference: Logger.debug / extra](https://docs.python.org/3/library/logging.html#logging.Logger.debug).
- `Formatter.formatException()` controls how exception information is rendered.
  Standard `Formatter` calls it and appends the result to the formatted record,
  so an override is the correct seam for removing exception messages from
  output while retaining the exception type and frame locations.
  [Python logging reference: Formatter.formatException](https://docs.python.org/3/library/logging.html#logging.Formatter.formatException).
- Root-handler configuration is preferred to attaching handlers to every module
  logger; multiple handlers on a logger and its ancestors can duplicate records.
  [Python logging reference: Logger.propagate](https://docs.python.org/3/library/logging.html#logging.Logger.propagate).
- `dictConfig()` is optional; direct handler/formatter construction is a
  supported configuration API and avoids default `disable_existing_loggers`
  behaviour. The implementation builds handlers directly.
  [logging.config reference](https://docs.python.org/3/library/logging.config.html).

### Uvicorn

- Uvicorn exposes `--log-config` (dictConfig/fileConfig), `--log-level`, and
  `--no-access-log`; access logging can be disabled without changing the error
  log level.
  [Uvicorn settings: Logging](https://github.com/encode/uvicorn/blob/master/docs/settings.md#logging).
- Uvicorn's default configuration installs `uvicorn`, `uvicorn.error`, and
  `uvicorn.access` loggers with their own handlers and `propagate=False`.
  Consequently, changing only the root logger does not reformat or silence
  Uvicorn's records. The API root must either keep Uvicorn's configuration and
  use `--no-access-log`, or pass a log config that routes Uvicorn loggers into
  the application's handler.
  [Uvicorn default LOGGING_CONFIG](https://github.com/encode/uvicorn/blob/master/uvicorn/config.py).

### FastStream 0.7.7

- FastStream's default module logger has its own `StreamHandler` and
  `propagate=False`; broker access loggers (`faststream.access.<broker>`) are
  created lazily with their own handler and INFO level by default. Root-level
  filtering alone cannot silence per-message/access INFO records.
  [faststream/_internal/logger/logging.py](https://github.com/ag2ai/faststream/blob/0.7.7/faststream/_internal/logger/logging.py),
  [faststream/rabbit/broker/logging.py](https://github.com/ag2ai/faststream/blob/0.7.7/faststream/rabbit/broker/logging.py).
- `RabbitBroker` accepts `logger` and `log_level`. Supplying `logger` replaces
  the default storage. FastStream treats `log_level` as the severity assigned to
  records that do not pass an explicit level, not as a threshold, so raising it
  to WARNING would mislabel ordinary service messages. The implementation keeps
  `log_level=INFO`, supplies a dedicated `third_party.faststream` logger, and
  sets that logger's threshold to WARNING; explicit ERROR records still pass.
  [FastStream logging guide](https://faststream.ag2.ai/latest/getting-started/observability/logging/),
  [RabbitBroker.__init__](https://github.com/ag2ai/faststream/blob/0.7.7/faststream/rabbit/broker/broker.py).
- FastStream confirms that its logger is a standard `logging.Logger`; suppressing
  it with `logger=None` would remove error diagnostics, so the configuration
  keeps a named logger and filters by threshold instead.
  [FastStream logging guide](https://faststream.ag2.ai/latest/getting-started/observability/logging/).

### OWASP logging guidance

- Never record access tokens, authentication passwords, database connection
  strings, encryption keys, or payment data directly. Remove/mask/sanitize them;
  descriptions and metadata can carry the same sensitive customer data.
  [OWASP Logging Cheat Sheet: Data to exclude](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html#data-to-exclude).
- Sanitize event data against log injection (CR, LF, delimiter/control
  characters) and validate data from other trust zones before logging.
  [OWASP Logging Cheat Sheet: Event collection](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html#event-collection).
- Record "when, where, what, result, reason" and include an interaction
  identifier so related events can be correlated. In this service the interaction
  identifiers are `payment_id` and `event_id`; `status`, `error_code`, and
  `http_status` carry result/reason.
  [OWASP Logging Cheat Sheet: Event attributes](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html#event-attributes).
- Log application errors and shutdown/startup events, and keep the configured
  level sufficient for operational/security needs; do not silently disable
  error logging.
  [OWASP Logging Cheat Sheet: Which events to log](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html#which-events-to-log).

## Configuration decision

`Settings` gains `APP_ENV` (`development` default), optional `LOG_LEVEL`
(explicit override; default INFO), and optional `LOG_FORMAT` (`text`/`json`;
derived default: text in development, JSON in production). Values are
case-normalized by typed Pydantic validators; invalid values fail startup rather
than silently falling back. `Settings.log_level` and `Settings.log_format`
resolve the effective values used by `configure_logging(settings)`.

`configure_logging` installs one `StreamHandler(sys.stderr)` on the root logger,
using either a text or JSON formatter. Reconfiguration replaces the root
handler, so calling it from both roots or twice in a process does not duplicate
records. Third-party loggers that emit per-request/per-message INFO are forced
to at least WARNING: `httpx`, `httpcore`, FastStream, `aio_pika`, `aiormq`,
`sqlalchemy.engine`, and `asyncpg`. Their WARNING/ERROR records remain enabled.
The broker itself receives a `third_party.faststream` logger with an effective
WARNING threshold and `log_level=INFO`, so FastStream per-delivery/access INFO
does not bypass the configuration while its errors remain diagnosable as
sanitized third-party records.

Existing FastStream/Uvicorn handlers are removed and propagation is restored
before applying the thresholds. This is required for ERROR records to reach
the safe process-wide formatter rather than a library's legacy plain-text handler.

## Format decision

The JSON object contains only:

- fixed identifiers: `time`, `level`, `logger`, `event`;
- business context: `payment_id`, `event_id`, `attempt`, `status`,
  `error_code`, `http_status`;
- optionally `component` (parent runtime: `inbox`/`outbox`) and
  `exception_type` (class name only).

All other `extra` keys are ignored by the formatter, so accidental
`description`, `metadata`, `webhook_url`, `api_key`, or `idempotency_key`
values cannot become structured output fields. Integers are range-checked;
strings are control-character/URL/secret-assignment sanitized and bounded.
The log message is never emitted as a separate JSON field. `event` is the
stable event name supplied by application code. For an application logger
without an explicit event, a sanitized bounded message supplies `event`. For a
third-party logger without an explicit event, `event` is the static
`third_party_log`, with `logger` and `exception_type` preserving diagnosis.

Exception records are never serialized from `str(exc)`, `exc_text`, or the
standard traceback. The formatter derives `exception_type` from `exc_info`.
Text mode prints only the exception class chain and a bounded list of
`file.py:line in function` frame locations. This avoids HTTPX URLs/query strings,
SQL parameter dumps (description/metadata/webhook URL), and arbitrary exception
messages reaching either format.

Dev text mode uses a readable timestamp/level/logger/message line followed by
the same sanitized `key=value` context. It intentionally does not emit raw
exception text.

## Event decision

Business INFO records are emitted only after the relevant commit:

- `payment.accepted` only when `create_or_get` inserted a new payment; an
  idempotent replay does not repeat it;
- `payment.financial_result` only when `finalize` committed;
- `payment.completed` only when the Inbox `completed` update committed.

Failure transitions log after their commit: `payment.retry_scheduled` at
WARNING with a safe `error_code`/`http_status`, `payment.recovered` when an
expired claim is transitioned, and `payment.processing_dead` at ERROR when the
Inbox reaches `dead`. Unexpected `processing_error` is logged at ERROR with
`exc_info`; the formatter exposes only `exception_type` (JSON) or the safe
type/frame summary (text). No polling success/no-work/duplicate-replay INFO is
emitted.

Transport/storage outages are expected states, not per-attempt business
failures. `PaymentReceiver` tracks storage, delivery transport, and diagnostic
publication outages separately, with one WARNING per outage and one INFO on
actual recovery. A successful source NACK does not prove that diagnostic
publication recovered. `OutboxPublisher` applies the same first-failure/recovery
policy to publication failures; runtime polling loops do so for storage.

Shutdown uses the same fields: `consumer.shutdown_timeout` is WARNING when the
grace period expires; `consumer.close_failed` is ERROR with `component` identifying
the broker, HTTP client or database engine that could not be closed.

## Test decision

`tests/unit/test_logging.py` exercises observable formatter/configuration
behaviour: derived defaults and validation, allowlisted JSON fields, rejection
of arbitrary sensitive extras, exception message/URL suppression in both
formats, quiet HTTPX/FastStream INFO with surviving errors, debug-level
application output, idempotent handler replacement, CR/LF and size sanitization,
and reconfiguration without duplicate output. Logging tests restore the root
and library logger state after capturing output.
