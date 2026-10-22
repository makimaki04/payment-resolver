"""Infrastructure helpers for the integration suite.

The suite runs against real PostgreSQL and RabbitMQ. Nothing here mocks a
database, a broker or an HTTP webhook receiver; the helpers only provision
isolated resources, read durable state and build deterministic adapters where
the specification explicitly asks for a controlled boundary (gateway outcome,
retry jitter).
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlsplit

import asyncpg
import httpx
from sqlalchemy.engine import make_url

from payment_resolver.core.payments.dtos import CreatePaymentRequestDTO
from payment_resolver.core.payments.enums import Currency, PaymentStatus

REQUIRED_ENV = (
    "TEST_POSTGRES_ADMIN_DSN",
    "TEST_RABBITMQ_MANAGEMENT_URL",
    "TEST_RABBITMQ_USERNAME",
    "TEST_RABBITMQ_PASSWORD",
)

BOOTSTRAP_DEADLINE_SECONDS = 30.0
API_KEY = "integration-test-api-key"
RETRY_BASE_SECONDS = 1.0


class ProvisioningError(RuntimeError):
    """Raised when the isolated test infrastructure cannot be provisioned."""


@dataclass(frozen=True)
class Infrastructure:
    database_name: str
    vhost: str
    admin_dsn: str
    database_url: str
    database_dsn: str
    management_url: str
    rabbitmq_url: str
    username: str
    password: str
    alembic_env: dict[str, str]


def _run_async(factory: Callable[[], Awaitable[Any]]) -> Any:
    """Run provisioning coroutines without assuming anything about the pytest loop."""
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(asyncio.run, factory()).result()


def missing_environment() -> list[str]:
    import os

    return [name for name in REQUIRED_ENV if not os.environ.get(name)]


def _asyncpg_dsn(raw: str) -> str:
    """asyncpg accepts plain postgresql:// DSNs, not the SQLAlchemy +driver prefix."""
    url = make_url(raw)
    driver = url.drivername.split("+", 1)[0]
    return url.set(drivername=driver).render_as_string(hide_password=False)


def _url_with_database(raw: str, database: str | None, driver: str) -> str:
    url = make_url(raw)
    replaced = url.set(drivername=driver)
    if database is not None:
        replaced = replaced.set(database=database)
    return replaced.render_as_string(hide_password=False)


def _amqp_url(management_url: str, username: str, password: str, vhost: str) -> str:
    parts = urlsplit(management_url)
    if parts.scheme not in ("http", "https"):
        raise ProvisioningError(
            f"TEST_RABBITMQ_MANAGEMENT_URL must be an http(s) URL, got {management_url!r}"
        )
    if parts.hostname is None:
        raise ProvisioningError(f"TEST_RABBITMQ_MANAGEMENT_URL has no hostname: {management_url!r}")
    import os

    amqp_port = int(os.environ.get("TEST_RABBITMQ_AMQP_PORT", "5672"))
    scheme = "amqps" if parts.scheme == "https" else "amqp"
    return (
        f"{scheme}://{quote(username, safe='')}:{quote(password, safe='')}"
        f"@{parts.hostname}:{amqp_port}/{quote(vhost, safe='')}"
    )


def _management_auth() -> tuple[str, str]:
    import os

    return os.environ["TEST_RABBITMQ_USERNAME"], os.environ["TEST_RABBITMQ_PASSWORD"]


def wait_for_health(management_url: str, admin_dsn: str) -> None:
    """Wait for PostgreSQL and the RabbitMQ management API with a 30s deadline.

    An open AMQP port is not enough: readiness requires ``SELECT 1`` and HTTP
    200 from the management API.
    """
    deadline = time.monotonic() + BOOTSTRAP_DEADLINE_SECONDS
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            _run_async(lambda: _check_postgres(admin_dsn))
            postgres_ready = True
        except Exception as exc:  # pragma: no cover - only on broken bootstrap
            postgres_ready = False
            last_error = f"PostgreSQL: {exc!r}"
        try:
            with httpx.Client(auth=_management_auth(), timeout=2) as client:
                response = client.get(f"{management_url.rstrip('/')}/api/overview")
            rabbit_ready = response.status_code == 200
            if not rabbit_ready:
                last_error = f"RabbitMQ management: HTTP {response.status_code}"
        except httpx.HTTPError as exc:  # pragma: no cover - only on broken bootstrap
            rabbit_ready = False
            last_error = f"RabbitMQ management: {exc!r}"
        if postgres_ready and rabbit_ready:
            return
        time.sleep(0.5)
    raise ProvisioningError(
        "PostgreSQL/RabbitMQ did not become healthy within "
        f"{BOOTSTRAP_DEADLINE_SECONDS:.0f}s ({last_error})"
    )


async def _check_postgres(admin_dsn: str) -> None:
    connection = await asyncpg.connect(admin_dsn, timeout=3)
    try:
        await connection.fetchval("SELECT 1")
    finally:
        await connection.close()


def _verify_rabbitmq_administrator(management_url: str) -> None:
    base = management_url.rstrip("/")
    with httpx.Client(auth=_management_auth(), timeout=5) as client:
        whoami = client.get(f"{base}/api/whoami")
        if whoami.status_code in (401, 403):
            raise ProvisioningError(
                "RabbitMQ management access denied for TEST_RABBITMQ_USERNAME "
                f"(HTTP {whoami.status_code}); vhost provisioning needs management "
                "access and the administrator tag"
            )
        whoami.raise_for_status()
        tags = whoami.json().get("tags", [])
        if "administrator" not in tags:
            raise ProvisioningError(
                "TEST_RABBITMQ_USERNAME lacks the RabbitMQ administrator tag "
                f"(tags={tags!r}); cannot create an isolated vhost"
            )


def _request_management(method: str, url: str, **kwargs: Any) -> httpx.Response:
    with httpx.Client(auth=_management_auth(), timeout=10) as client:
        return client.request(method, url, **kwargs)


def _create_database(admin_dsn: str, name: str) -> None:
    async def create() -> None:
        connection = await asyncpg.connect(admin_dsn)
        try:
            try:
                await connection.execute(f'CREATE DATABASE "{name}"')
            except asyncpg.InsufficientPrivilegeError as exc:
                raise ProvisioningError(
                    "TEST_POSTGRES_ADMIN_DSN lacks the CREATE DATABASE privilege; "
                    "integration tests never alter existing roles"
                ) from exc
        finally:
            await connection.close()

    _run_async(create)


def _drop_database(admin_dsn: str, name: str) -> None:
    async def drop() -> None:
        connection = await asyncpg.connect(admin_dsn)
        try:
            await connection.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = $1 AND pid <> pg_backend_pid()",
                name,
            )
            await connection.execute(f'DROP DATABASE IF EXISTS "{name}"')
        finally:
            await connection.close()

    _run_async(drop)


def provision(receiver_url: str) -> Infrastructure:
    """Provision an isolated database and vhost, then migrate the database to head."""
    import os
    import subprocess
    import sys
    from pathlib import Path

    if missing_environment():
        raise ProvisioningError(
            "Integration tests require explicit environment variables: "
            + ", ".join(missing_environment())
            + ". They are not skipped: configure TEST_POSTGRES_ADMIN_DSN, "
            "TEST_RABBITMQ_MANAGEMENT_URL, TEST_RABBITMQ_USERNAME and "
            "TEST_RABBITMQ_PASSWORD."
        )

    admin_dsn = _asyncpg_dsn(os.environ["TEST_POSTGRES_ADMIN_DSN"])
    management_url = os.environ["TEST_RABBITMQ_MANAGEMENT_URL"].rstrip("/")
    username = os.environ["TEST_RABBITMQ_USERNAME"]
    password = os.environ["TEST_RABBITMQ_PASSWORD"]

    try:
        wait_for_health(management_url, admin_dsn)
        _verify_rabbitmq_administrator(management_url)
    except httpx.HTTPError as exc:
        raise ProvisioningError(f"RabbitMQ management request failed: {exc!r}") from exc

    database_name = f"test_{uuid.uuid4().hex}"
    vhost = f"test_{uuid.uuid4().hex}"
    database_url = _url_with_database(admin_dsn, database_name, "postgresql+asyncpg")
    database_dsn = _url_with_database(admin_dsn, database_name, "postgresql")
    rabbitmq_url = _amqp_url(management_url, username, password, vhost)

    _create_database(admin_dsn, database_name)
    try:
        response = _request_management(
            "PUT", f"{management_url}/api/vhosts/{quote(vhost, safe='')}", json={}
        )
        if response.status_code not in (201, 204):
            raise ProvisioningError(
                f"Could not create RabbitMQ vhost {vhost!r}: "
                f"HTTP {response.status_code} {response.text}"
            )
        response = _request_management(
            "PUT",
            f"{management_url}/api/permissions/{quote(vhost, safe='')}/{quote(username, safe='')}",
            json={"configure": ".*", "write": ".*", "read": ".*"},
        )
        if response.status_code not in (201, 204):
            raise ProvisioningError(
                f"Could not grant permissions on vhost {vhost!r}: "
                f"HTTP {response.status_code} {response.text}"
            )

        alembic_env = os.environ.copy()
        alembic_env.update(
            DATABASE_URL=database_url,
            RABBITMQ_URL=rabbitmq_url,
            API_KEY=API_KEY,
            WEBHOOK_ALLOWED_ORIGINS=json.dumps([receiver_url]),
            RETRY_BASE_SECONDS="0.2",
        )
        root = Path(__file__).resolve().parents[2]
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "alembic",
                "-c",
                "alembic.ini",
                "-x",
                f"db_url={database_url}",
                "upgrade",
                "head",
            ],
            cwd=root,
            env=alembic_env,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise ProvisioningError(
                "Alembic migration failed for the isolated test database.\n"
                f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
            )
    except BaseException:
        try:
            _drop_database(admin_dsn, database_name)
        except Exception:
            pass
        try:
            _request_management(
                "DELETE",
                f"{management_url}/api/vhosts/{quote(vhost, safe='')}",
            )
        except Exception:
            pass
        raise

    return Infrastructure(
        database_name=database_name,
        vhost=vhost,
        admin_dsn=admin_dsn,
        database_url=database_url,
        database_dsn=database_dsn,
        management_url=management_url,
        rabbitmq_url=rabbitmq_url,
        username=username,
        password=password,
        alembic_env=alembic_env,
    )


def cleanup(infra: Infrastructure) -> None:
    """Remove only the resources owned by this test session."""
    try:
        _drop_database(infra.admin_dsn, infra.database_name)
    finally:
        try:
            _request_management(
                "DELETE",
                f"{infra.management_url}/api/vhosts/{quote(infra.vhost, safe='')}",
            )
        except httpx.HTTPError:
            pass


async def wait_until(
    probe: Callable[[], Awaitable[bool]],
    *,
    deadline: float = 30.0,
    interval: float = 0.1,
    description: str = "condition",
) -> None:
    limit = time.monotonic() + deadline
    while time.monotonic() < limit:
        if await probe():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"Timed out after {deadline:.1f}s waiting for {description}")


async def queue_stats(infra: Infrastructure, queue: str) -> dict[str, Any]:
    base = f"{infra.management_url}/api/queues/{quote(infra.vhost, safe='')}"
    async with httpx.AsyncClient(auth=(infra.username, infra.password), timeout=5) as client:
        response = await client.get(f"{base}/{quote(queue, safe='')}")
        if response.status_code == 404:
            return {"name": queue, "messages": 0, "messages_ready": 0, "messages_unacknowledged": 0}
        response.raise_for_status()
        return response.json()


async def set_binding(
    infra: Infrastructure,
    *,
    present: bool,
    exchange: str = "payments",
    queue: str = "payments.new",
    routing_key: str = "payments.new",
) -> None:
    """Remove or restore one topology binding through the management API."""
    binding = (
        f"{infra.management_url}/api/bindings/{quote(infra.vhost, safe='')}"
        f"/e/{quote(exchange, safe='')}/q/{quote(queue, safe='')}"
    )
    async with httpx.AsyncClient(auth=(infra.username, infra.password), timeout=5) as client:
        if present:
            response = await client.post(
                binding, json={"routing_key": routing_key, "arguments": {}}
            )
        else:
            response = await client.delete(f"{binding}/{quote(routing_key, safe='')}")
        if response.status_code not in (200, 201, 204, 404):
            response.raise_for_status()


async def force_outbox_due(connection: asyncpg.Connection, outbox_id: uuid.UUID) -> None:
    await connection.execute(
        "UPDATE outbox SET next_publish_at = clock_timestamp() - interval '1 second' "
        "WHERE id = $1 AND published_at IS NULL",
        outbox_id,
    )


async def purge_rabbitmq(
    infra: Infrastructure, queues: tuple[str, ...] = ("payments.new", "payments.dlq")
) -> None:
    """Purge only the queues of the isolated vhost, if they exist yet."""
    base = f"{infra.management_url}/api/queues/{quote(infra.vhost, safe='')}"
    async with httpx.AsyncClient(auth=(infra.username, infra.password), timeout=5) as client:
        for queue in queues:
            response = await client.delete(f"{base}/{quote(queue, safe='')}/contents")
            if response.status_code not in (200, 204, 404):
                response.raise_for_status()


async def read_queue_messages(
    infra: Infrastructure, queue: str, count: int = 50
) -> list[dict[str, Any]]:
    """Consume messages from a vhost queue through the management API.

    Used for DLQ inspection only: the product has no DLQ subscriber, so tests
    act as the operator reading the diagnostic queue. Messages are acknowledged
    (``ack_requeue_false``) and returned with decoded JSON payloads.
    """
    base = f"{infra.management_url}/api/queues/{quote(infra.vhost, safe='')}"
    async with httpx.AsyncClient(auth=(infra.username, infra.password), timeout=10) as client:
        response = await client.post(
            f"{base}/{quote(queue, safe='')}/get",
            json={"count": count, "ackmode": "ack_requeue_false", "encoding": "auto"},
        )
        response.raise_for_status()
        raw = response.json()

    messages: list[dict[str, Any]] = []
    for item in raw:
        payload = item.get("payload")
        if item.get("payload_encoding") == "base64":
            import base64

            payload = base64.b64decode(payload).decode("utf-8")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except ValueError:
                pass
        messages.append(
            {
                "payload": payload,
                "properties": item.get("properties", {}),
                "routing_key": item.get("routing_key"),
                "exchange": item.get("exchange"),
            }
        )
    return messages


async def wait_for_queue_message(
    infra: Infrastructure,
    queue: str,
    predicate: Callable[[dict[str, Any]], bool],
    *,
    deadline: float = 20.0,
) -> dict[str, Any]:
    latest: list[dict[str, Any]] = []
    limit = time.monotonic() + deadline
    while time.monotonic() < limit:
        latest = await read_queue_messages(infra, queue)
        for message in latest:
            if predicate(message):
                return message
        await asyncio.sleep(0.2)
    raise AssertionError(
        f"No message in {queue!r} matched within {deadline:.0f}s; last batch: {latest!r}"
    )


def payment_dto(
    webhook_url: str,
    *,
    amount: str = "100.25",
    currency: Currency = Currency.RUB,
    description: str = "integration payment",
    metadata: dict[str, Any] | None = None,
) -> CreatePaymentRequestDTO:
    return CreatePaymentRequestDTO(
        amount=amount,
        currency=currency,
        description=description,
        metadata={} if metadata is None else metadata,
        webhook_url=webhook_url,
    )


class ScriptedGateway:
    """A controlled gateway boundary for deterministic processing outcomes.

    It implements the real ``Gateway`` port; only the outcome (and a possible
    technical failure) is scripted. Every call is recorded, including the
    operation key, so tests can assert the stable gateway idempotency key.
    """

    def __init__(
        self, outcomes: list[Any] | None = None, default: Any = PaymentStatus.succeeded
    ) -> None:
        self.outcomes = list(outcomes or [])
        self.default = default
        self.calls: list[dict[str, Any]] = []

    async def charge(
        self, *, payment_id, amount, currency, description, idempotency_key
    ) -> PaymentStatus:
        self.calls.append(
            {
                "payment_id": payment_id,
                "amount": amount,
                "currency": currency,
                "description": description,
                "idempotency_key": idempotency_key,
            }
        )
        outcome = self.outcomes.pop(0) if self.outcomes else self.default
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    @property
    def count(self) -> int:
        return len(self.calls)


class ControlledJitter:
    """Zero-argument jitter callable feeding ProcessingService."""

    def __init__(self, factor: float = 1.0) -> None:
        self.factor = factor
        self.calls = 0

    def __call__(self) -> float:
        self.calls += 1
        return self.factor


class _FailingUnitOfWork:
    def __init__(self, inner, factory: FailOnCommitUowFactory) -> None:
        self._inner = inner
        self._factory = factory

    @property
    def payments(self):
        return self._inner.payments

    @property
    def inbox(self):
        return self._inner.inbox

    @property
    def outbox(self):
        return self._inner.outbox

    async def __aenter__(self):
        await self._inner.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return await self._inner.__aexit__(exc_type, exc, tb)

    async def now(self):
        return await self._inner.now()

    async def commit(self) -> None:
        self._factory.commits += 1
        if self._factory.fail_on_commit == self._factory.commits:
            from payment_resolver.core.payments.exceptions import StorageUnavailable

            raise StorageUnavailable("simulated storage failure after external I/O")
        await self._inner.commit()


class FailOnCommitUowFactory:
    """Delegates to the real DB UoW factory but fails one chosen commit.

    Used only to reproduce the specification's "database write fails after
    external I/O" window against the real PostgreSQL adapter; the UoW itself is
    the product implementation.
    """

    def __init__(self, factory, fail_on_commit: int) -> None:
        self.factory = factory
        self.fail_on_commit = fail_on_commit
        self.commits = 0

    def __call__(self):
        return _FailingUnitOfWork(self.factory(), self)


async def fetch_inbox(connection: asyncpg.Connection, event_id: uuid.UUID) -> dict[str, Any] | None:
    row = await connection.fetchrow("SELECT * FROM inbox WHERE event_id = $1", event_id)
    return dict(row) if row else None


async def fetch_payment(
    connection: asyncpg.Connection, payment_id: uuid.UUID
) -> dict[str, Any] | None:
    row = await connection.fetchrow("SELECT * FROM payments WHERE id = $1", payment_id)
    return dict(row) if row else None


async def fetch_outbox(
    connection: asyncpg.Connection, payment_id: uuid.UUID, kind: str
) -> list[dict[str, Any]]:
    rows = await connection.fetch(
        "SELECT * FROM outbox WHERE payment_id = $1 AND kind = $2 ORDER BY created_at",
        payment_id,
        kind,
    )
    return [dict(row) for row in rows]


async def initial_event_id(connection: asyncpg.Connection, payment_id: uuid.UUID) -> uuid.UUID:
    value = await connection.fetchval(
        "SELECT id FROM outbox WHERE payment_id = $1 AND kind = 'payment.created'",
        payment_id,
    )
    if value is None:
        raise AssertionError(f"No initial outbox event for payment {payment_id}")
    return value


async def force_due(connection: asyncpg.Connection, event_id: uuid.UUID) -> None:
    await connection.execute(
        "UPDATE inbox SET next_attempt_at = clock_timestamp() - interval '1 second' "
        "WHERE event_id = $1 AND next_attempt_at IS NOT NULL",
        event_id,
    )


async def expire_lease(connection: asyncpg.Connection, event_id: uuid.UUID) -> None:
    await connection.execute(
        "UPDATE inbox SET locked_until = clock_timestamp() - interval '1 second' "
        "WHERE event_id = $1",
        event_id,
    )


def expected_retry_seconds(base_seconds: float, attempt: int, factor: float) -> float:
    return base_seconds * 2 ** (attempt - 1) * factor


def assert_close(actual: float, expected: float, tolerance: float) -> None:
    assert abs(actual - expected) <= tolerance, (
        f"expected {actual} to be within {tolerance} of {expected}"
    )
