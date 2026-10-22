from __future__ import annotations

import json
import os
import threading
import time
from typing import Any

import asyncpg
import httpx
import pytest
import uvicorn

from payment_resolver.core.payments.dtos import PaymentCreatedEventDTO
from payment_resolver.modules.config import Settings
from payment_resolver.modules.database import create_engine_and_uow
from payment_resolver.modules.payments.logic import PaymentService, ProcessingService
from payment_resolver.modules.webhooks.client import HttpWebhookSender
from tests.integration import helpers


def _start_local_receiver() -> tuple[str, uvicorn.Server, threading.Thread]:
    from tests.support.webhook_receiver import create_app

    config = uvicorn.Config(
        create_app(),
        host="127.0.0.1",
        port=0,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, name="webhook-receiver", daemon=True)
    thread.start()
    deadline = time.monotonic() + 15
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.05)
    if not server.started or not server.servers:
        server.should_exit = True
        thread.join(timeout=5)
        raise RuntimeError("The in-process webhook receiver did not start")
    port = server.servers[0].sockets[0].getsockname()[1]
    return f"http://127.0.0.1:{port}", server, thread


def _external_receiver(url: str) -> None:
    deadline = time.monotonic() + helpers.BOOTSTRAP_DEADLINE_SECONDS
    last: str | None = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"{url.rstrip('/')}/receipts", timeout=2)
            if response.status_code == 200:
                return
            last = f"HTTP {response.status_code}"
        except httpx.HTTPError as exc:
            last = repr(exc)
        time.sleep(0.3)
    raise RuntimeError(f"TEST_WEBHOOK_URL {url!r} is not reachable: {last}")


@pytest.fixture(scope="session")
def webhook_url() -> Any:
    """Real HTTP receiver: external Compose service or an in-process uvicorn server."""
    external = os.environ.get("TEST_WEBHOOK_URL")
    if external:
        url = external.rstrip("/")
        _external_receiver(url)
        yield url
        return
    url, server, thread = _start_local_receiver()
    try:
        yield url
    finally:
        server.should_exit = True
        thread.join(timeout=10)


@pytest.fixture(scope="session")
def infrastructure(webhook_url: str) -> helpers.Infrastructure:
    try:
        infra = helpers.provision(webhook_url)
    except helpers.ProvisioningError as exc:
        pytest.fail(str(exc), pytrace=False)
    try:
        yield infra
    finally:
        helpers.cleanup(infra)


@pytest.fixture(scope="session")
def settings(infrastructure: helpers.Infrastructure, webhook_url: str) -> Settings:
    """Application settings point exclusively at the isolated DB and vhost."""
    return Settings(
        API_KEY=helpers.API_KEY,
        DATABASE_URL=infrastructure.database_url,
        RABBITMQ_URL=infrastructure.rabbitmq_url,
        WEBHOOK_ALLOWED_ORIGINS=[webhook_url],
        RETRY_BASE_SECONDS=helpers.RETRY_BASE_SECONDS,
    )


@pytest.fixture
async def database(settings: Settings):
    engine, uow_factory = create_engine_and_uow(settings.DATABASE_URL)
    try:
        yield engine, uow_factory
    finally:
        await engine.dispose()


@pytest.fixture
def uow_factory(database):
    return database[1]


@pytest.fixture
async def raw_db(infrastructure: helpers.Infrastructure):
    connection = await asyncpg.connect(infrastructure.database_dsn)
    for typename in ("json", "jsonb"):
        await connection.set_type_codec(
            typename,
            schema="pg_catalog",
            encoder=json.dumps,
            decoder=json.loads,
        )
    try:
        yield connection
    finally:
        await connection.close()


@pytest.fixture
def allowed_origins(webhook_url: str) -> list[str]:
    return [webhook_url]


@pytest.fixture
def payment_service(uow_factory, allowed_origins: list[str]) -> PaymentService:
    return PaymentService(uow_factory, allowed_origins)


@pytest.fixture
def gateway() -> helpers.ScriptedGateway:
    return helpers.ScriptedGateway()


@pytest.fixture
def jitter() -> helpers.ControlledJitter:
    return helpers.ControlledJitter(1.0)


@pytest.fixture
async def webhook_client():
    client = httpx.AsyncClient(
        trust_env=False, follow_redirects=False, timeout=httpx.Timeout(10, connect=3)
    )
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def webhook_sender(
    webhook_client: httpx.AsyncClient, allowed_origins: list[str]
) -> HttpWebhookSender:
    return HttpWebhookSender(webhook_client, allowed_origins)


@pytest.fixture
def processing_service(
    uow_factory,
    gateway: helpers.ScriptedGateway,
    webhook_sender: HttpWebhookSender,
    jitter: helpers.ControlledJitter,
    settings: Settings,
) -> ProcessingService:
    return ProcessingService(
        uow_factory,
        gateway,
        webhook_sender,
        retry_base_seconds=settings.RETRY_BASE_SECONDS,
        jitter=jitter,
    )


@pytest.fixture
def create_ready_payment(
    payment_service: PaymentService,
    processing_service: ProcessingService,
    raw_db: asyncpg.Connection,
    webhook_url: str,
):
    """Create a payment through PaymentService and hand its initial event to Inbox."""
    import uuid

    async def factory(*, path: str = "/ok", **dto_kwargs: Any):
        dto = helpers.payment_dto(f"{webhook_url}{path}", **dto_kwargs)
        payment = await payment_service.create(dto, f"key-{uuid.uuid4().hex}")
        event_id = await helpers.initial_event_id(raw_db, payment.id)
        await processing_service.receive(
            PaymentCreatedEventDTO(event_id=event_id, payment_id=payment.id),
        )
        return payment, event_id

    return factory


@pytest.fixture
async def receipts(webhook_client: httpx.AsyncClient, webhook_url: str):
    async def fetch(event_id: str | None = None) -> list[dict[str, Any]]:
        response = await webhook_client.get(f"{webhook_url}/receipts")
        response.raise_for_status()
        payload = response.json()
        if event_id is None:
            return payload
        return [item for item in payload if item.get("event_id") == event_id]

    return fetch


@pytest.fixture(autouse=True)
async def clean_state(raw_db: asyncpg.Connection, infrastructure: helpers.Infrastructure):
    """The isolated DB/vhost are reused per session; each test starts empty."""
    await raw_db.execute("TRUNCATE outbox, inbox, payments CASCADE")
    await helpers.purge_rabbitmq(infrastructure)


@pytest.fixture
async def broker(settings: Settings):
    from payment_resolver.modules.messaging.broker import create_broker, declare_topology

    instance = create_broker(settings)
    await instance.connect()
    await declare_topology(instance)
    try:
        yield instance
    finally:
        await instance.stop()


@pytest.fixture
def event_publisher(broker):
    from payment_resolver.modules.messaging.broker import RabbitEventPublisher

    return RabbitEventPublisher(broker)


@pytest.fixture
def outbox_publisher(uow_factory, event_publisher):
    from payment_resolver.modules.messaging.workers import OutboxPublisher

    return OutboxPublisher(uow_factory, event_publisher)


@pytest.fixture
def payment_receiver(uow_factory, event_publisher):
    from payment_resolver.modules.messaging.receiver import PaymentReceiver

    return PaymentReceiver(uow_factory, event_publisher)


@pytest.fixture
async def start_receiver(settings):
    """Start extra consumers on the isolated vhost, closing them after the test."""
    from payment_resolver.modules.messaging.broker import (
        create_broker,
        declare_topology,
        register_receiver,
    )

    created: list[Any] = []

    async def start(receiver):
        instance = create_broker(settings)
        await instance.connect()
        await declare_topology(instance)
        register_receiver(instance, receiver)
        await instance.start()
        created.append(instance)
        return instance

    try:
        yield start
    finally:
        for instance in reversed(created):
            await instance.stop()


@pytest.fixture
async def api_client(settings: Settings):
    from payment_resolver.app import create_app

    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://api.test", timeout=15
        ) as client:
            yield client


@pytest.fixture
def api_headers() -> dict[str, str]:
    return {"X-API-Key": helpers.API_KEY}
