from __future__ import annotations

import asyncio
import uuid

import httpx
import pytest

from payment_resolver.core.payments.dtos import PaymentCreatedEventDTO
from payment_resolver.core.payments.enums import PaymentStatus
from payment_resolver.modules.database import create_engine_and_uow
from payment_resolver.modules.messaging.broker import (
    RabbitEventPublisher,
    create_broker,
    register_receiver,
)
from payment_resolver.modules.messaging.receiver import FatalSignal, PaymentReceiver
from payment_resolver.modules.messaging.workers import OutboxPublisher
from payment_resolver.modules.payments.logic import ProcessingService
from payment_resolver.modules.processing.runtime import Consumer, build_consumer
from payment_resolver.modules.webhooks.client import HttpWebhookSender
from tests.integration import helpers


class ExplodingOutbox:
    async def publish_one(self) -> bool:
        raise RuntimeError("background loop crashed")


class _FailingPaymentsRepository:
    def __init__(self, inner) -> None:
        self._inner = inner

    async def get(self, *args, **kwargs):
        raise RuntimeError("receiver database failure")

    async def create_or_get(self, *args, **kwargs):
        return await self._inner.create_or_get(*args, **kwargs)

    async def finalize(self, *args, **kwargs):
        return await self._inner.finalize(*args, **kwargs)


class _FailingGetUnitOfWork:
    def __init__(self, inner) -> None:
        self._inner = inner

    @property
    def inbox(self):
        return self._inner.inbox

    @property
    def outbox(self):
        return self._inner.outbox

    async def __aenter__(self):
        await self._inner.__aenter__()
        self.payments = _FailingPaymentsRepository(self._inner.payments)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return await self._inner.__aexit__(exc_type, exc, tb)

    async def now(self):
        return await self._inner.now()

    async def commit(self) -> None:
        await self._inner.commit()


class FailOnPaymentsGetUowFactory:
    """Real UoW wrapper whose receiver-side payment lookup fails unexpectedly."""

    def __init__(self, factory) -> None:
        self._factory = factory

    def __call__(self):
        return _FailingGetUnitOfWork(self._factory())


class BlockingGateway:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls: list[dict] = []

    async def charge(
        self, *, payment_id, amount, currency, description, idempotency_key
    ) -> PaymentStatus:
        self.calls.append({"payment_id": payment_id, "idempotency_key": idempotency_key})
        self.started.set()
        await self.release.wait()
        return PaymentStatus.succeeded

    @property
    def count(self) -> int:
        return len(self.calls)


async def test_unexpected_background_failure_terminates_the_consumer(settings) -> None:
    consumer = build_consumer(settings)
    consumer.outbox_publisher = ExplodingOutbox()

    with pytest.raises(RuntimeError, match="background loop crashed"):
        await asyncio.wait_for(consumer.run(), timeout=20)

    # Cleanup ran: no half-alive process is left behind.
    assert consumer.http_client.is_closed


async def test_receiver_unexpected_error_is_fatal_and_source_remains_redeliverable(
    settings,
    payment_service,
    raw_db,
    broker,
    infrastructure,
    webhook_url,
    start_receiver,
    payment_receiver,
    gateway,
) -> None:
    payment = await payment_service.create(
        helpers.payment_dto(f"{webhook_url}/ok"),
        f"receiver-fatal-{uuid.uuid4().hex}",
    )
    event_id = await helpers.initial_event_id(raw_db, payment.id)
    # Keep the initial Outbox event out of the way; this test publishes one copy.
    await raw_db.execute(
        "UPDATE outbox SET published_at = clock_timestamp() "
        "WHERE payment_id = $1 AND kind = 'payment.created'",
        payment.id,
    )
    await broker.publish(
        {"event_id": str(event_id), "payment_id": str(payment.id)},
        exchange="payments",
        routing_key="payments.new",
        persist=True,
        mandatory=True,
        message_id=str(event_id),
    )

    engine, uow_factory = create_engine_and_uow(settings.DATABASE_URL)
    consumer_broker = create_broker(settings)
    http_client = httpx.AsyncClient(
        trust_env=False,
        follow_redirects=False,
        timeout=httpx.Timeout(5, connect=2),
    )
    publisher = RabbitEventPublisher(consumer_broker)
    fatal = FatalSignal()
    receiver = PaymentReceiver(FailOnPaymentsGetUowFactory(uow_factory), publisher, fatal)
    subscriber = register_receiver(consumer_broker, receiver)
    consumer = Consumer(
        settings,
        engine=engine,
        uow_factory=uow_factory,
        broker=consumer_broker,
        publisher=publisher,
        receiver=receiver,
        subscriber=subscriber,
        outbox_publisher=OutboxPublisher(uow_factory, publisher),
        processing=ProcessingService(
            uow_factory,
            gateway,
            HttpWebhookSender(http_client, [webhook_url]),
            retry_base_seconds=helpers.RETRY_BASE_SECONDS,
            jitter=helpers.ControlledJitter(1.0),
        ),
        http_client=http_client,
        fatal=fatal,
    )

    with pytest.raises(RuntimeError, match="receiver database failure"):
        await asyncio.wait_for(consumer.run(), timeout=20)

    # No work was accepted and no financial effect happened.
    assert (
        await raw_db.fetchval(
            "SELECT count(*) FROM inbox WHERE event_id = $1",
            event_id,
        )
        == 0
    )
    assert gateway.count == 0
    payment_row = await helpers.fetch_payment(raw_db, payment.id)
    assert payment_row["status"] == "pending"
    assert payment_row["processed_at"] is None
    assert consumer.http_client.is_closed

    # The unacknowledged source is requeued and can still be handed off later.
    await helpers.wait_until(
        lambda: _source_ready(infrastructure),
        deadline=30,
        description="fatal message redelivery",
    )
    await start_receiver(payment_receiver)
    await helpers.wait_until(
        lambda: _inbox_exists(raw_db, event_id),
        deadline=30,
        description="redelivery handoff",
    )
    inbox = await helpers.fetch_inbox(raw_db, event_id)
    assert inbox["status"] == "ready"
    assert inbox["attempts"] == 0


async def _source_ready(infrastructure) -> bool:
    stats = await helpers.queue_stats(infrastructure, "payments.new")
    return stats.get("messages_ready", 0) >= 1


async def _inbox_exists(raw_db, event_id) -> bool:
    return (
        await raw_db.fetchval(
            "SELECT count(*) FROM inbox WHERE event_id = $1",
            event_id,
        )
        == 1
    )


async def test_stop_prevents_new_claims_and_finishes_the_current_action(
    settings,
    payment_service,
    processing_service,
    webhook_url,
    raw_db,
) -> None:
    first_payment = await payment_service.create(
        helpers.payment_dto(f"{webhook_url}/ok"),
        f"lifecycle-1-{uuid.uuid4().hex}",
    )
    second_payment = await payment_service.create(
        helpers.payment_dto(f"{webhook_url}/ok"),
        f"lifecycle-2-{uuid.uuid4().hex}",
    )
    first_event = await helpers.initial_event_id(raw_db, first_payment.id)
    second_event = await helpers.initial_event_id(raw_db, second_payment.id)
    for payment, event_id in ((first_payment, first_event), (second_payment, second_event)):
        await processing_service.receive(
            PaymentCreatedEventDTO(event_id=event_id, payment_id=payment.id),
        )

    consumer = build_consumer(settings)
    gateway = BlockingGateway()
    consumer.processing = ProcessingService(
        consumer.uow_factory,
        gateway,
        HttpWebhookSender(consumer.http_client, [webhook_url]),
        retry_base_seconds=helpers.RETRY_BASE_SECONDS,
        jitter=helpers.ControlledJitter(1.0),
    )

    task = asyncio.create_task(consumer.run())
    try:
        await asyncio.wait_for(gateway.started.wait(), timeout=20)
        assert gateway.count == 1
        consumer.request_stop()
        gateway.release.set()
        await asyncio.wait_for(task, timeout=20)
    finally:
        if not task.done():
            consumer.request_stop()
            gateway.release.set()
            await asyncio.gather(task, return_exceptions=True)

    processed_id = gateway.calls[0]["payment_id"]
    assert processed_id in (first_payment.id, second_payment.id)
    processed_event = first_event if processed_id == first_payment.id else second_event
    other_event = second_event if processed_id == first_payment.id else first_event
    other_payment = second_payment if processed_id == first_payment.id else first_payment

    processed = await helpers.fetch_inbox(raw_db, processed_event)
    assert processed["status"] == "completed"
    assert processed["attempts"] == 1

    untouched = await helpers.fetch_inbox(raw_db, other_event)
    assert untouched["status"] == "ready"
    assert untouched["attempts"] == 0
    assert untouched["lock_token"] is None
    pending = await helpers.fetch_payment(raw_db, other_payment.id)
    assert pending["status"] == "pending"
    assert pending["processed_at"] is None

    # The single executor did not start a second action during shutdown.
    assert gateway.count == 1
    assert consumer.http_client.is_closed
