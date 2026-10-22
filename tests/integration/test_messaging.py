from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from urllib.parse import quote

import aiormq
import asyncpg
import httpx
import pytest
from faststream.rabbit import RabbitMessage

from payment_resolver.core.payments.dtos import PaymentCreatedEventDTO
from payment_resolver.core.payments.exceptions import (
    PublicationError,
    StorageUnavailable,
)
from payment_resolver.modules.database.repositories import SqlAlchemyOutboxRepository
from payment_resolver.modules.messaging.broker import (
    DLQ_QUEUE_NAME,
    PAYMENTS_EXCHANGE_NAME,
    PAYMENTS_QUEUE_NAME,
    PAYMENTS_ROUTING_KEY,
    RabbitEventPublisher,
    create_broker,
    declare_topology,
    register_receiver,
)
from payment_resolver.modules.messaging.receiver import PaymentReceiver
from payment_resolver.modules.messaging.workers import OutboxPublisher
from tests.integration import helpers


def _uuid() -> str:
    return uuid.uuid4().hex


def _diagnostic_id(message_id: str | None, body: bytes) -> str:
    digest = hashlib.sha256(body).hexdigest()
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"invalid-message:{message_id or ''}:{digest}"))


def _result_event_id(payment_id: uuid.UUID) -> str:
    """The financial-result webhook identity is derived from the payment."""
    return str(uuid.uuid5(payment_id, "payment.result"))


async def _publish_raw(settings, body: bytes, *, message_id: str) -> None:
    """Publish an unmodified body over AMQP with an explicit transport ID."""
    connection = await aiormq.connect(settings.RABBITMQ_URL)
    try:
        channel = await connection.channel()
        await channel.basic_publish(
            body,
            exchange=PAYMENTS_EXCHANGE_NAME,
            routing_key=PAYMENTS_ROUTING_KEY,
            properties=aiormq.spec.Basic.Properties(
                content_type="application/json",
                delivery_mode=2,
                message_id=message_id,
            ),
            mandatory=True,
            timeout=10.0,
        )
    finally:
        await connection.close()


async def _ready_messages(settings, queue: str) -> int:
    """Return the broker-authoritative ready count of one queue."""
    connection = await aiormq.connect(settings.RABBITMQ_URL)
    try:
        channel = await connection.channel()
        declaration = await channel.queue_declare(queue, durable=True, passive=True)
        assert declaration is not None, f"queue {queue!r} is not declared"
        return declaration.message_count
    finally:
        await connection.close()


async def _source_consumed(settings) -> bool:
    """True once the source queue has handed every copy to the consumer."""
    return await _ready_messages(settings, PAYMENTS_QUEUE_NAME) == 0


async def _source_available(settings) -> bool:
    """True while the broker holds a ready source copy (poll without consuming)."""
    return await _ready_messages(settings, PAYMENTS_QUEUE_NAME) >= 1


async def _assert_acked_after_stop(
    settings,
    consumer,
    queue: str = PAYMENTS_QUEUE_NAME,
) -> None:
    """Stop the consumer, then prove from the broker that nothing was requeued.

    An unACKed delivery is returned to the queue when its channel closes, so a
    fresh connection performing a passive declaration and Basic.Get observes
    it. Management-API counters lag and must never be treated as ACK success.
    """
    await consumer.stop()
    assert await _ready_messages(settings, queue) == 0, (
        f"{queue} still holds a ready message after the consumer stopped"
    )
    connection = await aiormq.connect(settings.RABBITMQ_URL)
    try:
        channel = await connection.channel()
        await channel.queue_declare(queue, durable=True, passive=True)
        delivered = await channel.basic_get(queue, no_ack=True)
        assert isinstance(delivered.delivery, aiormq.spec.Basic.GetEmpty), (
            f"{queue} requeued an unACKed message: {delivered.body!r}"
        )
    finally:
        await connection.close()


async def test_confirmed_publish_creates_inbox_and_completes_scenario(
    payment_service,
    uow_factory,
    raw_db,
    settings,
    outbox_publisher,
    start_receiver,
    payment_receiver,
    processing_service,
    webhook_url,
    receipts,
) -> None:
    payment = await payment_service.create(
        helpers.payment_dto(f"{webhook_url}/ok"),
        f"msg-{_uuid()}",
    )
    event_id = await helpers.initial_event_id(raw_db, payment.id)

    assert await outbox_publisher.publish_one() is True
    rows = await helpers.fetch_outbox(raw_db, payment.id, "payment.created")
    assert len(rows) == 1
    assert rows[0]["published_at"] is not None
    assert rows[0]["publish_attempts"] == 1
    assert rows[0]["last_error_code"] is None

    consumer = await start_receiver(payment_receiver)
    await helpers.wait_until(
        lambda: _inbox_exists(raw_db, event_id),
        description="Inbox handoff",
    )
    inbox = await helpers.fetch_inbox(raw_db, event_id)
    assert inbox["status"] == "ready"
    assert inbox["attempts"] == 0
    assert inbox["next_attempt_at"] is not None
    assert inbox["lock_token"] is None and inbox["locked_until"] is None
    await helpers.wait_until(
        lambda: _source_consumed(settings),
        description="source delivery",
    )
    await _assert_acked_after_stop(settings, consumer)

    claim = await processing_service.claim_next()
    assert claim is not None and claim.event_id == event_id
    await processing_service.process(claim)
    completed = await helpers.fetch_inbox(raw_db, event_id)
    assert completed["status"] == "completed"
    delivered = await receipts(_result_event_id(payment.id))
    assert len(delivered) == 1
    assert delivered[0]["body"]["payment_id"] == str(payment.id)


async def _inbox_exists(raw_db, event_id) -> bool:
    return await raw_db.fetchval("SELECT count(*) FROM inbox WHERE event_id = $1", event_id) == 1


async def test_duplicate_transport_delivery_does_not_reset_inbox_attempts(
    payment_service,
    uow_factory,
    raw_db,
    event_publisher,
    settings,
    start_receiver,
    payment_receiver,
    webhook_url,
) -> None:
    payment = await payment_service.create(
        helpers.payment_dto(f"{webhook_url}/ok"),
        f"dup-{_uuid()}",
    )
    # The stored webhook URL must pass the allowlist only when a webhook is sent;
    # this test never processes the Inbox row.
    async with uow_factory() as uow:
        entry = await uow.outbox.lock_next_due(await uow.now())
        assert entry is not None
        await uow.commit()

    consumer = await start_receiver(payment_receiver)
    await event_publisher.publish(entry)
    await helpers.wait_until(
        lambda: _inbox_exists(raw_db, entry.id),
        description="first handoff",
    )
    await helpers.wait_until(
        lambda: _source_consumed(settings),
        description="first delivery consumed",
    )
    await _assert_acked_after_stop(settings, consumer)
    after_first = await helpers.fetch_inbox(raw_db, entry.id)

    # Same stable message id delivered again, as after a lost broker confirm.
    consumer = await start_receiver(payment_receiver)
    await event_publisher.publish(entry)
    await helpers.wait_until(
        lambda: _source_consumed(settings),
        description="duplicate delivery consumed",
    )
    await _assert_acked_after_stop(settings, consumer)

    assert (
        await raw_db.fetchval("SELECT count(*) FROM inbox WHERE payment_id = $1", payment.id) == 1
    )
    inbox = await helpers.fetch_inbox(raw_db, entry.id)
    assert inbox["status"] == "ready"
    assert inbox["attempts"] == 0
    assert inbox["next_attempt_at"] == after_first["next_attempt_at"]


async def test_unroutable_publication_keeps_outbox_unpublished_until_rerouted(
    payment_service,
    raw_db,
    outbox_publisher,
    infrastructure,
    webhook_url,
) -> None:
    payment = await payment_service.create(
        helpers.payment_dto(f"{webhook_url}/ok"),
        f"unroutable-{_uuid()}",
    )
    event_id = await helpers.initial_event_id(raw_db, payment.id)

    await helpers.set_binding(infrastructure, present=False)
    try:
        assert await outbox_publisher.publish_one() is True
    finally:
        await helpers.set_binding(infrastructure, present=True)

    rows = await helpers.fetch_outbox(raw_db, payment.id, "payment.created")
    assert rows[0]["published_at"] is None
    assert rows[0]["publish_attempts"] == 1
    assert rows[0]["last_error_code"]
    assert rows[0]["next_publish_at"] > rows[0]["created_at"]

    await helpers.force_outbox_due(raw_db, event_id)
    assert await outbox_publisher.publish_one() is True
    rows = await helpers.fetch_outbox(raw_db, payment.id, "payment.created")
    assert rows[0]["published_at"] is not None
    assert rows[0]["last_error_code"] is None


async def test_payment_accepted_without_broker_and_processed_after_recovery(
    api_client,
    api_headers,
    settings,
    uow_factory,
    raw_db,
    broker,
    outbox_publisher,
    start_receiver,
    payment_receiver,
    processing_service,
    webhook_url,
    receipts,
) -> None:
    key = f"broker-down-{_uuid()}"
    response = await api_client.post(
        "/api/v1/payments",
        headers={**api_headers, "Idempotency-Key": key},
        json={
            "amount": "100.25",
            "currency": "RUB",
            "description": "broker down",
            "metadata": {},
            "webhook_url": f"{webhook_url}/ok",
        },
    )
    assert response.status_code == 202, response.text
    payment_id = uuid.UUID(response.json()["payment_id"])

    # The API never touches the broker: the Outbox obligation is durable and unpublished.
    rows = await helpers.fetch_outbox(raw_db, payment_id, "payment.created")
    assert rows[0]["published_at"] is None
    event_id = rows[0]["id"]

    # Lose a real, initialized transport. Publishing before connect() would be
    # a programmer error, not a broker-availability failure.
    broken_broker = create_broker(settings)
    connection = await broken_broker.connect()
    await declare_topology(broken_broker)
    await connection.close()
    try:
        broken_publisher = RabbitEventPublisher(broken_broker)
        broken_outbox = OutboxPublisher(uow_factory, broken_publisher)
        assert await broken_outbox.publish_one() is True
    finally:
        await broken_broker.stop()
    failed = await helpers.fetch_outbox(raw_db, payment_id, "payment.created")
    assert failed[0]["published_at"] is None
    assert failed[0]["publish_attempts"] == 1
    assert failed[0]["last_error_code"]

    await helpers.force_outbox_due(raw_db, event_id)
    assert await outbox_publisher.publish_one() is True
    published = await helpers.fetch_outbox(raw_db, payment_id, "payment.created")
    assert published[0]["published_at"] is not None

    await start_receiver(payment_receiver)
    await helpers.wait_until(
        lambda: _inbox_exists(raw_db, event_id),
        description="recovered handoff",
    )
    claim = await processing_service.claim_next()
    assert claim is not None and claim.event_id == event_id
    await processing_service.process(claim)
    assert (await helpers.fetch_inbox(raw_db, event_id))["status"] == "completed"
    delivered = await receipts(_result_event_id(payment_id))
    assert len(delivered) == 1
    assert delivered[0]["body"]["payment_id"] == str(payment_id)


async def test_invalid_body_is_diagnosed_and_acked(
    settings,
    start_receiver,
    payment_receiver,
    infrastructure,
    raw_db,
    gateway,
) -> None:
    consumer = await start_receiver(payment_receiver)
    body = b'{"event_id":"not-a-uuid"}'
    original_id = "bad-body-1"
    await _publish_raw(settings, body, message_id=original_id)

    expected = _diagnostic_id(original_id, body)
    message = await helpers.wait_for_queue_message(
        infrastructure,
        DLQ_QUEUE_NAME,
        lambda item: item["properties"].get("message_id") == expected,
    )
    payload = message["payload"]
    assert payload["reason"] == "invalid_message"
    assert payload["original_message_id"] == original_id
    assert payload["original_body_bytes"] == len(body)
    assert payload["body_truncated"] is False
    assert base64.b64decode(payload["body_base64"]) == body

    assert await raw_db.fetchval("SELECT count(*) FROM inbox") == 0
    assert gateway.count == 0
    await helpers.wait_until(
        lambda: _source_consumed(settings),
        description="invalid source delivery",
    )
    await _assert_acked_after_stop(settings, consumer)


async def test_valid_envelope_with_missing_payment_is_diagnosed_without_financial_action(
    settings,
    start_receiver,
    payment_receiver,
    infrastructure,
    raw_db,
    gateway,
) -> None:
    consumer = await start_receiver(payment_receiver)
    event_id = uuid.uuid4()
    unknown_payment = uuid.uuid4()
    body = json.dumps({"event_id": str(event_id), "payment_id": str(unknown_payment)}).encode()
    await _publish_raw(settings, body, message_id=str(event_id))

    expected = _diagnostic_id(str(event_id), body)
    message = await helpers.wait_for_queue_message(
        infrastructure,
        DLQ_QUEUE_NAME,
        lambda item: item["properties"].get("message_id") == expected,
    )
    assert message["payload"]["reason"] == "invalid_message"
    assert message["payload"]["original_message_id"] == str(event_id)
    assert await raw_db.fetchval("SELECT count(*) FROM inbox") == 0
    assert gateway.count == 0
    await helpers.wait_until(
        lambda: _source_consumed(settings),
        description="missing-payment source delivery",
    )
    await _assert_acked_after_stop(settings, consumer)


async def test_missing_amqp_message_id_uses_stable_diagnostic_id(
    settings,
    start_receiver,
    payment_receiver,
    infrastructure,
    raw_db,
) -> None:
    consumer = await start_receiver(payment_receiver)
    event_id = uuid.uuid4()
    payment_id = uuid.uuid4()
    body = json.dumps({"event_id": str(event_id), "payment_id": str(payment_id)}).encode()
    # Both FastStream and aiormq synthesize IDs. The management endpoint lets
    # the real broker publish a message with the AMQP property genuinely absent.
    endpoint = (
        f"{infrastructure.management_url}/api/exchanges/"
        f"{quote(infrastructure.vhost, safe='')}/{PAYMENTS_EXCHANGE_NAME}/publish"
    )
    async with httpx.AsyncClient(
        auth=(infrastructure.username, infrastructure.password),
        timeout=10,
    ) as client:
        response = await client.post(
            endpoint,
            json={
                "properties": {"content_type": "application/json", "delivery_mode": 2},
                "routing_key": PAYMENTS_ROUTING_KEY,
                "payload": base64.b64encode(body).decode("ascii"),
                "payload_encoding": "base64",
            },
        )
        response.raise_for_status()
        assert response.json()["routed"] is True

    expected = _diagnostic_id(None, body)
    message = await helpers.wait_for_queue_message(
        infrastructure,
        DLQ_QUEUE_NAME,
        lambda item: item["properties"].get("message_id") == expected,
    )
    payload = message["payload"]
    assert payload["reason"] == "invalid_message"
    assert "original_message_id" not in payload
    assert await raw_db.fetchval("SELECT count(*) FROM inbox") == 0
    await helpers.wait_until(
        lambda: _source_consumed(settings),
        description="missing-id source delivery",
    )
    await _assert_acked_after_stop(settings, consumer)


async def test_conflicting_identity_is_diagnosed_and_existing_inbox_is_unchanged(
    payment_service,
    processing_service,
    raw_db,
    settings,
    start_receiver,
    payment_receiver,
    infrastructure,
    gateway,
    webhook_url,
) -> None:
    payment_a = await payment_service.create(
        helpers.payment_dto(f"{webhook_url}/ok"),
        f"conflict-a-{_uuid()}",
    )
    payment_b = await payment_service.create(
        helpers.payment_dto(f"{webhook_url}/ok"),
        f"conflict-b-{_uuid()}",
    )
    event_id = await helpers.initial_event_id(raw_db, payment_a.id)
    await processing_service.receive(
        PaymentCreatedEventDTO(event_id=event_id, payment_id=payment_a.id),
    )
    before = await helpers.fetch_inbox(raw_db, event_id)
    assert before["payment_id"] == payment_a.id

    consumer = await start_receiver(payment_receiver)
    body = json.dumps({"event_id": str(event_id), "payment_id": str(payment_b.id)}).encode()
    await _publish_raw(settings, body, message_id=str(event_id))

    expected = _diagnostic_id(str(event_id), body)
    await helpers.wait_for_queue_message(
        infrastructure,
        DLQ_QUEUE_NAME,
        lambda item: item["properties"].get("message_id") == expected,
    )
    after = await helpers.fetch_inbox(raw_db, event_id)
    assert after == before
    assert (
        await raw_db.fetchval(
            "SELECT count(*) FROM inbox WHERE payment_id = $1",
            payment_b.id,
        )
        == 0
    )
    assert gateway.count == 0
    await helpers.wait_until(
        lambda: _source_consumed(settings),
        description="conflict source delivery",
    )
    await _assert_acked_after_stop(settings, consumer)


async def test_thirteen_mib_invalid_body_is_bounded_and_not_stuck(
    settings,
    start_receiver,
    payment_receiver,
    infrastructure,
    raw_db,
    gateway,
) -> None:
    consumer = await start_receiver(payment_receiver)
    body = b"{" + b"x" * (13 * 1024 * 1024 - 2) + b"}"
    assert len(body) == 13 * 1024 * 1024
    original_id = "large-invalid-1"
    await _publish_raw(settings, body, message_id=original_id)

    expected = _diagnostic_id(original_id, body)
    message = await helpers.wait_for_queue_message(
        infrastructure,
        DLQ_QUEUE_NAME,
        lambda item: item["properties"].get("message_id") == expected,
        deadline=30.0,
    )
    payload = message["payload"]
    assert payload["reason"] == "invalid_message"
    assert payload["original_body_bytes"] == len(body)
    assert payload["body_truncated"] is True
    truncated = base64.b64decode(payload["body_base64"])
    assert len(truncated) == 4096
    assert truncated == body[:4096]

    assert await raw_db.fetchval("SELECT count(*) FROM inbox") == 0
    assert gateway.count == 0
    await helpers.wait_until(
        lambda: _source_consumed(settings),
        description="large invalid source delivery",
        deadline=30.0,
    )
    await _assert_acked_after_stop(settings, consumer)


async def test_dead_letter_is_durable_and_published_with_specific_payload(
    create_ready_payment,
    processing_service,
    raw_db,
    broker,
    outbox_publisher,
    infrastructure,
    settings,
    uow_factory,
) -> None:
    payment, event_id = await create_ready_payment(path="/fail")
    for attempt in (1, 2, 3):
        claim = await processing_service.claim_next()
        assert claim is not None and claim.attempts == attempt
        await processing_service.process(claim)
        if attempt < 3:
            await helpers.force_due(raw_db, event_id)

    dead_letters = await helpers.fetch_outbox(raw_db, payment.id, "payment.processing_failed")
    assert len(dead_letters) == 1
    dead = dead_letters[0]
    assert dead["published_at"] is None
    # The initial event is irrelevant here; isolate the dead-letter row.
    await raw_db.execute(
        "UPDATE outbox SET published_at = clock_timestamp() "
        "WHERE payment_id = $1 AND kind = 'payment.created'",
        payment.id,
    )

    # The dead letter survives loss of an already initialized AMQP transport.
    broken_broker = create_broker(settings)
    connection = await broken_broker.connect()
    await declare_topology(broken_broker)
    await connection.close()
    try:
        broken_publisher = RabbitEventPublisher(broken_broker)
        assert await OutboxPublisher(uow_factory, broken_publisher).publish_one() is True
    finally:
        await broken_broker.stop()
    still_unpublished = await helpers.fetch_outbox(
        raw_db,
        payment.id,
        "payment.processing_failed",
    )
    assert still_unpublished[0]["published_at"] is None
    assert still_unpublished[0]["last_error_code"]
    await helpers.force_outbox_due(raw_db, dead["id"])

    # Dead-letter publication waits for the broker; the row survives independently.
    for _ in range(4):
        await outbox_publisher.publish_one()
        current = await helpers.fetch_outbox(
            raw_db,
            payment.id,
            "payment.processing_failed",
        )
        if current[0]["published_at"] is not None:
            break
    current = await helpers.fetch_outbox(raw_db, payment.id, "payment.processing_failed")
    assert current[0]["published_at"] is not None

    expected_id = str(uuid.uuid5(event_id, "dead-letter"))
    message = await helpers.wait_for_queue_message(
        infrastructure,
        DLQ_QUEUE_NAME,
        lambda item: item["properties"].get("message_id") == expected_id,
    )
    payload = message["payload"]
    assert payload["event_id"] == str(event_id)
    assert payload["payment_id"] == str(payment.id)
    assert payload["attempts"] == 3
    assert payload["payment_status"] == "succeeded"
    assert payload["error_code"] == "webhook_http_error"
    assert payload["last_http_status"] == 503
    assert payload["failed_at"] is not None

    payment_row = await helpers.fetch_payment(raw_db, payment.id)
    assert payment_row["status"] == "succeeded"


async def test_commit_failure_after_broker_confirm_republishes_same_message_id(
    payment_service,
    uow_factory,
    raw_db,
    broker,
    event_publisher,
    start_receiver,
    payment_receiver,
    webhook_url,
) -> None:
    payment = await payment_service.create(
        helpers.payment_dto(f"{webhook_url}/ok"),
        f"confirm-crash-{uuid.uuid4().hex}",
    )
    event_id = await helpers.initial_event_id(raw_db, payment.id)

    # Commit #1 is the Outbox success write after the broker confirm.
    failing_factory = helpers.FailOnCommitUowFactory(uow_factory, fail_on_commit=1)
    crashing = OutboxPublisher(failing_factory, event_publisher)
    with pytest.raises(StorageUnavailable):
        await crashing.publish_one()

    rows = await helpers.fetch_outbox(raw_db, payment.id, "payment.created")
    assert rows[0]["published_at"] is None
    assert rows[0]["publish_attempts"] == 0

    # The retry uses the same stable Outbox ID, so the broker duplicate is harmless.
    assert await OutboxPublisher(uow_factory, event_publisher).publish_one() is True
    rows = await helpers.fetch_outbox(raw_db, payment.id, "payment.created")
    assert rows[0]["published_at"] is not None

    await start_receiver(payment_receiver)
    await helpers.wait_until(
        lambda: _inbox_exists(raw_db, event_id),
        description="idempotent redelivery",
    )
    assert (
        await raw_db.fetchval("SELECT count(*) FROM inbox WHERE payment_id = $1", payment.id) == 1
    )
    inbox = await helpers.fetch_inbox(raw_db, event_id)
    assert inbox["status"] == "ready"
    assert inbox["attempts"] == 0


async def test_publication_deadline_covers_waiting_for_the_publisher_lock(
    payment_service,
    raw_db,
    broker,
    event_publisher,
    outbox_publisher,
    webhook_url,
) -> None:
    payment = await payment_service.create(
        helpers.payment_dto(f"{webhook_url}/ok"),
        f"lock-wait-{uuid.uuid4().hex}",
    )

    await event_publisher._lock.acquire()
    started = time.monotonic()
    try:
        assert await outbox_publisher.publish_one() is True
    finally:
        event_publisher._lock.release()
    elapsed = time.monotonic() - started

    assert 4.5 <= elapsed <= 7.5, elapsed
    rows = await helpers.fetch_outbox(raw_db, payment.id, "payment.created")
    assert rows[0]["published_at"] is None
    assert rows[0]["publish_attempts"] == 1
    assert rows[0]["last_error_code"] == "publish_timeout"
    assert rows[0]["next_publish_at"] > rows[0]["created_at"]


async def test_unencodable_payload_error_reaches_the_supervisor(
    monkeypatch,
    payment_service,
    uow_factory,
    raw_db,
    outbox_publisher,
    settings,
    webhook_url,
) -> None:
    payment = await payment_service.create(
        helpers.payment_dto(f"{webhook_url}/ok"),
        f"serialize-{uuid.uuid4().hex}",
    )

    real_lock = SqlAlchemyOutboxRepository.lock_next_due

    async def corrupt(self, now):
        entry = await real_lock(self, now)
        if entry is not None:
            # A programmer-supplied value the JSON encoder cannot serialize is
            # not a transport fault and must never become a bounded retry.
            entry.payload = {"event_id": str(entry.id), "payment_id": object()}
        return entry

    monkeypatch.setattr(SqlAlchemyOutboxRepository, "lock_next_due", corrupt)

    with pytest.raises(Exception) as excinfo:
        await outbox_publisher.publish_one()

    error = excinfo.value
    # Unexpected: it must not become a bounded PublicationError retry, and must
    # be the programmer's serialization error rather than a transport/storage one.
    assert not isinstance(error, PublicationError)
    assert isinstance(error, (TypeError, ValueError)), error

    rows = await helpers.fetch_outbox(raw_db, payment.id, "payment.created")
    assert rows[0]["published_at"] is None
    assert rows[0]["publish_attempts"] == 0
    assert rows[0]["last_error_code"] is None
    assert rows[0]["next_publish_at"] == rows[0]["created_at"]
    assert await _ready_messages(settings, PAYMENTS_QUEUE_NAME) == 0


class _AckAfterCommitProbe:
    """Real inbound message whose channel dies exactly at the ACK boundary."""

    def __init__(
        self,
        message: RabbitMessage,
        *,
        dsn: str,
        event_id: uuid.UUID,
        lose_channel: Callable[[], Awaitable[None]],
    ) -> None:
        self._message = message
        self._dsn = dsn
        self._event_id = event_id
        # An explicit close also disables aio-pika's robust auto-restore, so
        # the consumer cannot recapture the delivery before the test observes it.
        self._lose_channel = lose_channel
        self.channel = message.raw_message.channel
        self.persisted_status: str | None = None
        self.ack_error: BaseException | None = None

    @property
    def raw_message(self):
        return self._message.raw_message

    def __getattr__(self, name):
        # Stay a transparent stand-in for the real RabbitMessage; only ``ack``
        # is intercepted at the commit/ACK boundary.
        return getattr(self._message, name)

    async def ack(self) -> None:
        # Independent connection: it can only see what the receiver committed.
        connection = await asyncpg.connect(self._dsn)
        try:
            self.persisted_status = await connection.fetchval(
                "SELECT status FROM inbox WHERE event_id = $1",
                self._event_id,
            )
        finally:
            await connection.close()

        # Lose the channel before Basic.Ack can reach the broker.
        await self._lose_channel()
        try:
            await self._message.ack()
        except BaseException as exc:
            self.ack_error = exc
            raise


async def test_inbox_commit_survives_channel_loss_before_ack(
    settings,
    uow_factory,
    raw_db,
    payment_service,
    event_publisher,
    start_receiver,
    payment_receiver,
    infrastructure,
    webhook_url,
) -> None:
    payment = await payment_service.create(
        helpers.payment_dto(f"{webhook_url}/ok"),
        f"ack-loss-{uuid.uuid4().hex}",
    )
    event_id = await helpers.initial_event_id(raw_db, payment.id)

    capturing = create_broker(settings)
    captured: asyncio.Future[tuple[bytes, RabbitMessage]] = (
        asyncio.get_running_loop().create_future()
    )
    capturing_closed = False

    async def capture(body: bytes, message: RabbitMessage) -> None:
        # Keep the delivery unACKed; the test drives the ACK boundary below.
        if not captured.done():
            captured.set_result((body, message))

    async def lose_capturing_channel() -> None:
        nonlocal capturing_closed
        await capturing.stop()
        capturing_closed = True

    connection = None
    try:
        # Keep the real connection so cleanup can close it unconditionally.
        connection = await capturing.connect()
        await declare_topology(capturing)
        register_receiver(capturing, capture)
        await capturing.start()

        await _publish_raw(
            settings,
            json.dumps({"event_id": str(event_id), "payment_id": str(payment.id)}).encode(),
            message_id=str(event_id),
        )
        body, message = await asyncio.wait_for(captured, timeout=10)

        probe = _AckAfterCommitProbe(
            message,
            dsn=infrastructure.database_dsn,
            event_id=event_id,
            lose_channel=lose_capturing_channel,
        )
        receiver = PaymentReceiver(uow_factory, event_publisher)
        await asyncio.wait_for(receiver(body, probe), timeout=10)

        # The Inbox commit was already durable when the ACK boundary was
        # reached, and the ACK then failed on the lost channel.
        assert probe.persisted_status == "ready"
        assert probe.ack_error is not None
        assert probe.channel.is_closed
        inbox = await helpers.fetch_inbox(raw_db, event_id)
        assert inbox is not None
        assert inbox["status"] == "ready"
        assert inbox["attempts"] == 0
    finally:
        try:
            if not capturing_closed:
                await capturing.stop()
        finally:
            if connection is not None:
                # Idempotent even when the broker stop above already closed it.
                await connection.close()

    # The connection loss returned the unACKed delivery to the source queue.
    await helpers.wait_until(
        lambda: _source_available(settings),
        description="unacked source requeue",
    )

    # The broker redelivers the same message id; the duplicate must be inert.
    consumer = await start_receiver(payment_receiver)
    await helpers.wait_until(
        lambda: _source_consumed(settings),
        description="redelivery delivery",
    )
    await _assert_acked_after_stop(settings, consumer)

    assert (
        await raw_db.fetchval("SELECT count(*) FROM inbox WHERE payment_id = $1", payment.id) == 1
    )
    after = await helpers.fetch_inbox(raw_db, event_id)
    assert after["status"] == "ready"
    assert after["attempts"] == 0
    assert after["next_attempt_at"] == inbox["next_attempt_at"]


async def test_invalid_source_is_redelivered_when_diagnostic_cannot_be_routed(
    settings,
    infrastructure,
    start_receiver,
    payment_receiver,
    raw_db,
    gateway,
    caplog,
) -> None:
    attempt_finished = asyncio.Event()
    completed_attempts = 0
    receiver_logger = "payment_resolver.modules.messaging.receiver"
    caplog.set_level(logging.DEBUG, logger=receiver_logger)

    async def observed_receiver(body: bytes, message: RabbitMessage) -> None:
        nonlocal completed_attempts
        await payment_receiver(body, message)
        completed_attempts += 1
        if completed_attempts >= 2:
            attempt_finished.set()

    consumer = await start_receiver(observed_receiver)
    await helpers.set_binding(
        infrastructure,
        present=False,
        exchange="payments.dlx",
        queue=DLQ_QUEUE_NAME,
        routing_key=DLQ_QUEUE_NAME,
    )
    body = b"invalid payment event during diagnostic outage"
    message_id = "invalid-diagnostic-unroutable"
    try:
        await _publish_raw(settings, body, message_id=message_id)
        await asyncio.wait_for(attempt_finished.wait(), timeout=10)
        await consumer.stop()
        # A real failed mandatory publish must not ACK the original. Closing
        # the receiver makes any unACKed delivery synchronously observable.
        assert await _ready_messages(settings, PAYMENTS_QUEUE_NAME) == 1
        assert await _ready_messages(settings, DLQ_QUEUE_NAME) == 0
        assert await raw_db.fetchval("SELECT count(*) FROM inbox") == 0
        assert gateway.count == 0
        failures = [
            record
            for record in caplog.records
            if record.name == receiver_logger
            and getattr(record, "event", None) == "inbox.diagnostic_failed"
            and record.levelno >= logging.WARNING
        ]
        assert len(failures) == 1
        assert not any(
            record.name == receiver_logger
            and getattr(record, "event", None)
            in {
                "inbox.transport_recovered",
                "inbox.diagnostic_recovered",
            }
            for record in caplog.records
        )
    finally:
        await helpers.set_binding(
            infrastructure,
            present=True,
            exchange="payments.dlx",
            queue=DLQ_QUEUE_NAME,
            routing_key=DLQ_QUEUE_NAME,
        )
        await consumer.stop()

    recovered = await start_receiver(payment_receiver)
    diagnostic = await helpers.wait_for_queue_message(
        infrastructure,
        DLQ_QUEUE_NAME,
        lambda item: item["properties"].get("message_id") == _diagnostic_id(message_id, body),
    )
    assert diagnostic["payload"]["reason"] == "invalid_message"
    assert diagnostic["payload"]["original_message_id"] == message_id
    assert base64.b64decode(diagnostic["payload"]["body_base64"]) == body
    await _assert_acked_after_stop(settings, recovered)
    assert await raw_db.fetchval("SELECT count(*) FROM inbox") == 0
    assert gateway.count == 0
    recoveries = [
        record
        for record in caplog.records
        if record.name == receiver_logger
        and getattr(record, "event", None) == "inbox.diagnostic_recovered"
    ]
    assert len(recoveries) == 1
    assert all(
        getattr(record, "event_id", None) != message_id
        for record in caplog.records
        if record.name == receiver_logger
    )
