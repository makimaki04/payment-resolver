import asyncio
import base64
import hashlib
import logging
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import aiormq
from aio_pika.exceptions import AMQPError, ChannelInvalidStateError
from faststream.middlewares import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage
from faststream.rabbit.schemas import (
    Channel,
    ExchangeType,
    QueueType,
    RabbitExchange,
    RabbitQueue,
)

from payment_resolver.core.payments.exceptions import PublicationError
from payment_resolver.core.processing.models import OutboxEntry, OutboxKind
from payment_resolver.core.processing.retry import PUBLISH_TIMEOUT_SECONDS
from payment_resolver.modules.config import Settings

logger = logging.getLogger(__name__)

PAYMENTS_EXCHANGE_NAME = "payments"
PAYMENTS_QUEUE_NAME = "payments.new"
PAYMENTS_ROUTING_KEY = "payments.new"
DLX_EXCHANGE_NAME = "payments.dlx"
DLQ_QUEUE_NAME = "payments.dlq"
DLQ_ROUTING_KEY = "payments.dlq"

INVALID_BODY_LIMIT_BYTES = 4096

PAYMENTS_EXCHANGE = RabbitExchange(
    PAYMENTS_EXCHANGE_NAME,
    type=ExchangeType.DIRECT,
    durable=True,
    auto_delete=False,
)
PAYMENTS_QUEUE = RabbitQueue(
    PAYMENTS_QUEUE_NAME,
    queue_type=QueueType.QUORUM,
    durable=True,
    exclusive=False,
    auto_delete=False,
    # The broker must not drop a redelivered event before the Inbox handoff;
    # the application attempt budget lives in the Inbox, not in the broker.
    arguments={"x-delivery-limit": -1},
)
DLX_EXCHANGE = RabbitExchange(
    DLX_EXCHANGE_NAME,
    type=ExchangeType.DIRECT,
    durable=True,
    auto_delete=False,
)
DLQ_QUEUE = RabbitQueue(
    DLQ_QUEUE_NAME,
    queue_type=QueueType.QUORUM,
    durable=True,
    exclusive=False,
    auto_delete=False,
)


async def raw_body_decoder(message: RabbitMessage) -> bytes:
    """Return the untouched AMQP body so the receiver owns all validation."""
    return message.body


def create_broker(settings: Settings) -> RabbitBroker:
    # FastStream uses ``log_level`` as the severity of its service messages, not
    # just a threshold. Emit them at INFO and let the WARNING logger threshold
    # filter them; explicit ERROR records still reach the application formatter.
    faststream_logger = logging.getLogger("third_party.faststream")
    faststream_logger.setLevel(logging.WARNING)
    logging.getLogger("faststream").setLevel(logging.WARNING)
    return RabbitBroker(
        settings.RABBITMQ_URL,
        default_channel=Channel(
            publisher_confirms=True,
            on_return_raises=True,
            prefetch_count=1,
        ),
        graceful_timeout=25.0,
        logger=faststream_logger,
        log_level=logging.INFO,
    )


async def declare_topology(broker: RabbitBroker) -> None:
    payments_exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    dlx_exchange = await broker.declare_exchange(DLX_EXCHANGE)
    payments_queue = await broker.declare_queue(PAYMENTS_QUEUE)
    dlq_queue = await broker.declare_queue(DLQ_QUEUE)

    await payments_queue.bind(payments_exchange, routing_key=PAYMENTS_ROUTING_KEY)
    await dlq_queue.bind(dlx_exchange, routing_key=DLQ_ROUTING_KEY)


def register_receiver(
    broker: RabbitBroker,
    receiver: Callable[..., Awaitable[None]],
) -> Any:
    subscriber = broker.subscriber(
        PAYMENTS_QUEUE,
        PAYMENTS_EXCHANGE,
        ack_policy=AckPolicy.MANUAL,
        decoder=raw_body_decoder,
    )
    subscriber(receiver)
    return subscriber


class RabbitEventPublisher:
    """A publication succeeds only after a positive Basic.Ack."""

    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker
        # One shared channel is used by the Outbox loop and the receiver, so
        # publications (including diagnostics) must never interleave.
        self._lock = asyncio.Lock()

    async def publish(self, entry: OutboxEntry) -> None:
        if entry.kind == OutboxKind.payment_created:
            exchange, routing_key = PAYMENTS_EXCHANGE_NAME, PAYMENTS_ROUTING_KEY
        else:
            exchange, routing_key = DLX_EXCHANGE_NAME, DLQ_ROUTING_KEY

        await self._publish_confirmed(
            entry.payload,
            exchange=exchange,
            routing_key=routing_key,
            message_id=str(entry.id),
        )

    async def publish_invalid(self, body: bytes, original_message_id: str | None) -> None:
        """Publish a truncated copy of an invalid body to the diagnostic queue.

        The dedup id combines the original message id and the full-body digest,
        so repeated deliveries of one invalid message map to the same id.
        """
        original_message_id = original_message_id or None
        truncated = body[:INVALID_BODY_LIMIT_BYTES]
        digest = hashlib.sha256(body).hexdigest()
        transport_id = uuid5(
            NAMESPACE_URL,
            f"invalid-message:{original_message_id or ''}:{digest}",
        )

        payload: dict[str, Any] = {
            "reason": "invalid_message",
            "body_base64": base64.b64encode(truncated).decode("ascii"),
            "original_body_bytes": len(body),
            "body_truncated": len(body) > INVALID_BODY_LIMIT_BYTES,
        }
        if original_message_id is not None:
            payload["original_message_id"] = original_message_id

        await self._publish_confirmed(
            payload,
            exchange=DLX_EXCHANGE_NAME,
            routing_key=DLQ_ROUTING_KEY,
            message_id=str(transport_id),
        )

    async def _publish_confirmed(
        self,
        payload: Any,
        *,
        exchange: str,
        routing_key: str,
        message_id: str,
    ) -> None:
        try:
            # The outer deadline also covers waiting for the lock and for
            # robust reconnect / exchange resolution, not just socket I/O.
            async with asyncio.timeout(PUBLISH_TIMEOUT_SECONDS):
                async with self._lock:
                    result = await self._broker.publish(
                        payload,
                        exchange=exchange,
                        routing_key=routing_key,
                        persist=True,
                        mandatory=True,
                        message_id=message_id,
                        timeout=float(PUBLISH_TIMEOUT_SECONDS),
                    )
        except TimeoutError as exc:
            raise PublicationError("publish_timeout") from exc
        except asyncio.CancelledError:
            raise
        except (AMQPError, ChannelInvalidStateError, OSError) as exc:
            raise PublicationError("publish_failed") from exc

        if not isinstance(result, aiormq.spec.Basic.Ack):
            raise PublicationError("publish_not_confirmed")

        logger.debug("published message %s to exchange %s", message_id, exchange)
