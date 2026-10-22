import asyncio
import logging
from typing import Protocol
from uuid import UUID

from aio_pika.exceptions import AMQPError, ChannelInvalidStateError
from faststream.rabbit import RabbitMessage
from pydantic import ValidationError

from payment_resolver.core.payments.dtos import PaymentCreatedEventDTO
from payment_resolver.core.payments.exceptions import (
    InvalidIncomingEvent,
    PublicationError,
    StorageUnavailable,
)
from payment_resolver.core.ports import UnitOfWorkFactory
from payment_resolver.modules.logging import log_extra

logger = logging.getLogger(__name__)

REQUEUE_DELAY_SECONDS = 1.0


def _log_event_id(value: str | None) -> str | None:
    # Invalid AMQP properties are untrusted data, not log context.
    if value is None:
        return None
    try:
        return str(UUID(value))
    except ValueError:
        return None


class InvalidEventPublisher(Protocol):
    async def publish_invalid(self, body: bytes, original_message_id: str | None) -> None: ...


class FatalSignal:
    """Hands the first unexpected receiver failure to the process supervisor."""

    __slots__ = ("_future",)

    def __init__(self) -> None:
        self._future: asyncio.Future[BaseException] | None = None

    def bind(self) -> asyncio.Future[BaseException]:
        self._future = asyncio.get_running_loop().create_future()
        return self._future

    def report(self, error: BaseException) -> None:
        future = self._future
        if future is not None and not future.done():
            future.set_exception(error)


class PaymentReceiver:
    """Commit valid work to Inbox before ACK; confirm invalid-message diagnostics before ACK."""

    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        publisher: InvalidEventPublisher,
        fatal: FatalSignal | None = None,
        requeue_delay_seconds: float = REQUEUE_DELAY_SECONDS,
    ) -> None:
        self._uow_factory = uow_factory
        self._publisher = publisher
        self._fatal = fatal
        self._requeue_delay_seconds = requeue_delay_seconds
        # Outages persist across redeliveries; report first failure + recovery.
        self._storage_outage = False
        self._transport_outage = False
        self._diagnostic_outage = False

    async def __call__(self, body: bytes, message: RabbitMessage) -> None:
        try:
            await self._receive(body, message)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 - unexpected failures are fatal
            self._report_fatal(exc)
            raise

    async def _receive(self, body: bytes, message: RabbitMessage) -> None:
        original_message_id = self._message_id(message)
        event = self._decode(body)

        if (
            event is None
            or original_message_id is None
            or original_message_id != str(event.event_id)
        ):
            await self._discard(body, original_message_id, message)
            return

        try:
            async with self._uow_factory() as uow:
                if await uow.payments.get(event.payment_id) is None:
                    raise InvalidIncomingEvent("Unknown payment")
                await uow.inbox.receive(event, await uow.now())
                await uow.commit()
        except InvalidIncomingEvent:
            logger.warning(
                "Invalid incoming event",
                extra=log_extra("inbox.invalid_event", event_id=_log_event_id(original_message_id)),
            )
            await self._discard(body, original_message_id, message)
            return
        except StorageUnavailable:
            if not self._storage_outage:
                logger.warning(
                    "Inbox storage unavailable; requeueing",
                    extra=log_extra(
                        "inbox.storage_unavailable", event_id=_log_event_id(original_message_id)
                    ),
                )
                self._storage_outage = True
            await self._requeue(message)
            return

        if self._storage_outage:
            logger.info(
                "Inbox storage recovered",
                extra=log_extra(
                    "inbox.storage_recovered", event_id=_log_event_id(original_message_id)
                ),
            )
            self._storage_outage = False
        await self._ack(message)

    @staticmethod
    def _decode(body: bytes) -> PaymentCreatedEventDTO | None:
        try:
            return PaymentCreatedEventDTO.model_validate_json(body)
        except (ValidationError, ValueError):
            return None

    async def _discard(
        self,
        body: bytes,
        original_message_id: str | None,
        message: RabbitMessage,
    ) -> None:
        try:
            await self._publisher.publish_invalid(body, original_message_id)
        except PublicationError:
            if self._diagnostic_outage:
                logger.debug(
                    "Invalid-message diagnostic still failing",
                    extra=log_extra(
                        "inbox.diagnostic_failed", event_id=_log_event_id(original_message_id)
                    ),
                )
            else:
                logger.warning(
                    "Failed to publish invalid-message diagnostic; requeueing",
                    extra=log_extra(
                        "inbox.diagnostic_failed", event_id=_log_event_id(original_message_id)
                    ),
                )
                self._diagnostic_outage = True
            await self._requeue(message)
            return

        if self._diagnostic_outage:
            logger.info(
                "Invalid-message diagnostic publication recovered",
                extra=log_extra(
                    "inbox.diagnostic_recovered",
                    event_id=_log_event_id(original_message_id),
                ),
            )
            self._diagnostic_outage = False
        await self._ack(message)

    async def _ack(self, message: RabbitMessage) -> None:
        try:
            await message.ack()
        except (AMQPError, ChannelInvalidStateError, OSError):
            # The Inbox commit or the diagnostic publication is already durable.
            # Whether the ACK reached the broker is unknown; a redelivery
            # replays the stored event or republishes the diagnostic under the
            # same id, so a failed ACK must not stop the consumer.
            if not self._transport_outage:
                logger.warning(
                    "Failed to ACK a message",
                    extra=log_extra(
                        "inbox.ack_failed", event_id=_log_event_id(self._message_id(message))
                    ),
                    exc_info=True,
                )
                self._transport_outage = True
        else:
            self._report_transport_recovered(self._message_id(message))

    async def _requeue(self, message: RabbitMessage) -> None:
        await asyncio.sleep(self._requeue_delay_seconds)
        try:
            await message.nack(requeue=True)
        except (AMQPError, ChannelInvalidStateError, OSError):
            # Whether the NACK reached the broker is unknown. An unacknowledged
            # delivery returns to the queue when the channel closes.
            if not self._transport_outage:
                logger.warning(
                    "Failed to NACK a message",
                    extra=log_extra(
                        "inbox.nack_failed", event_id=_log_event_id(self._message_id(message))
                    ),
                    exc_info=True,
                )
                self._transport_outage = True
        else:
            self._report_transport_recovered(self._message_id(message))

    def _report_transport_recovered(self, event_id: str | None) -> None:
        if not self._transport_outage:
            return
        logger.info(
            "Broker transport recovered",
            extra=log_extra("inbox.transport_recovered", event_id=_log_event_id(event_id)),
        )
        self._transport_outage = False

    @staticmethod
    def _message_id(message: RabbitMessage) -> str | None:
        return message.raw_message.message_id

    def _report_fatal(self, error: BaseException) -> None:
        if self._fatal is not None:
            self._fatal.report(error)
