import asyncio
import logging
import random
from collections.abc import Callable
from datetime import timedelta
from uuid import UUID, uuid4, uuid5

from payment_resolver.core.payments.dtos import (
    CreatePaymentRequestDTO,
    PaymentCreatedEventDTO,
    PaymentResultWebhookDTO,
)
from payment_resolver.core.payments.enums import PaymentStatus
from payment_resolver.core.payments.exceptions import (
    GatewayTemporaryError,
    IdempotencyConflict,
    InvalidIncomingEvent,
    PaymentNotFound,
    StorageUnavailable,
    WebhookDeliveryError,
)
from payment_resolver.core.payments.models import Payment
from payment_resolver.core.payments.urls import require_allowed_url
from payment_resolver.core.ports import Gateway, UnitOfWork, UnitOfWorkFactory, WebhookSender
from payment_resolver.core.processing.models import (
    InboxEntry,
    InboxStatus,
    OutboxEntry,
    OutboxKind,
    ProcessingClaim,
)
from payment_resolver.core.processing.retry import (
    MAX_ATTEMPTS,
    PROCESSING_LEASE_SECONDS,
    PROCESSING_TIMEOUT_SECONDS,
    retry_delay,
)

logger = logging.getLogger(__name__)


class PaymentService:
    def __init__(self, uow_factory: UnitOfWorkFactory, allowed_origins: list[str]) -> None:
        self.uow_factory = uow_factory
        self.allowed_origins = allowed_origins

    async def create(self, dto: CreatePaymentRequestDTO, idempotency_key: str) -> Payment:
        """Create a payment and enqueue its event in the same transaction.

        A replay with the same key and fingerprint returns the stored payment.
        The same key with different request data raises ``IdempotencyConflict``.
        """
        require_allowed_url(dto.webhook_url, self.allowed_origins)
        async with self.uow_factory() as uow:
            now = await uow.now()
            candidate = Payment(
                id=uuid4(),
                amount=dto.amount,
                currency=dto.currency,
                description=dto.description,
                metadata=dto.metadata,
                status=PaymentStatus.pending,
                idempotency_key=idempotency_key,
                request_fingerprint=dto.fingerprint(),
                webhook_url=dto.webhook_url,
                created_at=now,
            )
            payment, created = await uow.payments.create_or_get(candidate)
            if payment.request_fingerprint != candidate.request_fingerprint:
                raise IdempotencyConflict()
            if created:
                event_id = uuid4()
                await uow.outbox.add(
                    OutboxEntry(
                        id=event_id,
                        payment_id=payment.id,
                        kind=OutboxKind.payment_created,
                        payload={"event_id": str(event_id), "payment_id": str(payment.id)},
                        created_at=now,
                        next_publish_at=now,
                    )
                )
            await uow.commit()

        if created:
            logger.info(
                "Payment accepted",
                extra={
                    "event": "payment.accepted",
                    "payment_id": str(payment.id),
                    "status": payment.status.value,
                },
            )
        return payment

    async def get(self, payment_id: UUID) -> Payment:
        async with self.uow_factory() as uow:
            payment = await uow.payments.get(payment_id)
            if payment is None:
                raise PaymentNotFound()
            return payment


class ProcessingService:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        gateway: Gateway,
        webhook_sender: WebhookSender,
        retry_base_seconds: float = 5,
        jitter: Callable[[], float] | None = None,
    ) -> None:
        self.uow_factory = uow_factory
        self.gateway = gateway
        self.webhook_sender = webhook_sender
        self.retry_base_seconds = retry_base_seconds
        self.jitter = jitter if jitter is not None else lambda: random.uniform(0.8, 1.2)

    async def receive(self, event: PaymentCreatedEventDTO) -> None:
        async with self.uow_factory() as uow:
            if await uow.payments.get(event.payment_id) is None:
                raise InvalidIncomingEvent("Unknown payment")
            await uow.inbox.receive(event, await uow.now())
            await uow.commit()

    async def claim_next(self) -> ProcessingClaim | None:
        """Take the earliest due Inbox event under ``SKIP LOCKED``.

        The attempt counter is incremented and a lease with a fresh lock token
        is committed before any external call starts.
        """
        async with self.uow_factory() as uow:
            now = await uow.now()
            entry = await uow.inbox.lock_next_due(now)
            if entry is None:
                return None
            entry.attempts += 1
            entry.status = InboxStatus.processing
            entry.lock_token = uuid4()
            entry.locked_until = now + timedelta(seconds=PROCESSING_LEASE_SECONDS)
            entry.next_attempt_at = None
            entry.updated_at = now
            await uow.inbox.save(entry)
            await uow.commit()
            return ProcessingClaim(
                entry.event_id,
                entry.payment_id,
                entry.attempts,
                entry.lock_token,
                entry.locked_until,
            )

    async def _owned(self, uow: UnitOfWork, claim: ProcessingClaim) -> InboxEntry | None:
        entry = await uow.inbox.lock(claim.event_id)
        # Read the clock after acquiring the row lock: waiting for another owner
        # must not make an already-expired lease look valid.
        now = await uow.now()
        if (
            entry is None
            or entry.status != InboxStatus.processing
            or entry.lock_token != claim.lock_token
            or entry.locked_until is None
            or entry.locked_until <= now
        ):
            return None
        return entry

    async def process(self, claim: ProcessingClaim) -> None:
        """Run one attempt; propagate storage failures for lease recovery."""
        try:
            async with asyncio.timeout(PROCESSING_TIMEOUT_SECONDS):
                await self._process(claim)
        except StorageUnavailable:
            # A failed commit has an uncertain outcome. Never repeat external I/O
            # in this invocation; the committed claim is recovered by its lease.
            raise
        except WebhookDeliveryError as exc:
            await self._fail(claim, exc.error_code, exc.http_status)
        except GatewayTemporaryError:
            await self._fail(claim, "gateway_error")
        except TimeoutError:
            await self._fail(claim, "processing_timeout")
        except Exception as exc:
            await self._fail(claim, "processing_error", unexpected=exc)

    async def _process(self, claim: ProcessingClaim) -> None:
        async with self.uow_factory() as uow:
            if await self._owned(uow, claim) is None:
                return
            payment = await uow.payments.get(claim.payment_id)
        if payment is None:
            raise PaymentNotFound()

        if payment.status == PaymentStatus.pending:
            status = await self.gateway.charge(
                payment_id=payment.id,
                amount=payment.amount,
                currency=payment.currency,
                description=payment.description,
                idempotency_key=str(payment.id),
            )
            async with self.uow_factory() as uow:
                if await self._owned(uow, claim) is None:
                    return
                payment = await uow.payments.finalize(payment.id, status, await uow.now())
                await uow.commit()
            logger.info(
                "Financial result committed",
                extra={
                    "event": "payment.financial_result",
                    "payment_id": str(payment.id),
                    "event_id": str(claim.event_id),
                    "attempt": claim.attempts,
                    "status": payment.status.value,
                },
            )

        async with self.uow_factory() as uow:
            if await self._owned(uow, claim) is None:
                return
            # Reload persisted values; every delivery, including a resumed one,
            # uses exactly the same financial result and timestamps.
            payment = await uow.payments.get(claim.payment_id)
        if payment is None:
            raise PaymentNotFound()
        await self.webhook_sender.send(
            payment.webhook_url, PaymentResultWebhookDTO.from_payment(payment)
        )
        async with self.uow_factory() as uow:
            entry = await self._owned(uow, claim)
            if entry is None:
                return
            entry.status = InboxStatus.completed
            entry.next_attempt_at = entry.lock_token = entry.locked_until = None
            entry.last_error_code = entry.last_http_status = None
            entry.updated_at = await uow.now()
            await uow.inbox.save(entry)
            await uow.commit()
        logger.info(
            "Payment processing completed",
            extra={
                "event": "payment.completed",
                "payment_id": str(claim.payment_id),
                "event_id": str(claim.event_id),
                "attempt": claim.attempts,
                "status": InboxStatus.completed.value,
            },
        )

    async def _fail(
        self,
        claim: ProcessingClaim,
        error_code: str,
        http_status: int | None = None,
        unexpected: BaseException | None = None,
    ) -> None:
        async with self.uow_factory() as uow:
            entry = await self._owned(uow, claim)
            if entry is None:
                return
            await self._transition_failure(uow, entry, error_code, http_status)
            await uow.commit()
        self._log_failure(entry, error_code, http_status, unexpected)

    def _log_failure(
        self,
        entry: InboxEntry,
        error_code: str,
        http_status: int | None,
        unexpected: BaseException | None = None,
        recovered: bool = False,
    ) -> None:
        if entry.status is InboxStatus.dead:
            event = "payment.processing_dead"
        elif unexpected is not None:
            event = "payment.processing_error"
        elif recovered:
            event = "payment.recovered"
        else:
            event = "payment.retry_scheduled"

        context: dict[str, object] = {
            "event": event,
            "payment_id": str(entry.payment_id),
            "event_id": str(entry.event_id),
            "attempt": entry.attempts,
            "status": entry.status.value,
            "error_code": error_code,
        }
        if http_status is not None:
            context["http_status"] = http_status
        if entry.status is InboxStatus.dead:
            logger.error(
                "Payment processing exhausted its attempts",
                extra=context,
                exc_info=unexpected,
            )
        elif unexpected is not None:
            logger.error(
                "Unexpected payment processing error",
                extra=context,
                exc_info=unexpected,
            )
        elif recovered:
            logger.warning("Interrupted payment attempt requeued for retry", extra=context)
        else:
            logger.warning("Payment attempt failed; retry scheduled", extra=context)

    async def _transition_failure(
        self, uow: UnitOfWork, entry: InboxEntry, error_code: str, http_status: int | None
    ) -> None:
        """Keep the dead transition and its DLQ Outbox row in the caller's transaction."""
        now = await uow.now()
        entry.last_error_code = error_code
        entry.last_http_status = http_status
        entry.lock_token = entry.locked_until = None
        entry.updated_at = now
        if entry.attempts < MAX_ATTEMPTS:
            entry.status = InboxStatus.retry_wait
            entry.next_attempt_at = now + retry_delay(
                entry.attempts,
                self.jitter(),
                self.retry_base_seconds,
            )
        else:
            entry.status = InboxStatus.dead
            entry.next_attempt_at = None
            payment = await uow.payments.get(entry.payment_id)
            if payment is None:
                raise PaymentNotFound()
            await uow.outbox.add(
                OutboxEntry(
                    id=uuid5(entry.event_id, "dead-letter"),
                    payment_id=entry.payment_id,
                    kind=OutboxKind.payment_processing_failed,
                    created_at=now,
                    next_publish_at=now,
                    payload={
                        "event_id": str(entry.event_id),
                        "payment_id": str(entry.payment_id),
                        "attempts": entry.attempts,
                        "payment_status": payment.status.value,
                        "error_code": error_code,
                        "last_http_status": http_status,
                        "failed_at": now.isoformat(),
                    },
                )
            )
        await uow.inbox.save(entry)

    async def recover_expired(self) -> bool:
        """Recover one expired lease.

        Returns ``True`` when an entry was transitioned to ``retry_wait`` or
        ``dead``.
        """
        async with self.uow_factory() as uow:
            entry = await uow.inbox.lock_next_expired(await uow.now())
            if entry is None:
                return False
            await self._transition_failure(uow, entry, "processing_interrupted", None)
            await uow.commit()
        self._log_failure(entry, "processing_interrupted", None, recovered=True)
        return True
