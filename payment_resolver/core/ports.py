from datetime import datetime
from decimal import Decimal
from types import TracebackType
from typing import Protocol
from uuid import UUID

from payment_resolver.core.payments.dtos import PaymentCreatedEventDTO, PaymentResultWebhookDTO
from payment_resolver.core.payments.enums import Currency, PaymentStatus
from payment_resolver.core.payments.models import Payment
from payment_resolver.core.processing.models import InboxEntry, OutboxEntry


class PaymentRepository(Protocol):
    async def create_or_get(self, candidate: Payment) -> tuple[Payment, bool]: ...
    async def get(self, payment_id: UUID) -> Payment | None: ...
    async def finalize(
        self, payment_id: UUID, status: PaymentStatus, processed_at: datetime
    ) -> Payment: ...


class InboxRepository(Protocol):
    async def receive(self, event: PaymentCreatedEventDTO, now: datetime) -> None: ...
    async def lock_next_due(self, now: datetime) -> InboxEntry | None: ...
    async def lock_next_expired(self, now: datetime) -> InboxEntry | None: ...
    async def lock(self, event_id: UUID) -> InboxEntry | None: ...
    async def save(self, entry: InboxEntry) -> None: ...


class OutboxRepository(Protocol):
    async def add(self, entry: OutboxEntry) -> None: ...
    async def lock_next_due(self, now: datetime) -> OutboxEntry | None: ...
    async def save(self, entry: OutboxEntry) -> None: ...


class UnitOfWork(Protocol):
    """One transaction; leaving without ``commit`` rolls back."""

    payments: PaymentRepository
    inbox: InboxRepository
    outbox: OutboxRepository

    async def __aenter__(self) -> "UnitOfWork": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...
    async def now(self) -> datetime: ...
    async def commit(self) -> None: ...


class UnitOfWorkFactory(Protocol):
    def __call__(self) -> UnitOfWork: ...


class Gateway(Protocol):
    async def charge(
        self,
        *,
        payment_id: UUID,
        amount: Decimal,
        currency: Currency,
        description: str,
        idempotency_key: str,
    ) -> PaymentStatus: ...


class WebhookSender(Protocol):
    async def send(self, url: str, event: PaymentResultWebhookDTO) -> None: ...


class EventPublisher(Protocol):
    async def publish(self, entry: OutboxEntry) -> None: ...
