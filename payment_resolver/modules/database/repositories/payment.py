from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from payment_resolver.core.payments.enums import Currency, PaymentStatus
from payment_resolver.core.payments.exceptions import (
    FinancialStateError,
    IdempotencyConflict,
    PaymentNotFound,
)
from payment_resolver.core.payments.models import Payment
from payment_resolver.modules.database.models import PaymentRow

from .base import _SessionRepository


def _payment_values(payment: Payment) -> dict[str, Any]:
    return {
        "id": payment.id,
        "amount": payment.amount,
        "currency": payment.currency.value,
        "description": payment.description,
        "metadata_json": payment.metadata,
        "status": payment.status.value,
        "idempotency_key": payment.idempotency_key,
        "request_fingerprint": payment.request_fingerprint,
        "webhook_url": payment.webhook_url,
        "created_at": payment.created_at,
        "processed_at": payment.processed_at,
    }


def _to_payment(row: PaymentRow) -> Payment:
    return Payment(
        id=row.id,
        amount=Decimal(row.amount),
        currency=Currency(row.currency),
        description=row.description,
        metadata=row.metadata_json,
        status=PaymentStatus(row.status),
        idempotency_key=row.idempotency_key,
        request_fingerprint=bytes(row.request_fingerprint),
        webhook_url=row.webhook_url,
        created_at=row.created_at,
        processed_at=row.processed_at,
    )


class SqlAlchemyPaymentRepository(_SessionRepository):
    async def create_or_get(self, candidate: Payment) -> tuple[Payment, bool]:
        """Insert the payment or return the row stored for its idempotency key.

        Raises ``IdempotencyConflict`` when the existing row came from a
        different request fingerprint.
        """
        insert_statement = (
            pg_insert(PaymentRow)
            .values(**_payment_values(candidate))
            .on_conflict_do_nothing(index_elements=["idempotency_key"])
            .returning(PaymentRow)
        )
        select_statement = select(PaymentRow).where(
            PaymentRow.idempotency_key == candidate.idempotency_key
        )
        # ON CONFLICT DO NOTHING waits for the competing transaction, so under
        # READ COMMITTED the following SELECT observes its committed row.
        inserted = await self._first(insert_statement)
        if inserted is not None:
            return _to_payment(inserted), True
        existing = await self._first(select_statement)
        if existing is None:
            raise RuntimeError("Conflicting payment disappeared after INSERT")
        if existing.request_fingerprint != candidate.request_fingerprint:
            raise IdempotencyConflict(candidate.idempotency_key)
        return _to_payment(existing), False

    async def get(self, payment_id: UUID) -> Payment | None:
        row = await self._first(select(PaymentRow).where(PaymentRow.id == payment_id))
        return None if row is None else _to_payment(row)

    async def finalize(
        self, payment_id: UUID, status: PaymentStatus, processed_at: datetime
    ) -> Payment:
        """Apply a terminal status to a pending payment.

        A repeated identical result returns the stored row unchanged; a
        different one raises ``FinancialStateError``.
        """
        if status not in (PaymentStatus.succeeded, PaymentStatus.failed):
            raise FinancialStateError("A financial result must be terminal")
        statement = (
            update(PaymentRow)
            .where(
                PaymentRow.id == payment_id,
                PaymentRow.status == PaymentStatus.pending.value,
            )
            .values(status=status.value, processed_at=processed_at)
            .returning(PaymentRow)
        )
        finalized = await self._first(statement)
        if finalized is not None:
            return _to_payment(finalized)
        existing = await self._first(
            select(PaymentRow)
            .where(PaymentRow.id == payment_id)
            .execution_options(populate_existing=True)
        )
        if existing is None:
            raise PaymentNotFound(payment_id)
        if existing.status == status.value:
            return _to_payment(existing)
        raise FinancialStateError("Financial result is immutable")
