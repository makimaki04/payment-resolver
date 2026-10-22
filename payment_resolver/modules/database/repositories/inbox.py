from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from payment_resolver.core.payments.dtos import PaymentCreatedEventDTO
from payment_resolver.core.payments.exceptions import InvalidIncomingEvent
from payment_resolver.core.processing.models import InboxEntry, InboxStatus
from payment_resolver.core.processing.retry import MAX_ATTEMPTS
from payment_resolver.modules.database.errors import raise_storage_error
from payment_resolver.modules.database.models import InboxRow

from .base import _DB_ERRORS, _SessionRepository


def _inbox_values(entry: InboxEntry) -> dict[str, Any]:
    return {
        "status": entry.status.value,
        "attempts": entry.attempts,
        "next_attempt_at": entry.next_attempt_at,
        "lock_token": entry.lock_token,
        "locked_until": entry.locked_until,
        "last_error_code": entry.last_error_code,
        "last_http_status": entry.last_http_status,
        "updated_at": entry.updated_at,
    }


def _to_inbox(row: InboxRow) -> InboxEntry:
    return InboxEntry(
        event_id=row.event_id,
        payment_id=row.payment_id,
        status=InboxStatus(row.status),
        attempts=row.attempts,
        next_attempt_at=row.next_attempt_at,
        lock_token=row.lock_token,
        locked_until=row.locked_until,
        last_error_code=row.last_error_code,
        last_http_status=row.last_http_status,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _is_foreign_key_violation(exc: IntegrityError) -> bool:
    orig = getattr(exc, "orig", None)
    sqlstate = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
    return isinstance(sqlstate, str) and sqlstate == "23503"


class SqlAlchemyInboxRepository(_SessionRepository):
    async def receive(self, event: PaymentCreatedEventDTO, now: datetime) -> None:
        """Insert one Inbox event or confirm an identical duplicate.

        The event id is the deduplication key. A duplicate with another payment
        id, a payment already linked to another event, or a missing payment row
        raises ``InvalidIncomingEvent``.
        """
        insert_statement = (
            pg_insert(InboxRow)
            .values(
                event_id=event.event_id,
                payment_id=event.payment_id,
                status=InboxStatus.ready.value,
                attempts=0,
                next_attempt_at=now,
                lock_token=None,
                locked_until=None,
                last_error_code=None,
                last_http_status=None,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_nothing()
            .returning(InboxRow.event_id)
        )
        try:
            async with self._session.begin_nested():
                inserted = await self._session.scalar(insert_statement)
        except IntegrityError as exc:
            if _is_foreign_key_violation(exc):
                raise InvalidIncomingEvent(
                    "Payment referenced by the event does not exist"
                ) from exc
            raise_storage_error(exc)
        except _DB_ERRORS as exc:
            raise_storage_error(exc)
        if inserted is not None:
            return
        existing = await self._first(select(InboxRow).where(InboxRow.event_id == event.event_id))
        if existing is not None:
            if existing.payment_id != event.payment_id:
                raise InvalidIncomingEvent("Event identity conflicts with an existing inbox entry")
            return
        conflicting = await self._first(
            select(InboxRow).where(InboxRow.payment_id == event.payment_id)
        )
        if conflicting is not None:
            raise InvalidIncomingEvent("Payment is already linked to another event")
        raise RuntimeError("Conflicting inbox entry disappeared after INSERT")

    async def lock_next_due(self, now: datetime) -> InboxEntry | None:
        """Lock the earliest due event; ``SKIP LOCKED`` keeps executors apart."""
        row = await self._first(
            select(InboxRow)
            .where(
                InboxRow.status.in_((InboxStatus.ready.value, InboxStatus.retry_wait.value)),
                InboxRow.next_attempt_at.is_not(None),
                InboxRow.next_attempt_at <= now,
                InboxRow.attempts < MAX_ATTEMPTS,
            )
            .order_by(InboxRow.next_attempt_at, InboxRow.event_id)
            .limit(1)
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
        return None if row is None else _to_inbox(row)

    async def lock_next_expired(self, now: datetime) -> InboxEntry | None:
        """Lock the oldest processing event whose lease has expired."""
        row = await self._first(
            select(InboxRow)
            .where(
                InboxRow.status == InboxStatus.processing.value,
                InboxRow.locked_until.is_not(None),
                InboxRow.locked_until <= now,
            )
            .order_by(InboxRow.locked_until, InboxRow.event_id)
            .limit(1)
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
        return None if row is None else _to_inbox(row)

    async def lock(self, event_id: UUID) -> InboxEntry | None:
        row = await self._first(
            select(InboxRow)
            .where(InboxRow.event_id == event_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        return None if row is None else _to_inbox(row)

    async def save(self, entry: InboxEntry) -> None:
        await self._execute(
            update(InboxRow)
            .where(InboxRow.event_id == entry.event_id)
            .values(**_inbox_values(entry))
        )
