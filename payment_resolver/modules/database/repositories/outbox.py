from datetime import datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from payment_resolver.core.processing.models import OutboxEntry, OutboxKind
from payment_resolver.modules.database.models import OutboxRow

from .base import _SessionRepository


def _outbox_values(entry: OutboxEntry) -> dict[str, Any]:
    return {
        "id": entry.id,
        "payment_id": entry.payment_id,
        "kind": entry.kind.value,
        "payload": entry.payload,
        "created_at": entry.created_at,
        "published_at": entry.published_at,
        "publish_attempts": entry.publish_attempts,
        "next_publish_at": entry.next_publish_at,
        "last_error_code": entry.last_error_code,
    }


def _to_outbox(row: OutboxRow) -> OutboxEntry:
    return OutboxEntry(
        id=row.id,
        payment_id=row.payment_id,
        kind=OutboxKind(row.kind),
        payload=row.payload,
        created_at=row.created_at,
        next_publish_at=row.next_publish_at,
        published_at=row.published_at,
        publish_attempts=row.publish_attempts,
        last_error_code=row.last_error_code,
    )


class SqlAlchemyOutboxRepository(_SessionRepository):
    async def add(self, entry: OutboxEntry) -> None:
        await self._execute(
            pg_insert(OutboxRow).values(**_outbox_values(entry)).on_conflict_do_nothing()
        )

    async def lock_next_due(self, now: datetime) -> OutboxEntry | None:
        """Lock the oldest unpublished row whose publish time has come."""
        row = await self._first(
            select(OutboxRow)
            .where(
                OutboxRow.published_at.is_(None),
                OutboxRow.next_publish_at <= now,
            )
            .order_by(OutboxRow.next_publish_at, OutboxRow.id)
            .limit(1)
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
        return None if row is None else _to_outbox(row)

    async def save(self, entry: OutboxEntry) -> None:
        await self._execute(
            update(OutboxRow)
            .where(OutboxRow.id == entry.id)
            .values(
                publish_attempts=entry.publish_attempts,
                published_at=entry.published_at,
                next_publish_at=entry.next_publish_at,
                last_error_code=entry.last_error_code,
            )
        )
