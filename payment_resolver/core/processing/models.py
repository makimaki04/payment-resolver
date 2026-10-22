from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID


class InboxStatus(StrEnum):
    ready = "ready"
    processing = "processing"
    retry_wait = "retry_wait"
    completed = "completed"
    dead = "dead"


class OutboxKind(StrEnum):
    payment_created = "payment.created"
    payment_processing_failed = "payment.processing_failed"


@dataclass
class InboxEntry:
    event_id: UUID
    payment_id: UUID
    status: InboxStatus
    attempts: int
    next_attempt_at: datetime | None
    lock_token: UUID | None
    locked_until: datetime | None
    last_error_code: str | None
    last_http_status: int | None
    created_at: datetime
    updated_at: datetime


@dataclass
class OutboxEntry:
    id: UUID
    payment_id: UUID
    kind: OutboxKind
    payload: dict[str, Any]
    created_at: datetime
    next_publish_at: datetime
    published_at: datetime | None = None
    publish_attempts: int = 0
    last_error_code: str | None = None


@dataclass(frozen=True)
class ProcessingClaim:
    """Snapshot of a claimed Inbox row: owner token and lease deadline."""

    event_id: UUID
    payment_id: UUID
    attempts: int
    lock_token: UUID
    locked_until: datetime
