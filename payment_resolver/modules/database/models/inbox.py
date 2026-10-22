from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    SmallInteger,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class InboxRow(Base):
    __tablename__ = "inbox"
    __table_args__ = (
        UniqueConstraint("payment_id", name="uq_inbox_payment_id"),
        CheckConstraint(
            "status IN ('ready', 'processing', 'retry_wait', 'completed', 'dead')",
            name="ck_inbox_status",
        ),
        CheckConstraint("attempts BETWEEN 0 AND 3", name="ck_inbox_attempts"),
        CheckConstraint(
            "last_http_status IS NULL OR last_http_status BETWEEN 100 AND 599",
            name="ck_inbox_last_http_status",
        ),
        CheckConstraint(
            "status <> 'processing' OR (attempts >= 1"
            " AND lock_token IS NOT NULL AND locked_until IS NOT NULL"
            " AND next_attempt_at IS NULL)",
            name="ck_inbox_processing_schedule",
        ),
        CheckConstraint(
            "status NOT IN ('ready', 'retry_wait') OR (next_attempt_at IS NOT NULL"
            " AND lock_token IS NULL AND locked_until IS NULL)",
            name="ck_inbox_waiting_schedule",
        ),
        CheckConstraint(
            "status NOT IN ('completed', 'dead') OR (next_attempt_at IS NULL"
            " AND lock_token IS NULL AND locked_until IS NULL)",
            name="ck_inbox_terminal_schedule",
        ),
        CheckConstraint("status <> 'ready' OR attempts = 0", name="ck_inbox_ready_attempts"),
        CheckConstraint(
            "status <> 'retry_wait' OR attempts IN (1, 2)",
            name="ck_inbox_retry_attempts",
        ),
        CheckConstraint(
            "status <> 'completed' OR attempts BETWEEN 1 AND 3",
            name="ck_inbox_completed_attempts",
        ),
        CheckConstraint("status <> 'dead' OR attempts = 3", name="ck_inbox_dead_attempts"),
        Index(
            "ix_inbox_due",
            "next_attempt_at",
            "event_id",
            postgresql_where=text("status IN ('ready', 'retry_wait')"),
        ),
        Index(
            "ix_inbox_expired",
            "locked_until",
            "event_id",
            postgresql_where=text("status = 'processing'"),
        ),
    )

    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    payment_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("payments.id", name="fk_inbox_payment_id", ondelete="RESTRICT"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(Text, nullable=False)
    attempts: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("0"))
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lock_token: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), nullable=True)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_http_status: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
