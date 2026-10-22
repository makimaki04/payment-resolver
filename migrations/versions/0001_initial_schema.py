"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-10-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("request_fingerprint", sa.LargeBinary(length=32), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        sa.CheckConstraint(
            "octet_length(request_fingerprint) = 32",
            name="ck_payments_fingerprint_size",
        ),
        sa.CheckConstraint(
            "(status = 'pending') = (processed_at IS NULL)",
            name="ck_payments_processed_at",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')",
            name="ck_payments_status",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_payments"),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
    )

    op.create_table(
        "inbox",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("payment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("attempts", sa.SmallInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lock_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.Text(), nullable=True),
        sa.Column("last_http_status", sa.SmallInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("attempts BETWEEN 0 AND 3", name="ck_inbox_attempts"),
        sa.CheckConstraint(
            "last_http_status IS NULL OR last_http_status BETWEEN 100 AND 599",
            name="ck_inbox_last_http_status",
        ),
        sa.CheckConstraint(
            "status IN ('ready', 'processing', 'retry_wait', 'completed', 'dead')",
            name="ck_inbox_status",
        ),
        sa.CheckConstraint(
            "status <> 'processing' OR (attempts >= 1"
            " AND lock_token IS NOT NULL AND locked_until IS NOT NULL"
            " AND next_attempt_at IS NULL)",
            name="ck_inbox_processing_schedule",
        ),
        sa.CheckConstraint(
            "status NOT IN ('ready', 'retry_wait') OR (next_attempt_at IS NOT NULL"
            " AND lock_token IS NULL AND locked_until IS NULL)",
            name="ck_inbox_waiting_schedule",
        ),
        sa.CheckConstraint(
            "status NOT IN ('completed', 'dead') OR (next_attempt_at IS NULL"
            " AND lock_token IS NULL AND locked_until IS NULL)",
            name="ck_inbox_terminal_schedule",
        ),
        sa.CheckConstraint("status <> 'ready' OR attempts = 0", name="ck_inbox_ready_attempts"),
        sa.CheckConstraint(
            "status <> 'retry_wait' OR attempts IN (1, 2)",
            name="ck_inbox_retry_attempts",
        ),
        sa.CheckConstraint(
            "status <> 'completed' OR attempts BETWEEN 1 AND 3",
            name="ck_inbox_completed_attempts",
        ),
        sa.CheckConstraint("status <> 'dead' OR attempts = 3", name="ck_inbox_dead_attempts"),
        sa.ForeignKeyConstraint(
            ["payment_id"],
            ["payments.id"],
            name="fk_inbox_payment_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("event_id", name="pk_inbox"),
        sa.UniqueConstraint("payment_id", name="uq_inbox_payment_id"),
    )
    op.create_index(
        "ix_inbox_due",
        "inbox",
        ["next_attempt_at", "event_id"],
        unique=False,
        postgresql_where=sa.text("status IN ('ready', 'retry_wait')"),
    )
    op.create_index(
        "ix_inbox_expired",
        "inbox",
        ["locked_until", "event_id"],
        unique=False,
        postgresql_where=sa.text("status = 'processing'"),
    )

    op.create_table(
        "outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("payment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "publish_attempts",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("next_publish_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_error_code", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "kind IN ('payment.created', 'payment.processing_failed')",
            name="ck_outbox_kind",
        ),
        sa.CheckConstraint("publish_attempts >= 0", name="ck_outbox_publish_attempts"),
        sa.ForeignKeyConstraint(
            ["payment_id"],
            ["payments.id"],
            name="fk_outbox_payment_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_outbox"),
        sa.UniqueConstraint("payment_id", "kind", name="uq_outbox_payment_kind"),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["next_publish_at", "id"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_unpublished", table_name="outbox")
    op.drop_table("outbox")
    op.drop_index("ix_inbox_expired", table_name="inbox")
    op.drop_index("ix_inbox_due", table_name="inbox")
    op.drop_table("inbox")
    op.drop_table("payments")
