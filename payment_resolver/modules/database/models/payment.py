from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import JsonValue
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    LargeBinary,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class PaymentRow(Base):
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"),
        CheckConstraint(
            "octet_length(request_fingerprint) = 32",
            name="ck_payments_fingerprint_size",
        ),
        CheckConstraint(
            "(status = 'pending') = (processed_at IS NULL)",
            name="ck_payments_processed_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[dict[str, JsonValue]] = mapped_column("metadata", JSONB, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_fingerprint: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    webhook_url: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
