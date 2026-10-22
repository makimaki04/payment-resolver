from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import JsonValue

from payment_resolver.core.payments.enums import Currency, PaymentStatus
from payment_resolver.core.payments.exceptions import FinancialStateError


@dataclass(frozen=True)
class Payment:
    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    status: PaymentStatus
    idempotency_key: str
    request_fingerprint: bytes
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None = None

    def finalize(self, status: PaymentStatus, processed_at: datetime) -> "Payment":
        """Return a copy with a terminal result.

        Repeating the stored result is a no-op; a different result raises
        ``FinancialStateError``. First finalization requires a timezone-aware timestamp.
        """
        if status not in (PaymentStatus.succeeded, PaymentStatus.failed):
            raise FinancialStateError("A financial result must be terminal")
        if self.status != PaymentStatus.pending:
            if self.status == status:
                return self
            raise FinancialStateError("Financial result is immutable")
        if processed_at.tzinfo is None or processed_at.utcoffset() is None:
            raise FinancialStateError("Financial timestamp must be timezone-aware")
        return replace(self, status=status, processed_at=processed_at)
