import hashlib
import json
import math
import re
from datetime import datetime
from decimal import Decimal
from typing import Any, Self
from uuid import UUID, uuid5

from pydantic import (
    BaseModel,
    ConfigDict,
    JsonValue,
    field_serializer,
    field_validator,
    model_validator,
)

from payment_resolver.core.payments.enums import Currency, PaymentStatus
from payment_resolver.core.payments.models import Payment
from payment_resolver.core.payments.urls import normalize_url

_AMOUNT = re.compile(r"(?:0|[1-9][0-9]{0,15})(?:\.[0-9]{1,2})?", re.ASCII)


class DTO(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class CreatePaymentRequestDTO(DTO):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str

    @field_validator("amount", mode="before", json_schema_input_type=str)
    @classmethod
    def validate_amount(cls, value: object) -> Decimal:
        if not isinstance(value, str) or _AMOUNT.fullmatch(value) is None:
            raise ValueError("Amount must be a plain decimal string with up to two fraction digits")
        amount = Decimal(value)
        if amount <= 0:
            raise ValueError("Amount must be positive")
        return amount.quantize(Decimal("0.01"))

    @field_validator("metadata", mode="before")
    @classmethod
    def validate_metadata(cls, value: object) -> dict[str, Any]:
        def check(item: object) -> None:
            if isinstance(item, float) and not math.isfinite(item):
                raise ValueError("Metadata numbers must be finite")
            if isinstance(item, dict):
                if any(not isinstance(key, str) for key in item):
                    raise ValueError("Metadata keys must be strings")
                for child in item.values():
                    check(child)
            elif isinstance(item, list):
                for child in item:
                    check(child)

        if not isinstance(value, dict):
            raise ValueError("Metadata must be an object")
        check(value)
        return value

    @field_validator("webhook_url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        return normalize_url(value)

    @field_serializer("amount")
    def serialize_amount(self, value: Decimal) -> str:
        return format(value, ".2f")

    def fingerprint(self) -> bytes:
        """Canonical hash of the request, used to detect idempotency key reuse."""
        canonical = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        return hashlib.sha256(canonical.encode("utf-8")).digest()


class CreatePaymentResponseDTO(DTO):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentResponseDTO(CreatePaymentResponseDTO):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    idempotency_key: str
    webhook_url: str
    processed_at: datetime | None

    @field_serializer("amount")
    def serialize_amount(self, value: Decimal) -> str:
        return format(value, ".2f")

    @classmethod
    def from_payment(cls, payment: Payment) -> Self:
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class PaymentCreatedEventDTO(DTO):
    """Inbox handoff event; ``event_id`` also serves as the transport message id."""

    event_id: UUID
    payment_id: UUID


class PaymentResultWebhookDTO(DTO):
    """Terminal result notification with an id derived from the payment."""

    event_id: UUID
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    created_at: datetime
    processed_at: datetime

    @model_validator(mode="after")
    def terminal_only(self) -> Self:
        if self.status == PaymentStatus.pending:
            raise ValueError("Webhook requires a financial result")
        return self

    @field_serializer("amount")
    def serialize_amount(self, value: Decimal) -> str:
        return format(value, ".2f")

    @classmethod
    def from_payment(cls, payment: Payment) -> Self:
        return cls.model_validate(
            {
                "event_id": uuid5(payment.id, "payment.result"),
                "payment_id": payment.id,
                "status": payment.status,
                "amount": payment.amount,
                "currency": payment.currency,
                "created_at": payment.created_at,
                "processed_at": payment.processed_at,
            }
        )
