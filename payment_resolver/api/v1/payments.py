from typing import Annotated, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from pydantic import AfterValidator

from payment_resolver.api.security import get_settings, require_api_key
from payment_resolver.core.payments.dtos import (
    CreatePaymentRequestDTO,
    CreatePaymentResponseDTO,
    PaymentResponseDTO,
)
from payment_resolver.core.payments.exceptions import (
    IdempotencyConflict,
    PaymentNotFound,
    StorageUnavailable,
)
from payment_resolver.core.payments.urls import require_allowed_url
from payment_resolver.modules.config import Settings
from payment_resolver.modules.payments.logic import PaymentService

router = APIRouter(
    prefix="/payments",
    dependencies=[Depends(require_api_key)],
)


def _validate_idempotency_key(value: str) -> str:
    if not value.strip():
        raise ValueError("Idempotency-Key must not be blank")
    return value


IdempotencyKey = Annotated[
    str,
    Header(alias="Idempotency-Key", min_length=1, max_length=255),
    AfterValidator(_validate_idempotency_key),
]


def get_payment_service(request: Request) -> PaymentService:
    return cast(PaymentService, request.app.state.payment_service)


PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


def _invalid_webhook_url(message: str, value: str) -> RequestValidationError:
    return RequestValidationError(
        [
            {
                "type": "value_error",
                "loc": ("body", "webhook_url"),
                "msg": message,
                "input": value,
            }
        ]
    )


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=CreatePaymentResponseDTO,
)
async def create_payment(
    payload: CreatePaymentRequestDTO,
    idempotency_key: IdempotencyKey,
    service: PaymentServiceDep,
    settings: SettingsDep,
) -> CreatePaymentResponseDTO:
    try:
        require_allowed_url(payload.webhook_url, settings.WEBHOOK_ALLOWED_ORIGINS)
    except ValueError as exc:
        raise _invalid_webhook_url(str(exc), payload.webhook_url) from exc

    try:
        payment = await service.create(payload, idempotency_key)
    except IdempotencyConflict as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key conflicts with an existing payment",
        ) from exc
    except StorageUnavailable as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage temporarily unavailable",
        ) from exc
    except ValueError as exc:
        # The service performs the same allowlist check; an invalid webhook URL
        # is a request validation failure, not a server error.
        raise _invalid_webhook_url(str(exc), payload.webhook_url) from exc

    return CreatePaymentResponseDTO(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentResponseDTO,
)
async def get_payment(
    payment_id: UUID,
    service: PaymentServiceDep,
) -> PaymentResponseDTO:
    try:
        payment = await service.get(payment_id)
    except PaymentNotFound as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found"
        ) from exc
    except StorageUnavailable as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage temporarily unavailable",
        ) from exc
    return PaymentResponseDTO.from_payment(payment)
