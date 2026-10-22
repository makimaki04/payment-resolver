from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from payment_resolver.core.payments.dtos import CreatePaymentRequestDTO, PaymentResultWebhookDTO
from payment_resolver.core.payments.enums import Currency, PaymentStatus
from payment_resolver.core.payments.exceptions import FinancialStateError
from payment_resolver.core.payments.models import Payment
from payment_resolver.core.payments.urls import require_allowed_url
from payment_resolver.core.processing.retry import retry_delay
from payment_resolver.modules.gateway.emulator import stable_outcome


def request(**overrides):
    return CreatePaymentRequestDTO.model_validate(
        {
            "amount": "100.25",
            "currency": "RUB",
            "description": "",
            "metadata": {},
            "webhook_url": "http://receiver:8081/ok",
            **overrides,
        }
    )


@pytest.mark.parametrize(
    "amount",
    [
        100,
        1.1,
        None,
        True,
        "0",
        "0.00",
        "-1",
        "+1",
        "01",
        "1e2",
        "1.",
        ".1",
        "1.001",
        "10000000000000000",
        " 1",
        "1\n",
        "NaN",
        "Infinity",
        "１２",
    ],
)
def test_rejects_amount_without_exact_supported_representation(amount):
    with pytest.raises(ValidationError):
        request(amount=amount)


@pytest.mark.parametrize(
    "amount,expected",
    [
        ("0.01", "0.01"),
        ("1", "1.00"),
        ("1.2", "1.20"),
        ("9999999999999999.99", "9999999999999999.99"),
    ],
)
def test_money_round_trip_is_exact(amount, expected):
    dto = request(amount=amount)
    assert dto.amount == Decimal(expected)
    assert dto.model_dump(mode="json")["amount"] == expected


def test_amount_input_schema_requires_a_string():
    schema = CreatePaymentRequestDTO.model_json_schema(mode="validation")
    assert schema["properties"]["amount"]["type"] == "string"


@pytest.mark.parametrize(
    "metadata", [None, [], 2, "x", {"nested": [float("nan")]}, {"x": {"y": float("inf")}}]
)
def test_metadata_requires_object_with_finite_nested_values(metadata):
    with pytest.raises(ValidationError):
        request(metadata=metadata)


def test_fingerprint_canonicalizes_amount_url_and_nested_object_order():
    first = request(
        amount="100",
        webhook_url="HTTP://RECEIVER:80/ok",
        metadata={"b": [True, {"z": "текст", "a": None}], "a": 1},
    )
    equivalent = request(
        amount="100.00",
        webhook_url="http://receiver/ok",
        metadata={"a": 1, "b": [True, {"a": None, "z": "текст"}]},
    )
    assert first.fingerprint() == equivalent.fingerprint()
    changed_type = equivalent.model_copy(
        update={"metadata": {"a": "1", "b": equivalent.metadata["b"]}}
    )
    assert first.fingerprint() != changed_type.fingerprint()
    changed_amount = equivalent.model_copy(update={"amount": Decimal("100.01")})
    assert first.fingerprint() != changed_amount.fingerprint()


@pytest.mark.parametrize("field", ["amount", "currency", "description", "metadata", "webhook_url"])
def test_every_input_field_is_required(field):
    data = request().model_dump(mode="json")
    del data[field]
    with pytest.raises(ValidationError):
        CreatePaymentRequestDTO.model_validate(data)


@pytest.mark.parametrize(
    "override", [{"extra": "value"}, {"currency": "rub"}, {"description": None}, {"metadata": None}]
)
def test_request_rejects_unknown_or_wrongly_typed_values(override):
    with pytest.raises(ValidationError):
        request(**override)


@pytest.mark.parametrize(
    "url",
    [
        "http://receiver:8081@evil/",
        "http://user@receiver:8081/",
        "http://receiver:8081/#x",
        "http://receiver:8081/#",
        "ftp://receiver/",
        "/relative",
        "http://@receiver:8081/ok",
    ],
)
def test_invalid_webhook_urls_are_rejected(url):
    with pytest.raises(ValidationError):
        request(webhook_url=url)


def test_allowed_origin_is_exact_and_url_keeps_path_and_query():
    assert (
        require_allowed_url("HTTPS://RECEIVER:443/a?q=1", ["https://receiver"])
        == "https://receiver/a?q=1"
    )
    for url in ("https://sub.receiver/a", "http://receiver/a", "https://receiver:444/a"):
        with pytest.raises(ValueError):
            require_allowed_url(url, ["https://receiver"])


@pytest.fixture
def pending():
    return Payment(
        id=uuid4(),
        amount=Decimal("1.00"),
        currency=Currency.USD,
        description="",
        metadata={},
        status=PaymentStatus.pending,
        idempotency_key="consumer-key",
        request_fingerprint=b"x" * 32,
        webhook_url="http://receiver:8081/ok",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


@pytest.mark.parametrize("status", [PaymentStatus.succeeded, PaymentStatus.failed])
def test_financial_result_and_time_are_immutable(pending, status):
    processed = pending.created_at + timedelta(seconds=3)
    final = pending.finalize(status, processed)
    assert pending.status == PaymentStatus.pending and pending.processed_at is None
    assert final.status == status and final.processed_at == processed
    assert final.finalize(status, processed + timedelta(days=1)) is final
    opposite = (
        PaymentStatus.failed if status == PaymentStatus.succeeded else PaymentStatus.succeeded
    )
    with pytest.raises(FinancialStateError):
        final.finalize(opposite, processed)
    with pytest.raises(FinancialStateError):
        pending.finalize(PaymentStatus.pending, processed)


def test_webhook_requires_saved_financial_result(pending):
    with pytest.raises(ValidationError):
        PaymentResultWebhookDTO.from_payment(pending)


def test_webhook_event_and_body_are_stable_for_saved_result(pending):
    final = pending.finalize(PaymentStatus.succeeded, pending.created_at + timedelta(seconds=2))
    first = PaymentResultWebhookDTO.from_payment(final)
    second = PaymentResultWebhookDTO.from_payment(
        replace(final, metadata={"secret": True}, description="private")
    )
    assert first.model_dump(mode="json") == second.model_dump(mode="json")
    assert first.event_id != final.id
    assert first.model_dump(mode="json")["amount"] == "1.00"


@pytest.mark.parametrize(
    "attempt,jitter,expected", [(1, 0.8, 4), (1, 1.2, 6), (2, 0.8, 8), (2, 1.2, 12)]
)
def test_retry_schedule_boundaries(attempt, jitter, expected):
    assert retry_delay(attempt, jitter, 5) == timedelta(seconds=expected)


@pytest.mark.parametrize(
    "attempt,jitter,base",
    [(0, 1, 5), (3, 1, 5), (1, 0.79, 5), (1, 1.21, 5), (1, 1, 0), (1, 1, float("nan"))],
)
def test_retry_cannot_allocate_fourth_attempt_or_invalid_delay(attempt, jitter, base):
    with pytest.raises(ValueError):
        retry_delay(attempt, jitter, base)


def test_gateway_outcome_known_vectors_and_stability():
    # Independently fixed SHA-256 bucket vectors, not a random distribution test.
    assert stable_outcome("00000000-0000-0000-0000-000000000000") == PaymentStatus.succeeded
    assert stable_outcome("00000000-0000-0000-0000-000000000001") == PaymentStatus.succeeded
    assert stable_outcome("00000000-0000-0000-0000-000000000002") == PaymentStatus.failed
