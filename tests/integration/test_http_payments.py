from __future__ import annotations

import asyncio
import json
import uuid

import asyncpg
import httpx
import pytest

from payment_resolver.core.payments.dtos import PaymentCreatedEventDTO
from tests.integration import helpers

_MISSING_PAYMENT = "00000000-0000-0000-0000-000000000001"


def _key() -> str:
    return f"http-{uuid.uuid4().hex}"


def _body(receiver_url: str, **overrides) -> dict:
    body = {
        "amount": "100.25",
        "currency": "RUB",
        "description": "http integration",
        "metadata": {"order": 1},
        "webhook_url": f"{receiver_url}/ok",
    }
    body.update(overrides)
    return body


async def test_authentication_rejects_missing_wrong_and_raw_non_ascii_keys(
    api_client: httpx.AsyncClient,
    api_headers: dict[str, str],
    webhook_url: str,
    raw_db,
) -> None:
    key = _key()

    missing = await api_client.get(f"/api/v1/payments/{_MISSING_PAYMENT}")
    assert missing.status_code == 401
    assert missing.json() == {"detail": "Invalid API key"}

    wrong = await api_client.get(
        f"/api/v1/payments/{_MISSING_PAYMENT}", headers={"X-API-Key": "wrong-key"}
    )
    assert wrong.status_code == 401
    assert wrong.json() == {"detail": "Invalid API key"}

    # Raw non-ASCII bytes must not raise a TypeError while comparing the key.
    raw = httpx.Headers([(b"x-api-key", b"\xff\xfe\x80")])
    raw_wrong = await api_client.get(f"/api/v1/payments/{_MISSING_PAYMENT}", headers=raw)
    assert raw_wrong.status_code == 401
    assert raw_wrong.json() == {"detail": "Invalid API key"}

    unauthorized_post = await api_client.post(
        "/api/v1/payments",
        headers={"Idempotency-Key": key},
        json=_body(webhook_url),
    )
    assert unauthorized_post.status_code == 401
    assert (
        await raw_db.fetchval(
            "SELECT count(*) FROM payments WHERE idempotency_key = $1",
            key,
        )
        == 0
    )

    valid = await api_client.post(
        "/api/v1/payments",
        headers={**api_headers, "Idempotency-Key": key},
        json=_body(webhook_url),
    )
    assert valid.status_code == 202, valid.text


@pytest.mark.parametrize(
    "amount",
    [
        "0",
        "0.00",
        "-1.00",
        "+1.00",
        "1.234",
        "01.00",
        "1e3",
        " 1.00",
        "1.00 ",
        ".50",
        "NaN",
        "Infinity",
        "10000000000000000.00",
    ],
)
async def test_invalid_amount_forms_are_rejected_without_storage(
    api_client: httpx.AsyncClient,
    api_headers: dict[str, str],
    webhook_url: str,
    raw_db,
    amount: str,
) -> None:
    key = _key()
    response = await api_client.post(
        "/api/v1/payments",
        headers={**api_headers, "Idempotency-Key": key},
        json=_body(webhook_url, amount=amount),
    )
    assert response.status_code == 422, (amount, response.text)
    assert (
        await raw_db.fetchval(
            "SELECT count(*) FROM payments WHERE idempotency_key = $1",
            key,
        )
        == 0
    )


async def test_json_number_amount_is_rejected(api_client, api_headers, webhook_url) -> None:
    key = _key()
    body = _body(webhook_url)
    body["amount"] = 100.25
    response = await api_client.post(
        "/api/v1/payments",
        headers={**api_headers, "Idempotency-Key": key},
        json=body,
    )
    assert response.status_code == 422


@pytest.mark.parametrize(
    "overrides",
    [
        {"currency": "GBP"},
        {"metadata": []},
        {"metadata": None},
        {"metadata": "not-an-object"},
        {"description": None},
        {"webhook_url": "https://untrusted.example/ok"},
        {"webhook_url": "relative/path"},
        {"webhook_url": "http://user:secret@127.0.0.1/ok"},
        {"webhook_url": "https://untrusted.example/ok#fragment"},
        {"extra": "unknown"},
    ],
)
async def test_invalid_payloads_return_422(
    api_client: httpx.AsyncClient,
    api_headers: dict[str, str],
    webhook_url: str,
    raw_db,
    overrides: dict,
) -> None:
    key = _key()
    body = _body(webhook_url, **overrides)
    response = await api_client.post(
        "/api/v1/payments",
        headers={**api_headers, "Idempotency-Key": key},
        json=body,
    )
    assert response.status_code == 422, (overrides, response.text)
    assert (
        await raw_db.fetchval(
            "SELECT count(*) FROM payments WHERE idempotency_key = $1",
            key,
        )
        == 0
    )


async def test_non_finite_metadata_is_rejected(api_client, api_headers, webhook_url) -> None:
    key = _key()
    raw_body = json.dumps(_body(webhook_url)).replace('{"order": 1}', '{"value": NaN}')
    response = await api_client.post(
        "/api/v1/payments",
        headers={**api_headers, "Idempotency-Key": key, "Content-Type": "application/json"},
        content=raw_body,
    )
    assert response.status_code == 422, response.text


@pytest.mark.parametrize("idempotency_key", [None, "", "   ", "x" * 256])
async def test_invalid_idempotency_key_is_rejected(
    api_client,
    api_headers,
    webhook_url,
    idempotency_key,
) -> None:
    headers = dict(api_headers)
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    response = await api_client.post("/api/v1/payments", headers=headers, json=_body(webhook_url))
    assert response.status_code == 422, response.text


async def test_wrong_uuid_forms_and_unknown_payment(
    api_client,
    api_headers,
) -> None:
    malformed = await api_client.get("/api/v1/payments/not-a-uuid", headers=api_headers)
    assert malformed.status_code == 422

    missing = await api_client.get(f"/api/v1/payments/{_MISSING_PAYMENT}", headers=api_headers)
    assert missing.status_code == 404
    assert missing.json() == {"detail": "Payment not found"}


async def test_create_and_get_round_trip(api_client, api_headers, webhook_url, raw_db) -> None:
    key = _key()
    created = await api_client.post(
        "/api/v1/payments",
        headers={**api_headers, "Idempotency-Key": key},
        json=_body(webhook_url, amount="100.2"),
    )
    assert created.status_code == 202, created.text
    payload = created.json()
    assert set(payload) == {"payment_id", "status", "created_at"}
    assert payload["status"] == "pending"
    payment_id = payload["payment_id"]

    row = await raw_db.fetchrow("SELECT * FROM payments WHERE id = $1", uuid.UUID(payment_id))
    assert row is not None
    assert str(row["amount"]) == "100.20"
    assert len(row["request_fingerprint"]) == 32

    fetched = await api_client.get(f"/api/v1/payments/{payment_id}", headers=api_headers)
    assert fetched.status_code == 200
    detail = fetched.json()
    assert set(detail) == {
        "payment_id",
        "amount",
        "currency",
        "description",
        "metadata",
        "status",
        "idempotency_key",
        "webhook_url",
        "created_at",
        "processed_at",
    }
    assert detail["payment_id"] == payment_id
    assert detail["amount"] == "100.20"
    assert detail["status"] == "pending"
    assert detail["processed_at"] is None
    assert detail["idempotency_key"] == key
    assert detail["metadata"] == {"order": 1}


async def test_equivalent_request_reuses_payment_and_current_status(
    api_client,
    api_headers,
    webhook_url,
    raw_db,
) -> None:
    key = _key()
    first = await api_client.post(
        "/api/v1/payments",
        headers={**api_headers, "Idempotency-Key": key},
        json=_body(webhook_url, amount="100.2", metadata={"b": 2, "a": 1}),
    )
    assert first.status_code == 202, first.text

    equivalent = await api_client.post(
        "/api/v1/payments",
        headers={**api_headers, "Idempotency-Key": key},
        json=_body(webhook_url, amount="100.20", metadata={"a": 1, "b": 2}),
    )
    assert equivalent.status_code == 202, equivalent.text
    assert equivalent.json()["payment_id"] == first.json()["payment_id"]
    assert equivalent.json()["created_at"] == first.json()["created_at"]
    assert equivalent.json()["status"] == "pending"

    assert (
        await raw_db.fetchval(
            "SELECT count(*) FROM payments WHERE idempotency_key = $1",
            key,
        )
        == 1
    )
    payment_id = uuid.UUID(first.json()["payment_id"])
    assert (
        await raw_db.fetchval(
            "SELECT count(*) FROM outbox WHERE payment_id = $1 AND kind = 'payment.created'",
            payment_id,
        )
        == 1
    )

    conflict = await api_client.post(
        "/api/v1/payments",
        headers={**api_headers, "Idempotency-Key": key},
        json=_body(webhook_url, amount="100.21", metadata={"a": 1, "b": 2}),
    )
    assert conflict.status_code == 409
    assert conflict.json() == {
        "detail": "Idempotency key conflicts with an existing payment",
    }


async def test_repeat_after_terminal_result_returns_current_status_without_new_work(
    api_client,
    api_headers,
    webhook_url,
    raw_db,
    processing_service,
    gateway,
) -> None:
    key = _key()
    created = await api_client.post(
        "/api/v1/payments",
        headers={**api_headers, "Idempotency-Key": key},
        json=_body(webhook_url),
    )
    assert created.status_code == 202, created.text
    payment_id = uuid.UUID(created.json()["payment_id"])

    event_id = await helpers.initial_event_id(raw_db, payment_id)
    await processing_service.receive(
        PaymentCreatedEventDTO(event_id=event_id, payment_id=payment_id),
    )
    claim = await processing_service.claim_next()
    assert claim is not None
    await processing_service.process(claim)

    row = await raw_db.fetchrow(
        "SELECT status, processed_at FROM payments WHERE id = $1", payment_id
    )
    assert row["status"] == "succeeded"
    assert row["processed_at"] is not None
    outbox_before = await raw_db.fetchval(
        "SELECT count(*) FROM outbox WHERE payment_id = $1",
        payment_id,
    )

    repeat = await api_client.post(
        "/api/v1/payments",
        headers={**api_headers, "Idempotency-Key": key},
        json=_body(webhook_url),
    )
    assert repeat.status_code == 202
    assert repeat.json()["payment_id"] == str(payment_id)
    assert repeat.json()["status"] == "succeeded"
    assert (
        await raw_db.fetchval(
            "SELECT count(*) FROM outbox WHERE payment_id = $1",
            payment_id,
        )
        == outbox_before
    )
    assert gateway.count == 1


async def test_twenty_concurrent_identical_requests_create_one_payment_and_one_outbox(
    api_client,
    api_headers,
    webhook_url,
    raw_db,
) -> None:
    key = _key()
    responses = await asyncio.gather(
        *[
            api_client.post(
                "/api/v1/payments",
                headers={**api_headers, "Idempotency-Key": key},
                json=_body(webhook_url, metadata={"n": 1}),
            )
            for _ in range(20)
        ]
    )
    assert all(response.status_code == 202 for response in responses), [
        response.text for response in responses if response.status_code != 202
    ]
    payment_ids = {response.json()["payment_id"] for response in responses}
    assert len(payment_ids) == 1
    payment_id = uuid.UUID(next(iter(payment_ids)))

    assert (
        await raw_db.fetchval(
            "SELECT count(*) FROM payments WHERE idempotency_key = $1",
            key,
        )
        == 1
    )
    assert (
        await raw_db.fetchval(
            "SELECT count(*) FROM outbox WHERE payment_id = $1 AND kind = 'payment.created'",
            payment_id,
        )
        == 1
    )


async def test_twenty_concurrent_conflicting_bodies_have_one_winner(
    api_client,
    api_headers,
    webhook_url,
    raw_db,
) -> None:
    key = _key()
    responses = await asyncio.gather(
        *[
            api_client.post(
                "/api/v1/payments",
                headers={**api_headers, "Idempotency-Key": key},
                json=_body(webhook_url, amount=f"{100 + index}.25"),
            )
            for index in range(20)
        ]
    )
    winners = [response for response in responses if response.status_code == 202]
    conflicts = [response for response in responses if response.status_code == 409]
    assert len(winners) == 1, [response.status_code for response in responses]
    assert len(conflicts) == 19
    assert all(
        response.json()
        == {
            "detail": "Idempotency key conflicts with an existing payment",
        }
        for response in conflicts
    )

    assert (
        await raw_db.fetchval(
            "SELECT count(*) FROM payments WHERE idempotency_key = $1",
            key,
        )
        == 1
    )
    payment_id = uuid.UUID(winners[0].json()["payment_id"])
    assert (
        await raw_db.fetchval(
            "SELECT count(*) FROM outbox WHERE payment_id = $1",
            payment_id,
        )
        == 1
    )


async def test_raw_driver_connection_rejection_is_503_without_accepting_payment(
    api_client,
    api_headers,
    webhook_url,
    raw_db,
    monkeypatch,
) -> None:
    key = _key()

    async def reject_connection(*args, **kwargs):
        raise asyncpg.TooManyConnectionsError("isolated connection failure")

    # SQLAlchemy lets this connect-time exception escape without wrapping it.
    # Keep the real already-open observer connection to check durable effects.
    monkeypatch.setattr(asyncpg, "connect", reject_connection)
    response = await api_client.post(
        "/api/v1/payments",
        headers={**api_headers, "Idempotency-Key": key},
        json=_body(webhook_url),
    )
    assert response.status_code == 503
    assert response.json() == {"detail": "Storage temporarily unavailable"}
    assert (
        await raw_db.fetchval(
            "SELECT count(*) FROM payments WHERE idempotency_key = $1",
            key,
        )
        == 0
    )
    assert await raw_db.fetchval("SELECT count(*) FROM outbox") == 0
