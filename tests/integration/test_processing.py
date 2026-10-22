from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime

import pytest

from payment_resolver.core.payments.dtos import PaymentResultWebhookDTO
from payment_resolver.core.payments.enums import PaymentStatus
from payment_resolver.core.payments.exceptions import (
    GatewayTemporaryError,
    StorageUnavailable,
    WebhookDeliveryError,
)
from payment_resolver.core.payments.models import Payment
from payment_resolver.core.processing.models import OutboxEntry, OutboxKind
from payment_resolver.core.processing.retry import PROCESSING_LEASE_SECONDS
from payment_resolver.modules.payments.logic import ProcessingService
from tests.integration import helpers


class SlowGateway:
    """Keeps the external call open well past the attempt deadline."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def charge(
        self, *, payment_id, amount, currency, description, idempotency_key
    ) -> PaymentStatus:
        self.calls.append({"payment_id": payment_id, "idempotency_key": idempotency_key})
        await asyncio.sleep(3600)
        return PaymentStatus.succeeded


_RACE_SIGNAL_TIMEOUT_SECONDS = 5.0


class BlockingGateway:
    """Parks the first charge call so a lease can expire while it is executing."""

    def __init__(self, outcomes: list[PaymentStatus | BaseException]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict] = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def charge(
        self, *, payment_id, amount, currency, description, idempotency_key
    ) -> PaymentStatus:
        self.calls.append({"payment_id": payment_id, "idempotency_key": idempotency_key})
        if len(self.calls) == 1:
            self.entered.set()
            await self.release.wait()
        outcome = self.outcomes.pop(0) if self.outcomes else PaymentStatus.succeeded
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    @property
    def count(self) -> int:
        return len(self.calls)


def _webhook_event_id(payment: Payment) -> str:
    """The result notification's stable id; it is not the Inbox event id."""
    return str(uuid.uuid5(payment.id, "payment.result"))


_WEBHOOK_KEYS = {
    "event_id",
    "payment_id",
    "status",
    "amount",
    "currency",
    "created_at",
    "processed_at",
}


def _delay(row: dict) -> float:
    return (row["next_attempt_at"] - row["updated_at"]).total_seconds()


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [(PaymentStatus.succeeded, "succeeded"), (PaymentStatus.failed, "failed")],
)
async def test_gateway_outcome_is_persisted_once_and_notified(
    create_ready_payment,
    processing_service,
    gateway,
    raw_db,
    receipts,
    outcome: PaymentStatus,
    expected: str,
) -> None:
    gateway.outcomes = [outcome]
    payment, event_id = await create_ready_payment(path="/ok")
    webhook_event_id = _webhook_event_id(payment)

    claim = await processing_service.claim_next()
    assert claim is not None and claim.attempts == 1
    await processing_service.process(claim)

    payment_row = await helpers.fetch_payment(raw_db, payment.id)
    inbox = await helpers.fetch_inbox(raw_db, event_id)
    assert payment_row["status"] == expected
    assert payment_row["processed_at"] is not None
    assert inbox["status"] == "completed"
    assert inbox["attempts"] == 1
    assert inbox["next_attempt_at"] is None
    assert inbox["lock_token"] is None and inbox["locked_until"] is None
    assert inbox["last_error_code"] is None

    assert gateway.count == 1
    assert gateway.calls[0]["idempotency_key"] == str(payment.id)
    assert gateway.calls[0]["amount"] == payment.amount

    delivered = await receipts(webhook_event_id)
    assert len(delivered) == 1
    receipt = delivered[0]
    assert receipt["path"] == "/ok"
    assert set(receipt["body"]) == _WEBHOOK_KEYS
    assert receipt["body"]["payment_id"] == str(payment.id)
    assert receipt["body"]["event_id"] == webhook_event_id
    assert receipt["body"]["status"] == expected
    assert receipt["body"]["amount"] == "100.25"
    assert receipt["body"]["currency"] == "RUB"
    assert receipt["body"]["processed_at"] is not None
    assert "x-api-key" not in receipt["headers"]
    assert "idempotency-key" not in receipt["headers"]


async def test_flaky_webhook_uses_three_attempts_with_stable_event_and_no_recharge(
    create_ready_payment,
    processing_service,
    gateway,
    raw_db,
    receipts,
    jitter,
) -> None:
    payment, event_id = await create_ready_payment(path="/flaky")
    webhook_event_id = _webhook_event_id(payment)
    assert jitter.factor == 1.0

    for attempt in (1, 2):
        claim = await processing_service.claim_next()
        assert claim is not None and claim.attempts == attempt
        await processing_service.process(claim)

        inbox = await helpers.fetch_inbox(raw_db, event_id)
        assert inbox["status"] == "retry_wait"
        assert inbox["attempts"] == attempt
        assert inbox["last_error_code"] == "webhook_http_error"
        assert inbox["last_http_status"] == 503
        assert inbox["lock_token"] is None and inbox["locked_until"] is None
        assert inbox["next_attempt_at"] is not None
        helpers.assert_close(
            _delay(inbox),
            helpers.expected_retry_seconds(helpers.RETRY_BASE_SECONDS, attempt, 1.0),
            0.05,
        )
        still_future = await raw_db.fetchval(
            "SELECT next_attempt_at > clock_timestamp() FROM inbox WHERE event_id = $1",
            event_id,
        )
        assert still_future is True
        assert await processing_service.claim_next() is None
        await helpers.force_due(raw_db, event_id)

    claim = await processing_service.claim_next()
    assert claim is not None and claim.attempts == 3
    await processing_service.process(claim)

    inbox = await helpers.fetch_inbox(raw_db, event_id)
    assert inbox["status"] == "completed"
    assert inbox["attempts"] == 3
    assert inbox["next_attempt_at"] is None
    assert inbox["last_error_code"] is None and inbox["last_http_status"] is None
    payment_row = await helpers.fetch_payment(raw_db, payment.id)
    assert payment_row["status"] == "succeeded"
    assert payment_row["processed_at"] is not None

    # The financial result was saved once; later attempts only re-deliver it.
    assert gateway.count == 1
    assert await helpers.fetch_outbox(raw_db, payment.id, "payment.processing_failed") == []

    delivered = await receipts(webhook_event_id)
    assert [item["path"] for item in delivered] == ["/flaky", "/flaky", "/flaky"]
    bodies = {json.dumps(item["body"], sort_keys=True) for item in delivered}
    assert len(bodies) == 1
    assert delivered[-1]["body"]["event_id"] == webhook_event_id
    assert delivered[-1]["body"]["status"] == "succeeded"


async def test_permanent_webhook_failure_is_dead_with_dlq_outbox(
    create_ready_payment,
    processing_service,
    gateway,
    raw_db,
    receipts,
) -> None:
    payment, event_id = await create_ready_payment(path="/fail")
    webhook_event_id = _webhook_event_id(payment)

    for attempt in (1, 2, 3):
        claim = await processing_service.claim_next()
        assert claim is not None and claim.attempts == attempt
        await processing_service.process(claim)
        if attempt == 1:
            scheduled = await helpers.fetch_inbox(raw_db, event_id)
            # The receiver sends "Retry-After: 3600"; it must not shift our schedule.
            helpers.assert_close(
                _delay(scheduled),
                helpers.expected_retry_seconds(helpers.RETRY_BASE_SECONDS, 1, 1.0),
                0.05,
            )
            assert scheduled["last_http_status"] == 503
        if attempt < 3:
            await helpers.force_due(raw_db, event_id)

    inbox = await helpers.fetch_inbox(raw_db, event_id)
    assert inbox["status"] == "dead"
    assert inbox["attempts"] == 3
    assert inbox["next_attempt_at"] is None
    assert inbox["lock_token"] is None and inbox["locked_until"] is None
    assert inbox["last_error_code"] == "webhook_http_error"
    assert inbox["last_http_status"] == 503

    payment_row = await helpers.fetch_payment(raw_db, payment.id)
    assert payment_row["status"] == "succeeded"
    assert payment_row["processed_at"] is not None
    assert gateway.count == 1

    dead_letters = await helpers.fetch_outbox(
        raw_db,
        payment.id,
        "payment.processing_failed",
    )
    assert len(dead_letters) == 1
    payload = dead_letters[0]["payload"]
    assert payload["event_id"] == str(event_id)
    assert payload["payment_id"] == str(payment.id)
    assert payload["attempts"] == 3
    assert payload["payment_status"] == "succeeded"
    assert payload["error_code"] == "webhook_http_error"
    assert payload["last_http_status"] == 503
    assert payload["failed_at"] is not None
    assert dead_letters[0]["published_at"] is None

    delivered = await receipts(webhook_event_id)
    assert len(delivered) == 3
    assert {item["path"] for item in delivered} == {"/fail"}

    # No fourth attempt is scheduled or handed out.
    assert await processing_service.claim_next() is None


async def test_webhook_timeout_exhausts_three_attempts(
    create_ready_payment,
    processing_service,
    gateway,
    raw_db,
    receipts,
) -> None:
    payment, event_id = await create_ready_payment(path="/timeout")
    webhook_event_id = _webhook_event_id(payment)

    for attempt in (1, 2, 3):
        claim = await processing_service.claim_next()
        assert claim is not None and claim.attempts == attempt
        await processing_service.process(claim)
        inbox = await helpers.fetch_inbox(raw_db, event_id)
        assert inbox["last_error_code"] == "webhook_timeout"
        assert inbox["last_http_status"] is None
        if attempt < 3:
            await helpers.force_due(raw_db, event_id)

    inbox = await helpers.fetch_inbox(raw_db, event_id)
    assert inbox["status"] == "dead"
    assert inbox["attempts"] == 3
    assert gateway.count == 1
    delivered = await receipts(webhook_event_id)
    assert len(delivered) == 3
    dead_letters = await helpers.fetch_outbox(
        raw_db,
        payment.id,
        "payment.processing_failed",
    )
    assert len(dead_letters) == 1
    assert dead_letters[0]["payload"]["error_code"] == "webhook_timeout"
    assert dead_letters[0]["payload"]["last_http_status"] is None
    assert dead_letters[0]["payload"]["attempts"] == 3


async def test_permanent_400_exhausts_three_attempts(
    create_ready_payment,
    processing_service,
    gateway,
    raw_db,
    receipts,
) -> None:
    payment, event_id = await create_ready_payment(path="/bad-request")
    webhook_event_id = _webhook_event_id(payment)

    for attempt in (1, 2, 3):
        claim = await processing_service.claim_next()
        assert claim is not None and claim.attempts == attempt
        await processing_service.process(claim)
        if attempt < 3:
            await helpers.force_due(raw_db, event_id)

    inbox = await helpers.fetch_inbox(raw_db, event_id)
    assert inbox["status"] == "dead"
    assert inbox["attempts"] == 3
    assert inbox["last_error_code"] == "webhook_http_error"
    assert inbox["last_http_status"] == 400
    assert gateway.count == 1
    assert len(await receipts(webhook_event_id)) == 3
    dead_letters = await helpers.fetch_outbox(
        raw_db,
        payment.id,
        "payment.processing_failed",
    )
    assert len(dead_letters) == 1
    assert dead_letters[0]["payload"]["error_code"] == "webhook_http_error"
    assert dead_letters[0]["payload"]["last_http_status"] == 400
    assert dead_letters[0]["payload"]["attempts"] == 3


async def test_redirect_is_an_http_failure_and_target_is_never_called(
    create_ready_payment,
    processing_service,
    raw_db,
    receipts,
) -> None:
    payment, event_id = await create_ready_payment(path="/redirect")
    webhook_event_id = _webhook_event_id(payment)

    for attempt in (1, 2, 3):
        claim = await processing_service.claim_next()
        assert claim is not None
        await processing_service.process(claim)
        if attempt < 3:
            inbox = await helpers.fetch_inbox(raw_db, event_id)
            assert inbox["last_error_code"] == "webhook_http_error"
            assert inbox["last_http_status"] == 307
            await helpers.force_due(raw_db, event_id)

    inbox = await helpers.fetch_inbox(raw_db, event_id)
    assert inbox["status"] == "dead"
    delivered = await receipts(webhook_event_id)
    assert len(delivered) == 3
    assert all(item["path"] == "/redirect" for item in delivered)
    all_receipts = await receipts()
    redirected = [
        item
        for item in all_receipts
        if item["path"] == "/redirect-target"
        and (item.get("body") or {}).get("event_id") == webhook_event_id
    ]
    assert redirected == []


async def test_sender_rejects_disallowed_origin_userinfo_and_fragment(
    processing_service,
    create_ready_payment,
    payment_service,
    webhook_sender,
    webhook_url,
) -> None:
    payment, _event_id = await create_ready_payment(path="/ok")
    claim = await processing_service.claim_next()
    await processing_service.process(claim)
    saved = await payment_service.get(payment.id)
    event = PaymentResultWebhookDTO.from_payment(saved)

    with pytest.raises(WebhookDeliveryError) as untrusted:
        await webhook_sender.send("http://untrusted.example/hook", event)
    assert untrusted.value.error_code == "webhook_url_not_allowed"

    with pytest.raises(WebhookDeliveryError) as userinfo:
        await webhook_sender.send(f"{webhook_url.replace('://', '://user:secret@')}/ok", event)
    assert userinfo.value.error_code == "webhook_url_not_allowed"

    with pytest.raises(WebhookDeliveryError) as fragment:
        await webhook_sender.send(f"{webhook_url}/ok#fragment", event)
    assert fragment.value.error_code == "webhook_url_not_allowed"


async def test_expired_lease_recovery_consumes_slot_and_stale_success_cannot_overwrite(
    create_ready_payment,
    uow_factory,
    webhook_sender,
    jitter,
    raw_db,
    receipts,
) -> None:
    payment, event_id = await create_ready_payment(path="/ok")
    webhook_event_id = _webhook_event_id(payment)
    # The stale executor's success races the second owner's failure result.
    gateway = BlockingGateway([PaymentStatus.succeeded, PaymentStatus.failed])
    service = ProcessingService(
        uow_factory,
        gateway,
        webhook_sender,
        retry_base_seconds=helpers.RETRY_BASE_SECONDS,
        jitter=jitter,
    )

    first = await service.claim_next()
    assert first is not None and first.attempts == 1
    old_executor = asyncio.create_task(service.process(first))
    try:
        # The old executor is already inside the external charge call.
        await asyncio.wait_for(gateway.entered.wait(), _RACE_SIGNAL_TIMEOUT_SECONDS)
        assert gateway.count == 1
        pending = await helpers.fetch_payment(raw_db, payment.id)
        assert pending["status"] == "pending" and pending["processed_at"] is None

        await helpers.expire_lease(raw_db, event_id)
        assert await service.recover_expired() is True

        recovered = await helpers.fetch_inbox(raw_db, event_id)
        assert recovered["status"] == "retry_wait"
        assert recovered["attempts"] == 1
        assert recovered["last_error_code"] == "processing_interrupted"
        assert recovered["lock_token"] is None and recovered["locked_until"] is None

        await helpers.force_due(raw_db, event_id)
        second = await service.claim_next()
        assert second is not None and second.attempts == 2
        assert second.lock_token != first.lock_token
        assert second.locked_until > datetime.now(UTC)
        assert (second.locked_until - datetime.now(UTC)).total_seconds() <= (
            PROCESSING_LEASE_SECONDS + 1
        )

        # The old executor succeeds while the second owner holds the row.
        gateway.release.set()
        await asyncio.wait_for(old_executor, _RACE_SIGNAL_TIMEOUT_SECONDS)
    finally:
        gateway.release.set()
        if not old_executor.done():
            old_executor.cancel()
        await asyncio.gather(old_executor, return_exceptions=True)

    stale_payment = await helpers.fetch_payment(raw_db, payment.id)
    stale_inbox = await helpers.fetch_inbox(raw_db, event_id)
    assert stale_payment["status"] == "pending"
    assert stale_payment["processed_at"] is None
    assert stale_inbox["status"] == "processing"
    assert stale_inbox["attempts"] == 2
    assert stale_inbox["lock_token"] == second.lock_token
    assert stale_inbox["locked_until"] == second.locked_until
    assert stale_inbox["next_attempt_at"] is None
    # Recovery's diagnostic is preserved until this owner finalizes.
    assert stale_inbox["last_error_code"] == "processing_interrupted"
    assert gateway.count == 1
    assert gateway.calls[0]["idempotency_key"] == str(payment.id)
    assert await receipts(webhook_event_id) == []

    # The second owner's outcome is authoritative; the stale success lost.
    await service.process(second)
    completed = await helpers.fetch_inbox(raw_db, event_id)
    final_payment = await helpers.fetch_payment(raw_db, payment.id)
    assert completed["status"] == "completed"
    assert completed["attempts"] == 2
    assert completed["last_error_code"] is None
    assert final_payment["status"] == "failed"
    assert final_payment["processed_at"] is not None
    assert gateway.count == 2
    assert gateway.calls[1]["idempotency_key"] == str(payment.id)
    delivered = await receipts(webhook_event_id)
    assert len(delivered) == 1
    assert delivered[0]["body"]["event_id"] == webhook_event_id
    assert delivered[0]["body"]["status"] == "failed"


async def test_stale_executor_error_cannot_overwrite_new_owner(
    create_ready_payment,
    uow_factory,
    webhook_sender,
    jitter,
    raw_db,
    receipts,
) -> None:
    payment, event_id = await create_ready_payment(path="/ok")
    webhook_event_id = _webhook_event_id(payment)
    # The stale executor raises after the second owner has claimed the row.
    gateway = BlockingGateway(
        [
            GatewayTemporaryError("stale technical failure"),
            PaymentStatus.succeeded,
        ]
    )
    service = ProcessingService(
        uow_factory,
        gateway,
        webhook_sender,
        retry_base_seconds=helpers.RETRY_BASE_SECONDS,
        jitter=jitter,
    )

    first = await service.claim_next()
    assert first is not None and first.attempts == 1
    old_executor = asyncio.create_task(service.process(first))
    try:
        await asyncio.wait_for(gateway.entered.wait(), _RACE_SIGNAL_TIMEOUT_SECONDS)
        assert gateway.count == 1

        await helpers.expire_lease(raw_db, event_id)
        assert await service.recover_expired() is True
        await helpers.force_due(raw_db, event_id)
        second = await service.claim_next()
        assert second is not None and second.attempts == 2
        assert second.lock_token != first.lock_token

        # The old executor fails while the second owner holds the row.
        gateway.release.set()
        await asyncio.wait_for(old_executor, _RACE_SIGNAL_TIMEOUT_SECONDS)
    finally:
        gateway.release.set()
        if not old_executor.done():
            old_executor.cancel()
        await asyncio.gather(old_executor, return_exceptions=True)

    stale_inbox = await helpers.fetch_inbox(raw_db, event_id)
    stale_payment = await helpers.fetch_payment(raw_db, payment.id)
    assert stale_inbox["status"] == "processing"
    assert stale_inbox["attempts"] == 2
    assert stale_inbox["lock_token"] == second.lock_token
    assert stale_inbox["locked_until"] == second.locked_until
    assert stale_inbox["next_attempt_at"] is None
    # The stale error did not replace recovery's diagnostic.
    assert stale_inbox["last_error_code"] == "processing_interrupted"
    assert stale_payment["status"] == "pending"
    assert stale_payment["processed_at"] is None
    assert await receipts(webhook_event_id) == []
    # The stale claim did reach the gateway, but its outcome was discarded.
    assert gateway.count == 1
    assert gateway.calls[0]["idempotency_key"] == str(payment.id)

    await service.process(second)
    completed = await helpers.fetch_inbox(raw_db, event_id)
    final_payment = await helpers.fetch_payment(raw_db, payment.id)
    assert completed["status"] == "completed"
    assert completed["attempts"] == 2
    assert completed["last_error_code"] is None
    assert final_payment["status"] == "succeeded"
    assert final_payment["processed_at"] is not None
    assert gateway.count == 2
    assert gateway.calls[1]["idempotency_key"] == str(payment.id)
    delivered = await receipts(webhook_event_id)
    assert len(delivered) == 1
    assert delivered[0]["body"]["event_id"] == webhook_event_id
    assert delivered[0]["body"]["status"] == "succeeded"


async def test_third_interrupted_attempt_dead_letters_without_a_fourth_call(
    create_ready_payment,
    processing_service,
    gateway,
    raw_db,
    receipts,
) -> None:
    payment, event_id = await create_ready_payment(path="/ok")
    webhook_event_id = _webhook_event_id(payment)

    for attempt in (1, 2):
        claim = await processing_service.claim_next()
        assert claim is not None and claim.attempts == attempt
        await helpers.expire_lease(raw_db, event_id)
        assert await processing_service.recover_expired() is True
        inbox = await helpers.fetch_inbox(raw_db, event_id)
        assert inbox["status"] == "retry_wait"
        assert inbox["attempts"] == attempt
        assert inbox["last_error_code"] == "processing_interrupted"
        await helpers.force_due(raw_db, event_id)

    third = await processing_service.claim_next()
    assert third is not None and third.attempts == 3
    await helpers.expire_lease(raw_db, event_id)
    assert await processing_service.recover_expired() is True

    inbox = await helpers.fetch_inbox(raw_db, event_id)
    assert inbox["status"] == "dead"
    assert inbox["attempts"] == 3
    assert inbox["next_attempt_at"] is None
    assert inbox["last_error_code"] == "processing_interrupted"
    assert inbox["last_http_status"] is None
    payment_row = await helpers.fetch_payment(raw_db, payment.id)
    assert payment_row["status"] == "pending"
    assert payment_row["processed_at"] is None
    assert gateway.count == 0
    assert await receipts(webhook_event_id) == []

    dead_letters = await helpers.fetch_outbox(
        raw_db,
        payment.id,
        "payment.processing_failed",
    )
    assert len(dead_letters) == 1
    assert dead_letters[0]["payload"]["attempts"] == 3
    assert dead_letters[0]["payload"]["payment_status"] == "pending"
    assert dead_letters[0]["payload"]["error_code"] == "processing_interrupted"
    assert dead_letters[0]["payload"]["last_http_status"] is None

    assert await processing_service.recover_expired() is False
    assert await processing_service.claim_next() is None


async def test_gateway_technical_failure_retries_with_the_same_operation_key(
    create_ready_payment,
    processing_service,
    gateway,
    raw_db,
    receipts,
) -> None:
    payment, event_id = await create_ready_payment(path="/ok")
    webhook_event_id = _webhook_event_id(payment)
    gateway.outcomes = [GatewayTemporaryError("provider unavailable")]

    first = await processing_service.claim_next()
    await processing_service.process(first)
    inbox = await helpers.fetch_inbox(raw_db, event_id)
    assert inbox["status"] == "retry_wait"
    assert inbox["last_error_code"] == "gateway_error"
    assert inbox["last_http_status"] is None
    payment_row = await helpers.fetch_payment(raw_db, payment.id)
    assert payment_row["status"] == "pending"
    assert await receipts(webhook_event_id) == []

    await helpers.force_due(raw_db, event_id)
    second = await processing_service.claim_next()
    await processing_service.process(second)
    completed = await helpers.fetch_inbox(raw_db, event_id)
    assert completed["status"] == "completed"
    assert gateway.count == 2
    assert gateway.calls[0]["idempotency_key"] == str(payment.id)
    assert gateway.calls[1]["idempotency_key"] == str(payment.id)
    delivered = await receipts(webhook_event_id)
    assert len(delivered) == 1
    assert delivered[0]["body"]["event_id"] == webhook_event_id


async def test_storage_failure_after_webhook_does_not_repeat_http_in_the_same_call(
    create_ready_payment,
    uow_factory,
    gateway,
    webhook_sender,
    jitter,
    raw_db,
    receipts,
) -> None:
    payment, event_id = await create_ready_payment(path="/ok")
    webhook_event_id = _webhook_event_id(payment)

    # Claim commits #1; finalize commits #2; the completed transition commits #3.
    failing_factory = helpers.FailOnCommitUowFactory(uow_factory, fail_on_commit=3)
    service = ProcessingService(
        failing_factory,
        gateway,
        webhook_sender,
        retry_base_seconds=helpers.RETRY_BASE_SECONDS,
        jitter=jitter,
    )

    claim = await service.claim_next()
    assert claim is not None
    with pytest.raises(StorageUnavailable):
        await service.process(claim)

    # The financial result was committed and exactly one webhook attempt happened.
    persisted = await helpers.fetch_payment(raw_db, payment.id)
    assert persisted["status"] == "succeeded"
    assert persisted["processed_at"] is not None
    assert gateway.count == 1
    deliverable = await receipts(webhook_event_id)
    assert len(deliverable) == 1

    # The completion write is still processing, so recovery owns it now.
    inbox = await helpers.fetch_inbox(raw_db, event_id)
    assert inbox["status"] == "processing"
    assert inbox["attempts"] == 1

    await helpers.expire_lease(raw_db, event_id)
    assert await service.recover_expired() is True
    recovered = await helpers.fetch_inbox(raw_db, event_id)
    assert recovered["status"] == "retry_wait"
    assert recovered["last_error_code"] == "processing_interrupted"

    await helpers.force_due(raw_db, event_id)
    second = await service.claim_next()
    assert second is not None and second.attempts == 2
    await service.process(second)

    completed = await helpers.fetch_inbox(raw_db, event_id)
    assert completed["status"] == "completed"
    assert completed["attempts"] == 2
    # No second charge; the resumed attempt only re-delivers the stored result.
    assert gateway.count == 1
    delivered = await receipts(webhook_event_id)
    assert len(delivered) == 2
    assert delivered[0]["body"] == delivered[1]["body"]


async def test_unit_of_work_without_commit_rolls_back(
    payment_service,
    uow_factory,
    raw_db,
    webhook_url,
) -> None:
    payment = await payment_service.create(
        helpers.payment_dto(f"{webhook_url}/ok"),
        f"rollback-{uuid.uuid4().hex}",
    )
    before = await raw_db.fetchval("SELECT count(*) FROM outbox WHERE payment_id = $1", payment.id)

    async with uow_factory() as uow:
        now = await uow.now()
        await uow.outbox.add(
            OutboxEntry(
                id=uuid.uuid4(),
                payment_id=payment.id,
                kind=OutboxKind.payment_processing_failed,
                payload={"payment_id": str(payment.id)},
                created_at=now,
                next_publish_at=now,
            )
        )
        # Leaving without commit must roll the insert back.

    after = await raw_db.fetchval("SELECT count(*) FROM outbox WHERE payment_id = $1", payment.id)
    assert after == before == 1


async def test_processing_timeout_is_shorter_than_the_lease(
    create_ready_payment,
    uow_factory,
    webhook_sender,
    jitter,
    raw_db,
    receipts,
) -> None:
    payment, event_id = await create_ready_payment(path="/ok")
    webhook_event_id = _webhook_event_id(payment)
    slow = SlowGateway()
    service = ProcessingService(
        uow_factory,
        slow,
        webhook_sender,
        retry_base_seconds=helpers.RETRY_BASE_SECONDS,
        jitter=jitter,
    )

    claim = await service.claim_next()
    assert claim is not None
    lease_seconds = await raw_db.fetchval(
        "SELECT extract(epoch FROM locked_until - clock_timestamp()) "
        "FROM inbox WHERE event_id = $1",
        event_id,
    )
    assert 28.0 <= float(lease_seconds) <= 30.5

    # The attempt deadline is 20s, so the handler is cancelled before the lease.
    await service.process(claim)

    inbox = await helpers.fetch_inbox(raw_db, event_id)
    assert inbox["status"] == "retry_wait"
    assert inbox["attempts"] == 1
    assert inbox["last_error_code"] == "processing_timeout"
    assert inbox["last_http_status"] is None
    assert inbox["lock_token"] is None and inbox["locked_until"] is None
    assert len(slow.calls) == 1
    payment_row = await helpers.fetch_payment(raw_db, payment.id)
    assert payment_row["status"] == "pending"
    assert payment_row["processed_at"] is None
    assert await receipts(webhook_event_id) == []


async def test_dead_transition_and_dlq_outbox_are_atomic_on_commit_failure(
    create_ready_payment,
    processing_service,
    uow_factory,
    gateway,
    webhook_sender,
    jitter,
    raw_db,
    receipts,
) -> None:
    payment, event_id = await create_ready_payment(path="/fail")
    webhook_event_id = _webhook_event_id(payment)

    for attempt in (1, 2):
        claim = await processing_service.claim_next()
        assert claim is not None and claim.attempts == attempt
        await processing_service.process(claim)
        await helpers.force_due(raw_db, event_id)

    third = await processing_service.claim_next()
    assert third is not None and third.attempts == 3

    # The dead transition and its DLQ Outbox share commit #1 of this factory.
    failing_factory = helpers.FailOnCommitUowFactory(uow_factory, fail_on_commit=1)
    failing_service = ProcessingService(
        failing_factory,
        gateway,
        webhook_sender,
        retry_base_seconds=helpers.RETRY_BASE_SECONDS,
        jitter=jitter,
    )
    with pytest.raises(StorageUnavailable):
        await failing_service.process(third)

    rolled_back = await helpers.fetch_inbox(raw_db, event_id)
    assert rolled_back["status"] == "processing"
    assert rolled_back["attempts"] == 3
    assert rolled_back["lock_token"] is not None
    assert (
        await helpers.fetch_outbox(
            raw_db,
            payment.id,
            "payment.processing_failed",
        )
        == []
    )
    payment_row = await helpers.fetch_payment(raw_db, payment.id)
    assert payment_row["status"] == "succeeded"
    assert len(await receipts(webhook_event_id)) == 3

    # Recovery after the lease creates the dead Inbox and the dead letter atomically.
    await helpers.expire_lease(raw_db, event_id)
    assert await processing_service.recover_expired() is True
    dead = await helpers.fetch_inbox(raw_db, event_id)
    assert dead["status"] == "dead"
    assert dead["attempts"] == 3
    dead_letters = await helpers.fetch_outbox(
        raw_db,
        payment.id,
        "payment.processing_failed",
    )
    assert len(dead_letters) == 1
    assert dead_letters[0]["payload"]["attempts"] == 3
    assert dead_letters[0]["payload"]["error_code"] == "processing_interrupted"
