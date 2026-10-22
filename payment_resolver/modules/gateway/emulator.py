import asyncio
import hashlib
import random
from decimal import Decimal
from uuid import UUID

from payment_resolver.core.payments.enums import Currency, PaymentStatus


def stable_outcome(idempotency_key: str) -> PaymentStatus:
    """Map a key to a fixed outcome; one bucket in ten fails."""
    bucket = (
        int.from_bytes(hashlib.sha256(idempotency_key.encode("ascii")).digest()[:8], "big") % 10
    )
    return PaymentStatus.failed if bucket == 0 else PaymentStatus.succeeded


class SimulatedGateway:
    async def charge(
        self,
        *,
        payment_id: UUID,
        amount: Decimal,
        currency: Currency,
        description: str,
        idempotency_key: str,
    ) -> PaymentStatus:
        await asyncio.sleep(random.uniform(2, 5))
        return stable_outcome(idempotency_key)
