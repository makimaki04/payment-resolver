import asyncio

import httpx

from payment_resolver.core.payments.dtos import PaymentResultWebhookDTO
from payment_resolver.core.payments.exceptions import WebhookDeliveryError
from payment_resolver.core.payments.urls import require_allowed_url
from payment_resolver.core.processing.retry import WEBHOOK_TIMEOUT_SECONDS


class HttpWebhookSender:
    def __init__(self, client: httpx.AsyncClient, allowed_origins: list[str]) -> None:
        self.client = client
        self.allowed_origins = allowed_origins

    async def send(self, url: str, event: PaymentResultWebhookDTO) -> None:
        try:
            normalized = require_allowed_url(url, self.allowed_origins)
        except ValueError as exc:
            raise WebhookDeliveryError("webhook_url_not_allowed") from exc
        try:
            async with asyncio.timeout(WEBHOOK_TIMEOUT_SECONDS):
                async with self.client.stream(
                    "POST",
                    normalized,
                    json=event.model_dump(mode="json"),
                    timeout=httpx.Timeout(WEBHOOK_TIMEOUT_SECONDS, connect=2),
                    follow_redirects=False,
                ) as response:
                    if not 200 <= response.status_code < 300:
                        raise WebhookDeliveryError("webhook_http_error", response.status_code)
        except (TimeoutError, httpx.TimeoutException) as exc:
            raise WebhookDeliveryError("webhook_timeout") from exc
        except httpx.RequestError as exc:
            raise WebhookDeliveryError("webhook_network_error") from exc
