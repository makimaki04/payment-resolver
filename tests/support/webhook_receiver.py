"""Real HTTP webhook receiver for integration tests and the test-profile service.

Run standalone (Compose ``webhook`` service)::

    uvicorn tests.support.webhook_receiver:app --host 0.0.0.0 --port 8081

The receiver only records receipts per ``event_id``; it never performs financial
actions. Routes:

* ``POST /ok``           -> 204
* ``POST /fail``         -> 503 with a huge Retry-After (ignored by the service)
* ``POST /bad-request``  -> 400
* ``POST /flaky``        -> 503 for the first two attempts of an event, then 204
* ``POST /timeout``      -> records the receipt, then answers after 6 seconds
* ``POST /redirect``     -> 307 to ``/redirect-target``
* ``POST /redirect-target`` -> records a redirect hit, 204
* ``GET  /receipts``     -> JSON list of receipts ``{path, headers, body, ...}``
* ``GET  /health``       -> readiness marker

Receipts are indexed by the ``event_id`` present in the parsed JSON body so
tests can assert stable notification bodies. Header-inspection data is recorded
as well so tests can verify that the API key and the client idempotency key
never travel to the receiver.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict
from typing import Any

from fastapi import FastAPI, Request, Response

_REDIRECT_DELAY_SECONDS = 6


class ReceiptStore:
    def __init__(self) -> None:
        self.receipts: list[dict[str, Any]] = []
        self._attempts: dict[str, int] = defaultdict(int)

    def _next_attempt(self, event_id: str | None) -> int:
        key = event_id or "<missing>"
        self._attempts[key] += 1
        return self._attempts[key]

    def record(
        self, path: str, body: bytes, headers: dict[str, str], *, count_attempt: bool = True
    ) -> dict[str, Any]:
        try:
            payload: Any = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            payload = None
        event_id = payload.get("event_id") if isinstance(payload, dict) else None
        receipt = {
            "path": path,
            "event_id": event_id,
            "body": payload,
            "raw_body_bytes": len(body),
            "headers": headers,
            "attempt": self._next_attempt(event_id) if count_attempt else None,
            "received_at": time.time(),
        }
        self.receipts.append(receipt)
        return receipt

    def for_event(self, event_id: str) -> list[dict[str, Any]]:
        return [item for item in self.receipts if item["event_id"] == event_id]

    def for_path(self, path: str) -> list[dict[str, Any]]:
        return [item for item in self.receipts if item["path"] == path]


def _headers(request: Request) -> dict[str, str]:
    return {key.lower(): value for key, value in request.headers.items()}


async def _body(request: Request) -> bytes:
    return await request.body()


def create_app() -> FastAPI:
    app = FastAPI(
        title="payment-resolver webhook receiver", docs_url=None, redoc_url=None, openapi_url=None
    )
    store = ReceiptStore()
    app.state.store = store

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/receipts")
    async def receipts() -> list[dict[str, Any]]:
        return list(store.receipts)

    @app.post("/ok")
    async def ok(request: Request) -> Response:
        store.record("/ok", await _body(request), _headers(request))
        return Response(status_code=204)

    @app.post("/fail")
    async def fail(request: Request) -> Response:
        store.record("/fail", await _body(request), _headers(request))
        # A huge Retry-After must not influence the service's own schedule.
        return Response(status_code=503, headers={"Retry-After": "3600"})

    @app.post("/bad-request")
    async def bad_request(request: Request) -> Response:
        store.record("/bad-request", await _body(request), _headers(request))
        return Response(status_code=400)

    @app.post("/flaky")
    async def flaky(request: Request) -> Response:
        receipt = store.record("/flaky", await _body(request), _headers(request))
        return Response(status_code=204 if receipt["attempt"] > 2 else 503)

    @app.post("/timeout")
    async def timeout(request: Request) -> Response:
        store.record("/timeout", await _body(request), _headers(request))
        await asyncio.sleep(_REDIRECT_DELAY_SECONDS)
        return Response(status_code=204)

    @app.post("/redirect")
    async def redirect(request: Request) -> Response:
        store.record("/redirect", await _body(request), _headers(request))
        return Response(status_code=307, headers={"Location": "/redirect-target"})

    @app.post("/redirect-target")
    async def redirect_target(request: Request) -> Response:
        store.record(
            "/redirect-target", await _body(request), _headers(request), count_attempt=False
        )
        return Response(status_code=204)

    return app


app = create_app()
