import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response

from payment_resolver.api.v1.payments import router as payments_router
from payment_resolver.modules.config import Settings
from payment_resolver.modules.database import create_engine_and_uow
from payment_resolver.modules.logging import configure_logging
from payment_resolver.modules.payments.logic import PaymentService

logger = logging.getLogger(__name__)


def _safe_validation_errors(exc: RequestValidationError) -> list[dict[str, Any]]:
    """Build a structured 422 without echoing rejected input.

    Rejected ``NaN``/``Infinity`` values (and huge raw bodies) must never be
    serialized back: Starlette's ``JSONResponse`` forbids non-finite numbers,
    so echoing them would turn a validation error into a 500.
    """
    errors: list[dict[str, Any]] = []
    for error in exc.errors():
        errors.append(
            {
                "type": str(error.get("type", "value_error")),
                "loc": list(error.get("loc", ())),
                "msg": str(error.get("msg", "Invalid value")),
            }
        )
    return errors


async def _validation_exception_handler(request: Request, exc: Exception) -> Response:
    if not isinstance(exc, RequestValidationError):
        raise exc
    return JSONResponse(status_code=422, content={"detail": _safe_validation_errors(exc)})


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the HTTP application; it owns only HTTP and database resources."""
    resolved = settings if settings is not None else Settings()
    configure_logging(resolved)
    engine, uow_factory = create_engine_and_uow(resolved.DATABASE_URL)
    payment_service = PaymentService(uow_factory, resolved.WEBHOOK_ALLOWED_ORIGINS)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        logger.info("API started", extra={"event": "api.started"})
        try:
            yield
        finally:
            await engine.dispose()
            logger.info("API stopped", extra={"event": "api.stopped"})

    app = FastAPI(
        title="Payment Resolver",
        docs_url="/docs",
        redoc_url=None,
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )
    app.state.settings = resolved
    app.state.engine = engine
    app.state.payment_service = payment_service
    app.add_exception_handler(RequestValidationError, _validation_exception_handler)
    app.include_router(payments_router, prefix="/api/v1")
    return app
