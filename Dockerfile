FROM ghcr.io/astral-sh/uv:0.11.25 AS uv
FROM python:3.12-slim AS base
COPY --from=uv /uv /uvx /bin/
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY payment_resolver ./payment_resolver
COPY alembic.ini ./
COPY migrations ./migrations
# Cache a fresh project build and its backend for frozen/offline execution.
RUN uv sync --frozen --no-dev --reinstall-package payment-resolver

FROM base AS runtime
CMD ["uv", "run", "--no-sync", "python", "-m", "payment_resolver.consumer"]

FROM base AS test
COPY tests ./tests
RUN uv sync --frozen --reinstall-package payment-resolver
CMD ["uv", "run", "--frozen", "pytest", "tests/unit", "tests/integration", "-q"]
