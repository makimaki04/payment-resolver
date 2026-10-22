import asyncio
import logging

from pydantic import ValidationError

from payment_resolver.modules.config import Settings
from payment_resolver.modules.logging import configure_logging
from payment_resolver.modules.processing import runtime

logger = logging.getLogger(__name__)


def main() -> None:
    try:
        settings = Settings()
    except ValidationError as exc:
        fields = sorted({".".join(map(str, error["loc"])) for error in exc.errors()})
        logger.error(
            "Invalid consumer configuration for fields: %s",
            ", ".join(fields),
            extra={
                "event": "consumer.invalid_configuration",
                "error_code": "invalid_configuration",
            },
        )
        raise SystemExit(1) from None
    configure_logging(settings)
    try:
        asyncio.run(runtime.run_consumer(settings))
    except KeyboardInterrupt:
        return
    except Exception:
        logger.exception("Consumer failed", extra={"event": "consumer.failed"})
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
