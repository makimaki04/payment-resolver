import logging
from datetime import timedelta

from payment_resolver.core.payments.exceptions import PublicationError
from payment_resolver.core.ports import EventPublisher, UnitOfWorkFactory
from payment_resolver.modules.logging import log_extra

logger = logging.getLogger(__name__)

MAX_PUBLISH_BACKOFF_SECONDS = 30
PUBLISH_BACKOFF_CAP_EXPONENT = 5


class OutboxPublisher:
    def __init__(self, uow_factory: UnitOfWorkFactory, publisher: EventPublisher) -> None:
        self._uow_factory = uow_factory
        self._publisher = publisher
        # A broker outage lasts many poll cycles; report its first failure and
        # the later recovery, not every retry.
        self._publish_outage = False

    async def publish_one(self) -> bool:
        """Publish the oldest due row and commit its new state.

        Returns ``False`` when no row is due. A handled publish failure still
        returns ``True`` because it advances the row's retry schedule.
        """
        async with self._uow_factory() as uow:
            entry = await uow.outbox.lock_next_due(await uow.now())
            if entry is None:
                return False

            entry.publish_attempts += 1
            try:
                # The row lock intentionally spans the bounded broker confirm;
                # this is the single allowed external I/O inside a transaction.
                await self._publisher.publish(entry)
            except PublicationError as exc:
                now = await uow.now()
                delay = min(
                    MAX_PUBLISH_BACKOFF_SECONDS,
                    2 ** min(entry.publish_attempts - 1, PUBLISH_BACKOFF_CAP_EXPONENT),
                )
                error_code = str(exc) or "publish_error"
                entry.last_error_code = error_code
                entry.next_publish_at = now + timedelta(seconds=delay)
                entry.published_at = None
                await uow.outbox.save(entry)
                await uow.commit()
                context = log_extra(
                    "outbox.publish_failed",
                    payment_id=str(entry.payment_id),
                    event_id=str(entry.id),
                    attempt=entry.publish_attempts,
                    error_code=error_code,
                    component="outbox",
                )
                if self._publish_outage:
                    logger.debug("Outbox publication still failing", extra=context)
                else:
                    logger.warning("Outbox publication failed; retrying", extra=context)
                    self._publish_outage = True
                return True

            entry.published_at = await uow.now()
            entry.last_error_code = None
            await uow.outbox.save(entry)
            await uow.commit()
            if self._publish_outage:
                logger.info(
                    "Outbox publication recovered",
                    extra=log_extra(
                        "outbox.recovered",
                        payment_id=str(entry.payment_id),
                        event_id=str(entry.id),
                        component="outbox",
                    ),
                )
                self._publish_outage = False
            logger.debug("published outbox row %s", entry.id)
            return True
