import asyncio
import logging
import signal
from typing import Any

import httpx
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncEngine

from payment_resolver.core.payments.exceptions import StorageUnavailable
from payment_resolver.core.ports import UnitOfWorkFactory
from payment_resolver.core.processing.retry import POLL_INTERVAL_SECONDS
from payment_resolver.modules.config import Settings
from payment_resolver.modules.database import create_engine_and_uow
from payment_resolver.modules.gateway.emulator import SimulatedGateway
from payment_resolver.modules.messaging.broker import (
    RabbitEventPublisher,
    create_broker,
    declare_topology,
    register_receiver,
)
from payment_resolver.modules.messaging.receiver import FatalSignal, PaymentReceiver
from payment_resolver.modules.messaging.workers import OutboxPublisher
from payment_resolver.modules.payments.logic import ProcessingService
from payment_resolver.modules.webhooks.client import HttpWebhookSender

logger = logging.getLogger(__name__)

SHUTDOWN_GRACE_SECONDS = 25.0
STORAGE_PAUSE_SECONDS = 5.0


async def _wait_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    if seconds <= 0:
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except TimeoutError:
        pass


class Consumer:
    """Supervises one AMQP receiver, one Outbox loop and one Inbox executor.

    A stop request stops the loops from taking new claims or publishing. Work
    already running gets ``SHUTDOWN_GRACE_SECONDS`` to finish before the loops
    are cancelled.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        engine: AsyncEngine,
        uow_factory: UnitOfWorkFactory,
        broker: RabbitBroker,
        publisher: RabbitEventPublisher,
        receiver: PaymentReceiver,
        subscriber: Any,
        outbox_publisher: OutboxPublisher,
        processing: ProcessingService,
        http_client: httpx.AsyncClient,
        fatal: FatalSignal,
    ) -> None:
        self.settings = settings
        self.engine = engine
        self.uow_factory = uow_factory
        self.broker = broker
        self.publisher = publisher
        self.receiver = receiver
        self.subscriber = subscriber
        self.outbox_publisher = outbox_publisher
        self.processing = processing
        self.http_client = http_client
        self.fatal = fatal
        self.stop_event = asyncio.Event()

    def request_stop(self, *_: Any) -> None:
        self.stop_event.set()

    async def close(self) -> None:
        await self._close()

    async def run(self) -> None:
        fatal = self.fatal.bind()
        loops: list[asyncio.Task[None]] = []
        stop_waiter: asyncio.Task[bool] | None = None
        error: BaseException | None = None

        try:
            await self.broker.connect()
            await declare_topology(self.broker)
            await self.broker.start()
            logger.info("Consumer started", extra={"event": "consumer.started"})

            loops = [
                asyncio.create_task(self._run_outbox_loop(), name="outbox-loop"),
                asyncio.create_task(self._run_inbox_loop(), name="inbox-loop"),
            ]
            stop_waiter = asyncio.create_task(self.stop_event.wait(), name="stop-waiter")
            await self._supervise(loops, stop_waiter, fatal)
        except BaseException as exc:  # noqa: BLE001 - re-raised after cleanup
            error = exc
        finally:
            workers: list[asyncio.Task[None] | asyncio.Task[bool]] = [*loops]
            if stop_waiter is not None:
                workers.append(stop_waiter)
            for task in workers:
                task.cancel()
            if workers:
                await asyncio.gather(*workers, return_exceptions=True)

            if not fatal.done():
                fatal.cancel()
            elif not fatal.cancelled():
                fatal_error = fatal.exception()
                if error is None and fatal_error is not None:
                    error = fatal_error

            await self._close()
            logger.info("Consumer stopped", extra={"event": "consumer.stopped"})

        if error is not None:
            raise error

    async def _supervise(
        self,
        loops: list[asyncio.Task[None]],
        stop_waiter: asyncio.Task[bool],
        fatal: asyncio.Future[BaseException],
    ) -> None:
        watched: set[asyncio.Future[Any]] = {*loops, stop_waiter, fatal}
        await asyncio.wait(
            watched,
            return_when=asyncio.FIRST_COMPLETED,
        )

        if fatal.done():
            raise self._fatal_error(fatal)

        stopping = stop_waiter.done() or self.stop_event.is_set()
        if stopping:
            for task in loops:
                if task.done():
                    self._raise_if_error(task)
        else:
            for task in loops:
                if task.done():
                    self._raise_worker_error(task)

        deadline = asyncio.timeout(SHUTDOWN_GRACE_SECONDS)
        try:
            async with deadline:
                pending = [task for task in loops if not task.done()]
                while pending:
                    if fatal.done():
                        raise self._fatal_error(fatal)
                    watched = {*pending, fatal}
                    await asyncio.wait(
                        watched,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for task in list(pending):
                        if task.done():
                            pending.remove(task)
                            self._raise_if_error(task)
        except TimeoutError:
            if not deadline.expired():
                raise
            logger.warning(
                "Graceful shutdown exceeded %ss; cancelling workers",
                SHUTDOWN_GRACE_SECONDS,
                extra={"event": "consumer.shutdown_timeout", "component": "consumer"},
            )
            for task in loops:
                task.cancel()
            await asyncio.gather(*loops, return_exceptions=True)

    @staticmethod
    def _fatal_error(fatal: asyncio.Future[BaseException]) -> BaseException:
        error = fatal.exception()
        return error if error is not None else RuntimeError("fatal receiver failure")

    @staticmethod
    def _raise_worker_error(task: asyncio.Task[None]) -> None:
        if task.cancelled():
            raise RuntimeError(f"{task.get_name()} was cancelled unexpectedly")
        error = task.exception()
        if error is not None:
            raise error
        raise RuntimeError(f"{task.get_name()} stopped unexpectedly")

    @staticmethod
    def _raise_if_error(task: asyncio.Task[None]) -> None:
        if task.cancelled():
            raise RuntimeError(f"{task.get_name()} was cancelled unexpectedly")
        error = task.exception()
        if error is not None:
            raise error

    async def _run_outbox_loop(self) -> None:
        unavailable = False
        while not self.stop_event.is_set():
            try:
                processed = await self.outbox_publisher.publish_one()
            except StorageUnavailable:
                if not unavailable:
                    logger.warning(
                        "Outbox storage unavailable",
                        extra={"event": "storage.unavailable", "component": "outbox"},
                    )
                    unavailable = True
                await _wait_or_stop(self.stop_event, STORAGE_PAUSE_SECONDS)
                continue
            if unavailable:
                logger.info(
                    "Outbox storage recovered",
                    extra={"event": "storage.recovered", "component": "outbox"},
                )
                unavailable = False
            if not processed:
                await _wait_or_stop(self.stop_event, POLL_INTERVAL_SECONDS)

    async def _run_inbox_loop(self) -> None:
        unavailable = False
        while not self.stop_event.is_set():
            try:
                await self.processing.recover_expired()
                if self.stop_event.is_set():
                    break
                claim = await self.processing.claim_next()
                if unavailable:
                    logger.info(
                        "Inbox storage recovered",
                        extra={"event": "storage.recovered", "component": "inbox"},
                    )
                    unavailable = False
                if claim is None:
                    await _wait_or_stop(self.stop_event, POLL_INTERVAL_SECONDS)
                    continue
                await self.processing.process(claim)
            except StorageUnavailable:
                if not unavailable:
                    logger.warning(
                        "Inbox storage unavailable",
                        extra={"event": "storage.unavailable", "component": "inbox"},
                    )
                    unavailable = True
                await _wait_or_stop(self.stop_event, STORAGE_PAUSE_SECONDS)

    async def _close(self) -> None:
        try:
            await self.broker.stop()
        except BaseException:  # noqa: BLE001 - cleanup must continue
            logger.exception(
                "Failed to close the broker",
                extra={"event": "consumer.close_failed", "component": "broker"},
            )
        try:
            await self.http_client.aclose()
        except BaseException:  # noqa: BLE001
            logger.exception(
                "Failed to close the HTTP client",
                extra={"event": "consumer.close_failed", "component": "http_client"},
            )
        try:
            await self.engine.dispose()
        except BaseException:  # noqa: BLE001
            logger.exception(
                "Failed to dispose the database engine",
                extra={"event": "consumer.close_failed", "component": "database"},
            )


def build_consumer(settings: Settings) -> Consumer:
    """Build resources without opening connections; ``run`` connects and starts the workers."""
    engine, uow_factory = create_engine_and_uow(settings.DATABASE_URL)
    broker = create_broker(settings)

    http_client = httpx.AsyncClient(
        trust_env=False,
        follow_redirects=False,
        transport=httpx.AsyncHTTPTransport(retries=0, verify=True, trust_env=False),
        timeout=httpx.Timeout(5.0, connect=2.0),
    )

    publisher = RabbitEventPublisher(broker)
    fatal = FatalSignal()
    receiver = PaymentReceiver(uow_factory, publisher, fatal)
    subscriber = register_receiver(broker, receiver)

    processing = ProcessingService(
        uow_factory,
        SimulatedGateway(),
        HttpWebhookSender(http_client, settings.WEBHOOK_ALLOWED_ORIGINS),
        retry_base_seconds=settings.RETRY_BASE_SECONDS,
    )
    outbox_publisher = OutboxPublisher(uow_factory, publisher)

    return Consumer(
        settings,
        engine=engine,
        uow_factory=uow_factory,
        broker=broker,
        publisher=publisher,
        receiver=receiver,
        subscriber=subscriber,
        outbox_publisher=outbox_publisher,
        processing=processing,
        http_client=http_client,
        fatal=fatal,
    )


async def run_consumer(settings: Settings) -> None:
    consumer = build_consumer(settings)
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, consumer.request_stop)
        except (NotImplementedError, RuntimeError, ValueError):
            continue
        installed.append(sig)
    try:
        await consumer.run()
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)
