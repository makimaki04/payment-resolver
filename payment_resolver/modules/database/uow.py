from collections.abc import Callable
from datetime import datetime
from types import TracebackType

import asyncpg
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from payment_resolver.core.ports import InboxRepository, OutboxRepository, PaymentRepository
from payment_resolver.modules.database.errors import raise_storage_error
from payment_resolver.modules.database.repositories import (
    SqlAlchemyInboxRepository,
    SqlAlchemyOutboxRepository,
    SqlAlchemyPaymentRepository,
)

_DB_ERRORS = (SQLAlchemyError, OSError, asyncpg.PostgresError)


class SqlAlchemyUnitOfWork:
    payments: PaymentRepository
    inbox: InboxRepository
    outbox: OutboxRepository

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None
        self._committed = False

    async def __aenter__(self) -> "SqlAlchemyUnitOfWork":
        session = self._session_factory()
        self._session = session
        self._committed = False
        self.payments = SqlAlchemyPaymentRepository(session)
        self.inbox = SqlAlchemyInboxRepository(session)
        self.outbox = SqlAlchemyOutboxRepository(session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        session = self._session
        self._session = None
        if session is None:
            return
        try:
            if exc_type is not None or not self._committed:
                await session.rollback()
        except _DB_ERRORS as error:
            # Nothing was committed; a lost connection is safe to discard and
            # must not mask the error that caused the rollback.
            if exc_type is None:
                raise_storage_error(error)
        finally:
            await session.close()

    def _require_session(self) -> AsyncSession:
        if self._session is None:
            raise RuntimeError("UnitOfWork is used outside of its context")
        return self._session

    async def now(self) -> datetime:
        """Use the database clock so lease and retry deadlines agree across processes."""
        try:
            value = await self._require_session().scalar(select(func.clock_timestamp()))
        except _DB_ERRORS as exc:
            raise_storage_error(exc)
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise RuntimeError("Database returned a timestamp without timezone")
        return value

    async def commit(self) -> None:
        """Commit the session; a connection failure here becomes ``StorageUnavailable``."""
        session = self._require_session()
        try:
            await session.commit()
        except _DB_ERRORS as exc:
            raise_storage_error(exc)
        self._committed = True


def create_engine_and_uow(
    database_url: str,
) -> tuple[AsyncEngine, Callable[[], SqlAlchemyUnitOfWork]]:
    engine = create_async_engine(
        database_url,
        isolation_level="READ COMMITTED",
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)

    def uow_factory() -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(session_factory)

    return engine, uow_factory
