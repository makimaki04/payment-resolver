from typing import Any, cast

import asyncpg
from sqlalchemy.engine import Result
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Executable
from sqlalchemy.sql.selectable import TypedReturnsRows

from payment_resolver.modules.database.errors import raise_storage_error

_DB_ERRORS = (SQLAlchemyError, OSError, asyncpg.PostgresError)


class _SessionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def _execute(self, statement: Executable) -> Result[Any]:
        try:
            return await self._session.execute(statement)
        except _DB_ERRORS as exc:
            raise_storage_error(exc)

    async def _first[T](self, statement: TypedReturnsRows[tuple[T]]) -> T | None:
        result = await self._execute(statement)
        return cast(T | None, result.scalars().first())
