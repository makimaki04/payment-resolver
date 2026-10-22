"""Translation of expected database availability failures.

Only unambiguous connection/availability failures become ``StorageUnavailable``;
integrity, programming and statement errors keep propagating so that bugs are
never disguised as a transient outage.
"""

import asyncio
from typing import NoReturn

import asyncpg
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError

from payment_resolver.core.payments.exceptions import StorageUnavailable

_CONNECTION_CLASS_SQLSTATES = frozenset({"57P01", "57P02", "57P03", "53300"})
_CONNECTION_NAMES = frozenset({"InterfaceError", "OperationalError"})


def _sqlstate(exc: BaseException) -> str | None:
    value = getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)
    return value if isinstance(value, str) and value else None


def is_availability_error(exc: BaseException) -> bool:
    if isinstance(exc, (ConnectionError, OSError, TimeoutError, asyncio.TimeoutError)):
        return True
    if isinstance(exc, asyncpg.PostgresError):
        # Connect-time failures are not wrapped by the SQLAlchemy dialect.
        sqlstate = _sqlstate(exc)
        return sqlstate is not None and (
            sqlstate.startswith("08") or sqlstate in _CONNECTION_CLASS_SQLSTATES
        )
    if not isinstance(exc, SQLAlchemyError):
        return False
    orig = getattr(exc, "orig", None)
    if isinstance(orig, (ConnectionError, OSError, TimeoutError)):
        return True
    sqlstate = _sqlstate(orig) if orig is not None else None
    if sqlstate is not None:
        return sqlstate.startswith("08") or sqlstate in _CONNECTION_CLASS_SQLSTATES
    if isinstance(exc, (OperationalError, InterfaceError)) and orig is not None:
        # asyncpg raises ``InterfaceError`` when the connection is already
        # closed and wraps transport failures without a SQLSTATE.
        return type(orig).__name__ in _CONNECTION_NAMES
    return False


def raise_storage_error(exc: BaseException) -> NoReturn:
    if is_availability_error(exc):
        raise StorageUnavailable("database is temporarily unavailable") from exc
    raise exc
