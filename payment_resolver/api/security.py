import secrets
from typing import Annotated, cast

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader

from payment_resolver.modules.config import Settings

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_settings(request: Request) -> Settings:
    return cast(Settings, request.app.state.settings)


async def require_api_key(
    api_key: Annotated[str | None, Depends(_api_key_header)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> None:
    """Constant-time comparison over UTF-8 bytes.

    Comparing bytes keeps a raw non-ASCII header a plain 401 instead of a
    ``TypeError`` from ``secrets.compare_digest`` on non-ASCII strings.
    """
    expected = settings.API_KEY.get_secret_value().encode("utf-8")
    provided = api_key.encode("utf-8") if api_key is not None else b""
    if api_key is None or not secrets.compare_digest(provided, expected):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
