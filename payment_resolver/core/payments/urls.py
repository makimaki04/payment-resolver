from typing import cast
from urllib.parse import urlsplit

from pydantic import AnyHttpUrl, TypeAdapter

_URL = TypeAdapter(AnyHttpUrl)
Origin = tuple[str, str, int]


def normalize_url(value: str) -> str:
    """Validate an absolute HTTP(S) URL and reject userinfo and fragments."""
    url = _URL.validate_python(value)
    # Pydantic removes an empty userinfo component (http://@host), so retain
    # the raw component-presence check before returning its normalized URL.
    if (
        urlsplit(value).username is not None
        or url.username is not None
        or url.password is not None
        or url.fragment is not None
    ):
        raise ValueError("Webhook URL cannot contain userinfo or fragment")
    return str(url)


def url_origin(value: str) -> Origin:
    """Return the scheme, host and effective port used for allowlist matching."""
    url = _URL.validate_python(normalize_url(value))
    return (url.scheme, cast(str, url.host), url.port or (443 if url.scheme == "https" else 80))


def normalize_origin(value: str) -> str:
    """Normalize an allowed origin; a path or query is rejected."""
    normalized = normalize_url(value)
    url = _URL.validate_python(normalized)
    if url.path not in (None, "", "/") or url.query is not None:
        raise ValueError("Allowed origins cannot contain path or query")
    return normalized


def require_allowed_url(value: str, allowed_origins: list[str] | tuple[str, ...]) -> str:
    """Return the normalized URL when its origin is allowed.

    Raises ``ValueError`` for an invalid URL or an origin outside the list.
    """
    normalized = normalize_url(value)
    if url_origin(normalized) not in {url_origin(origin) for origin in allowed_origins}:
        raise ValueError("Webhook origin is not allowed")
    return normalized
