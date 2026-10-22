from datetime import timedelta

MAX_ATTEMPTS = 3
PROCESSING_TIMEOUT_SECONDS = 20
PROCESSING_LEASE_SECONDS = 30
WEBHOOK_TIMEOUT_SECONDS = 5
PUBLISH_TIMEOUT_SECONDS = 5
POLL_INTERVAL_SECONDS = 0.5


def retry_delay(attempts_started: int, jitter_factor: float, base_seconds: float) -> timedelta:
    if attempts_started not in (1, 2):
        raise ValueError("Retry is only permitted after attempts 1 and 2")
    if not 0.8 <= jitter_factor <= 1.2 or not 0 < base_seconds < float("inf"):
        raise ValueError("Invalid retry parameters")
    return timedelta(seconds=base_seconds * 2 ** (attempts_started - 1) * jitter_factor)
