class PaymentNotFound(Exception):
    pass


class IdempotencyConflict(Exception):
    pass


class InvalidIncomingEvent(Exception):
    pass


class StorageUnavailable(Exception):
    pass


class GatewayTemporaryError(Exception):
    pass


class PublicationError(Exception):
    pass


class FinancialStateError(ValueError):
    pass


class WebhookDeliveryError(Exception):
    def __init__(self, error_code: str, http_status: int | None = None) -> None:
        super().__init__(error_code)
        self.error_code = error_code
        self.http_status = http_status
