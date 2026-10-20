"""Errors adapters translate into, so use cases do not know SDK exceptions."""


class SourceUnavailable(Exception):
    pass


class SessionExpired(Exception):
    pass


class RateLimited(Exception):
    def __init__(self, seconds: int, *, stop_reason: str = "flood-wait"):
        self.seconds = seconds
        self.stop_reason = stop_reason
        super().__init__(f"rate limited for {seconds} seconds")


class QueueFull(Exception):
    def __init__(self, scope: str):
        self.scope = scope
        super().__init__(scope)


class QueueExpired(Exception):
    pass


class AccessDenied(Exception):
    pass


class DeliveryUnknown(Exception):
    """A submitted send has no definitive acknowledgement; never resend it."""

    def __init__(self, *, sent_files: int | None = None,
                 total_files: int | None = None, delivered_bytes: int | None = None,
                 delivery_outcomes=()):
        self.sent_files = sent_files
        self.total_files = total_files
        self.delivered_bytes = delivered_bytes
        self.delivery_outcomes = tuple(delivery_outcomes)
        super().__init__("delivery acknowledgement unknown")


class WorkspaceBudgetExceeded(Exception):
    """A request's bounded logical workspace reservation exceeds the process cap."""

    def __init__(self, requested_bytes: int, budget_bytes: int):
        super().__init__("workspace reservation exceeds configured budget")
        self.requested_bytes = requested_bytes
        self.budget_bytes = budget_bytes


class DownloadLimitExceeded(Exception):
    """A streamed download attempted to exceed its per-file send limit."""

    def __init__(self, requested_bytes: int, limit_bytes: int):
        self.requested_bytes = requested_bytes
        self.limit_bytes = limit_bytes
        super().__init__("download exceeded configured per-file limit")
