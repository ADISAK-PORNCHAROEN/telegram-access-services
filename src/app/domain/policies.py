"""Pure decisions shared by use cases and adapters."""

from app.domain.models import MediaKind

MAX_SEND_BYTES = 2_000_000_000


def matches(kind: MediaKind | None, preference: MediaKind | None) -> bool:
    return preference is None or kind == preference


def exceeds_send_limit(size: int | None) -> bool:
    return size is not None and size > MAX_SEND_BYTES
