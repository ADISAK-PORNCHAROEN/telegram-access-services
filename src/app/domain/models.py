"""Values exchanged between application ports; no Telegram SDK objects."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Literal

MediaKind = Literal["photo", "video", "file"]


@dataclass(frozen=True)
class ParsedLink:
    chat: int | str
    message_id: int
    is_private: bool


@dataclass(frozen=True)
class MediaItem:
    id: int
    caption: str = ""
    kind: MediaKind | None = None
    size: int | None = None
    suffix: str = ""


@dataclass(frozen=True)
class DownloadedMedia:
    item: MediaItem
    path: Path
    position: int | None = None


class CopyStatus(str, Enum):
    OK = "ok"
    TEXT = "text"
    MISSING = "missing"
    FILTERED = "skipped: filter"
    TOO_BIG = "skipped: too-big"
    FAILED = "fail"
    PARTIAL = "partial"
    EXPIRED = "expired"
    DELIVERY_UNKNOWN = "delivery_unknown"


@dataclass(frozen=True)
class CopyJobInput:
    """Persistable request data captured at the private-message boundary."""

    job_id: str
    receiving_bot_id: int
    requester_id: int
    destination_chat: int
    request_message_id: int
    link_index: int
    source: ParsedLink
    filter_kind: MediaKind | None
    deadline_at: datetime | None = None
    deadline_remaining_seconds: float | None = None
    lease_generation: int | None = None
    claim_queue_wait_seconds: float | None = None


@dataclass(frozen=True)
class JobAdmission:
    accepted: bool
    job_id: str | None = None
    replayed: bool = False
    rejection_reason: str | None = None


class JobState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_RATE_LIMIT = "waiting_rate_limit"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    DELIVERY_UNKNOWN = "delivery_unknown"


@dataclass(frozen=True)
class CopyJobSnapshot:
    """Requester-safe durable job view; source and destination are omitted."""

    job_id: str
    requester_id: int
    state: JobState
    stage: str
    created_at: datetime
    updated_at: datetime
    next_eligible_at: datetime | None = None
    result: dict | None = None
    rate_limit_kind: str | None = None
    rate_limit_account: str | None = None
    rate_limit_required_seconds: float | None = None


class JobCancellationDisposition(str, Enum):
    NOT_FOUND = "not_found"
    CANCELLED = "cancelled"
    CANCELLATION_REQUESTED = "cancellation_requested"
    ALREADY_CANCELLED = "already_cancelled"
    TOO_LATE = "too_late"
    ALREADY_FINISHED = "already_finished"


@dataclass(frozen=True)
class JobCancellationDecision:
    disposition: JobCancellationDisposition
    state: JobState | None = None
    stop_worker: bool = False


@dataclass
class TransferStats:
    files: int = 0
    bytes: int = 0
    download_s: float | None = None
    upload_s: float | None = None
    delivered_bytes: int | None = None

    def as_dict(self) -> dict:
        return {"n": self.files, "bytes": self.bytes,
                "dl": self.download_s, "up": self.upload_s,
                "delivered_bytes": self.delivered_bytes}


@dataclass(frozen=True)
class DeliveryIntentItem:
    position: int
    item_id: int
    size_bytes: int


@dataclass(frozen=True)
class DeliveryIntent:
    """SDK-free description persisted immediately before a final-send RPC."""

    kind: Literal["text", "media", "album"]
    items: tuple[DeliveryIntentItem, ...] = ()


@dataclass(frozen=True)
class DeliveryOutcome:
    """A definite per-item result; Telegram objects never cross the adapter."""

    position: int | None
    item_id: int | None
    status: Literal["sent", "rejected"]
    message_id: int | None = None
    delivered_bytes: int | None = None

    def as_dict(self) -> dict:
        return {
            "position": self.position,
            "item_id": self.item_id,
            "status": self.status,
            "message_id": self.message_id,
            "delivered_bytes": self.delivered_bytes,
        }


@dataclass(frozen=True)
class DeliveryResult:
    sent: int
    total: int
    delivered_bytes: int | None = None
    outcomes: tuple[DeliveryOutcome, ...] = ()


@dataclass
class CopyResult:
    status: CopyStatus
    stats: TransferStats = field(default_factory=TransferStats)
    reason: str = ""
    sent_files: int | None = None
    total_files: int | None = None
    delivery_outcomes: tuple[DeliveryOutcome, ...] = ()

    @property
    def audit_status(self) -> str:
        return f"{self.status.value}: {self.reason}" if self.reason else self.status.value


@dataclass(frozen=True)
class CopyRequest:
    sender: int
    reply_chat: int
    link: ParsedLink
    filter_kind: MediaKind | None = None
    job_id: str = ""
