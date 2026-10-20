"""Values for the one-time owner-session authorization flow."""

from dataclasses import dataclass
from enum import StrEnum


class OwnerAuthOutcome(StrEnum):
    CODE_SENT = "code_sent"
    PASSWORD_REQUIRED = "password_required"
    AUTHORIZED = "authorized"
    INVALID_PHONE = "invalid_phone"
    INVALID_CODE = "invalid_code"
    EXPIRED_CODE = "expired_code"
    INVALID_PASSWORD = "invalid_password"
    RATE_LIMITED = "rate_limited"
    UNAVAILABLE = "unavailable"


class OwnerSetupPhase(StrEnum):
    PHONE = "phone"
    CODE = "code"
    PASSWORD = "password"
    COMPLETE = "complete"


@dataclass(frozen=True)
class OwnerAuthResult:
    outcome: OwnerAuthOutcome
    challenge_id: str | None = None
    retry_after_seconds: int | None = None


@dataclass(frozen=True)
class OwnerSetupState:
    phase: OwnerSetupPhase
    issue: OwnerAuthOutcome | None = None
    retry_after_seconds: int | None = None
