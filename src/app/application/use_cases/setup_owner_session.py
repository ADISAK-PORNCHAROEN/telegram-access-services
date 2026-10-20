"""Coordinate the temporary owner-session login flow."""

import asyncio

from app.application.ports import OwnerSessionAuthentication
from app.domain.owner_setup import (
    OwnerAuthOutcome,
    OwnerSetupPhase,
    OwnerSetupState,
)


class SetupOwnerSession:
    def __init__(self, authentication: OwnerSessionAuthentication):
        self._authentication = authentication
        self._phase = OwnerSetupPhase.PHONE
        self._phone: str | None = None
        self._challenge_id: str | None = None
        self._issue: OwnerAuthOutcome | None = None
        self._retry_after_seconds: int | None = None
        self._lock = asyncio.Lock()
        self._completed = asyncio.Event()

    @property
    def completed(self) -> asyncio.Event:
        return self._completed

    @property
    def is_complete(self) -> bool:
        return self._phase is OwnerSetupPhase.COMPLETE

    async def start(self) -> OwnerSetupState:
        await self._authentication.connect()
        if await self._authentication.is_authorized():
            self._complete()
        return self.state()

    def state(self) -> OwnerSetupState:
        return OwnerSetupState(
            self._phase,
            self._issue,
            self._retry_after_seconds,
        )

    async def request_code(self, phone: str) -> OwnerSetupState:
        async with self._lock:
            if self._phase is not OwnerSetupPhase.PHONE:
                return self._reject(OwnerAuthOutcome.UNAVAILABLE)
            cleaned_phone = phone.strip()
            if not cleaned_phone or len(cleaned_phone) > 64:
                return self._reject(OwnerAuthOutcome.INVALID_PHONE)
            result = await self._authentication.request_code(cleaned_phone)
            if result.outcome is OwnerAuthOutcome.CODE_SENT and result.challenge_id:
                self._phone = cleaned_phone
                self._challenge_id = result.challenge_id
                self._phase = OwnerSetupPhase.CODE
                self._clear_issue()
            else:
                self._apply_failure(result)
            return self.state()

    async def resend_code(self) -> OwnerSetupState:
        async with self._lock:
            if self._phase is not OwnerSetupPhase.CODE or self._phone is None:
                return self._reject(OwnerAuthOutcome.UNAVAILABLE)
            result = await self._authentication.request_code(self._phone)
            if result.outcome is OwnerAuthOutcome.CODE_SENT and result.challenge_id:
                self._challenge_id = result.challenge_id
                self._clear_issue()
            else:
                self._apply_failure(result)
            return self.state()

    async def submit_code(self, code: str) -> OwnerSetupState:
        async with self._lock:
            if (self._phase is not OwnerSetupPhase.CODE or self._challenge_id is None
                    or not code.strip() or len(code) > 64):
                return self._reject(OwnerAuthOutcome.INVALID_CODE)
            result = await self._authentication.sign_in_code(
                self._challenge_id, code.strip())
            if result.outcome is OwnerAuthOutcome.PASSWORD_REQUIRED:
                self._phase = OwnerSetupPhase.PASSWORD
                self._challenge_id = None
                self._clear_issue()
            elif result.outcome is OwnerAuthOutcome.AUTHORIZED:
                self._complete()
            else:
                if result.outcome is OwnerAuthOutcome.EXPIRED_CODE:
                    self._challenge_id = None
                self._apply_failure(result)
            return self.state()

    async def submit_password(self, password: str) -> OwnerSetupState:
        async with self._lock:
            if self._phase is not OwnerSetupPhase.PASSWORD or not password:
                return self._reject(OwnerAuthOutcome.INVALID_PASSWORD)
            result = await self._authentication.sign_in_password(password)
            if result.outcome is OwnerAuthOutcome.AUTHORIZED:
                self._complete()
            else:
                self._apply_failure(result)
            return self.state()

    async def verify_authorized(self) -> bool:
        return self.is_complete and await self._authentication.is_authorized()

    def _apply_failure(self, result) -> None:
        self._issue = result.outcome
        self._retry_after_seconds = result.retry_after_seconds

    def _reject(self, issue: OwnerAuthOutcome) -> OwnerSetupState:
        self._issue = issue
        self._retry_after_seconds = None
        return self.state()

    def _clear_issue(self) -> None:
        self._issue = None
        self._retry_after_seconds = None

    def _complete(self) -> None:
        self._phase = OwnerSetupPhase.COMPLETE
        self._phone = None
        self._challenge_id = None
        self._clear_issue()
        self._completed.set()
