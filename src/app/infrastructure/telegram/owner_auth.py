"""Telethon implementation of the owner-session authentication port."""

import asyncio
import secrets

from telethon.errors import (
    FloodPremiumWaitError,
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeEmptyError,
    PhoneCodeExpiredError,
    PhoneCodeHashEmptyError,
    PhoneCodeInvalidError,
    PhoneNumberBannedError,
    PhoneNumberFloodError,
    PhoneNumberInvalidError,
    PhoneNumberUnoccupiedError,
    RPCError,
    SessionPasswordNeededError,
    UnauthorizedError,
)

from app.domain.owner_setup import OwnerAuthOutcome, OwnerAuthResult


class TelegramOwnerAuthentication:
    def __init__(self, client):
        self._client = client
        self._challenges: dict[str, tuple[str, str]] = {}

    async def connect(self) -> None:
        await self._client.connect()

    async def is_authorized(self) -> bool:
        try:
            return await self._client.get_me() is not None
        except UnauthorizedError:
            return False

    async def request_code(self, phone: str) -> OwnerAuthResult:
        try:
            sent = await self._client.send_code_request(phone)
        except (PhoneNumberInvalidError, PhoneNumberBannedError,
                PhoneNumberUnoccupiedError):
            return OwnerAuthResult(OwnerAuthOutcome.INVALID_PHONE)
        except (FloodWaitError, FloodPremiumWaitError) as exc:
            return OwnerAuthResult(OwnerAuthOutcome.RATE_LIMITED, retry_after_seconds=exc.seconds)
        except PhoneNumberFloodError:
            return OwnerAuthResult(OwnerAuthOutcome.RATE_LIMITED)
        except (RPCError, OSError, asyncio.TimeoutError):
            return OwnerAuthResult(OwnerAuthOutcome.UNAVAILABLE)
        challenge_id = secrets.token_urlsafe(24)
        self._challenges.clear()
        self._challenges[challenge_id] = (phone, sent.phone_code_hash)
        return OwnerAuthResult(OwnerAuthOutcome.CODE_SENT, challenge_id=challenge_id)

    async def sign_in_code(self, challenge_id: str, code: str) -> OwnerAuthResult:
        challenge = self._challenges.get(challenge_id)
        if challenge is None:
            return OwnerAuthResult(OwnerAuthOutcome.EXPIRED_CODE)
        phone, phone_code_hash = challenge
        try:
            await self._client.sign_in(
                phone, code, phone_code_hash=phone_code_hash)
        except SessionPasswordNeededError:
            self._challenges.clear()
            return OwnerAuthResult(OwnerAuthOutcome.PASSWORD_REQUIRED)
        except (PhoneCodeEmptyError, PhoneCodeInvalidError):
            return OwnerAuthResult(OwnerAuthOutcome.INVALID_CODE)
        except (PhoneCodeExpiredError, PhoneCodeHashEmptyError):
            self._challenges.pop(challenge_id, None)
            return OwnerAuthResult(OwnerAuthOutcome.EXPIRED_CODE)
        except (FloodWaitError, FloodPremiumWaitError) as exc:
            return OwnerAuthResult(OwnerAuthOutcome.RATE_LIMITED, retry_after_seconds=exc.seconds)
        except (RPCError, OSError, asyncio.TimeoutError):
            return OwnerAuthResult(OwnerAuthOutcome.UNAVAILABLE)
        self._challenges.clear()
        return OwnerAuthResult(OwnerAuthOutcome.AUTHORIZED)

    async def sign_in_password(self, password: str) -> OwnerAuthResult:
        try:
            await self._client.sign_in(password=password)
        except PasswordHashInvalidError:
            return OwnerAuthResult(OwnerAuthOutcome.INVALID_PASSWORD)
        except (FloodWaitError, FloodPremiumWaitError) as exc:
            return OwnerAuthResult(OwnerAuthOutcome.RATE_LIMITED, retry_after_seconds=exc.seconds)
        except (RPCError, OSError, asyncio.TimeoutError):
            return OwnerAuthResult(OwnerAuthOutcome.UNAVAILABLE)
        self._challenges.clear()
        return OwnerAuthResult(OwnerAuthOutcome.AUTHORIZED)
