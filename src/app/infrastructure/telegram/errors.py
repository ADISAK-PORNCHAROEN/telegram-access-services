"""Translate SDK failures at the adapter boundary."""

from contextlib import contextmanager

from telethon.errors import (AuthKeyDuplicatedError, AuthKeyUnregisteredError,
                             ChannelInvalidError, ChannelPrivateError,
                             FloodPremiumWaitError, FloodWaitError,
                             SessionRevokedError)

from app.domain.errors import RateLimited, SessionExpired, SourceUnavailable


@contextmanager
def telegram_errors():
    try:
        yield
    except (FloodWaitError, FloodPremiumWaitError) as exc:
        raise RateLimited(int(exc.seconds)) from exc
    except (AuthKeyDuplicatedError, AuthKeyUnregisteredError, SessionRevokedError) as exc:
        raise SessionExpired() from exc
    except (ChannelPrivateError, ChannelInvalidError) as exc:
        raise SourceUnavailable() from exc
    except ValueError as exc:
        if "input entity" in str(exc).lower():
            raise SourceUnavailable() from exc
        raise
