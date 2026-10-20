"""Resolve a post into a request-owned adapter; raw Messages stay here."""

from pathlib import Path

from app.application.ports import BoundedDownloadTarget
from app.domain.models import MediaItem, ParsedLink
from app.application.performance import JobTrace
from app.infrastructure.telegram.errors import telegram_errors
from app.infrastructure.telegram.media_mapping import to_media_item

ALBUM_LOOKBACK = 10


class TelegramPost:
    def __init__(self, message, siblings, engine, trace: JobTrace | None = None):
        self.text = message.raw_text or ""
        self.has_media = bool(message.media)
        self.items = tuple(to_media_item(item) for item in siblings)
        self._messages = {item.id: item for item in siblings}
        self._engine = engine
        self._trace = trace

    async def download(self, item: MediaItem, destination: Path | BoundedDownloadTarget, progress,
                       trace: JobTrace | None = None) -> Path | None:
        return await self._engine.download(
            self._messages[item.id], destination, progress,
            trace if trace is not None else self._trace)


class TelegramSource:
    def __init__(self, client, engine):
        self.client, self.engine = client, engine

    async def resolve(self, link: ParsedLink, trace: JobTrace | None = None) -> TelegramPost | None:
        with telegram_errors():
            async def fetch_message():
                return await self.client.get_messages(link.chat, ids=[link.message_id])

            retry = getattr(self.engine, "owner_retry", None)
            fetched = (await retry.call(fetch_message, trace, safe_retry=True) if retry else
                       await fetch_message())
            message = fetched[0] if isinstance(fetched, list) and fetched else fetched
            if not message:
                return None
            siblings = [message]
            if getattr(message, "grouped_id", None):
                lo = max(1, message.id - ALBUM_LOOKBACK)
                async def fetch_album():
                    return await self.client.get_messages(
                        link.chat, ids=list(range(lo, message.id + ALBUM_LOOKBACK + 1)))

                nearby = (await retry.call(fetch_album, trace, safe_retry=True) if retry else
                          await fetch_album())
                siblings = sorted((item for item in nearby if item and
                                   item.grouped_id == message.grouped_id), key=lambda item: item.id)
                if not siblings:
                    siblings = [message]
            return TelegramPost(message, siblings, self.engine, trace)
