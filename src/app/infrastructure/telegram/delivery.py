"""Upload once and retain handles when an album needs individual delivery."""

import logging
from collections.abc import AsyncIterable
from pathlib import Path

from telethon.errors import (FloodPremiumWaitError, FloodWaitError, MediaInvalidError,
                             RPCError, RpcCallFailError, ServerError)

from app.application.concurrency import gather_owned
from app.application.performance import JobTrace
from app.domain.errors import AccessDenied, DeliveryUnknown, RateLimited
from app.domain.models import (DeliveryIntent, DeliveryIntentItem, DeliveryOutcome,
                               DeliveryResult, DownloadedMedia)
from app.infrastructure.telegram.errors import telegram_errors
from app.infrastructure.telegram.transfers.parts import is_streamable_video

logger = logging.getLogger(__name__)


def _message_id(message) -> int | None:
    if isinstance(message, int):
        return message
    value = getattr(message, "id", None)
    return value if isinstance(value, int) else None


def _returned_messages(response, count: int) -> list:
    if isinstance(response, (list, tuple)):
        return list(response[:count])
    return [response] if count else []


class TelegramDelivery:
    def __init__(self, bot, engine):
        self.bot, self.engine = bot, engine

    async def send_text(self, chat: int, text: str,
                        trace: JobTrace | None = None) -> None:
        intent = DeliveryIntent("text")
        with telegram_errors():
            attempt_id = await trace.before_final_send(intent) if trace else None
            try:
                response = await self.bot.send_message(
                    chat, text or "(ข้อความนี้ไม่มีตัวอักษรและไม่มีไฟล์)")
            except (OSError, TimeoutError, ServerError, RpcCallFailError) as exc:
                raise DeliveryUnknown() from exc
            except RPCError:
                await self._record_outcomes(
                    trace, attempt_id,
                    (DeliveryOutcome(None, None, "rejected"),))
                raise
            message_id = _message_id(response)
            if message_id is None:
                # Telethon can receive an update without a usable response
                # mapping. Keep the intent unresolved; the send may have landed.
                raise DeliveryUnknown()
            outcome = DeliveryOutcome(
                None, None, "sent", message_id=message_id)
            try:
                await self._record_outcomes(trace, attempt_id, (outcome,))
            except Exception as exc:
                raise DeliveryUnknown() from exc

    async def send_media(self, chat: int, files: list[DownloadedMedia], progress,
                         trace: JobTrace | None = None) -> DeliveryResult:
        sizes = [file.path.stat().st_size for file in files]
        marks = [0] * len(files)

        async def prepare(index, file):
            def report(done, _total):
                marks[index] = done
                progress(sum(marks), sum(sizes))
            return (await self.engine.upload(file.path, report, trace) if trace else
                    await self.engine.upload(file.path, report))

        if trace:
            with trace.measure("upload_prepare"):
                entries = await gather_owned(*(prepare(i, file) for i, file in enumerate(files)))
        else:
            entries = await gather_owned(*(prepare(i, file) for i, file in enumerate(files)))

        return await self._send_prepared(chat, files, entries, sizes, trace)

    async def send_media_stream(self, chat: int, files: AsyncIterable[DownloadedMedia],
                                expected_total_bytes: int, progress,
                                trace: JobTrace | None = None) -> DeliveryResult:
        prepared = []
        marks = {}

        async def prepare_stream():
            async for file in files:
                position = file.position if file.position is not None else len(prepared)
                size = file.path.stat().st_size
                marks[position] = 0

                def report(done, _total):
                    marks[position] = done
                    progress(sum(marks.values()), expected_total_bytes)

                if trace:
                    with trace.measure("upload_prepare"):
                        entry = await self.engine.upload(file.path, report, trace)
                else:
                    entry = await self.engine.upload(file.path, report)
                prepared.append((position, file, entry, size))

        await prepare_stream()

        prepared.sort(key=lambda row: row[0])
        ordered_files = [row[1] for row in prepared]
        entries = [row[2] for row in prepared]
        sizes = [row[3] for row in prepared]
        if not prepared:
            return DeliveryResult(0, 0, 0)
        return await self._send_prepared(chat, ordered_files, entries, sizes, trace)

    async def _record_outcomes(self, trace, attempt_id, outcomes) -> None:
        if trace is not None and attempt_id is not None:
            await trace.record_final_send_outcomes(attempt_id, outcomes)

    async def _send_prepared(self, chat, files, entries, sizes,
                             trace: JobTrace | None) -> DeliveryResult:
        captions = [file.item.caption for file in files]
        confirmed: list[DeliveryOutcome] = []
        total = len(files)
        source_positions = {
            id(file): (file.position if file.position is not None else index)
            for index, file in enumerate(files)
        }

        def position_for(file):
            return source_positions[id(file)]

        def intent_for(selected_files):
            items = tuple(
                DeliveryIntentItem(
                    position=position_for(file),
                    item_id=file.item.id,
                    size_bytes=file.path.stat().st_size)
                for index, file in enumerate(selected_files))
            return DeliveryIntent("album" if len(items) > 1 else "media", items)

        def rejected_outcomes(selected_files):
            return tuple(DeliveryOutcome(
                position_for(file),
                file.item.id, "rejected")
                for index, file in enumerate(selected_files))

        async def send(selected_entries, selected_files, selected_captions,
                       supports_streaming):
            intent = intent_for(selected_files)

            async def submit():
                attempt_id = await trace.before_final_send(intent) if trace else None
                try:
                    response = await self.bot.send_file(
                        chat,
                        selected_entries[0] if len(selected_entries) == 1 else selected_entries,
                        caption=(selected_captions[0] or None)
                        if len(selected_entries) == 1 else selected_captions,
                        supports_streaming=supports_streaming)
                except (OSError, TimeoutError, ServerError, RpcCallFailError) as exc:
                    raise DeliveryUnknown(
                        sent_files=len(confirmed), total_files=total,
                        delivered_bytes=sum(item.delivered_bytes or 0
                                            for item in confirmed),
                        delivery_outcomes=tuple(confirmed)) from exc
                except RPCError:
                    await self._record_outcomes(
                        trace, attempt_id, rejected_outcomes(selected_files))
                    raise

                messages = _returned_messages(response, len(selected_files))
                outcomes = tuple(
                    DeliveryOutcome(
                        position_for(file), file.item.id, "sent",
                        message_id=message_id,
                        delivered_bytes=file.path.stat().st_size)
                    for index, file in enumerate(selected_files)
                    if index < len(messages)
                    and (message_id := _message_id(messages[index])) is not None)
                try:
                    if outcomes:
                        await self._record_outcomes(trace, attempt_id, outcomes)
                except Exception as exc:
                    # Telegram replied, but the durable confirmation did not commit.
                    # Leave its intent unresolved so recovery reports uncertainty.
                    raise DeliveryUnknown(
                        sent_files=len(confirmed), total_files=total,
                        delivered_bytes=sum(item.delivered_bytes or 0
                                            for item in confirmed),
                        delivery_outcomes=tuple(confirmed)) from exc
                confirmed.extend(outcomes)
                if len(outcomes) != len(selected_files):
                    # Some items may have been delivered, but Telethon did not
                    # map every response. Persist mapped items above and leave
                    # the attempt unresolved so no missing item is replayed.
                    raise DeliveryUnknown(
                        sent_files=len(confirmed), total_files=total,
                        delivered_bytes=delivered_bytes(),
                        delivery_outcomes=tuple(confirmed))
                return response

            retry = getattr(self.engine, "bot_retry", None)
            if retry:
                contains_path = any(
                    isinstance(entry, (str, Path)) or
                    isinstance(entry, (list, tuple)) and
                    any(isinstance(item, (str, Path)) for item in entry)
                    for entry in selected_entries)
                await retry.call(submit, trace, safe_retry=False,
                                 retry_flood_wait=not contains_path)
            else:
                try:
                    await submit()
                except (ServerError, RpcCallFailError) as exc:
                    # Network-level failures have no definitive send outcome.
                    raise DeliveryUnknown(
                        sent_files=len(confirmed), total_files=total,
                        delivered_bytes=sum(item.delivered_bytes or 0
                                            for item in confirmed),
                        delivery_outcomes=tuple(confirmed)) from exc

        delivered_bytes = lambda: sum(
            item.delivered_bytes or 0 for item in confirmed)

        with telegram_errors():
            try:
                if trace:
                    with trace.measure("final_send"):
                        await send(entries, files, captions,
                                   any(is_streamable_video(str(file.path)) for file in files))
                else:
                    await send(entries, files, captions,
                               any(is_streamable_video(str(file.path)) for file in files))
                return DeliveryResult(len(confirmed), total, delivered_bytes(),
                                      tuple(confirmed))
            except FloodWaitError:
                raise
            except MediaInvalidError as exc:
                if len(entries) == 1:
                    raise
                logger.warning("album rejected; using individual sends (%s)",
                               type(exc).__name__)

            # The album rejection is definite; retry each retained handle once.
            for entry, file in zip(entries, files):
                try:
                    if trace:
                        with trace.measure("final_send"):
                            await send([entry], [file], [file.item.caption],
                                       is_streamable_video(str(file.path)))
                    else:
                        await send([entry], [file], [file.item.caption],
                                   is_streamable_video(str(file.path)))
                except DeliveryUnknown:
                    raise
                except (FloodWaitError, FloodPremiumWaitError, RateLimited,
                        AccessDenied):
                    if confirmed:
                        return DeliveryResult(len(confirmed), total, delivered_bytes(),
                                              tuple(confirmed))
                    raise
                except RPCError as exc:
                    logger.warning("individual media rejected (%s)", type(exc).__name__)
            return DeliveryResult(len(confirmed), total, delivered_bytes(),
                                  tuple(confirmed))
