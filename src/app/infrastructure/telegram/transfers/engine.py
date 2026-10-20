"""One engine per process owns all download/upload request budgets."""

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from telethon.client.downloads import _CdnRedirect
from telethon.errors import FloodPremiumWaitError, FloodWaitError

from app.application.ports import BoundedDownloadTarget
from app.application.performance import JobTrace
from app.domain.errors import DownloadLimitExceeded
from app.infrastructure.telegram.errors import telegram_errors
from app.infrastructure.telegram.media_mapping import _media_suffix, file_size
from app.infrastructure.telegram.retry import TelegramRetryCoordinator
from app.infrastructure.telegram.transfers.parts import (
    DEFAULT_STREAMS, DOWNLOAD_STREAMS_TOTAL, FAST_THRESHOLD, UPLOAD_PART_STREAMS,
    fast_download, fast_upload,
)

logger = logging.getLogger(__name__)


class _RequestBudget:
    """Process-wide request gate that counts in-flight requests on each job trace."""

    def __init__(self, limit: int):
        self._semaphore = asyncio.Semaphore(limit)

    @asynccontextmanager
    async def slot(self, trace: JobTrace | None):
        await self._semaphore.acquire()
        tracked = False
        try:
            if trace:
                trace.request_started()
                tracked = True
            yield
        finally:
            if trace and tracked:
                trace.request_finished()
            self._semaphore.release()


class TransferEngine:
    def __init__(self, user, bot, *, owner_retry=None, bot_retry=None,
                 per_file_streams: int = DEFAULT_STREAMS,
                 download_request_budget: int = DOWNLOAD_STREAMS_TOTAL,
                 upload_request_budget: int = UPLOAD_PART_STREAMS):
        self.user, self.bot = user, bot
        self.owner_retry = owner_retry or TelegramRetryCoordinator(account_role="owner")
        self.bot_retry = bot_retry or TelegramRetryCoordinator(account_role="bot")
        self.per_file_streams = per_file_streams
        self.download_request_budget = download_request_budget
        self.upload_request_budget = upload_request_budget
        self._download_gate = _RequestBudget(download_request_budget)
        self._upload_gate = _RequestBudget(upload_request_budget)

    async def download(self, message, destination: Path | BoundedDownloadTarget, progress,
                       trace: JobTrace | None = None) -> Path | None:
        size = file_size(message)
        bounded_target = not isinstance(destination, Path)
        if bounded_target and size is not None and size > destination.max_bytes:
            raise DownloadLimitExceeded(size, destination.max_bytes)
        started = time.perf_counter()
        route = "mono"
        with telegram_errors():
            if size and size >= FAST_THRESHOLD and not bounded_target:
                route = "fast"
                if trace:
                    trace.route(f"download:{route}")
                path = str(destination) + _media_suffix(message)
                downloaded = 0

                def track_progress(done, total):
                    nonlocal downloaded
                    downloaded = max(downloaded, int(done))
                    if progress:
                        progress(downloaded, total)

                try:
                    await fast_download(
                        self.user, message, path, size, progress=track_progress,
                        streams=self.per_file_streams, gate=self._download_gate,
                        retry=self.owner_retry, trace=trace)
                except _CdnRedirect:
                    # Only the high-level helper handles CDN redirects in Telethon 1.45.
                    # That opaque fallback restarts the file; it cannot resume these ranges.
                    route = "cdn-fallback"
                    if trace:
                        trace.record_retransferred(downloaded)
                        trace.record_discarded_progress_bytes(downloaded)
                        trace.fallback("download_cdn")
                        trace.route(f"download:{route}")
                        trace.mark_request_tracking_unknown()
                    async def transfer_serial():
                        return await self.user.download_media(
                            message, file=path, progress_callback=track_progress)

                    path = await self.owner_retry.call(
                        transfer_serial, trace, safe_retry=True,
                        repeated_bytes=lambda: downloaded)
            elif bounded_target:
                if trace:
                    trace.route("download:bounded-stream")
                    trace.mark_request_tracking_unknown()
                downloaded = 0

                def track_progress(done, total):
                    nonlocal downloaded
                    downloaded = max(downloaded, int(done))
                    if progress:
                        progress(downloaded, total)

                async def transfer_serial():
                    # Retry may reuse the same stream after a partial SDK write.
                    destination.seek(0)
                    destination.truncate(0)
                    return await self.user.download_media(
                        message, file=destination, progress_callback=track_progress)

                result = await self.owner_retry.call(
                    transfer_serial, trace, safe_retry=True,
                    repeated_bytes=lambda: downloaded)
                if result is None:
                    path = None
                else:
                    destination.flush()
                    path = str(destination.path)
                    route = "bounded-stream"
            else:
                if trace:
                    trace.route(f"download:{route}")
                    trace.mark_request_tracking_unknown()
                downloaded = 0

                def track_progress(done, total):
                    nonlocal downloaded
                    downloaded = max(downloaded, int(done))
                    if progress:
                        progress(downloaded, total)

                async def transfer_serial():
                    return await self.user.download_media(
                        message, file=str(destination), progress_callback=track_progress)

                path = await self.owner_retry.call(
                    transfer_serial, trace, safe_retry=True,
                    repeated_bytes=lambda: downloaded)
            if path is not None and size is not None and Path(path).stat().st_size != size:
                raise OSError("downloaded file size differs from source metadata")
            if trace and path is not None and route != "fast":
                trace.record_verified_download_bytes(Path(path).stat().st_size)
        if trace:
            trace.profile.update(self.profile())
        logger.info("download job_id=%s route=%s bytes=%s duration_s=%.3f",
                    trace.job_id if trace else None, route, size, time.perf_counter() - started)
        return Path(path) if path is not None else None

    async def upload(self, path: Path, progress, trace: JobTrace | None = None):
        size = path.stat().st_size
        if size < FAST_THRESHOLD:
            if trace:
                trace.route("upload:serial")
                trace.profile.update(self.profile())
                trace.mark_upload_attempts_unknown()
                trace.mark_uploaded_bytes_unknown()
                trace.mark_request_tracking_unknown()
            return str(path)
        started = time.perf_counter()
        with telegram_errors():
            try:
                def track_progress(done, total):
                    if progress:
                        progress(done, total)

                if trace:
                    trace.route("upload:fast")
                    trace.profile.update(self.profile())
                    trace.start_upload_tracking()
                    trace.start_upload_byte_tracking()
                handle = await fast_upload(self.bot, str(path), size, progress=track_progress,
                                           streams=self.per_file_streams, gate=self._upload_gate,
                                           retry=self.bot_retry, trace=trace)
            except (FloodWaitError, FloodPremiumWaitError):
                raise  # Respect account cooldown; never bypass it through another route.
            except (ConnectionError, OSError, TimeoutError) as exc:
                logger.warning("parallel upload failed; using serial route job_id=%s error=%s",
                               trace.job_id if trace else None, type(exc).__name__)
                if trace:
                    # A lost acknowledgement can hide an accepted part; do not report
                    # the acknowledged-byte lower bound as a complete count.
                    trace.mark_retransferred_unknown()
                    trace.mark_upload_attempts_unknown()
                    trace.mark_uploaded_bytes_unknown()
                    trace.mark_request_tracking_unknown()
                    trace.fallback("upload_serial")
                    trace.route("upload:serial-fallback")
                return str(path)
        if trace:
            trace.profile.update(self.profile())
        logger.info("upload job_id=%s route=fast bytes=%s duration_s=%.3f",
                    trace.job_id if trace else None, size, time.perf_counter() - started)
        return handle

    def profile(self) -> dict[str, int]:
        return {
            "per_file_streams": self.per_file_streams,
            "download_request_budget": self.download_request_budget,
            "upload_request_budget": self.upload_request_budget,
            "fast_threshold_bytes": FAST_THRESHOLD,
        }
