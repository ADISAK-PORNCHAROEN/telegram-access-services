"""Resolve → filter → download → deliver → cleanup through injected ports."""

import asyncio
import logging
import sys
import time
from contextlib import AbstractContextManager, asynccontextmanager

from app.application.concurrency import gather_owned
from app.application.ports import AccessStore, CopyObserver, Delivery, MediaSource, Workspace
from app.application.performance import JobTrace
from app.domain.errors import (AccessDenied, DeliveryUnknown, DownloadLimitExceeded,
                               RateLimited, SessionExpired, SourceUnavailable,
                               WorkspaceBudgetExceeded)
from app.domain.models import CopyRequest, CopyResult, CopyStatus, DownloadedMedia, TransferStats
from app.domain.policies import MAX_SEND_BYTES, exceeds_send_limit, matches

logger = logging.getLogger(__name__)


class _BestEffortObserver:
    """Keep progress and chat notices from changing a copy outcome."""

    def __init__(self, observer, trace: JobTrace):
        self.observer, self.trace = observer, trace

    def _failed(self, operation: str, exc: Exception) -> None:
        self.trace.observer_error(type(exc).__name__)
        logger.warning("copy observer failed job_id=%s operation=%s error=%s",
                       self.trace.job_id, operation, type(exc).__name__)

    async def notice(self, code: str, **data) -> None:
        try:
            await self.observer.notice(code, **data)
        except Exception as exc:
            self._failed(f"notice:{code}", exc)

    def download_progress(self, index: int, done: int, total: int) -> None:
        try:
            self.observer.download_progress(index, done, total)
        except Exception as exc:
            self._failed("download_progress", exc)

    def upload_progress(self, done: int, total: int) -> None:
        try:
            self.observer.upload_progress(done, total)
        except Exception as exc:
            self._failed("upload_progress", exc)

    def resume_progress(self) -> None:
        try:
            self.observer.resume_progress()
        except Exception as exc:
            self._failed("resume_progress", exc)


class _PipelineBufferBudget:
    """Reserve known album bytes before downloads; one process-wide cap."""

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.used = 0
        self.peak = 0
        self._condition = asyncio.Condition()

    @asynccontextmanager
    async def reserve(self, size: int):
        async with self._condition:
            await self._condition.wait_for(lambda: self.used + size <= self.capacity)
            self.used += size
            self.peak = max(self.peak, self.used)
        try:
            yield
        finally:
            async with self._condition:
                self.used -= size
                self._condition.notify_all()


class _TimedWorkspace(AbstractContextManager):
    """Measure the adapter's __exit__ cleanup without including transfer time."""

    def __init__(self, manager, trace: JobTrace):
        self.manager, self.trace = manager, trace

    def __enter__(self):
        return self.manager.__enter__()

    def __exit__(self, exc_type, exc, tb):
        with self.trace.measure("workspace_cleanup"):
            try:
                return self.manager.__exit__(exc_type, exc, tb)
            except Exception as cleanup_error:
                self.trace.cleanup_errors.append(type(cleanup_error).__name__)
                logger.error("workspace cleanup failed job_id=%s error=%s",
                             self.trace.job_id, type(cleanup_error).__name__)
                # Keep the original CopyResult/exception intact; the terminal trace
                # records the failed cleanup separately.
                return False


class _ReservedTimedWorkspace:
    """Acquire capacity before creating the directory; release after cleanup."""

    def __init__(self, workspace, message_id: int, size: int, trace: JobTrace,
                 on_wait):
        self.workspace = workspace
        self.message_id = message_id
        self.size = size
        self.trace = trace
        self.on_wait = on_wait
        self._reservation_context = None
        self._timed_workspace = None

    async def __aenter__(self):
        self._reservation_context = self.workspace.reserve(
            self.size, on_wait=self.on_wait)
        reservation = await self._reservation_context.__aenter__()
        self.trace.observations["workspace_logical_reserved_bytes"] = self.size
        self.trace.observations["filesystem_free_bytes_at_reservation"] = (
            reservation.filesystem_free_bytes)
        self.trace.observations["filesystem_used_bytes_at_reservation"] = (
            reservation.filesystem_used_bytes)
        self._timed_workspace = _TimedWorkspace(
            self.workspace.create(self.message_id, reservation=reservation), self.trace)
        try:
            return self._timed_workspace.__enter__()
        except BaseException:
            await self._reservation_context.__aexit__(*sys.exc_info())
            raise

    async def __aexit__(self, exc_type, exc, tb):
        try:
            return self._timed_workspace.__exit__(exc_type, exc, tb)
        finally:
            await self._reservation_context.__aexit__(exc_type, exc, tb)


class CopyMedia:
    def __init__(self, source: MediaSource, delivery: Delivery, workspace: Workspace,
                 access: AccessStore, *, album_transfer_strategy: str = "sequential",
                 album_pipeline_max_active_files: int = 2,
                 album_pipeline_max_buffer_bytes: int = 64 * 1024 * 1024):
        self.source, self.delivery = source, delivery
        self.workspace, self.access = workspace, access
        self.album_transfer_strategy = album_transfer_strategy
        self.album_pipeline_max_active_files = album_pipeline_max_active_files
        self.album_pipeline_max_buffer_bytes = album_pipeline_max_buffer_bytes
        self._pipeline_buffer = _PipelineBufferBudget(album_pipeline_max_buffer_bytes)

    async def _download_item(self, post, index, item, directory, observer, trace,
                             on_progress=None):
        def progress(done, total):
            observer.download_progress(index, done, total)
            if on_progress:
                on_progress(index, done, total)

        if item.size is None or item.size <= 0:
            destination = directory / f"m{item.id}{item.suffix or '.bin'}"
            with self.workspace.bounded_writer(destination, MAX_SEND_BYTES) as target:
                path = await post.download(item, target, progress, trace)
        else:
            path = await post.download(item, directory / f"m{item.id}", progress, trace)
        if path is None:
            return None
        if on_progress:
            actual_size = path.stat().st_size
            on_progress(index, actual_size, item.size or actual_size)
        return DownloadedMedia(item, path, index)

    async def _pipeline_downloads(self, post, items, directory, observer, trace,
                                 stats, started):
        queue = asyncio.Queue(maxsize=self.album_pipeline_max_active_files)
        worker_count = min(len(items), max(1, self.album_pipeline_max_active_files - 1))
        completed = 0
        downloaded_at = None
        buffered_by_file = {}
        peak_buffered = 0

        def record_buffered(index, done, _total):
            nonlocal peak_buffered
            buffered_by_file[index] = max(0, int(done))
            current_buffered = sum(buffered_by_file.values())
            peak_buffered = max(peak_buffered, current_buffered)
            trace.record_workspace_payload(current_buffered)

        async def worker(worker_index):
            nonlocal completed, downloaded_at
            try:
                for index in range(worker_index, len(items), worker_count):
                    item = items[index]
                    media = await self._download_item(
                        post, index, item, directory, observer, trace, record_buffered)
                    if media is not None:
                        stats.files += 1
                        stats.bytes += int(media.item.size or 0)
                    completed += 1
                    if completed == len(items):
                        downloaded_at = time.perf_counter()
                    await queue.put((index, media, None))
            except Exception as exc:
                await queue.put((None, None, exc))
                raise

        producer = asyncio.create_task(gather_owned(*(worker(i) for i in range(worker_count))))

        async def next_result():
            if not queue.empty():
                return queue.get_nowait()
            getter = asyncio.create_task(queue.get())
            try:
                done, _ = await asyncio.wait(
                    (getter, producer), return_when=asyncio.FIRST_COMPLETED)
                if getter in done:
                    return getter.result()
                if producer.cancelled():
                    raise asyncio.CancelledError
                error = producer.exception()
                if error is not None:
                    raise error
                raise RuntimeError("pipeline producer ended before all downloads completed")
            finally:
                if not getter.done():
                    getter.cancel()
                await asyncio.gather(getter, return_exceptions=True)

        try:
            for _ in items:
                _index, media, error = await next_result()
                if error is not None:
                    raise error
                if media is not None:
                    yield media
            await producer
        finally:
            if not producer.done():
                producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
            elapsed = max(0.0, (downloaded_at or time.perf_counter()) - started)
            stats.download_s = elapsed
            trace.add_duration("download", elapsed)
            trace.observations["pipeline_peak_buffered_bytes"] = peak_buffered

    async def execute(self, request: CopyRequest, observer: CopyObserver,
                      trace: JobTrace | None = None) -> CopyResult:
        trace = trace or JobTrace(request.job_id or "local")
        original_observer = observer
        observer = _BestEffortObserver(observer, trace)
        trace.profile.update({
            "album_strategy_requested": self.album_transfer_strategy,
            "pipeline_max_active_files": self.album_pipeline_max_active_files,
            "pipeline_buffer_limit_bytes": self.album_pipeline_max_buffer_bytes,
        })
        stats = TransferStats()
        started = None
        stage = "resolve"
        workspace_progress = {}

        def record_workspace_progress(index, done, _total):
            workspace_progress[index] = max(
                workspace_progress.get(index, 0), max(0, int(done)))
            trace.record_workspace_payload(sum(workspace_progress.values()))

        try:
            if not self.access.is_allowed(request.sender) or request.reply_chat != request.sender:
                raise AccessDenied()
            await trace.persist_stage("resolve")
            with trace.measure("resolve"):
                post = await self.source.resolve(request.link, trace)
            if post is None:
                await observer.notice("missing")
                return CopyResult(CopyStatus.MISSING)
            if not post.has_media:
                stage = "text"
                await trace.persist_stage("send")
                await self.delivery.send_text(request.reply_chat, post.text, trace)
                return CopyResult(CopyStatus.TEXT)

            keeps = [item for item in post.items if matches(item.kind, request.filter_kind)]
            if not keeps:
                await observer.notice("filtered", preference=request.filter_kind,
                                      kind=post.items[0].kind if post.items else None)
                return CopyResult(CopyStatus.FILTERED)
            allowed = [item for item in keeps if not exceeds_send_limit(item.size)]
            if not allowed:
                await observer.notice("too_big", size=keeps[0].size)
                return CopyResult(CopyStatus.TOO_BIG)
            if len(allowed) != len(keeps):
                await observer.notice("large_skipped", skipped=len(keeps) - len(allowed),
                                      remaining=len(allowed))

            pipeline_reason = None
            stream_delivery = getattr(self.delivery, "send_media_stream", None)
            if self.album_transfer_strategy != "pipeline":
                pipeline_reason = "disabled"
            elif len(allowed) < 2:
                pipeline_reason = "single-file"
            elif not callable(stream_delivery):
                pipeline_reason = "delivery-stream-unavailable"
            elif any(item.size is None or item.size <= 0 for item in allowed):
                pipeline_reason = "unknown-size"
            elif sum(item.size for item in allowed) > self.album_pipeline_max_buffer_bytes:
                pipeline_reason = "buffer-limit"
            use_pipeline = pipeline_reason is None
            trace.observations["album_strategy"] = "pipeline" if use_pipeline else "sequential"
            if self.album_transfer_strategy == "pipeline" and pipeline_reason:
                trace.observations["pipeline_fallback_reason"] = pipeline_reason

            workspace_bytes = sum(
                item.size if item.size is not None and item.size > 0 else MAX_SEND_BYTES
                for item in allowed)

            async def notify_capacity_wait():
                await observer.notice("capacity_wait")

            async with _ReservedTimedWorkspace(
                    self.workspace, request.link.message_id, workspace_bytes, trace,
                    notify_capacity_wait) as directory:
                await observer.notice("download", items=allowed)

                async def notify_rate_wait(seconds, wait_kind, account_role):
                    await observer.notice(
                        "rate_wait", seconds=seconds, wait_kind=wait_kind,
                        account_role=account_role)

                async def finish_rate_wait():
                    observer.resume_progress()

                trace.set_wait_notifier(notify_rate_wait, finish_rate_wait)
                stage, started = "download", time.perf_counter()
                await trace.persist_stage("download")
                if use_pipeline:
                    expected_bytes = sum(item.size for item in allowed)
                    trace.observations["pipeline_reserved_bytes"] = expected_bytes
                    stage = "pipeline"
                    buffer_wait_started = time.perf_counter()
                    async with self._pipeline_buffer.reserve(expected_bytes):
                        pipeline_started = time.perf_counter()
                        trace.add_duration(
                            "pipeline_buffer_wait", pipeline_started - buffer_wait_started)
                        stream = self._pipeline_downloads(
                            post, allowed, directory, observer, trace, stats,
                            pipeline_started)
                        try:
                            await observer.notice("upload")
                            await trace.persist_stage("upload")
                            upload_started = time.perf_counter()
                            try:
                                sent = await stream_delivery(
                                    request.reply_chat, stream, expected_bytes,
                                    observer.upload_progress, trace)
                            finally:
                                stats.upload_s = time.perf_counter() - upload_started
                        finally:
                            await stream.aclose()
                    if not stats.files:
                        await observer.notice("no_media")
                        return CopyResult(CopyStatus.FAILED, stats, "no-media-body")
                    if sent.total != stats.files:
                        raise RuntimeError("pipeline delivery did not prepare every downloaded file")
                else:
                    async def download(index, item):
                        return await self._download_item(
                            post, index, item, directory, observer, trace,
                            record_workspace_progress)

                    with trace.measure("download"):
                        downloaded = await gather_owned(
                            *(download(i, item) for i, item in enumerate(allowed)))
                    files = [item for item in downloaded if item is not None]
                    stats.download_s = time.perf_counter() - started
                    stats.files = len(files)
                    stats.bytes = sum(item.path.stat().st_size for item in files)
                    if not files:
                        await observer.notice("no_media")
                        return CopyResult(CopyStatus.FAILED, stats, "no-media-body")
                    await observer.notice("upload")
                    await trace.persist_stage("upload")
                    stage, started = "upload", time.perf_counter()
                    sent = await self.delivery.send_media(
                        request.reply_chat, files, observer.upload_progress, trace)
                    stats.upload_s = time.perf_counter() - started
                stats.delivered_bytes = sent.delivered_bytes
                if sent.sent != sent.total:
                    try:
                        await observer.notice("partial", sent=sent.sent, total=sent.total)
                    except Exception as exc:
                        logger.warning("partial delivery notice failed error=%s",
                                       type(exc).__name__)
                    return CopyResult(CopyStatus.PARTIAL if sent.sent else CopyStatus.FAILED,
                                      stats, "send-all", sent.sent, sent.total,
                                      sent.outcomes)
                return CopyResult(CopyStatus.OK, stats, sent_files=sent.sent,
                                  total_files=sent.total,
                                  delivery_outcomes=sent.outcomes)
        except WorkspaceBudgetExceeded as exc:
            await observer.notice("workspace_too_big", size=exc.requested_bytes,
                                  limit=exc.budget_bytes)
            return CopyResult(CopyStatus.TOO_BIG, stats, "workspace-budget-exceeded")
        except DownloadLimitExceeded as exc:
            await observer.notice("too_big", size=exc.requested_bytes)
            return CopyResult(CopyStatus.TOO_BIG, stats, "download-size-limit-exceeded")
        except DeliveryUnknown as exc:
            stats.delivered_bytes = exc.delivered_bytes
            return CopyResult(CopyStatus.DELIVERY_UNKNOWN, stats,
                              "send-acknowledgement-unknown", exc.sent_files,
                              exc.total_files, exc.delivery_outcomes)
        except AccessDenied:
            raise
        except (SourceUnavailable, SessionExpired) as exc:
            await observer.notice("source_unavailable" if isinstance(exc, SourceUnavailable)
                                  else "session_expired")
            return CopyResult(CopyStatus.FAILED, stats, type(exc).__name__)
        except RateLimited as exc:
            trace.record_retry_stop(exc.stop_reason)
            await observer.notice("rate_limited", seconds=exc.seconds,
                                  reason=exc.stop_reason)
            return CopyResult(CopyStatus.FAILED, stats,
                              f"rate-limit-{exc.stop_reason}")
        except Exception as exc:
            logger.error("copy failed during %s (%s)", stage, type(exc).__name__)
            await observer.notice("error", stage=stage, error=type(exc).__name__)
            return CopyResult(CopyStatus.FAILED, stats, type(exc).__name__)
        finally:
            if started is not None:
                if stage == "download" and stats.download_s is None:
                    stats.download_s = time.perf_counter() - started
                if stage == "upload" and stats.upload_s is None:
                    stats.upload_s = time.perf_counter() - started
            with trace.measure("observer_cleanup"):
                try:
                    await original_observer.close()
                except Exception as cleanup_error:
                    # Observer/progress cleanup must not replace the copy outcome.
                    trace.cleanup_errors.append(f"observer:{type(cleanup_error).__name__}")
                    logger.error("copy observer cleanup failed job_id=%s error=%s",
                                 trace.job_id, type(cleanup_error).__name__)
