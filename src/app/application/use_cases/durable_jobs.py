"""Durable admission and process-owned workers for copy jobs."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from uuid import uuid4

from app.application.performance import JobTrace, use_active_job_deadline
from app.application.ports import (
    AccessStore, CopyObserver, JobStore, PerformanceSink,
)
from app.application.use_cases.copy_media import CopyMedia
from app.domain.errors import AccessDenied
from app.domain.models import (
    CopyJobInput, CopyRequest, CopyResult, CopyStatus, DeliveryIntent,
    DeliveryOutcome, JobAdmission, JobState, MediaKind, ParsedLink,
)

logger = logging.getLogger(__name__)
LEASE_HEARTBEAT_SECONDS = 5.0


class AcceptCopyJob:
    def __init__(self, store: JobStore, access: AccessStore):
        self.store, self.access = store, access
        self._admission_open = True
        self._in_flight_admissions = 0
        self._admissions_idle = asyncio.Event()
        self._admissions_idle.set()

    def stop_admission(self) -> None:
        """Synchronously reject durable jobs that have not started admission."""
        self._admission_open = False

    async def wait_for_admissions(self) -> None:
        """Wait until every admission that started before shutdown has finished."""
        await self._admissions_idle.wait()

    async def execute(self, *, receiving_bot_id: int, requester_id: int,
                      destination_chat: int, request_message_id: int,
                      link_index: int, source: ParsedLink,
                      filter_kind: MediaKind | None,
                      max_pending: int, max_pending_per_user: int) -> JobAdmission:
        if (not self.access.is_allowed(requester_id)
                or destination_chat != requester_id):
            raise AccessDenied()
        if not self._admission_open:
            return JobAdmission(False, rejection_reason="shutting-down")

        self._in_flight_admissions += 1
        self._admissions_idle.clear()
        try:
            job = CopyJobInput(
                job_id=uuid4().hex,
                receiving_bot_id=receiving_bot_id,
                requester_id=requester_id,
                destination_chat=destination_chat,
                request_message_id=request_message_id,
                link_index=link_index,
                source=source,
                filter_kind=filter_kind,
            )
            return await self.store.accept(
                job, max_pending=max_pending,
                max_pending_per_user=max_pending_per_user)
        finally:
            self._in_flight_admissions -= 1
            if self._in_flight_admissions == 0:
                self._admissions_idle.set()


ObserverFactory = Callable[[CopyJobInput, JobTrace], CopyObserver]
TerminalNotifier = Callable[[CopyJobInput, CopyResult], Awaitable[None]]


class DurableCopyWorker:
    """Claim committed jobs and run the existing CopyMedia use case."""

    def __init__(self, store: JobStore, copy_media: CopyMedia,
                 access: AccessStore, observer_factory: ObserverFactory,
                 terminal_notifier: TerminalNotifier, *, concurrency: int,
                 clock: Callable[[], datetime] | None = None,
                 performance: PerformanceSink | None = None):
        self.store, self.copy_media, self.access = store, copy_media, access
        self.observer_factory = observer_factory
        self.terminal_notifier = terminal_notifier
        self.performance = performance
        self.concurrency = max(1, concurrency)
        self._tasks: list[asyncio.Task] = []
        self._active_jobs: dict[
            str, tuple[asyncio.Task, asyncio.Event, CopyJobInput]] = {}
        self._cancel_before_start: set[str] = set()
        self._wake = asyncio.Event()
        self._stopping = False
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    async def start(self) -> None:
        if self._tasks:
            return
        await self.store.recover_interrupted()
        self._stopping = False
        self._tasks = [asyncio.create_task(self._run(), name=f"copy-worker-{i}")
                       for i in range(self.concurrency)]

    def notify(self) -> None:
        self._wake.set()

    async def cancel(self, job_id: str) -> None:
        active = self._active_jobs.get(job_id)
        if active is None:
            if not self._stopping:
                self._cancel_before_start.add(job_id)
            return
        task, started, job = active
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        if not started.is_set():
            try:
                await asyncio.shield(self.store.release_interrupted(
                    job_id, self._lease_generation(job)))
            except Exception as exc:
                logger.error("durable pre-start cancellation update failed error=%s",
                             type(exc).__name__)

    async def stop(self) -> None:
        self._stopping = True
        self._wake.set()
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run(self) -> None:
        while not self._stopping:
            try:
                job = await self.store.claim_next()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("durable queue poll failed error=%s",
                               type(exc).__name__)
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=1.0)
                except TimeoutError:
                    pass
                continue
            if job is None:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=1.0)
                except TimeoutError:
                    pass
                continue
            if job.job_id in self._cancel_before_start:
                self._cancel_before_start.discard(job.job_id)
                await self.store.release_interrupted(
                    job.job_id, self._lease_generation(job))
                continue
            started = asyncio.Event()
            task = asyncio.create_task(self._run_job(job, started),
                                       name=f"copy-job-{job.job_id}")
            self._active_jobs[job.job_id] = (task, started, job)
            try:
                await task
            except asyncio.CancelledError:
                if self._stopping:
                    raise
            finally:
                self._active_jobs.pop(job.job_id, None)

    async def _run_job(self, job: CopyJobInput, started: asyncio.Event) -> None:
        started.set()
        if job.lease_generation is None:
            logger.error("claimed durable job is missing its lease job_id=%s",
                         job.job_id)
            return

        heartbeat_stop = asyncio.Event()
        heartbeat_guard = asyncio.Lock()
        operation = asyncio.create_task(
            self._execute(job, heartbeat_stop, heartbeat_guard),
            name=f"copy-operation-{job.job_id}")
        heartbeat = asyncio.create_task(
            self._heartbeat(job, heartbeat_stop, heartbeat_guard),
            name=f"copy-heartbeat-{job.job_id}")
        try:
            done, _pending = await asyncio.wait(
                (operation, heartbeat), return_when=asyncio.FIRST_COMPLETED)
            if operation in done:
                await operation
                return

            try:
                heartbeat_stopped = heartbeat.result()
            except Exception as exc:
                logger.warning("durable job heartbeat failed job_id=%s error=%s",
                               job.job_id, type(exc).__name__)
            else:
                if heartbeat_stopped:
                    # A terminal database write stops the heartbeat before the
                    # result notifier performs its network request. The worker
                    # must keep awaiting that operation instead of treating the
                    # intentional heartbeat stop as lease loss.
                    await operation
                    return
                logger.warning("durable job lease was lost job_id=%s",
                               job.job_id)
            operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
        except asyncio.CancelledError:
            operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
            raise
        except Exception as exc:
            logger.error("durable job task failed job_id=%s error=%s",
                         job.job_id, type(exc).__name__)
        finally:
            if not heartbeat.done():
                heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    async def _heartbeat(self, job: CopyJobInput, stop: asyncio.Event,
                         guard: asyncio.Lock) -> bool:
        assert job.lease_generation is not None
        while True:
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=LEASE_HEARTBEAT_SECONDS)
                return True
            except TimeoutError:
                pass
            async with guard:
                if stop.is_set():
                    return True
                async with asyncio.timeout(LEASE_HEARTBEAT_SECONDS):
                    if not await self.store.heartbeat(
                            job.job_id, job.lease_generation):
                        return False

    async def _execute(self, job: CopyJobInput,
                       heartbeat_stop: asyncio.Event,
                       heartbeat_guard: asyncio.Lock) -> None:
        if job.deadline_at is None:
            logger.error("durable job missing accepted deadline job_id=%s",
                         job.job_id)
            await self._finish_result(
                job, CopyResult(CopyStatus.FAILED,
                                reason="missing-accepted-deadline"),
                heartbeat_stop, heartbeat_guard)
            return

        deadline_at = job.deadline_at
        if deadline_at.tzinfo is None:
            deadline_at = deadline_at.replace(tzinfo=timezone.utc)
        if job.deadline_remaining_seconds is not None:
            # Postgres calculates this duration from its own clock, which also
            # created the accepted deadline. Avoid early/late expiry from host
            # clock skew between the worker and database.
            remaining = max(0.0, float(job.deadline_remaining_seconds))
        else:
            now = self._clock()
            if now.tzinfo is None:
                now = now.replace(tzinfo=timezone.utc)
            remaining = (deadline_at - now).total_seconds()
        if remaining <= 0:
            await self._expire_deadline(
                job, CopyResult(CopyStatus.EXPIRED,
                                reason="accepted-job-deadline"),
                heartbeat_stop, heartbeat_guard)
            return

        trace = JobTrace(job.job_id,
                         queue_wait_s=job.claim_queue_wait_seconds)
        trace.start_active()
        loop_deadline = asyncio.get_running_loop().time() + remaining

        async def save_stage(stage):
            try:
                await self.store.set_stage(
                    job.job_id, stage, self._lease_generation(job))
            except Exception:
                if await self.store.cancellation_requested(job.job_id):
                    raise asyncio.CancelledError
                raise

        async def mark_waiting(seconds, wait_kind, wait_account):
            marked = await self.store.mark_waiting(
                job.job_id, max(1.0, float(seconds)), wait_kind, wait_account,
                self._lease_generation(job))
            if not marked:
                if await self.store.cancellation_requested(job.job_id):
                    raise asyncio.CancelledError
                raise RuntimeError("job could not enter waiting state")

        async def resume_waiting():
            resumed = await self.store.resume_waiting(
                job.job_id, self._lease_generation(job))
            if not resumed:
                if await self.store.cancellation_requested(job.job_id):
                    raise asyncio.CancelledError
                raise RuntimeError("job could not resume from waiting state")

        async def mark_send_intent(intent: DeliveryIntent | None = None):
            if (job.destination_chat != job.requester_id
                    or not self.access.is_allowed(job.requester_id)):
                raise AccessDenied()
            attempt_id = await self.store.mark_send_intent(
                job.job_id, self._lease_generation(job), intent)
            if not attempt_id:
                if await self.store.cancellation_requested(job.job_id):
                    raise asyncio.CancelledError
                raise RuntimeError("job is no longer eligible to send")
            return attempt_id

        async def persist_send_outcomes(attempt_id: str,
                                        outcomes: tuple[DeliveryOutcome, ...]):
            await self.store.record_delivery_outcomes(
                job.job_id, attempt_id, outcomes,
                self._lease_generation(job))

        trace.set_job_hooks(stage_notifier=save_stage,
                            before_final_send=mark_send_intent,
                            final_send_outcomes=persist_send_outcomes,
                            wait_started=mark_waiting,
                            wait_finished=resume_waiting)
        request = CopyRequest(
            sender=job.requester_id,
            reply_chat=job.destination_chat,
            link=job.source,
            filter_kind=job.filter_kind,
            job_id=job.job_id,
        )
        observer = None
        result = None
        deadline_timeout = asyncio.timeout(remaining)
        try:
            observer = self.observer_factory(job, trace)
            with use_active_job_deadline(loop_deadline):
                async with deadline_timeout:
                    result = await self.copy_media.execute(
                        request, observer, trace)
        except TimeoutError as exc:
            if deadline_timeout.expired():
                trace.record_retry_stop("active-deadline")
                result = CopyResult(CopyStatus.EXPIRED,
                                    reason="accepted-job-deadline")
            else:
                # Preserve the pre-deadline failure path for a TimeoutError
                # raised by a transfer adapter or another dependency.
                result = CopyResult(CopyStatus.FAILED,
                                    reason=type(exc).__name__)
        except AccessDenied:
            result = CopyResult(CopyStatus.FAILED, reason="access-revoked")
        except asyncio.CancelledError:
            try:
                await asyncio.shield(self.store.release_interrupted(
                    job.job_id, self._lease_generation(job)))
            except Exception:
                logger.error("durable job shutdown state update failed job_id=%s",
                             job.job_id)
            raise
        except Exception as exc:
            logger.error("durable job failed job_id=%s error=%s",
                         job.job_id, type(exc).__name__)
            result = CopyResult(CopyStatus.FAILED, reason=type(exc).__name__)
        finally:
            trace.stop_active()

        if result is not None:
            if (result.status == CopyStatus.EXPIRED
                    or result.reason == "rate-limit-active-deadline"):
                expired = CopyResult(
                    CopyStatus.EXPIRED, result.stats,
                    reason="accepted-job-deadline", sent_files=result.sent_files,
                    total_files=result.total_files)
                await self._expire_deadline(
                    job, expired, heartbeat_stop, heartbeat_guard, trace)
                return
            await self._finish_result(
                job, result, heartbeat_stop, heartbeat_guard, trace)

    async def _finish_result(self, job: CopyJobInput, result: CopyResult,
                             heartbeat_stop: asyncio.Event,
                             heartbeat_guard: asyncio.Lock,
                             trace: JobTrace) -> None:
        try:
            _apply_delivery_summary(result,
                                    await self.store.delivery_summary(job.job_id))
            async with heartbeat_guard:
                await self.store.finish(job.job_id, _terminal_state(result),
                                        _result_payload(result),
                                        self._lease_generation(job))
                heartbeat_stop.set()
        except asyncio.CancelledError:
            try:
                await asyncio.shield(self.store.release_interrupted(
                    job.job_id, self._lease_generation(job)))
            except Exception:
                logger.error("durable canceled finalization update failed job_id=%s",
                             job.job_id)
            raise
        except Exception as exc:
            try:
                cancellation_requested = await self.store.cancellation_requested(
                    job.job_id)
            except Exception:
                cancellation_requested = False
            if cancellation_requested:
                try:
                    await asyncio.shield(self.store.release_interrupted(
                        job.job_id, self._lease_generation(job)))
                except Exception:
                    logger.error("durable cancellation state update failed job_id=%s",
                                 job.job_id)
                return
            # If a send intent exists, startup recovery will report unknown rather
            # than risk replaying a message with an uncertain outcome.
            logger.error("durable terminal state update failed job_id=%s error=%s",
                         job.job_id, type(exc).__name__)
            return
        self._record_performance(trace, result)
        await self._notify_terminal(job, result)

    async def _expire_deadline(self, job: CopyJobInput,
                               result: CopyResult,
                               heartbeat_stop: asyncio.Event,
                               heartbeat_guard: asyncio.Lock,
                               trace: JobTrace | None = None) -> None:
        try:
            async with heartbeat_guard:
                state = await self.store.expire_deadline(
                    job.job_id, _result_payload(result),
                    self._lease_generation(job))
                if state is not None:
                    heartbeat_stop.set()
        except asyncio.CancelledError:
            try:
                await asyncio.shield(
                    self.store.release_interrupted(
                        job.job_id, self._lease_generation(job)))
            except Exception:
                logger.error("durable deadline cancellation cleanup failed job_id=%s",
                             job.job_id)
            raise
        except Exception as exc:
            logger.error("durable deadline update failed job_id=%s error=%s",
                         job.job_id, type(exc).__name__)
            return
        if state == JobState.EXPIRED:
            result.status = CopyStatus.EXPIRED
            result.reason = "accepted-job-deadline"
        elif state == JobState.DELIVERY_UNKNOWN:
            result.status = CopyStatus.DELIVERY_UNKNOWN
            result.reason = "send-acknowledgement-unknown"
            _apply_delivery_summary(
                result, await self.store.delivery_summary(job.job_id))
        elif state == JobState.COMPLETED:
            summary = await self.store.delivery_summary(job.job_id)
            result.status = (CopyStatus.TEXT if summary.get("text_sent")
                             else CopyStatus.OK)
            result.reason = ""
            _apply_delivery_summary(result, summary)
        elif state == JobState.PARTIAL:
            result.status = CopyStatus.PARTIAL
            result.reason = "confirmed-partial-before-finalize"
            _apply_delivery_summary(
                result, await self.store.delivery_summary(job.job_id))
        elif state == JobState.FAILED:
            result.status = CopyStatus.FAILED
            result.reason = "definitive-send-rejection-before-finalize"
            _apply_delivery_summary(
                result, await self.store.delivery_summary(job.job_id))
        else:
            # Cancellation may race with expiry; its own response remains the
            # authoritative notice for that terminal state.
            return
        if trace is not None:
            self._record_performance(trace, result)
        await self._notify_terminal(job, result)

    def _record_performance(self, trace: JobTrace, result: CopyResult) -> None:
        if self.performance is None:
            return
        event = trace.finish(
            status=result.status.value, reason=result.reason or None,
            downloaded_bytes=result.stats.bytes,
            delivered_bytes=result.stats.delivered_bytes,
        )
        try:
            self.performance.write(event)
        except Exception as exc:
            logger.error("durable performance write failed job_id=%s error=%s",
                         trace.job_id, type(exc).__name__)

    @staticmethod
    def _lease_generation(job: CopyJobInput) -> int:
        if job.lease_generation is None:
            raise RuntimeError("durable job is missing its lease generation")
        return job.lease_generation

    async def _notify_terminal(self, job: CopyJobInput,
                               result: CopyResult) -> None:
        try:
            await self.terminal_notifier(job, result)
        except Exception as exc:
            logger.warning("durable terminal notice failed job_id=%s error=%s",
                           job.job_id, type(exc).__name__)


def _terminal_state(result: CopyResult) -> str:
    if result.status in {CopyStatus.OK, CopyStatus.TEXT}:
        return "completed"
    if result.status == CopyStatus.DELIVERY_UNKNOWN:
        return "delivery_unknown"
    if result.status == CopyStatus.EXPIRED:
        return "expired"
    if result.status == CopyStatus.PARTIAL:
        return "partial"
    return "failed"


def _result_payload(result: CopyResult) -> dict:
    return {
        "status": result.status.value,
        "reason": result.reason or None,
        "stats": result.stats.as_dict(),
        "sent_files": result.sent_files,
        "total_files": result.total_files,
        "delivery_outcomes": [outcome.as_dict()
                              for outcome in result.delivery_outcomes],
    }


def _apply_delivery_summary(result: CopyResult, summary: dict) -> None:
    outcomes = summary.get("delivery_outcomes", ())
    result.delivery_outcomes = tuple(
        DeliveryOutcome(**outcome) if isinstance(outcome, dict) else outcome
        for outcome in outcomes)
    if summary.get("total_files") is not None:
        result.sent_files = summary.get("sent_files", 0)
        result.total_files = summary["total_files"]
        result.stats.delivered_bytes = summary.get("delivered_bytes", 0)
    if summary.get("unresolved"):
        result.status = CopyStatus.DELIVERY_UNKNOWN
        result.reason = "send-acknowledgement-unknown"
