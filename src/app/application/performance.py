"""Request-scoped performance measurements shared across application ports."""

import asyncio
import time
from contextvars import ContextVar
from contextlib import contextmanager

MAX_JOB_RETRIES = 8
MAX_COOLDOWN_WAIT_SECONDS = 120
EVENT_LOOP_LAG_SAMPLE_INTERVAL_S = 0.1
MAX_UPLOAD_WAIT_INTERVALS = 32
_ACTIVE_JOB_DEADLINE: ContextVar[float | None] = ContextVar(
    "active_job_deadline", default=None)


def active_job_deadline() -> float | None:
    """Return the absolute loop-time deadline for the admitted operation, if any."""
    return _ACTIVE_JOB_DEADLINE.get()


@contextmanager
def use_active_job_deadline(deadline: float):
    token = _ACTIVE_JOB_DEADLINE.set(deadline)
    try:
        yield
    finally:
        _ACTIVE_JOB_DEADLINE.reset(token)


class JobTrace:
    """Collect bounded, content-free measurements for one admitted copy job."""

    def __init__(self, job_id: str, *, queue_wait_s: float | None = None):
        self.job_id = job_id
        self.admitted_at = time.perf_counter()
        if queue_wait_s is not None:
            self.admitted_at -= max(0.0, float(queue_wait_s))
        self.active_at: float | None = None
        self.active_ended_at: float | None = None
        self.queue_wait_s = (max(0.0, float(queue_wait_s))
                             if queue_wait_s is not None else None)
        self.stages: dict[str, float] = {}
        self.routes: dict[str, int] = {}
        self.profile: dict[str, int | str] = {}
        self.observations: dict[str, int | str] = {}
        self.fallbacks: dict[str, int] = {}
        self.cleanup_errors: list[str] = []
        self.observer_errors: set[str] = set()
        self.retry_tracking = False
        self.retry_attempt_count = 0
        self.safe_retry_attempt_count = 0
        self.retry_reserved_count = 0
        self.retry_reserved_wait_s = 0.0
        self._retry_reservations: dict[int, float] = {}
        self._retry_reservation_sequence = 0
        self.cooldown_wait_total_s = 0.0
        self.retry_delay_total_s = 0.0
        self.sdk_backoff_total_s = 0.0
        self.retry_wait_total_s = 0.0
        self._retry_wait_intervals: dict[str, list[tuple[float, float]]] = {}
        self.retransferred_byte_total: int | None = 0
        self.rate_limit_wait_count_by_kind: dict[str, int] = {}
        self.rate_limit_required_wait_s_by_kind: dict[str, float] = {}
        self.rate_limit_observed_wait_s_by_kind: dict[str, float] = {}
        self.rate_limit_account_roles: set[str] = set()
        self.verified_download_byte_total = 0
        self.discarded_progress_byte_total = 0
        self.discarded_progress_tracking = False
        self.retry_stop_reason: str | None = None
        self.upload_tracking = False
        self.upload_attempt_total: int | None = 0
        self.upload_byte_tracking = False
        self.uploaded_byte_total: int | None = None
        self.upload_diagnostics: UploadDiagnostics | None = None
        self.request_tracking = False
        self.request_tracking_unknown = False
        self.inflight_requests = 0
        self.peak_inflight_requests = 0
        self.event_loop_lag_max_s: float | None = None
        self.event_loop_lag_sample_count = 0
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._next_lag_sample_at: float | None = None
        self._lag_sample_handle: asyncio.TimerHandle | None = None
        self._finished_at: float | None = None
        self._wait_notifier = None
        self._wait_finished_notifier = None
        self._notified_wait_generations: set[tuple[int, int]] = set()
        self._latest_wait_generation: dict[int, int] = {}
        self._active_wait_generations: dict[int, set[int]] = {}
        self._wait_notice_lock = asyncio.Lock()
        self._job_stage_notifier = None
        self._before_final_send = None
        self._final_send_outcomes = None
        self._job_wait_started = None
        self._job_wait_finished = None

    def start_active(self) -> None:
        if self.active_at is None:
            self.active_at = time.perf_counter()
            if self.queue_wait_s is None:
                self.queue_wait_s = max(0.0, self.active_at - self.admitted_at)
            try:
                self._event_loop = asyncio.get_running_loop()
            except RuntimeError:
                return
            self.profile["event_loop_lag_interval_ms"] = round(
                EVENT_LOOP_LAG_SAMPLE_INTERVAL_S * 1000)
            self._next_lag_sample_at = self._event_loop.time() + EVENT_LOOP_LAG_SAMPLE_INTERVAL_S
            self._lag_sample_handle = self._event_loop.call_at(
                self._next_lag_sample_at, self._sample_event_loop_lag)

    def _sample_event_loop_lag(self) -> None:
        loop = self._event_loop
        expected_at = self._next_lag_sample_at
        if loop is None or expected_at is None:
            return
        now = loop.time()
        lag = max(0.0, now - expected_at)
        self.event_loop_lag_max_s = (
            lag if self.event_loop_lag_max_s is None else
            max(self.event_loop_lag_max_s, lag))
        self.event_loop_lag_sample_count += 1
        self._next_lag_sample_at = now + EVENT_LOOP_LAG_SAMPLE_INTERVAL_S
        self._lag_sample_handle = loop.call_at(
            self._next_lag_sample_at, self._sample_event_loop_lag)

    def _stop_event_loop_lag_sampling(self) -> None:
        if self._lag_sample_handle is not None:
            self._lag_sample_handle.cancel()
        self._lag_sample_handle = None
        self._next_lag_sample_at = None
        self._event_loop = None

    def stop_active(self) -> None:
        if self.active_at is not None and self.active_ended_at is None:
            self.active_ended_at = time.perf_counter()
            self._stop_event_loop_lag_sampling()

    def stop_queue_wait(self) -> None:
        if self.active_at is None and self.queue_wait_s is None:
            self.queue_wait_s = max(0.0, time.perf_counter() - self.admitted_at)

    @contextmanager
    def measure(self, stage: str):
        started = time.perf_counter()
        try:
            yield
        finally:
            elapsed = max(0.0, time.perf_counter() - started)
            self.stages[stage] = self.stages.get(stage, 0.0) + elapsed

    def add_duration(self, stage: str, elapsed: float) -> None:
        self.stages[stage] = self.stages.get(stage, 0.0) + max(0.0, elapsed)

    def route(self, route: str) -> None:
        self.routes[route] = self.routes.get(route, 0) + 1

    def fallback(self, name: str) -> None:
        self.fallbacks[name] = self.fallbacks.get(name, 0) + 1

    def observer_error(self, error_type: str) -> None:
        self.observer_errors.add(error_type)

    def record_workspace_payload(self, byte_count: int) -> None:
        current = self.observations.get("workspace_payload_peak_bytes", 0)
        self.observations["workspace_payload_peak_bytes"] = max(
            int(current), max(0, int(byte_count)))

    def start_retry_tracking(self, profile: dict[str, int | str] | None = None) -> None:
        self.retry_tracking = True
        if profile:
            self.profile.update(profile)

    def reserve_retry(self, wait_s: float, *, bounded: bool = True) -> int | None:
        wait = max(0.0, wait_s)
        # Parallel operations share account cooldown time; reserve the longest
        # concurrent wait while still counting each retry attempt separately.
        if bounded and (
                self.retry_attempt_count + self.retry_reserved_count >= MAX_JOB_RETRIES
                or self.retry_wait_total_s + max(self.retry_reserved_wait_s, wait)
                > MAX_COOLDOWN_WAIT_SECONDS):
            return None
        self.retry_tracking = True
        self._retry_reservation_sequence += 1
        token = self._retry_reservation_sequence
        self._retry_reservations[token] = wait
        self.retry_reserved_count += 1
        self.retry_reserved_wait_s = max(self.retry_reserved_wait_s, wait)
        return token

    def release_retry_reservation(self, token: int) -> None:
        if self._retry_reservations.pop(token, None) is None:
            return
        self.retry_reserved_count = len(self._retry_reservations)
        self.retry_reserved_wait_s = max(self._retry_reservations.values(), default=0.0)

    def set_wait_notifier(self, notifier, finished_notifier=None) -> None:
        self._wait_notifier = notifier
        self._wait_finished_notifier = finished_notifier

    def set_job_hooks(self, *, stage_notifier=None, before_final_send=None,
                      final_send_outcomes=None,
                      wait_started=None, wait_finished=None) -> None:
        """Attach durable-job callbacks without coupling the trace to storage."""
        self._job_stage_notifier = stage_notifier
        self._before_final_send = before_final_send
        self._final_send_outcomes = final_send_outcomes
        self._job_wait_started = wait_started
        self._job_wait_finished = wait_finished

    async def persist_stage(self, stage: str) -> None:
        if self._job_stage_notifier is not None:
            await self._job_stage_notifier(stage)

    async def before_final_send(self, intent=None):
        if self._before_final_send is not None:
            return await self._before_final_send(intent)
        return None

    async def record_final_send_outcomes(self, attempt_id, outcomes) -> None:
        if self._final_send_outcomes is not None and attempt_id is not None:
            await self._final_send_outcomes(attempt_id, tuple(outcomes))

    async def notify_rate_wait(self, seconds: float, generation: int, account,
                              wait_kind: str = "flood") -> None:
        if self._wait_notifier is None and self._job_wait_started is None:
            return
        account_id = id(account)
        async with self._wait_notice_lock:
            key = (account_id, generation)
            latest_generation = self._latest_wait_generation.get(account_id, -1)
            if generation < latest_generation or key in self._notified_wait_generations:
                return
            self._latest_wait_generation[account_id] = generation
            self._notified_wait_generations.add(key)
            self._active_wait_generations.setdefault(account_id, set()).add(generation)
            account_role = getattr(account, "account_role", "unknown")
            if self._job_wait_started is not None:
                await self._job_wait_started(seconds, wait_kind, account_role)
            if self._wait_notifier is None:
                return
            try:
                await self._wait_notifier(seconds, wait_kind, account_role)
            except Exception as exc:
                self.observer_error(type(exc).__name__)

    async def finish_rate_wait(self, generation: int, account) -> None:
        """Resume progress edits once the latest account cooldown has elapsed."""
        if (self._wait_finished_notifier is None
                and self._job_wait_finished is None):
            return
        account_id = id(account)
        async with self._wait_notice_lock:
            latest_generation = self._latest_wait_generation.get(account_id, -1)
            if generation < latest_generation:
                return
            active = self._active_wait_generations.get(account_id, set())
            completed = {value for value in active if value <= generation}
            if not completed:
                return
            remaining = active - completed
            if remaining:
                self._active_wait_generations[account_id] = remaining
            else:
                self._active_wait_generations.pop(account_id, None)
            if self._job_wait_finished is not None:
                await self._job_wait_finished()
            if self._wait_finished_notifier is None:
                return
            try:
                await self._wait_finished_notifier()
            except Exception as exc:
                self.observer_error(type(exc).__name__)

    def record_retry(self, retransferred_bytes: int = 0, *,
                     reserved_token: int | None = None) -> None:
        self.retry_tracking = True
        if reserved_token is not None:
            self.release_retry_reservation(reserved_token)
        self.retry_attempt_count += 1
        self.safe_retry_attempt_count += 1
        self.record_retransferred(retransferred_bytes)

    def record_rate_limit_required_wait(self, wait_kind: str, account_role: str,
                                        seconds: float) -> None:
        if wait_kind not in {"flood", "premium"}:
            wait_kind = "unknown"
        if account_role not in {"owner", "bot", "unknown"}:
            account_role = "unknown"
        self.rate_limit_wait_count_by_kind[wait_kind] = (
            self.rate_limit_wait_count_by_kind.get(wait_kind, 0) + 1)
        self.rate_limit_required_wait_s_by_kind[wait_kind] = (
            self.rate_limit_required_wait_s_by_kind.get(wait_kind, 0.0)
            + max(0.0, float(seconds)))
        self.rate_limit_account_roles.add(account_role)

    def record_verified_download_bytes(self, byte_count: int) -> None:
        self.verified_download_byte_total += max(0, int(byte_count))

    def record_discarded_progress_bytes(self, byte_count: int) -> None:
        self.discarded_progress_tracking = True
        self.discarded_progress_byte_total += max(0, int(byte_count))

    def record_retry_stop(self, reason: str) -> None:
        if reason in {"active-deadline", "retry-budget-exhausted", "not-retryable"}:
            self.retry_stop_reason = reason
        else:
            self.retry_stop_reason = "unknown"

    def record_retransferred(self, byte_count: int) -> None:
        self.retry_tracking = True
        if self.retransferred_byte_total is not None and isinstance(byte_count, int) and byte_count > 0:
            self.retransferred_byte_total += byte_count

    def mark_retransferred_unknown(self) -> None:
        self.retry_tracking = True
        self.retransferred_byte_total = None
        if self.upload_diagnostics is not None:
            self.upload_diagnostics.retransferred_bytes_complete = False

    def start_upload_diagnostics(self) -> None:
        if self.upload_diagnostics is None:
            self.upload_diagnostics = UploadDiagnostics(self.admitted_at)

    def record_upload_phase(self, phase: str, elapsed_s: float) -> None:
        self.start_upload_diagnostics()
        self.upload_diagnostics.record_phase(phase, elapsed_s)

    def record_upload_attempt(self, byte_count: int = 0) -> None:
        self.start_upload_tracking()
        if self.upload_attempt_total is not None:
            self.upload_attempt_total += 1
        self.start_upload_diagnostics()
        self.upload_diagnostics.record_attempt(byte_count)

    def start_upload_tracking(self) -> None:
        self.upload_tracking = True

    def start_upload_byte_tracking(self) -> None:
        if not self.upload_byte_tracking:
            self.upload_byte_tracking = True
            self.uploaded_byte_total = 0

    def record_uploaded_bytes(self, byte_count: int) -> None:
        self.start_upload_byte_tracking()
        if self.uploaded_byte_total is not None:
            self.uploaded_byte_total += max(0, int(byte_count))
        self.start_upload_diagnostics()
        self.upload_diagnostics.record_acknowledged(byte_count)

    def record_upload_retry(self, repeated_bytes: int) -> None:
        self.start_upload_diagnostics()
        self.upload_diagnostics.record_retry(repeated_bytes)

    def record_upload_wait(self, kind: str, started_at: float, ended_at: float) -> None:
        self.start_upload_diagnostics()
        self.upload_diagnostics.record_wait(kind, started_at, ended_at)

    def finish_upload_diagnostics(self, outcome: str) -> None:
        if self.upload_diagnostics is not None:
            self.upload_diagnostics.finish(outcome)

    def mark_uploaded_bytes_unknown(self) -> None:
        self.upload_byte_tracking = True
        self.uploaded_byte_total = None

    def mark_upload_attempts_unknown(self) -> None:
        self.upload_tracking = True
        self.upload_attempt_total = None

    def request_started(self) -> None:
        self.request_tracking = True
        self.inflight_requests += 1
        self.peak_inflight_requests = max(
            self.peak_inflight_requests, self.inflight_requests)

    def request_finished(self) -> None:
        self.inflight_requests = max(0, self.inflight_requests - 1)

    def mark_request_tracking_unknown(self) -> None:
        self.request_tracking_unknown = True

    def record_cooldown_wait(self, started_at: float, ended_at: float,
                             wait_kind: str = "flood") -> None:
        if wait_kind not in {"flood", "premium"}:
            wait_kind = "unknown"
        self._record_wait_interval(f"cooldown:{wait_kind}", started_at, ended_at)

    def record_retry_delay(self, started_at: float, ended_at: float) -> None:
        self._record_wait_interval("retry", started_at, ended_at)

    def _record_wait_interval(self, kind: str, started_at: float, ended_at: float) -> None:
        self.retry_tracking = True
        interval = (started_at, max(started_at, ended_at))
        intervals = sorted([*self._retry_wait_intervals.get(kind, []), interval])
        merged: list[tuple[float, float]] = []
        for start, end in intervals:
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))
        self._retry_wait_intervals[kind] = merged
        total = sum(end - start for start, end in merged)
        if kind.startswith("cooldown:"):
            wait_kind = kind.partition(":")[2]
            self.rate_limit_observed_wait_s_by_kind[wait_kind] = total
            cooldown_intervals = sorted(
                interval
                for key, intervals in self._retry_wait_intervals.items()
                if key.startswith("cooldown:")
                for interval in intervals)
            cooldown_union: list[tuple[float, float]] = []
            for start, end in cooldown_intervals:
                if cooldown_union and start <= cooldown_union[-1][1]:
                    cooldown_union[-1] = (
                        cooldown_union[-1][0], max(cooldown_union[-1][1], end))
                else:
                    cooldown_union.append((start, end))
            self.cooldown_wait_total_s = sum(
                end - start for start, end in cooldown_union)
        elif kind == "retry":
            self.retry_delay_total_s = total
        self.retry_wait_total_s = (self.cooldown_wait_total_s + self.retry_delay_total_s
                                   + self.sdk_backoff_total_s)

    def record_sdk_backoff(self, elapsed_s: float) -> None:
        self.retry_tracking = True
        elapsed = max(0.0, elapsed_s)
        self.sdk_backoff_total_s += elapsed
        self.retry_wait_total_s = (self.cooldown_wait_total_s + self.retry_delay_total_s
                                   + self.sdk_backoff_total_s)

    def finish(self, *, status: str, reason: str | None,
               downloaded_bytes: int | None, delivered_bytes: int | None,
               upload_attempts: int | None = None,
               retry_attempts: int | None = None, cooldown_wait_s: float | None = None,
               retry_delay_s: float | None = None,
               sdk_backoff_wait_s: float | None = None,
               retransferred_bytes: int | None = None) -> dict:
        self._stop_event_loop_lag_sampling()
        if self._finished_at is None:
            self._finished_at = time.perf_counter()
        end = self._finished_at
        active_end = self.active_ended_at or end
        queue_wait = self.queue_wait_s
        if queue_wait is None:
            queue_end = self.active_at if self.active_at is not None else end
            queue_wait = max(0.0, queue_end - self.admitted_at)
        return {
            "schema_version": 1,
            "event": "copy.terminal",
            "job_id": self.job_id,
            "status": status,
            "reason": reason,
            "queue_wait_s": _rounded(queue_wait),
            "active_wall_s": (_rounded(max(0.0, active_end - self.active_at))
                               if self.active_at is not None else None),
            "total_wall_s": _rounded(max(0.0, end - self.admitted_at)),
            "stages_s": {key: _rounded(value) for key, value in sorted(self.stages.items())},
            "downloaded_bytes": downloaded_bytes,
            "delivered_bytes": delivered_bytes,
            # Per-job outcomes and measurements are separate from stable settings.
            "album_strategy": self.observations.get("album_strategy"),
            "pipeline_fallback_reason": self.observations.get("pipeline_fallback_reason"),
            "pipeline_reserved_bytes": self.observations.get("pipeline_reserved_bytes"),
            "pipeline_peak_buffered_bytes": self.observations.get(
                "pipeline_peak_buffered_bytes"),
            # Lag is unknown when an active operation ended before its first sample.
            "event_loop_lag_max_s": _rounded(self.event_loop_lag_max_s),
            "event_loop_lag_sample_count": self.event_loop_lag_sample_count,
            "event_loop_lag_interval_ms": round(EVENT_LOOP_LAG_SAMPLE_INTERVAL_S * 1000),
            "workspace_payload_peak_bytes": self.observations.get(
                "workspace_payload_peak_bytes"),
            "workspace_logical_reserved_bytes": self.observations.get(
                "workspace_logical_reserved_bytes"),
            "filesystem_free_bytes_at_reservation": self.observations.get(
                "filesystem_free_bytes_at_reservation"),
            "filesystem_used_bytes_at_reservation": self.observations.get(
                "filesystem_used_bytes_at_reservation"),
            # Counts acknowledged custom upload-part payload; opaque Telethon paths stay unknown.
            "uploaded_bytes": (self.uploaded_byte_total
                               if self.upload_byte_tracking else None),
            "upload_diagnostics": (self.upload_diagnostics.snapshot()
                                   if self.upload_diagnostics is not None else None),
            "peak_inflight_requests": (
                None if self.request_tracking_unknown or not self.request_tracking else
                self.peak_inflight_requests),
            # Count only custom Save*FilePart requests; Telethon's serial upload path
            # hides its request attempts, so any operation using it is unknown.
            "upload_attempts": (upload_attempts if upload_attempts is not None else
                                self.upload_attempt_total if self.upload_tracking else None),
            "routes": dict(sorted(self.routes.items())),
            "effective_settings": dict(sorted(self.profile.items())),
            "fallbacks": dict(sorted(self.fallbacks.items())),
            "cleanup_errors": sorted(self.cleanup_errors),
            "observer_errors": sorted(self.observer_errors),
            # The coordinator supplies bounded retry/wait metrics; unused traces stay unknown.
            "retry_attempts": (retry_attempts if retry_attempts is not None else
                                self.retry_attempt_count if self.retry_tracking else None),
            "safe_retry_attempts": (self.safe_retry_attempt_count
                                    if self.retry_tracking else None),
            "retry_stop_reason": self.retry_stop_reason,
            "rate_limit_wait_count_by_kind": dict(sorted(
                self.rate_limit_wait_count_by_kind.items())),
            "rate_limit_required_wait_s_by_kind": {
                key: _rounded(value) for key, value in sorted(
                    self.rate_limit_required_wait_s_by_kind.items())},
            "rate_limit_observed_wait_s_by_kind": {
                key: _rounded(value) for key, value in sorted(
                    self.rate_limit_observed_wait_s_by_kind.items())},
            "rate_limit_account_roles": sorted(self.rate_limit_account_roles),
            "verified_download_bytes": (self.verified_download_byte_total
                                         if self.retry_tracking else None),
            "discarded_progress_bytes": (self.discarded_progress_byte_total
                                          if self.discarded_progress_tracking else None),
            "cooldown_wait_s": _rounded(
                cooldown_wait_s if cooldown_wait_s is not None else
                self.cooldown_wait_total_s if self.retry_tracking else None),
            "retry_delay_s": _rounded(
                retry_delay_s if retry_delay_s is not None else
                self.retry_delay_total_s if self.retry_tracking else None),
            "sdk_backoff_wait_s": _rounded(
                sdk_backoff_wait_s if sdk_backoff_wait_s is not None else
                self.sdk_backoff_total_s if self.retry_tracking else None),
            "retransferred_bytes": (retransferred_bytes if retransferred_bytes is not None else
                                    self.retransferred_byte_total if self.retry_tracking else None),
        }


def _rounded(value: float | None) -> float | None:
    return round(value, 6) if value is not None else None


class UploadDiagnostics:
    PHASES = {"setup", "part_prepare", "request_gate_wait", "telegram_rpc_await"}
    WAITS = {"flood", "premium", "retry_backoff"}

    def __init__(self, origin: float):
        self.origin = origin
        self.phases = {
            name: {"count": 0, "total_s": 0.0, "max_s": 0.0}
            for name in sorted(self.PHASES)
        }
        self.attempts = 0
        self.attempted_bytes = 0
        self.acknowledged_parts = 0
        self.acknowledged_bytes = 0
        self.setup_bytes_read = 0
        self.retries = 0
        self.retransferred_bytes = 0
        self.retransferred_bytes_complete = True
        self.waits = {
            name: {"count": 0, "intervals": [], "dropped_intervals": 0}
            for name in sorted(self.WAITS)
        }
        self.outcome = "running"

    def record_phase(self, phase: str, elapsed_s: float) -> None:
        if phase not in self.PHASES:
            return
        metric = self.phases[phase]
        elapsed = max(0.0, float(elapsed_s))
        metric["count"] += 1
        metric["total_s"] += elapsed
        metric["max_s"] = max(metric["max_s"], elapsed)

    def record_attempt(self, byte_count: int) -> None:
        self.attempts += 1
        self.attempted_bytes += max(0, int(byte_count))

    def record_setup_bytes(self, byte_count: int) -> None:
        self.setup_bytes_read += max(0, int(byte_count))

    def record_acknowledged(self, byte_count: int) -> None:
        self.acknowledged_parts += 1
        self.acknowledged_bytes += max(0, int(byte_count))

    def record_retry(self, repeated_bytes: int) -> None:
        self.retries += 1
        self.retransferred_bytes += max(0, int(repeated_bytes))

    def record_wait(self, kind: str, started_at: float, ended_at: float) -> None:
        if kind not in self.WAITS:
            return
        metric = self.waits[kind]
        metric["count"] += 1
        if len(metric["intervals"]) >= MAX_UPLOAD_WAIT_INTERVALS:
            metric["dropped_intervals"] += 1
            return
        start = max(0.0, started_at - self.origin)
        end = max(start, ended_at - self.origin)
        metric["intervals"].append((start, end))

    def finish(self, outcome: str) -> None:
        self.outcome = outcome if outcome in {"success", "error", "cancelled"} else "error"

    def snapshot(self) -> dict:
        phases = {
            name: {"count": values["count"],
                   "total_s": _rounded(values["total_s"]),
                   "max_s": _rounded(values["max_s"])}
            for name, values in self.phases.items()
        }
        waits = {}
        for name, metric in self.waits.items():
            intervals = _merge_intervals(metric["intervals"])
            waits[name] = {
                "count": metric["count"],
                "union_s": _rounded(sum(end - start for start, end in intervals)),
                "intervals_s_from_job_start": [
                    [_rounded(start), _rounded(end)] for start, end in intervals],
                "dropped_intervals": metric["dropped_intervals"],
            }
        return {
            "outcome": self.outcome,
            "phases": phases,
            "attempts": self.attempts,
            "attempted_bytes": self.attempted_bytes,
            "acknowledged_parts": self.acknowledged_parts,
            "acknowledged_bytes": self.acknowledged_bytes,
            "setup_bytes_read": self.setup_bytes_read,
            "retries": self.retries,
            "retransferred_bytes": self.retransferred_bytes,
            "retransferred_bytes_complete": self.retransferred_bytes_complete,
            "waits": waits,
            "wait_interval_limit_per_kind": MAX_UPLOAD_WAIT_INTERVALS,
        }


def _merge_intervals(intervals: list[tuple[float, float]]) -> list[tuple[float, float]]:
    merged: list[list[float]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(start, end) for start, end in merged]
