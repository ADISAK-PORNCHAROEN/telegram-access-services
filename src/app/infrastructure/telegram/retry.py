"""Per-account safe retries and shared asynchronous cooldowns for Telegram calls."""

import asyncio
import math
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager

from telethon.errors import (FloodPremiumWaitError, FloodWaitError,
                             RpcCallFailError, ServerError)

from app.application.performance import (
    MAX_COOLDOWN_WAIT_SECONDS, MAX_JOB_RETRIES, active_job_deadline,
)
from app.domain.errors import RateLimited

RETRIES_PER_OPERATION = 2
SERVER_RETRY_DELAY_SECONDS = 1
TELETHON_SERVER_ERROR_BACKOFF_SECONDS = 2
TELETHON_REQUEST_RETRIES = 0
TELETHON_FLOOD_SLEEP_THRESHOLD_SECONDS = 0
FLOOD_WAIT_ERRORS = (FloodWaitError, FloodPremiumWaitError)


class TelegramRetryCoordinator:
    """Share one flood-wait deadline per Telegram account without retrying unsafe sends."""

    def __init__(self, *, clock=None, sleep=None, account_role: str = "unknown"):
        if account_role not in {"owner", "bot", "unknown"}:
            raise ValueError("unsupported Telegram account role")
        self.account_role = account_role
        self._cooldown_until = 0.0
        self._cooldown_kind = "flood"
        self._cooldown_generation = 0
        self._lock = asyncio.Lock()
        self._clock = clock or time.monotonic
        self._sleep = sleep or asyncio.sleep
        self._published_flood_errors: set[int] = set()

    @staticmethod
    def profile() -> dict[str, int | str]:
        return {
            "retry_attempts_per_operation": RETRIES_PER_OPERATION,
            "retry_attempts_per_job": MAX_JOB_RETRIES,
            "retry_wait_budget_seconds": MAX_COOLDOWN_WAIT_SECONDS,
            "explicit_wait_retry_policy": "active-deadline",
            "retry_delay_seconds": SERVER_RETRY_DELAY_SECONDS,
            "telethon_server_error_backoff_seconds": TELETHON_SERVER_ERROR_BACKOFF_SECONDS,
            "telethon_request_retries": TELETHON_REQUEST_RETRIES,
            "telethon_flood_sleep_threshold_seconds": TELETHON_FLOOD_SLEEP_THRESHOLD_SECONDS,
            "telethon_raise_last_call_error": 1,
        }

    async def call(self, operation: Callable[[], Awaitable], trace=None, *,
                   safe_retry: bool = False, retry_flood_wait: bool = True,
                   repeated_bytes: int | Callable[[], int] = 0,
                   scope: str | None = None):
        """Retry explicit Telegram wait errors and safe idempotent server failures only."""
        if trace:
            trace.start_retry_tracking(self.profile())
        operation_retries = 0
        server_error_retries = 0
        local_wait_s = 0.0
        pending_retry: tuple[int | Callable[[], int], int | None] | None = None
        while True:
            reserved_token = pending_retry[1] if pending_retry is not None else None
            try:
                local_wait_s += await self._wait_for_cooldown(
                    trace, local_wait_s, scope=scope)
            except BaseException:
                if reserved_token is not None and trace:
                    trace.release_retry_reservation(reserved_token)
                raise
            deadline = active_job_deadline()
            if deadline is not None and deadline <= self._clock():
                if reserved_token is not None and trace:
                    trace.release_retry_reservation(reserved_token)
                if trace:
                    trace.record_retry_stop("active-deadline")
                raise RateLimited(0, stop_reason="active-deadline")
            if pending_retry is not None:
                self._record_retry(trace, pending_retry[0],
                                   reserved_token=reserved_token, scope=scope)
                pending_retry = None
            try:
                return await operation()
            except FLOOD_WAIT_ERRORS as exc:
                requested_delay = max(1, int(exc.seconds))
                wait_kind = self._wait_kind(exc)
                if not self._consume_published_flood_wait(exc):
                    await self._extend_cooldown(requested_delay, wait_kind)
                if trace:
                    trace.record_rate_limit_required_wait(
                        wait_kind, self.account_role, requested_delay)
                remaining, _kind, _generation = await self._cooldown_state()
                retry_delay = remaining
                deadline = active_job_deadline()
                reservation = (
                    self._can_retry(trace, operation_retries, local_wait_s, retry_delay,
                                    deadline=deadline, explicit_wait=True)
                    if retry_flood_wait else False)
                if not reservation:
                    if not retry_flood_wait:
                        reason = "not-retryable"
                    elif deadline is not None and deadline - self._clock() <= retry_delay:
                        reason = "active-deadline"
                    else:
                        reason = "retry-budget-exhausted"
                    if trace:
                        trace.record_retry_stop(reason)
                    raise RateLimited(math.ceil(retry_delay), stop_reason=reason) from exc
                operation_retries += 1
                pending_retry = (repeated_bytes, reservation if trace else None)
            except (ServerError, RpcCallFailError):
                if trace:
                    # The installed Telethon _call contract keeps this two-second backoff.
                    trace.record_sdk_backoff(TELETHON_SERVER_ERROR_BACKOFF_SECONDS)
                if not safe_retry:
                    raise
                delay = SERVER_RETRY_DELAY_SECONDS
                await self._extend_cooldown(delay, "retry")
                deadline = active_job_deadline()
                reservation = self._can_retry(
                    trace,
                    server_error_retries if deadline is not None else operation_retries,
                    local_wait_s, delay, deadline=deadline)
                if not reservation:
                    raise
                operation_retries += 1
                server_error_retries += 1
                pending_retry = (repeated_bytes, reservation if trace else None)

    async def publish_flood_wait(self, error: BaseException) -> None:
        """Publish a range read's flood wait before releasing its request slot."""
        if not isinstance(error, FLOOD_WAIT_ERRORS):
            return
        await self._extend_cooldown(max(1, int(error.seconds)),
                                    self._wait_kind(error))
        self._published_flood_errors.add(id(error))

    @staticmethod
    def _wait_kind(error: BaseException) -> str:
        return "premium" if isinstance(error, FloodPremiumWaitError) else "flood"

    def forget_published_flood_wait(self, error: BaseException) -> None:
        """Drop a publication marker if iterator cleanup prevents retry handling."""
        self._published_flood_errors.discard(id(error))

    def _consume_published_flood_wait(self, error: BaseException) -> bool:
        try:
            self._published_flood_errors.remove(id(error))
        except KeyError:
            return False
        return True

    async def _extend_cooldown(self, delay: float, kind: str) -> None:
        async with self._lock:
            now = self._clock()
            was_active = self._cooldown_until > now
            due = now + delay
            if kind in {"flood", "premium"} and (
                    not was_active or due > self._cooldown_until):
                self._cooldown_generation += 1
            if due >= self._cooldown_until:
                self._cooldown_until = due
                self._cooldown_kind = kind

    async def _cooldown_state(self) -> tuple[float, str, int]:
        async with self._lock:
            return (max(0.0, self._cooldown_until - self._clock()),
                    self._cooldown_kind, self._cooldown_generation)

    @asynccontextmanager
    async def request_slot(self, gate, trace=None, *, scope: str | None = None):
        """Wait outside the gate, then recheck cooldown and job deadline."""
        while True:
            await self._wait_for_cooldown(trace, 0.0, scope=scope)
            slot = getattr(gate, "slot", None)
            slot_context = slot(trace) if slot is not None else gate
            gate_started = time.perf_counter()
            async with slot_context:
                if trace and scope == "upload_part":
                    trace.record_upload_phase(
                        "request_gate_wait", time.perf_counter() - gate_started)
                remaining, _kind, _generation = await self._cooldown_state()
                deadline = active_job_deadline()
                if deadline is not None and deadline <= self._clock():
                    if trace:
                        trace.record_retry_stop("active-deadline")
                    raise RateLimited(0, stop_reason="active-deadline")
                if remaining <= 0:
                    yield
                    return
            # Another request extended cooldown while this one waited for the gate.
            # The context exits first, so no request permit is held while sleeping.

    async def _wait_for_cooldown(self, trace, local_wait_s: float, *,
                                 scope: str | None = None) -> float:
        total_waited = 0.0
        while True:
            remaining, kind, generation = await self._cooldown_state()
            if remaining <= 0:
                if trace:
                    await trace.finish_rate_wait(generation, self)
                return total_waited
            deadline = active_job_deadline()
            if deadline is not None and deadline - self._clock() <= remaining:
                if trace:
                    trace.record_retry_stop("active-deadline")
                raise RateLimited(math.ceil(remaining), stop_reason="active-deadline")
            already_waited = trace.retry_wait_total_s if trace else local_wait_s
            if deadline is None and already_waited + remaining > MAX_COOLDOWN_WAIT_SECONDS:
                if trace:
                    trace.record_retry_stop("retry-budget-exhausted")
                raise RateLimited(math.ceil(remaining), stop_reason="retry-budget-exhausted")
            is_rate_wait = kind in {"flood", "premium"}
            if trace and is_rate_wait:
                await trace.notify_rate_wait(
                    remaining, generation, self, wait_kind=kind)
            started = self._clock()
            measured_started = time.perf_counter()
            try:
                await self._sleep(remaining)
            finally:
                ended = self._clock()
                measured_ended = time.perf_counter()
                elapsed = max(0.0, ended - started)
                total_waited += elapsed
                if trace:
                    if is_rate_wait:
                        trace.record_cooldown_wait(started, ended, kind)
                    else:
                        trace.record_retry_delay(started, ended)
                    if scope == "upload_part":
                        trace.record_upload_wait(
                            kind if is_rate_wait else "retry_backoff",
                            measured_started, measured_ended)

    def _can_retry(self, trace, operation_retries: int, local_wait_s: float,
                   delay: float, *, deadline: float | None = None,
                   explicit_wait: bool = False) -> bool | int:
        if deadline is not None:
            if deadline - self._clock() <= delay:
                return False
            if not explicit_wait and operation_retries >= RETRIES_PER_OPERATION:
                return False
            return trace.reserve_retry(delay, bounded=False) if trace else True
        if operation_retries >= RETRIES_PER_OPERATION:
            return False
        if trace:
            return trace.reserve_retry(delay)
        return operation_retries < MAX_JOB_RETRIES and local_wait_s + delay <= MAX_COOLDOWN_WAIT_SECONDS

    @staticmethod
    def _record_retry(trace, repeated_bytes: int | Callable[[], int], *,
                      reserved_token: int | None = None,
                      scope: str | None = None) -> None:
        if trace:
            try:
                value = repeated_bytes() if callable(repeated_bytes) else repeated_bytes
            except BaseException:
                if reserved_token is not None:
                    trace.release_retry_reservation(reserved_token)
                raise
            trace.record_retry(value, reserved_token=reserved_token)
            if scope == "upload_part":
                trace.record_upload_retry(value)
