"""Bounded, cancellation-safe copy admission with round-robin user fairness."""

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

from app.application.performance import use_active_job_deadline
from app.domain.errors import QueueExpired, QueueFull

T = TypeVar("T")
COPY_TIMEOUT_S = 30 * 60
DEFAULT_PENDING_COPIES = 128
DEFAULT_PENDING_PER_USER = 4
DEFAULT_QUEUE_WAIT_S = 5 * 60


@dataclass
class _Waiter:
    user_id: int
    ready: asyncio.Future | None = None
    queued: bool = True
    active: bool = False


class CopyScheduler:
    """Own active slots and a bounded per-user round-robin pending queue."""

    def __init__(self, slots: int, timeout: float = COPY_TIMEOUT_S, *,
                 pending_limit: int = DEFAULT_PENDING_COPIES,
                 per_user_pending_limit: int = DEFAULT_PENDING_PER_USER,
                 queue_timeout: float = DEFAULT_QUEUE_WAIT_S):
        if slots < 1 or timeout <= 0 or queue_timeout <= 0:
            raise ValueError("slots and timeouts must be positive")
        if pending_limit < 1 or per_user_pending_limit < 1:
            raise ValueError("pending limits must be positive")
        if per_user_pending_limit > pending_limit:
            raise ValueError("per-user pending limit cannot exceed global pending limit")
        self.capacity = slots
        self.timeout = timeout
        self.pending_limit = pending_limit
        self.per_user_pending_limit = per_user_pending_limit
        self.queue_timeout = queue_timeout
        self.pending = 0
        self.active_count = 0
        self._pending_users: dict[int, deque[_Waiter]] = {}
        self._rotation: deque[int] = deque()
        self._pending_by_user: dict[int, int] = {}
        self._lock = asyncio.Lock()

    async def run(self, operation: Callable[[], Awaitable[T]],
                  on_wait: Callable[[int], Awaitable[None]], *, user_id: int = 0) -> T:
        loop = asyncio.get_running_loop()
        waiter = _Waiter(user_id)
        queued = False
        enqueued_at = loop.time()
        async with self._lock:
            if self.active_count < self.capacity and self.pending == 0:
                self.active_count += 1
                waiter.queued = False
                waiter.active = True
            else:
                user_pending = self._pending_by_user.get(user_id, 0)
                if self.pending >= self.pending_limit:
                    raise QueueFull("global")
                if user_pending >= self.per_user_pending_limit:
                    raise QueueFull("per-user")
                waiter.ready = loop.create_future()
                user_queue = self._pending_users.get(user_id)
                if user_queue is None:
                    user_queue = self._pending_users[user_id] = deque()
                    self._rotation.append(user_id)
                user_queue.append(waiter)
                self.pending += 1
                self._pending_by_user[user_id] = user_pending + 1
                queued = True
                queue_size = self.pending

        try:
            if queued:
                await on_wait(queue_size)
                remaining = self.queue_timeout - (loop.time() - enqueued_at)
                if not waiter.active:
                    if remaining <= 0:
                        raise QueueExpired()
                    assert waiter.ready is not None
                    try:
                        await asyncio.wait_for(waiter.ready, timeout=remaining)
                    except TimeoutError:
                        raise QueueExpired() from None
            deadline = loop.time() + self.timeout
            with use_active_job_deadline(deadline):
                return await asyncio.wait_for(
                    operation(), timeout=max(0.0, deadline - loop.time()))
        finally:
            async with self._lock:
                if waiter.active:
                    self.active_count -= 1
                    waiter.active = False
                elif waiter.queued:
                    self._remove_waiter_locked(waiter)
                self._promote_locked()

    def _remove_waiter_locked(self, waiter: _Waiter) -> None:
        queue = self._pending_users.get(waiter.user_id)
        if queue is None:
            return
        try:
            queue.remove(waiter)
        except ValueError:
            return
        self.pending -= 1
        count = self._pending_by_user[waiter.user_id] - 1
        if count:
            self._pending_by_user[waiter.user_id] = count
        else:
            del self._pending_by_user[waiter.user_id]
        waiter.queued = False
        if not queue:
            del self._pending_users[waiter.user_id]
            try:
                self._rotation.remove(waiter.user_id)
            except ValueError:
                pass

    def _promote_locked(self) -> None:
        while self.active_count < self.capacity and self._rotation:
            user_id = self._rotation.popleft()
            queue = self._pending_users[user_id]
            waiter = queue.popleft()
            self.pending -= 1
            count = self._pending_by_user[user_id] - 1
            if count:
                self._pending_by_user[user_id] = count
            else:
                del self._pending_by_user[user_id]
            waiter.queued = False
            if queue:
                self._rotation.append(user_id)
            else:
                del self._pending_users[user_id]
            if waiter.ready is None or waiter.ready.cancelled():
                continue
            waiter.active = True
            self.active_count += 1
            assert waiter.ready is not None
            waiter.ready.set_result(None)
