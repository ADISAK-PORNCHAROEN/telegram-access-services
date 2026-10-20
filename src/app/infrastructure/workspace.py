"""Capacity-reserved workspaces with cancellation-safe ownership and cleanup."""

import asyncio
import json
import shutil
import tempfile
from collections import deque
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from app.domain.errors import DownloadLimitExceeded, WorkspaceBudgetExceeded
from app.infrastructure.process_lock import FileLock

RESERVATION_MARKER = ".workspace-reservation.json"


class SessionOwnership(Protocol):
    @property
    def is_held(self) -> bool: ...


@dataclass
class _Waiter:
    size: int


class _CapacityLease:
    def __init__(self, manager: "TempWorkspace", size: int, disk_usage):
        self.manager = manager
        self.reserved_bytes = size
        self.filesystem_free_bytes = disk_usage.free
        self.filesystem_used_bytes = disk_usage.used
        self.filesystem_total_bytes = disk_usage.total
        self.attached = False
        self.cleaned = False
        self.released = False

    def attach(self) -> None:
        self.attached = True

    def cleanup_succeeded(self) -> None:
        self.cleaned = True


class BoundedDownloadWriter:
    """File-like Telethon target that checks each write before touching disk."""

    def __init__(self, path: Path, max_bytes: int):
        self.path = path
        self.name = str(path)
        self.max_bytes = max_bytes
        self._file = path.open("w+b")
        self._closed = False
        self.high_water_bytes = 0

    def write(self, data: bytes) -> int:
        position = self._file.tell()
        end = position + len(data)
        if end > self.max_bytes:
            raise DownloadLimitExceeded(end, self.max_bytes)
        written = self._file.write(data)
        self.high_water_bytes = max(self.high_water_bytes, position + written)
        return written

    def seek(self, offset: int, whence: int = 0) -> int:
        position = self._file.seek(offset, whence)
        if position > self.max_bytes:
            raise DownloadLimitExceeded(position, self.max_bytes)
        return position

    def tell(self) -> int:
        return self._file.tell()

    def truncate(self, size: int | None = None) -> int:
        target = self._file.tell() if size is None else size
        if target > self.max_bytes:
            raise DownloadLimitExceeded(target, self.max_bytes)
        return self._file.truncate(size)

    def flush(self) -> None:
        self._file.flush()

    def close(self) -> None:
        if not self._closed:
            self._file.close()
            self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


class TempWorkspace:
    def __init__(self, root: Path, *, logical_budget_bytes: int = 20 * 1024**3,
                 free_disk_floor_bytes: int = 5 * 1024**3):
        self.root = root
        self.logical_budget_bytes = logical_budget_bytes
        self.free_disk_floor_bytes = free_disk_floor_bytes
        self._active: set[Path] = set()
        self._retained: set[int] = set()
        self._logical_reserved_bytes = 0
        self._disk_reserved_bytes = 0
        self._waiters: deque[_Waiter] = deque()
        self._changed = asyncio.Event()
        self._loop = None
        self._startup_reconciled = False

    @property
    def logical_reserved_bytes(self) -> int:
        """Current logical payload reservation; excludes filesystem block usage."""
        return self._logical_reserved_bytes

    def _ensure_loop(self):
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            if self._waiters:
                raise RuntimeError("workspace capacity manager cannot span event loops")
            self._loop = loop
            self._changed = asyncio.Event()

    def _wake_waiters(self) -> None:
        old = self._changed
        self._changed = asyncio.Event()
        old.set()

    def _disk_usage(self):
        self.root.mkdir(parents=True, exist_ok=True)
        return shutil.disk_usage(self.root)

    def _can_reserve(self, size: int):
        if self._logical_reserved_bytes + size > self.logical_budget_bytes:
            return None
        usage = self._disk_usage()
        if usage.free - self._disk_reserved_bytes - size < self.free_disk_floor_bytes:
            return None
        return usage

    @asynccontextmanager
    async def reserve(self, size: int, *, on_wait=None):
        if size < 0:
            raise ValueError("workspace reservation cannot be negative")
        if size > self.logical_budget_bytes:
            raise WorkspaceBudgetExceeded(size, self.logical_budget_bytes)
        self._ensure_loop()
        waiter = _Waiter(size)
        self._waiters.append(waiter)
        notified_wait = False
        lease = None
        try:
            while lease is None:
                changed = self._changed
                if self._waiters[0] is waiter:
                    usage = self._can_reserve(size)
                    if usage is not None:
                        self._waiters.popleft()
                        self._logical_reserved_bytes += size
                        self._disk_reserved_bytes += size
                        lease = _CapacityLease(self, size, usage)
                        self._wake_waiters()
                        break
                if not notified_wait:
                    notified_wait = True
                    if on_wait is not None:
                        await on_wait()
                try:
                    await asyncio.wait_for(changed.wait(), timeout=0.5)
                except TimeoutError:
                    # External disk usage can change without releasing an in-process lease.
                    pass
        except BaseException:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            self._wake_waiters()
            raise

        try:
            yield lease
        finally:
            if not lease.attached or lease.cleaned:
                self._release(lease)
            else:
                # Keep the logical and physical reservation if cleanup failed.
                self._retained.add(id(lease))

    def _release(self, lease: _CapacityLease) -> None:
        if lease.released:
            return
        lease.released = True
        self._logical_reserved_bytes = max(
            0, self._logical_reserved_bytes - lease.reserved_bytes)
        self._disk_reserved_bytes = max(
            0, self._disk_reserved_bytes - lease.reserved_bytes)
        self._retained.discard(id(lease))
        self._wake_waiters()

    @contextmanager
    def bounded_writer(self, path: Path, max_bytes: int):
        path.parent.mkdir(parents=True, exist_ok=True)
        writer = BoundedDownloadWriter(path, max_bytes)
        try:
            yield writer
        finally:
            writer.close()

    @contextmanager
    def create(self, message_id: int, *, reservation: _CapacityLease | None = None):
        self.root.mkdir(parents=True, exist_ok=True)
        directory = Path(tempfile.mkdtemp(prefix=f"copy_{message_id}_", dir=self.root))
        owner = FileLock(directory / ".owner.lock")
        if not owner.acquire():
            shutil.rmtree(directory, ignore_errors=True)
            raise RuntimeError("new temporary workspace is already owned")
        try:
            if reservation is not None:
                if reservation.manager is not self or reservation.attached:
                    raise RuntimeError("workspace reservation is not available")
                (directory / RESERVATION_MARKER).write_text(
                    json.dumps({"reserved_bytes": reservation.reserved_bytes}),
                    encoding="utf-8")
                reservation.attach()
            self._active.add(directory)
            yield directory
        finally:
            self._active.discard(directory)
            owner.release()
            shutil.rmtree(directory)
            if reservation is not None:
                reservation.cleanup_succeeded()

    def _payload_size(self, directory: Path) -> int:
        total = 0
        try:
            for path in directory.rglob("*"):
                if path.is_symlink() or not path.is_file():
                    continue
                if path.name in {".owner.lock", RESERVATION_MARKER}:
                    continue
                total += path.stat().st_size
        except OSError:
            return self.logical_budget_bytes
        return total

    def _survivor_reservation(self, directory: Path) -> int:
        marker = directory / ".owner.lock"
        if marker.is_symlink() or not marker.is_file():
            # An old or unknown workspace can still be growing outside this process.
            return self.logical_budget_bytes
        try:
            metadata = json.loads((directory / RESERVATION_MARKER).read_text(
                encoding="utf-8"))
            reserved = metadata.get("reserved_bytes")
            if not isinstance(reserved, int) or reserved < 0:
                return self.logical_budget_bytes
            return min(self.logical_budget_bytes,
                       max(reserved, self._payload_size(directory)))
        except (OSError, ValueError, AttributeError, TypeError):
            return self.logical_budget_bytes

    def _reconcile_survivors(self) -> None:
        if self._startup_reconciled:
            return
        self._startup_reconciled = True
        if not self.root.exists():
            return
        for directory in self.root.iterdir():
            if (directory in self._active or directory.is_symlink()
                    or not directory.is_dir() or not directory.name.startswith("copy_")):
                continue
            self._logical_reserved_bytes = min(
                self.logical_budget_bytes,
                self._logical_reserved_bytes + self._survivor_reservation(directory))

    def reclaim_orphans(self, owner_lock: SessionOwnership) -> tuple[Path, ...]:
        """Reclaim only dead marked workspaces; preserve and account unknown owners."""
        if not owner_lock.is_held:
            raise RuntimeError("session ownership is required to reclaim workspaces")
        if not self.root.exists():
            self._reconcile_survivors()
            return ()

        removed = []
        for directory in self.root.iterdir():
            if (directory in self._active or directory.is_symlink()
                    or not directory.is_dir() or not directory.name.startswith("copy_")):
                continue
            marker = directory / ".owner.lock"
            if not marker.exists() and not marker.is_symlink():
                continue
            if marker.is_symlink() or not marker.is_file():
                continue
            owner = FileLock(marker)
            if not owner.acquire():
                continue
            try:
                owner.release()
                shutil.rmtree(directory)
            except OSError:
                continue
            finally:
                owner.release()
            removed.append(directory)
        self._reconcile_survivors()
        return tuple(removed)
