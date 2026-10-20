"""Small cross-platform OS locks for local process ownership."""

from __future__ import annotations

import errno
import os
from pathlib import Path


class FileLock:
    """Hold an exclusive, non-blocking lock for the lifetime of this object."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._stream = None

    @property
    def is_held(self) -> bool:
        return self._stream is not None

    def acquire(self) -> bool:
        if self._stream is not None:
            return True
        self.path.parent.mkdir(parents=True, exist_ok=True)
        stream = self.path.open("a+b")
        try:
            stream.seek(0, os.SEEK_END)
            if stream.tell() == 0:
                stream.write(b"\0")
                stream.flush()
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            elif os.name == "posix":
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:  # pragma: no cover - Python only supports these deployment OSes.
                raise OSError(f"unsupported operating system: {os.name}")
        except OSError as exc:
            stream.close()
            if isinstance(exc, BlockingIOError) or exc.errno in {
                    errno.EACCES, errno.EAGAIN}:
                return False
            raise
        self._stream = stream
        return True

    def release(self) -> None:
        stream, self._stream = self._stream, None
        if stream is None:
            return
        try:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            elif os.name == "posix":
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        finally:
            stream.close()

    def __enter__(self) -> FileLock:
        if not self.acquire():
            raise BlockingIOError(f"lock is already held: {self.path.name}")
        return self

    def __exit__(self, *_exc) -> None:
        self.release()


class SessionAlreadyOwnedError(RuntimeError):
    """The configured Telethon session is already open in another process."""


class SessionOwnerLock:
    """Process-wide ownership guard for one local Telethon session."""

    def __init__(self, path: Path):
        self._lock = FileLock(path)

    @property
    def is_held(self) -> bool:
        return self._lock.is_held

    def acquire(self) -> None:
        if not self._lock.acquire():
            raise SessionAlreadyOwnedError("Telegram session is already in use")

    def release(self) -> None:
        self._lock.release()


class SessionOwnerLocks:
    """Atomically acquire local ownership for every Telethon session path."""

    def __init__(self, paths: tuple[Path, ...] | list[Path]):
        normalized = {Path(path).resolve(strict=False) for path in paths}
        ordered = sorted(normalized, key=lambda path: os.path.normcase(str(path)))
        self._locks = tuple(FileLock(path) for path in ordered)

    @property
    def is_held(self) -> bool:
        return bool(self._locks) and all(lock.is_held for lock in self._locks)

    def acquire(self) -> None:
        acquired = []
        try:
            for lock in self._locks:
                if not lock.acquire():
                    raise SessionAlreadyOwnedError(
                        "a Telegram session is already in use")
                acquired.append(lock)
        except Exception:
            for lock in reversed(acquired):
                lock.release()
            raise

    def release(self) -> None:
        for lock in reversed(self._locks):
            lock.release()
