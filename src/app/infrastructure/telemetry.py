"""Audit sink and rotating operational logs; paths are injected by bootstrap."""

import importlib.metadata
import importlib.util
import json
import logging
import sys
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from telethon.crypto import aes as telethon_aes


class AuditLog:
    def __init__(self, directory: Path):
        self.directory = directory

    def write(self, sender: int | None, action: str) -> None:
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            with (self.directory / "audit.log").open("a", encoding="utf-8") as handle:
                handle.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | user={sender} | {action}\n")
        except OSError:
            logging.getLogger(__name__).exception("audit write failed")


def configure_logging(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("app")
    if logger.handlers:
        return
    handler = RotatingFileHandler(directory / "runtime.log", maxBytes=5 * 1024**2,
                                  backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)


class PerformanceLog:
    """Rotating JSONL terminal records with software metadata and no source content."""

    def __init__(self, directory: Path):
        directory.mkdir(parents=True, exist_ok=True)
        path = (directory / "performance.jsonl").resolve()
        self.logger = logging.getLogger(f"app.performance.{hash(path)}")
        if not self.logger.handlers:
            handler = RotatingFileHandler(path, maxBytes=5 * 1024**2,
                                          backupCount=3, encoding="utf-8")
            handler.setFormatter(logging.Formatter("%(message)s"))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
            self.logger.propagate = False
        if telethon_aes.cryptg:
            crypto_backend = "cryptg"
            crypto_backend_version = _version("cryptg")
        elif telethon_aes.libssl.encrypt_ige and telethon_aes.libssl.decrypt_ige:
            crypto_backend = "libssl"
            crypto_backend_version = None
        else:
            crypto_backend = "pyaes"
            crypto_backend_version = _version("pyaes")
        self.software = {
            "python": sys.version.split()[0],
            "telethon": _version("telethon"),
            "cryptg": _version("cryptg") if importlib.util.find_spec("cryptg") else None,
            "crypto_backend": crypto_backend,
            "crypto_backend_version": crypto_backend_version,
            "telegram_service": _version("telegram-service"),
        }

    def write(self, event: dict) -> None:
        try:
            record = {**event, "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
                      "software": self.software}
            self.logger.info("%s", json.dumps(record, sort_keys=True, separators=(",", ":")))
        except Exception:
            # Instrumentation must never replace a transfer result or exception.
            logging.getLogger(__name__).exception("performance record write failed")


def _version(package: str) -> str | None:
    try:
        return importlib.metadata.version(package)
    except importlib.metadata.PackageNotFoundError:
        return None
