"""อ่านค่าและตรวจ .env ของบอท — ไฟล์เดียวที่โหลด env (ตาม AGENTS.md)."""

import ipaddress
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

REQUIRED_KEYS = ("BOT_TOKEN", "API_ID", "API_HASH", "SESSION_NAME", "ALLOWED_USER_ID")

# งานคัดลอกที่ทำพร้อมกันได้กี่ชิ้น (คิวขนาน) — มีได้จาก .env: MAX_CONCURRENT_COPIES
DEFAULT_MAX_CONCURRENT = 3
MAX_CONCURRENT_LIMITS = (1, 16)
DEFAULT_MAX_PENDING = 128
MAX_PENDING_LIMITS = (1, 128)
DEFAULT_MAX_PENDING_PER_USER = 4
MAX_PENDING_PER_USER_LIMITS = (1, 32)
DEFAULT_QUEUE_WAIT_SECONDS = 300
QUEUE_WAIT_LIMITS = (1, 3600)
DEFAULT_TRANSFER_STREAMS_PER_FILE = 4
TRANSFER_STREAMS_PER_FILE_LIMITS = (1, 8)
DEFAULT_DOWNLOAD_REQUEST_BUDGET = 20
DEFAULT_UPLOAD_REQUEST_BUDGET = 20
TRANSFER_REQUEST_BUDGET_LIMITS = (1, 20)
DEFAULT_ALBUM_TRANSFER_STRATEGY = "sequential"
ALBUM_TRANSFER_STRATEGIES = {"sequential", "pipeline"}
DEFAULT_ALBUM_PIPELINE_MAX_ACTIVE_FILES = 2
ALBUM_PIPELINE_MAX_ACTIVE_FILES_LIMITS = (2, 8)
DEFAULT_ALBUM_PIPELINE_MAX_BUFFER_BYTES = 64 * 1024 * 1024
ALBUM_PIPELINE_MAX_BUFFER_BYTES_LIMITS = (1024 * 1024, 512 * 1024 * 1024)
DEFAULT_WORKSPACE_BUDGET_BYTES = 20 * 1024 * 1024 * 1024
WORKSPACE_BUDGET_BYTES_LIMITS = (1024 * 1024, 1024 * 1024 * 1024 * 1024)
DEFAULT_WORKSPACE_FREE_DISK_FLOOR_BYTES = 5 * 1024 * 1024 * 1024
WORKSPACE_FREE_DISK_FLOOR_BYTES_LIMITS = (0, 1024 * 1024 * 1024 * 1024)
JOB_EXECUTION_MODES = {"inline", "durable"}
DEFAULT_SETUP_HOST = "127.0.0.1"
DEFAULT_SETUP_PORT = 8765
SETUP_PORT_LIMITS = (1, 65535)
DEFAULT_SETUP_BIND_ADDRESS = "127.0.0.1"
_SETUP_ALLOWED_BIND_NETWORKS = tuple(map(ipaddress.ip_network, (
    "127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
    "100.64.0.0/10",
)))


@dataclass(frozen=True)
class BotConfig:
    bot_token: str
    api_id: int
    api_hash: str
    session_name: str  # ชื่อไฟล์ session ของบัญชีเจ้าของ (เก็บใน session/ ไม่ใส่นามสกุล)
    allowed_user_id: int
    max_concurrent_copies: int  # งานพร้อมกันสูงสุด (และสล็อตเปิดให้ user หลายคนแบ่งใช้)
    max_pending_copies: int
    max_pending_per_user: int
    max_queue_wait_seconds: int
    transfer_streams_per_file: int
    download_request_budget: int
    upload_request_budget: int
    album_transfer_strategy: str
    album_pipeline_max_active_files: int
    album_pipeline_max_buffer_bytes: int
    setup_host: str
    setup_port: int
    setup_bind_address: str
    workspace_budget_bytes: int = DEFAULT_WORKSPACE_BUDGET_BYTES
    workspace_free_disk_floor_bytes: int = DEFAULT_WORKSPACE_FREE_DISK_FLOOR_BYTES
    job_execution_mode: str = "inline"
    database_url: str | None = None


def _as_int(name: str, raw: str) -> int:
    try:
        return int(str(raw).strip())
    except ValueError:
        raise ValueError(f"{name} ต้องเป็นตัวเลข แต่ตอนนี้คือ '{raw}' — แก้ใน .env (อ้างรูปแบบจาก .env.example)") from None


def _optional_int(name: str, raw: str | None, default: int, lo: int, hi: int) -> int:
    """ค่าไม่จำเป็น: ไม่กรอก = ค่าเริ่มต้น — กรอกแล้วต้องเป็นเลขในช่วงที่กำหนด."""
    text = (raw or "").strip()
    if not text:
        return default
    value = _as_int(name, text)
    if not lo <= value <= hi:
        raise ValueError(
            f"{name} ต้องเป็นตัวเลขระหว่าง {lo}–{hi} แต่ตอนนี้คือ {value} — แก้ใน .env (อ้างรูปแบบจาก .env.example)"
        )
    return value


def _setup_host(raw: str | None) -> str:
    value = (raw or DEFAULT_SETUP_HOST).strip()
    if value not in {"127.0.0.1", "0.0.0.0"}:
        raise ValueError("SETUP_HOST must be 127.0.0.1 or 0.0.0.0")
    return value


def _setup_bind_address(raw: str | None) -> str:
    value = (raw or DEFAULT_SETUP_BIND_ADDRESS).strip()
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        raise ValueError(
            "SETUP_BIND_ADDRESS must be a loopback or private LAN/VPN IPv4 address; "
            "public HTTP binding is not supported"
        ) from None
    if address.version != 4 or not any(
        address in network for network in _SETUP_ALLOWED_BIND_NETWORKS
    ):
        raise ValueError(
            "SETUP_BIND_ADDRESS must be a loopback or private LAN/VPN IPv4 address; "
            "public HTTP binding is not supported"
        )
    return str(address)


def load(env_path: str | Path | None = None) -> BotConfig:
    """โหลด .env (หรือ env ที่มีอยู่แล้ว) แล้วตรวจครบ/ถูกรูปแบบ ติดให้มันแจ้งทั้งชุดเลย."""
    load_dotenv(dotenv_path=str(env_path) if env_path else None)
    missing = [key for key in REQUIRED_KEYS if not (os.getenv(key) or "").strip()]
    if missing:
        raise ValueError(
            "ตั้งค่าไม่ครบ: " + ", ".join(missing) + " — กรอกใน .env (คัดลอกโครงจาก .env.example) แล้วรันใหม่"
        )
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if bot_token.count(":") < 1:
        raise ValueError("BOT_TOKEN รูปแบบไม่ถูกต้อง (ควรเป็นเลข:ตัวอักษร) — ขอ token ใหม่จาก @BotFather แล้ววางใน .env")
    max_pending = _optional_int("MAX_PENDING_COPIES", os.getenv("MAX_PENDING_COPIES"),
                                DEFAULT_MAX_PENDING, *MAX_PENDING_LIMITS)
    max_pending_per_user = _optional_int(
        "MAX_PENDING_PER_USER", os.getenv("MAX_PENDING_PER_USER"),
        DEFAULT_MAX_PENDING_PER_USER, *MAX_PENDING_PER_USER_LIMITS)
    if max_pending_per_user > max_pending:
        raise ValueError("MAX_PENDING_PER_USER ต้องไม่เกิน MAX_PENDING_COPIES")
    transfer_streams_per_file = _optional_int(
        "TRANSFER_STREAMS_PER_FILE", os.getenv("TRANSFER_STREAMS_PER_FILE"),
        DEFAULT_TRANSFER_STREAMS_PER_FILE, *TRANSFER_STREAMS_PER_FILE_LIMITS)
    download_request_budget = _optional_int(
        "DOWNLOAD_REQUEST_BUDGET", os.getenv("DOWNLOAD_REQUEST_BUDGET"),
        DEFAULT_DOWNLOAD_REQUEST_BUDGET, *TRANSFER_REQUEST_BUDGET_LIMITS)
    upload_request_budget = _optional_int(
        "UPLOAD_REQUEST_BUDGET", os.getenv("UPLOAD_REQUEST_BUDGET"),
        DEFAULT_UPLOAD_REQUEST_BUDGET, *TRANSFER_REQUEST_BUDGET_LIMITS)
    if transfer_streams_per_file > download_request_budget:
        raise ValueError(
            "TRANSFER_STREAMS_PER_FILE ต้องไม่เกิน DOWNLOAD_REQUEST_BUDGET")
    if transfer_streams_per_file > upload_request_budget:
        raise ValueError(
            "TRANSFER_STREAMS_PER_FILE ต้องไม่เกิน UPLOAD_REQUEST_BUDGET")
    album_transfer_strategy = (os.getenv("ALBUM_TRANSFER_STRATEGY") or
                               DEFAULT_ALBUM_TRANSFER_STRATEGY).strip().lower()
    if album_transfer_strategy not in ALBUM_TRANSFER_STRATEGIES:
        raise ValueError(
            "ALBUM_TRANSFER_STRATEGY ต้องเป็น sequential หรือ pipeline")
    job_execution_mode = (os.getenv("JOB_EXECUTION_MODE") or "inline").strip().lower()
    if job_execution_mode not in JOB_EXECUTION_MODES:
        raise ValueError("JOB_EXECUTION_MODE ต้องเป็น inline หรือ durable")
    database_url = (os.getenv("DATABASE_URL") or "").strip() or None
    if job_execution_mode == "durable" and database_url is None:
        raise ValueError("JOB_EXECUTION_MODE=durable ต้องกำหนด DATABASE_URL")
    setup_host = _setup_host(os.getenv("SETUP_HOST"))
    setup_port = _optional_int(
        "SETUP_PORT", os.getenv("SETUP_PORT"), DEFAULT_SETUP_PORT,
        *SETUP_PORT_LIMITS)
    setup_bind_address = _setup_bind_address(os.getenv("SETUP_BIND_ADDRESS"))
    if not ipaddress.ip_address(setup_bind_address).is_loopback and setup_host != "0.0.0.0":
        raise ValueError("SETUP_HOST must be 0.0.0.0 for a private host bind address")
    return BotConfig(
        bot_token=bot_token,
        api_id=_as_int("API_ID", os.getenv("API_ID", "")),
        api_hash=os.getenv("API_HASH", "").strip(),
        session_name=os.getenv("SESSION_NAME", "").strip(),
        allowed_user_id=_as_int("ALLOWED_USER_ID", os.getenv("ALLOWED_USER_ID", "")),
        max_concurrent_copies=_optional_int(
            "MAX_CONCURRENT_COPIES",
            os.getenv("MAX_CONCURRENT_COPIES"),
            DEFAULT_MAX_CONCURRENT,
            *MAX_CONCURRENT_LIMITS,
        ),
        max_pending_copies=max_pending,
        max_pending_per_user=max_pending_per_user,
        max_queue_wait_seconds=_optional_int(
            "MAX_QUEUE_WAIT_SECONDS", os.getenv("MAX_QUEUE_WAIT_SECONDS"),
            DEFAULT_QUEUE_WAIT_SECONDS, *QUEUE_WAIT_LIMITS),
        transfer_streams_per_file=transfer_streams_per_file,
        download_request_budget=download_request_budget,
        upload_request_budget=upload_request_budget,
        album_transfer_strategy=album_transfer_strategy,
        album_pipeline_max_active_files=_optional_int(
            "ALBUM_PIPELINE_MAX_ACTIVE_FILES", os.getenv("ALBUM_PIPELINE_MAX_ACTIVE_FILES"),
            DEFAULT_ALBUM_PIPELINE_MAX_ACTIVE_FILES,
            *ALBUM_PIPELINE_MAX_ACTIVE_FILES_LIMITS),
        album_pipeline_max_buffer_bytes=_optional_int(
            "ALBUM_PIPELINE_MAX_BUFFER_BYTES", os.getenv("ALBUM_PIPELINE_MAX_BUFFER_BYTES"),
            DEFAULT_ALBUM_PIPELINE_MAX_BUFFER_BYTES,
            *ALBUM_PIPELINE_MAX_BUFFER_BYTES_LIMITS),
        workspace_budget_bytes=_optional_int(
            "WORKSPACE_BUDGET_BYTES", os.getenv("WORKSPACE_BUDGET_BYTES"),
            DEFAULT_WORKSPACE_BUDGET_BYTES, *WORKSPACE_BUDGET_BYTES_LIMITS),
        workspace_free_disk_floor_bytes=_optional_int(
            "WORKSPACE_FREE_DISK_FLOOR_BYTES",
            os.getenv("WORKSPACE_FREE_DISK_FLOOR_BYTES"),
            DEFAULT_WORKSPACE_FREE_DISK_FLOOR_BYTES,
            *WORKSPACE_FREE_DISK_FLOOR_BYTES_LIMITS),
        setup_host=setup_host,
        setup_port=setup_port,
        setup_bind_address=setup_bind_address,
        job_execution_mode=job_execution_mode,
        database_url=database_url,
    )


def load_database_url(env_path: str | Path | None = None) -> str:
    """Read DATABASE_URL, loading only a specifically supplied environment file."""
    if env_path is not None:
        load_dotenv(dotenv_path=str(env_path))
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if not database_url:
        raise ValueError("DATABASE_URL is not configured")
    return database_url
