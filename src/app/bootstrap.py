"""Composition root: this is the only place concrete adapters are assembled."""

import asyncio
import signal
import sys
from pathlib import Path

import asyncpg
from telethon import TelegramClient, events
from telethon.errors import AuthKeyDuplicatedError

from app.application.scheduler import CopyScheduler
from app.application.use_cases.durable_jobs import AcceptCopyJob, DurableCopyWorker
from app.application.use_cases.copy_media import CopyMedia
from app.application.use_cases.cancel_copy_job import CancelCopyJob
from app.application.use_cases.get_copy_job_status import GetCopyJobStatus
from app.application.use_cases.join_chat import JoinChat
from app.application.use_cases.manage_access import ManageAccess
from app.application.use_cases.set_filter import SetFilter
from app.application.use_cases.setup_owner_session import SetupOwnerSession
from app.config import load, load_database_url
from app.infrastructure.storage.prefs import PrefsStore
from app.infrastructure.storage.migrations.runner import (
    MigrationError,
    PostgresMigrationRunner,
)
from app.infrastructure.storage.postgres_jobs import PostgresJobStore
from app.infrastructure.storage.users import UserStore
from app.infrastructure.telegram.delivery import TelegramDelivery
from app.infrastructure.telegram.membership import TelegramMembership
from app.infrastructure.telegram.owner_auth import TelegramOwnerAuthentication
from app.infrastructure.telegram.retry import (
    TELETHON_FLOOD_SLEEP_THRESHOLD_SECONDS, TELETHON_REQUEST_RETRIES,
)
from app.infrastructure.telegram.source import TelegramSource
from app.infrastructure.telegram.transfers.engine import TransferEngine
from app.infrastructure.telemetry import AuditLog, PerformanceLog, configure_logging
from app.infrastructure.process_lock import SessionOwnerLocks
from app.infrastructure.workspace import TempWorkspace
from app.presentation.telegram.commands import Commands
from app.presentation.telegram.handlers import TelegramHandlers
from app.presentation.telegram.jobs import TelegramJobNotifier
from app.presentation.telegram.progress import TelegramCopyObserver
from app.presentation.web.setup_server import SetupWebServer


def _register_sigterm_handler(loop, handler) -> bool:
    """Let POSIX service managers trigger the normal async cleanup path."""
    if sys.platform == "win32":
        return False
    loop.add_signal_handler(signal.SIGTERM, handler)
    return True


async def _apply_database_migrations(root: Path) -> None:
    pool = await asyncpg.create_pool(load_database_url(root / ".env"))
    try:
        await PostgresMigrationRunner(pool).apply()
    finally:
        await pool.close()


def migrate_database() -> None:
    """Apply additive PostgreSQL migrations without starting Telegram clients."""
    try:
        asyncio.run(_apply_database_migrations(Path.cwd().resolve()))
    except MigrationError as exc:
        print(f"[database] {exc}", file=sys.stderr)
        raise SystemExit(1) from None
    except Exception:
        print(
            "[database] migration failed; check PostgreSQL connectivity and configuration.",
            file=sys.stderr,
        )
        raise SystemExit(1) from None
    print("PostgreSQL migrations applied.")


async def main(root: Path) -> None:
    root = root.resolve()
    try:
        config = load(root / ".env")
        access = UserStore(root / "users.json")
        access.ensure_admin(config.allowed_user_id)
        preferences = PrefsStore(root / "prefs.json")
    except ValueError as exc:
        print(f"[ตั้งค่า] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    session_dir = root / "session"
    session_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(root / "logs")
    performance = PerformanceLog(root / "logs")
    session_locks = SessionOwnerLocks((
        session_dir / "bot.owner.lock",
        session_dir / f"{config.session_name}.owner.lock",
    ))
    workspace = TempWorkspace(
        root / "tmp", logical_budget_bytes=config.workspace_budget_bytes,
        free_disk_floor_bytes=config.workspace_free_disk_floor_bytes)
    bot = user = None
    pool = None
    job_store = None
    worker = None
    accept_copy_job = None
    handlers = None
    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()
    shutdown_requested = False
    sigterm_registered = False

    def request_shutdown() -> None:
        nonlocal shutdown_requested
        if shutdown_requested:
            return
        shutdown_requested = True
        try:
            if accept_copy_job is not None:
                accept_copy_job.stop_admission()
            if bot is not None and handlers is not None:
                bot.remove_event_handler(handlers.handle)
        finally:
            if main_task is not None and not main_task.done():
                main_task.cancel()

    try:
        sigterm_registered = _register_sigterm_handler(loop, request_shutdown)
        session_locks.acquire()
        workspace.reclaim_orphans(session_locks)
        # The OS lock must be held before either Telethon session is opened.
        bot = TelegramClient(str(session_dir / "bot"), config.api_id, config.api_hash,
                             request_retries=TELETHON_REQUEST_RETRIES,
                             flood_sleep_threshold=TELETHON_FLOOD_SLEEP_THRESHOLD_SECONDS,
                             raise_last_call_error=True)
        user = TelegramClient(str(session_dir / config.session_name), config.api_id,
                              config.api_hash,
                              request_retries=TELETHON_REQUEST_RETRIES,
                              flood_sleep_threshold=TELETHON_FLOOD_SLEEP_THRESHOLD_SECONDS,
                              raise_last_call_error=True)
        engine = TransferEngine(
            user, bot,
            per_file_streams=config.transfer_streams_per_file,
            download_request_budget=config.download_request_budget,
            upload_request_budget=config.upload_request_budget,
        )
        copy_media = CopyMedia(
            TelegramSource(user, engine), TelegramDelivery(bot, engine),
            workspace, access,
            album_transfer_strategy=config.album_transfer_strategy,
            album_pipeline_max_active_files=config.album_pipeline_max_active_files,
            album_pipeline_max_buffer_bytes=config.album_pipeline_max_buffer_bytes,
        )
        owner_setup = SetupOwnerSession(TelegramOwnerAuthentication(user))
        await owner_setup.start()
        if not owner_setup.is_complete:
            await SetupWebServer(
                owner_setup,
                host=config.setup_host,
                port=config.setup_port,
                bind_address=config.setup_bind_address,
            ).run_until_complete()
        if not await owner_setup.verify_authorized():
            raise RuntimeError("Telegram owner authorization could not be confirmed")
        if getattr(config, "job_execution_mode", "inline") == "durable":
            pool = await asyncpg.create_pool(config.database_url)
            job_store = PostgresJobStore(pool)
            accept_copy_job = AcceptCopyJob(job_store, access)
        else:
            job_store = accept_copy_job = None
        get_copy_job_status = cancel_copy_job = None
        if job_store is not None:
            notifier = TelegramJobNotifier(bot)
            worker = DurableCopyWorker(
                job_store, copy_media, access,
                lambda job, trace: TelegramCopyObserver(
                    bot, job.destination_chat, job_id=job.job_id, trace=trace),
                notifier.terminal, concurrency=config.max_concurrent_copies,
                performance=performance)
            get_copy_job_status = GetCopyJobStatus(job_store, access)
            cancel_copy_job = CancelCopyJob(job_store, access, worker)
        commands = Commands(ManageAccess(access), SetFilter(access, preferences),
                            JoinChat(access, TelegramMembership(user)), preferences,
                            get_copy_job_status=get_copy_job_status,
                            cancel_copy_job=cancel_copy_job)
        scheduler = CopyScheduler(
            config.max_concurrent_copies,
            pending_limit=config.max_pending_copies,
            per_user_pending_limit=config.max_pending_per_user,
            queue_timeout=config.max_queue_wait_seconds)
        await bot.start(bot_token=config.bot_token)
        bot_identity = await bot.get_me()
        if job_store is not None:
            await worker.start()
        handlers = TelegramHandlers(
            bot, access, preferences, commands, copy_media, scheduler,
            AuditLog(root / "logs"), performance,
            accept_copy_job=accept_copy_job,
            receiving_bot_id=getattr(bot_identity, "id", None),
            durable_limits={"max_pending": config.max_pending_copies,
                            "max_pending_per_user": config.max_pending_per_user},
            wake_worker=worker.notify if worker is not None else None,
        )
        bot.add_event_handler(handlers.handle, events.NewMessage())
        username = bot_identity.username
        print(f"✅ พร้อมใช้งาน — ส่งลิงก์โพสต์มาที่ @{username} · งานพร้อมกันสูงสุด {config.max_concurrent_copies} ลิงก์")
        await bot.run_until_disconnected()
    except asyncio.CancelledError:
        if not shutdown_requested:
            raise
    except AuthKeyDuplicatedError:
        print("[session] session หมดอายุ/ไม่ตรงกัน — ให้เจ้าของตรวจสอบและล็อกอินใหม่", file=sys.stderr)
        raise SystemExit(1)
    finally:
        if sigterm_registered:
            loop.remove_signal_handler(signal.SIGTERM)
        try:
            if accept_copy_job is not None:
                accept_copy_job.stop_admission()
                await accept_copy_job.wait_for_admissions()
        finally:
            try:
                if worker is not None:
                    await worker.stop()
            finally:
                try:
                    if bot is not None:
                        await bot.disconnect()
                finally:
                    try:
                        if user is not None:
                            await user.disconnect()
                    finally:
                        try:
                            if pool is not None:
                                await pool.close()
                        finally:
                            session_locks.release()
