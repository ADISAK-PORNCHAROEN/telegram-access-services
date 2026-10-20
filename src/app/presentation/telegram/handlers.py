"""Telegram events are translated into framework-free application requests."""

import asyncio
import logging
import time
from uuid import uuid4

from app.domain.errors import AccessDenied, QueueExpired, QueueFull
from app.application.performance import JobTrace
from app.domain.models import CopyRequest, CopyResult, CopyStatus
from app.presentation.telegram.link_parser import parse_links
from app.presentation.telegram.messages import (
    ADMIN_HELP_TEXT, DURABLE_HELP_TEXT, HELP_TEXT,
)
from app.presentation.telegram.progress import TelegramCopyObserver
from app.presentation.telegram.transfer_formatting import _fmt_transfer, _totals_body

logger = logging.getLogger(__name__)


class TelegramHandlers:
    def __init__(self, bot, access, preferences, commands, copy_media, scheduler, audit,
                 performance=None, *, accept_copy_job=None, receiving_bot_id=None,
                 durable_limits=None, wake_worker=None):
        self.bot, self.access, self.preferences = bot, access, preferences
        self.commands, self.copy_media, self.scheduler = commands, copy_media, scheduler
        self.audit = audit
        self.performance = performance
        self.accept_copy_job = accept_copy_job
        self.receiving_bot_id = receiving_bot_id
        self.durable_limits = durable_limits or {}
        self.wake_worker = wake_worker

    async def handle(self, event) -> None:
        if not event.is_private:
            return
        sender = event.sender_id
        if sender is None or event.chat_id != sender:
            return
        text = (event.raw_text or "").strip()
        if not self.access.is_allowed(sender):
            if text.lower() == "/start":
                await event.reply(f"บอทนี้เปิดให้เฉพาะผู้ได้รับอนุญาต — id ของคุณคือ {sender} ส่งเลขนี้ให้เจ้าของเพื่อขอสิทธิ")
            return
        if text.lower() in {"/start", "/help"}:
            help_text = HELP_TEXT
            if self.access.is_admin(sender):
                help_text += ADMIN_HELP_TEXT
            if self.accept_copy_job is not None:
                help_text += DURABLE_HELP_TEXT
            await event.reply(help_text)
            return
        reply = await self.commands.execute(sender, text, self.access.is_admin(sender))
        if reply is not None:
            await event.reply(reply)
            words = text.split()
            action = f"command: {words[0].lower()}"
            if words[0].lower() in {"/grant", "/revoke"} and len(words) == 2 and words[1].isdigit():
                action += f" target={int(words[1])}"
            # Membership invite hashes are deliberately excluded from audit.
            self.audit.write(sender, action)
            return
        try:
            links = parse_links(text)
        except ValueError as exc:
            await event.reply(str(exc))
            return
        if self.accept_copy_job is not None:
            request_message_id = getattr(event, "id", None)
            if not request_message_id or self.receiving_bot_id is None:
                await event.reply("ไม่สามารถรับงานได้ เพราะข้อมูลคำขอไม่ครบ — ลองส่งลิงก์ใหม่")
                return
            filter_kind = self.preferences.get(sender)
            for link_index, link in enumerate(links):
                try:
                    admission = await self.accept_copy_job.execute(
                        receiving_bot_id=self.receiving_bot_id,
                        requester_id=sender,
                        destination_chat=event.chat_id,
                        request_message_id=int(request_message_id),
                        link_index=link_index,
                        source=link,
                        filter_kind=filter_kind,
                        max_pending=self.durable_limits["max_pending"],
                        max_pending_per_user=self.durable_limits["max_pending_per_user"],
                    )
                except AccessDenied:
                    return
                except Exception as exc:
                    logger.error("durable admission failed requester_id=%s error=%s",
                                 sender, type(exc).__name__)
                    await event.reply("ระบบยังรับงานนี้ไม่ได้ — ยังไม่ได้บันทึกงาน กรุณาลองใหม่")
                    return
                if not admission.accepted:
                    rejection = {
                        "shutting-down":
                            "ระบบกำลังปิด — ยังไม่ได้รับงานนี้ กรุณาลองใหม่ภายหลัง",
                        "global-pending-limit":
                            "คิวเต็ม — งานนี้ยังไม่ถูกบันทึก ลองส่งใหม่ภายหลัง",
                        "user-pending-limit":
                            f"คุณมีงานรอคิวครบ {self.durable_limits.get('max_pending_per_user', 4)} งานแล้ว — งานนี้ยังไม่ถูกบันทึก",
                    }.get(admission.rejection_reason,
                          "ระบบปฏิเสธงานนี้ — ยังไม่ได้บันทึกคำขอ")
                    await event.reply(rejection)
                    break
                if self.wake_worker is not None:
                    self.wake_worker()
                await event.reply(f"✅ รับงานแล้ว รหัสงาน {admission.job_id}")
            return
        results = []
        totals = {"n": 0, "bytes": 0, "dl": 0.0, "up": 0.0}
        started = time.perf_counter()

        unaccepted_links = 0
        for link_index, link in enumerate(links):
            job_id = uuid4().hex
            trace = JobTrace(job_id)
            trace.profile["active_copy_slots"] = self.scheduler.capacity
            request = CopyRequest(sender, event.chat_id, link, self.preferences.get(sender), job_id)
            observer = TelegramCopyObserver(self.bot, event.chat_id, job_id=job_id, trace=trace)
            result = None
            terminal_status = "failed"
            terminal_reason = "unknown"

            async def reply_safely(message):
                try:
                    await event.reply(message)
                except Exception as notice_error:
                    trace.observer_error(type(notice_error).__name__)
                    logger.warning("copy result notice failed job_id=%s error=%s",
                                   job_id, type(notice_error).__name__)

            async def on_wait(_pending):
                await event.reply(f"⏳ กำลังรอคิว · งาน {job_id[:8]}")

            async def execute():
                trace.start_active()
                try:
                    return await self.copy_media.execute(request, observer, trace)
                finally:
                    trace.stop_active()

            try:
                result = await self.scheduler.run(execute, on_wait, user_id=sender)
                terminal_status, terminal_reason = result.status.value, result.reason or None
                if result.status == CopyStatus.OK:
                    await reply_safely(f"ส่งกลับครบ {result.stats.files} ไฟล์แล้ว · งาน {job_id[:8]}")
                elif result.status == CopyStatus.TEXT:
                    await reply_safely(f"คัดลอกข้อความแล้ว · งาน {job_id[:8]}")
                elif result.status == CopyStatus.DELIVERY_UNKNOWN:
                    confirmed = (
                        f" (ยืนยันแล้ว {result.sent_files}/{result.total_files} ไฟล์)"
                        if result.total_files is not None else "")
                    await reply_safely(
                        f"⚠️ Telegram ยังไม่ยืนยันผลการส่ง{confirmed} "
                        f"กรุณาตรวจสอบไฟล์ในแชตก่อนลองใหม่ ระบบจะไม่ส่งซ้ำอัตโนมัติ · งาน {job_id[:8]}")
            except asyncio.TimeoutError:
                result = CopyResult(CopyStatus.FAILED, reason="timeout")
                terminal_status, terminal_reason = "timeout", "active-timeout"
                await reply_safely(f"รายการนี้ใช้เวลาเกิน 30 นาที — ยกเลิกแล้ว ส่งลิงก์ใหม่ได้เลย · งาน {job_id[:8]}")
            except QueueFull as exc:
                trace.stop_queue_wait()
                result = CopyResult(CopyStatus.FAILED, reason=f"queue-full-{exc.scope}")
                terminal_status, terminal_reason = "rejected", result.reason
                unaccepted_links = len(links) - link_index - 1
                remainder = f" อีก {unaccepted_links} ลิงก์ที่เหลือยังไม่ได้รับ" if unaccepted_links else ""
                await reply_safely(f"คิวเต็ม ยังไม่ได้รับรายการนี้ — ส่งใหม่ภายหลังได้{remainder} · งาน {job_id[:8]}")
            except QueueExpired:
                trace.stop_queue_wait()
                result = CopyResult(CopyStatus.FAILED, reason="queue-wait-timeout")
                terminal_status, terminal_reason = "expired", "queue-wait-timeout"
                await reply_safely(f"รอคิวนานเกินกำหนด — รายการถูกยกเลิก ส่งลิงก์ใหม่ได้ · งาน {job_id[:8]}")
            except AccessDenied:
                terminal_status, terminal_reason = "rejected", "access-revoked"
                return  # A queued user's access may have been revoked.
            except asyncio.CancelledError:
                trace.stop_queue_wait()
                if result is None:
                    terminal_status, terminal_reason = "cancelled", "handler-cancelled"
                else:
                    # The transfer may already have completed before cancellation
                    # interrupted only its user-facing result notification.
                    terminal_status = result.status.value
                    terminal_reason = result.reason or None
                raise
            except Exception:
                trace.stop_queue_wait()
                logger.error("copy handler failed job_id=%s", job_id)
                terminal_status, terminal_reason = "failed", "handler"
                await reply_safely(f"ทำรายการไม่สำเร็จ — ลองใหม่ภายหลัง · งาน {job_id[:8]}")
                result = CopyResult(CopyStatus.FAILED, reason="handler")
            finally:
                with trace.measure("observer_cleanup"):
                    try:
                        await observer.close()
                    except Exception as cleanup_error:
                        trace.cleanup_errors.append(f"observer:{type(cleanup_error).__name__}")
                        logger.error("handler observer cleanup failed job_id=%s error=%s",
                                     job_id, type(cleanup_error).__name__)
                trace_event = trace.finish(
                    status=terminal_status, reason=terminal_reason,
                    downloaded_bytes=(None if result is not None and
                                      "download" in trace.stages and not result.stats.files
                                      else result.stats.bytes if result is not None else None),
                    delivered_bytes=(result.stats.delivered_bytes if result is not None else None),
                )
                if self.performance is not None:
                    try:
                        self.performance.write(trace_event)
                    except Exception:
                        logger.exception("performance sink failed job_id=%s", job_id)
                if result is not None:
                    stats = result.stats.as_dict()
                    audit_action = (f"job={job_id} copy {link.chat}/{link.message_id} → "
                                    f"{result.audit_status}{_fmt_transfer(stats)}")
                else:
                    audit_action = f"job={job_id} terminal={terminal_status} reason={terminal_reason}"
                self.audit.write(sender, audit_action)
            results.append(result)
            stats = result.stats.as_dict()
            totals["n"] += stats["n"]
            totals["bytes"] += stats["bytes"]
            totals["dl"] += stats["dl"] or 0
            totals["up"] += stats["up"] or 0
            if result.reason and result.reason.startswith("queue-full-"):
                break
        if len(results) + unaccepted_links > 1:
            ok = sum(result.status in {CopyStatus.OK, CopyStatus.TEXT} for result in results)
            skipped = sum(result.status in {CopyStatus.FILTERED, CopyStatus.TOO_BIG} for result in results)
            failed = len(results) - ok - skipped + unaccepted_links
            await event.reply(f"📋 รวมผลของ {len(results) + unaccepted_links} ลิงก์: สำเร็จ {ok} · "
                              f"ล้มเหลว/ไม่ครบ {failed} · ข้าม {skipped} · ไม่รับ {unaccepted_links}")
            self.audit.write(sender, f"batch {len(results)} → ok {ok} fail {failed} skip {skipped} · "
                             f"{_totals_body(totals)} · elapsed {time.perf_counter() - started:.1f}s")
