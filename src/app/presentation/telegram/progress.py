"""One tracked progress editor per job, shared by every media download."""

import asyncio
import logging
import time

from app.presentation.telegram.formatting import KIND_LABELS_TH, fmt_size

logger = logging.getLogger(__name__)

PER_FILE_PROGRESS_MIN_BYTES = 1024 * 1024


def _per_file_text(index, total, pct, done, file_total):
    return f"⬇️ ไฟล์ที่ {index}/{total}: {pct}% ({fmt_size(done)} / {fmt_size(file_total)})"


class ProgressReporter:
    def __init__(self, bot, chat, message_id, label="⬇️ กำลังดาวน์โหลด", fmt=None,
                 on_error=None):
        self.bot, self.chat, self.message_id = bot, chat, message_id
        self.label, self.fmt = label, fmt
        self.on_error = on_error
        self._task = None
        self._last_time = 0.0
        self._closed = False
        self._paused = False

    def __call__(self, done, total):
        if (self._closed or self._paused or total <= 0
                or (self._task and not self._task.done())):
            return
        now = time.monotonic()
        if now - self._last_time < 3:
            return
        self._last_time = now
        pct = min(100, int(done * 100 / total))
        text = self.fmt(pct, done, total) if self.fmt else (
            f"{self.label} {pct}% ({fmt_size(done)} / {fmt_size(total)})")
        self._task = asyncio.create_task(self._edit(text))

    async def _edit(self, text):
        try:
            await self.bot.edit_message(self.chat, self.message_id, text)
        except Exception as exc:
            if self.on_error:
                self.on_error(type(exc).__name__)
            logger.warning("progress update failed error=%s", type(exc).__name__)

    async def pause(self):
        self._paused = True
        task = self._task
        if task:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            if self._task is task:
                self._task = None

    def resume(self):
        if not self._closed:
            self._paused = False

    async def close(self):
        self._closed = True
        await self.pause()


def _progress_reporter(bot, reply_chat, prog_id, label="⬇️ กำลังดาวน์โหลด", fmt=None):
    return ProgressReporter(bot, reply_chat, prog_id, label, fmt)


def _per_file_progress(bot, reply_chat, prog_id, index, total, size):
    if not size or size < PER_FILE_PROGRESS_MIN_BYTES:
        return None
    return ProgressReporter(bot, reply_chat, prog_id,
                            fmt=lambda pct, done, count: _per_file_text(index, total, pct, done, count))


class TelegramCopyObserver:
    def __init__(self, bot, chat, job_id: str = "", trace=None):
        self.bot, self.chat = bot, chat
        self.job_id = job_id[:8]
        self.trace = trace
        self.message_id = None
        self._reporter = None
        self._sizes = []
        self._marks = []
        self._closed = False

    async def notice(self, code, **data):
        if code == "download":
            items = data["items"]
            self._sizes = [item.size or 0 for item in items]
            self._marks = [0] * len(items)
            message = await self.bot.send_message(self.chat, f"⬇️ เริ่มดาวน์โหลด {len(items)} ไฟล์…")
            self.message_id = message.id
            self._reporter = ProgressReporter(
                self.bot, self.chat, message.id,
                on_error=self.trace.observer_error if self.trace else None)
            return
        if code == "upload":
            if self._reporter:
                await self._reporter.close()
            await self._edit("⬆️ ดาวน์โหลดเสร็จ — กำลังส่งไฟล์กลับ…")
            self._reporter = ProgressReporter(
                self.bot, self.chat, self.message_id, "⬆️ กำลังอัพโหลด",
                on_error=self.trace.observer_error if self.trace else None)
            return
        if code == "rate_wait":
            seconds = max(1, int(data["seconds"] + 0.999))
            kind = data.get("wait_kind")
            kind_label = {
                "flood": "FloodWait",
                "premium": "FloodPremiumWait",
            }.get(kind, "Telegram")
            account_label = {
                "owner": "บัญชีต้นทาง",
                "bot": "บัญชีส่งกลับ",
            }.get(data.get("account_role"), "บัญชี Telegram")
            if self._reporter:
                await self._reporter.pause()
            await self._edit(
                f"⏳ {kind_label} จำกัดความถี่ที่{account_label} — "
                f"รอประมาณ {seconds} วินาที แล้วงานจะทำต่ออัตโนมัติ"
                f"{self._job_suffix()}")
            return
        if code == "capacity_wait":
            await self._edit(f"พื้นที่จัดเก็บชั่วคราวเต็ม กำลังรอให้มีพื้นที่ว่างก่อนเริ่มดาวน์โหลด"
                             f"{self._job_suffix()}")
            return
        if code == "filtered":
            text = (f"ข้ามลิงก์นี้: กรองของคุณตั้งไว้เป็น «{KIND_LABELS_TH.get(data['preference'], '?')}» "
                    f"— ลิงก์นี้เป็น «{KIND_LABELS_TH.get(data['kind'], 'ชนิดพิเศษ')}» (ปิดกรองด้วย /only off)")
        elif code == "too_big":
            text = f"ไฟล์นี้ใหญ่ {fmt_size(data['size'])} — เกินขีดส่งกลับประมาณ 2GB ของบอท จึงไม่เริ่มดาวน์โหลด"
        elif code == "workspace_too_big":
            text = (f"ชุดไฟล์นี้ต้องใช้พื้นที่ชั่วคราวประมาณ {fmt_size(data['size'])} "
                    f"ซึ่งเกินงบ {fmt_size(data['limit'])} จึงไม่เริ่มดาวน์โหลด")
        elif code == "large_skipped":
            text = f"⚠️ ข้าม {data['skipped']} ไฟล์ที่ใหญ่เกิน 2GB — จะส่งเฉพาะ {data['remaining']} ไฟล์ที่ส่งได้"
        elif code == "partial":
            text = f"ส่งกลับได้ {data['sent']}/{data['total']} ไฟล์ — บางไฟล์ส่งไม่สำเร็จ{self._job_suffix()}"
        elif code == "rate_limited":
            seconds = int(data["seconds"]) + 1
            reason = data.get("reason")
            if reason == "active-deadline":
                text = f"Telegram จำกัดความถี่ รอประมาณ {seconds} วินาที ซึ่งเกินเวลาของงานนี้ จึงหยุดงาน"
            elif reason == "retry-budget-exhausted":
                text = f"Telegram จำกัดความถี่และรอเกินขีดจำกัด {seconds} วินาที จึงหยุดงาน"
            elif reason == "not-retryable":
                text = "Telegram จำกัดความถี่และไม่สามารถลองคำขอนี้ซ้ำได้อย่างปลอดภัย"
            else:
                text = f"Telegram จำกัดความถี่ — รอประมาณ {seconds} วินาที แล้วลองใหม่"
        elif code == "error":
            text = ("ส่งข้อความไม่สำเร็จ — ข้อความอาจยาวเกิน 4096 ตัวอักษร" if data["stage"] == "text"
                    else f"ดึง/ส่งไฟล์ไม่สำเร็จ ({data['error']}) — ลองใหม่ภายหลัง")
        else:
            text = {
                "missing": "ไม่เจอข้อความจากลิงก์นี้ (อาจถูกลบไปแล้ว)",
                "source_unavailable": "session ของคุณไม่ได้อยู่ในกลุ่ม/ช่องนี้ หรือไม่มีสิทธิ์เข้าถึง — ให้บัญชีเจ้าของเข้ากลุ่มก่อน",
                "session_expired": "session หมดอายุ — ให้เจ้าของบอทตรวจสอบและล็อกอินใหม่",
                "no_media": "media ชนิดนี้ดึงเนื้อหาไม่ได้ (ประเภทพิเศษ เช่น poll/ควิซ)",
            }[code]
            if code in {"rate_limited", "error", "source_unavailable", "session_expired", "no_media"}:
                text += self._job_suffix()
        await self.bot.send_message(self.chat, text)

    def _job_suffix(self):
        return f" · งาน {self.job_id}" if self.job_id else ""

    def download_progress(self, index, done, total):
        self._marks[index] = done
        if self._reporter:
            self._reporter(sum(self._marks), sum(self._sizes))

    def upload_progress(self, done, total):
        if self._reporter:
            self._reporter(done, total)

    def resume_progress(self):
        if self._reporter:
            self._reporter.resume()

    async def _edit(self, text):
        if self.message_id is not None:
            try:
                await self.bot.edit_message(self.chat, self.message_id, text)
            except Exception:
                pass

    async def close(self):
        if self._closed:
            return
        self._closed = True
        if self._reporter:
            await self._reporter.close()
        if self.message_id is not None:
            try:
                await self.bot.delete_messages(self.chat, self.message_id)
            except Exception as exc:
                if self.trace:
                    self.trace.cleanup_errors.append(f"observer:{type(exc).__name__}")
                logger.warning("progress message cleanup failed job_id=%s error=%s",
                               self.job_id, type(exc).__name__)
