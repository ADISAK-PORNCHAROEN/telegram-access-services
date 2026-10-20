"""Command parsing and Thai replies; mutations go through use cases."""

import math
import re
from zoneinfo import ZoneInfo

from app.application.use_cases.join_chat import JoinChat
from app.application.use_cases.manage_access import ManageAccess
from app.application.use_cases.set_filter import SetFilter
from app.domain.errors import RateLimited
from app.domain.models import (
    JobCancellationDisposition, JobState,
)
from app.presentation.telegram.formatting import KIND_LABELS_TH, resolve_kind_arg
from app.presentation.telegram.link_parser import parse_invite_link


class Commands:
    def __init__(self, access: ManageAccess, filters: SetFilter, join: JoinChat,
                 preferences, *, get_copy_job_status=None, cancel_copy_job=None):
        self.access, self.filters = access, filters
        self.join, self.preferences = join, preferences
        self.get_copy_job_status = get_copy_job_status
        self.cancel_copy_job = cancel_copy_job

    async def execute(self, actor: int, text: str, is_admin: bool) -> str | None:
        words = text.split()
        command = words[0].lower() if words else ""
        if command in {"/status", "/cancel"}:
            return await self._job_command(actor, command, words)
        if command in {"/grant", "/revoke", "/users"}:
            if not is_admin:
                return None
            if command == "/users":
                admins, users = self.access.execute(actor, "users")
                return (f"👑 admin: {', '.join(map(str, admins)) or '—'}\n"
                        f"👥 users ({len(users)}): {', '.join(map(str, users)) or 'ยังไม่มี'}")
            if len(words) != 2 or not words[1].isdigit():
                return f"ใช้แบบนี้: {command} <ตัวเลข id>"
            changed = self.access.execute(actor, command[1:], int(words[1]))
            if command == "/grant":
                return f"✓ เปิดให้ {words[1]} ใช้บอทแล้ว" if changed else f"{words[1]} อยู่ในรายชื่ออยู่แล้ว"
            return f"✓ ปิดสิทธิ {words[1]} แล้ว" if changed else f"{words[1]} ไม่ได้อยู่ในรายชื่อ user อยู่แล้ว"
        if command == "/only":
            arg = text.split(maxsplit=1)[1] if len(words) > 1 else ""
            if not arg:
                kind = self.preferences.get(actor)
                return f"กรองของคุณตอนนี้: {KIND_LABELS_TH.get(kind, 'ไม่กรอง — รับทุกชนิด')}\nตั้งค่า: /only รูป · /only วีดิโอ · /only ไฟล์ · /only off"
            try:
                kind = resolve_kind_arg(arg)
            except ValueError as exc:
                return str(exc)
            self.filters.execute(actor, kind)
            return (f"✓ จากนี้ดึงเฉพาะ «{KIND_LABELS_TH[kind]}» ให้คุณ (ปิดกรองด้วย /only off)"
                    if kind else "✓ ปิดกรองแล้ว — จากนี้รับทุกชนิด")
        if command == "/join":
            if not is_admin:
                return "คำสั่ง /join เปิดให้เฉพาะเจ้าของบอท"
            try:
                kind, value = parse_invite_link(text.split(maxsplit=1)[1] if len(words) > 1 else "")
                result = await self.join.execute(actor, kind, value)
            except ValueError as exc:
                return str(exc)
            except RateLimited as exc:
                return f"Telegram จำกัดความถี่ — รอประมาณ {exc.seconds + 1} วินาทีแล้วสั่ง /join ใหม่"
            except Exception:
                return "เข้ากลุ่มไม่สำเร็จ — ตรวจสอบลิงก์และ session แล้วลองใหม่"
            return {
                "already": "อยู่ในกลุ่มนี้อยู่แล้ว (ไม่ต้องเข้าซ้ำ)",
                "expired": "ลิงก์เชิญหมดอายุ — ขอลิงก์ใหม่จาก admin กลุ่ม",
                "requested": "ส่งคำขอเข้ากลุ่มแล้ว — รอ admin กลุ่มเห็นชอบ",
                "joined": "เข้ากลุ่มสำเร็จ — วางลิงก์โพสต์จากกลุ่มนี้ได้เลย",
            }[result]
        return None

    async def _job_command(self, actor: int, command: str,
                           words: list[str]) -> str:
        use_case = (self.get_copy_job_status if command == "/status"
                    else self.cancel_copy_job)
        if use_case is None:
            return "คำสั่งดูสถานะและยกเลิกงานใช้ได้เมื่อเปิดโหมดงานถาวร"
        if (len(words) != 2
                or re.fullmatch(r"[0-9a-fA-F]{32}", words[1]) is None):
            return f"ใช้แบบนี้: {command} <รหัสงาน>"
        job_id = words[1].lower()
        if command == "/status":
            job = await use_case.execute(actor, job_id)
            return ("ไม่พบงานนี้ หรือคุณไม่มีสิทธิ์ดู"
                    if job is None else _format_job_status(job))

        decision = await use_case.execute(actor, job_id)
        if decision.disposition == JobCancellationDisposition.NOT_FOUND:
            return "ไม่พบงานนี้ หรือคุณไม่มีสิทธิ์ยกเลิก"
        if decision.disposition == JobCancellationDisposition.CANCELLED:
            return f"ยกเลิกงาน {job_id} แล้ว และหยุดงานที่กำลังทำอยู่เรียบร้อย"
        if decision.disposition == JobCancellationDisposition.CANCELLATION_REQUESTED:
            return f"รับคำขอยกเลิกงาน {job_id} แล้ว กำลังหยุดและล้างไฟล์ชั่วคราว"
        if decision.disposition == JobCancellationDisposition.ALREADY_CANCELLED:
            return f"งาน {job_id} ถูกยกเลิกแล้ว"
        if decision.disposition == JobCancellationDisposition.TOO_LATE:
            return (f"งาน {job_id} เริ่มส่งไฟล์แล้ว จึงยกเลิกไม่ได้เพื่อรักษาผลการส่ง — "
                    f"ใช้ /status {job_id} ตรวจผล")
        return f"งาน {job_id} จบแล้ว: {_state_label(decision.state)}"


def _state_label(state: JobState | None) -> str:
    return {
        JobState.QUEUED: "รอคิว",
        JobState.RUNNING: "กำลังทำงาน",
        JobState.WAITING_RATE_LIMIT: "รอ Telegram",
        JobState.COMPLETED: "สำเร็จ",
        JobState.PARTIAL: "ส่งได้บางส่วน",
        JobState.FAILED: "ไม่สำเร็จ",
        JobState.EXPIRED: "หมดเวลา",
        JobState.CANCELLED: "ยกเลิกแล้ว",
        JobState.DELIVERY_UNKNOWN: "ไม่ทราบผลการส่ง",
    }.get(state, "ไม่ทราบสถานะ")


def _format_job_status(job) -> str:
    state_text = _state_label(job.state)
    if job.state == JobState.RUNNING:
        stage_text = {
            "resolve": "กำลังตรวจโพสต์ต้นทาง",
            "download": "กำลังดาวน์โหลด",
            "upload": "กำลังอัปโหลด",
            "send": "กำลังส่ง",
        }.get(job.stage, "กำลังทำงาน")
        state_text = stage_text
    elif job.state == JobState.WAITING_RATE_LIMIT or (
            job.state == JobState.QUEUED
            and job.next_eligible_at is not None
            and getattr(job, "rate_limit_kind", None) in {"flood", "premium"}):
        eligible = job.next_eligible_at
        wait_kind = {
            "flood": "FloodWait",
            "premium": "FloodPremiumWait",
        }.get(getattr(job, "rate_limit_kind", None), "Telegram")
        account = {
            "owner": "บัญชีต้นทาง",
            "bot": "บัญชีส่งกลับ",
        }.get(getattr(job, "rate_limit_account", None), "บัญชี Telegram")
        required_seconds = getattr(job, "rate_limit_required_seconds", None)
        wait_text = (f" รอประมาณ {max(1, math.ceil(required_seconds))} วินาที"
                     if isinstance(required_seconds, (int, float)) else "")
        if eligible is not None:
            if eligible.tzinfo is None:
                eligible = eligible.replace(tzinfo=ZoneInfo("UTC"))
            state_text = (f"รอ {wait_kind} จาก{account}{wait_text} — "
                          "จะทำต่ออัตโนมัติ ถึง "
                          f"{eligible.astimezone(ZoneInfo('Asia/Bangkok')).strftime('%H:%M น. %d/%m')}")
        else:
            state_text = f"รอ {wait_kind} จาก{account}{wait_text} — จะทำต่ออัตโนมัติ"

    details = []
    result = job.result or {}
    sent, total = result.get("sent_files"), result.get("total_files")
    if isinstance(sent, int) and isinstance(total, int):
        details.append(f"ยืนยัน {sent}/{total} ไฟล์")
    if result.get("status") == "text":
        details.append("คัดลอกข้อความแล้ว")
    suffix = f" · {' · '.join(details)}" if details else ""
    return f"งาน {job.job_id}: {state_text}{suffix}"
