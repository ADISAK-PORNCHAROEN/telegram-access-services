"""User-facing notices for jobs processed after durable admission."""

from app.domain.models import CopyResult, CopyStatus


class TelegramJobNotifier:
    def __init__(self, bot):
        self.bot = bot

    async def terminal(self, job, result: CopyResult) -> None:
        suffix = f" · งาน {job.job_id}"
        if result.status == CopyStatus.OK:
            text = f"✅ ส่งกลับครบ {result.sent_files} ไฟล์แล้ว{suffix}"
        elif result.status == CopyStatus.TEXT:
            text = f"✅ คัดลอกข้อความแล้ว{suffix}"
        elif result.status == CopyStatus.PARTIAL:
            text = f"⚠️ ส่งกลับได้ {result.sent_files}/{result.total_files} ไฟล์ แต่ไม่ครบ{suffix}"
        elif result.status == CopyStatus.DELIVERY_UNKNOWN:
            confirmed = (f" ยืนยันแล้ว {result.sent_files}/{result.total_files} ไฟล์"
                         if result.total_files is not None else "")
            text = (f"⚠️ ไม่ทราบผลการส่ง{confirmed} กรุณาตรวจสอบไฟล์ในแชท "
                    f"ระบบจะไม่ส่งซ้ำอัตโนมัติ{suffix}")
        elif result.status == CopyStatus.EXPIRED:
            text = (f"⌛ งานหมดอายุหลังครบ 30 นาที ยังไม่ได้ส่งไฟล์ต่อแล้ว "
                    f"ส่งลิงก์มาใหม่ได้{suffix}")
        else:
            reason = result.reason or result.status.value
            if reason == "definitive-send-rejection-before-finalize":
                text = f"❌ Telegram ปฏิเสธการส่ง จึงไม่ได้ส่งไฟล์กลับ{suffix}"
            else:
                text = f"❌ งานไม่สำเร็จ ({reason}){suffix}"
        await self.bot.send_message(job.destination_chat, text)
