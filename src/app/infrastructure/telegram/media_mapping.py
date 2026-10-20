"""Translate Telegram metadata into framework-free values."""

import mimetypes
from pathlib import Path

from app.domain.models import MediaItem
from app.domain.policies import exceeds_send_limit

def file_size(msg) -> int | None:
    """ขนาดไฟล์ของ media ในข้อความ (byte) — ไม่รู้ขนาดคืน None."""
    size = getattr(getattr(msg, "file", None), "size", None)
    return int(size) if size else None

def media_kind(msg) -> str | None:
    """คืนชนิด media ของข้อความ: photo / video / file หรือ None ถ้าไม่ใช่ media ที่จำแนกได้."""
    if getattr(msg, "photo", None):
        return "photo"
    if getattr(msg, "video", None) or getattr(msg, "gif", None) or getattr(msg, "video_note", None):
        return "video"
    if (getattr(msg, "document", None) or getattr(msg, "audio", None)
            or getattr(msg, "voice", None) or getattr(msg, "sticker", None)):
        return "file"
    return None

def _media_suffix(keep) -> str:
    """ส่วนขยายปลายทางแบบเดียวกับที่ download_media ทำเอง — กุญแจตัดสิน "รูป-วีดิโอ-ness" ตอนส่งกลับ.

    ลำดับชนิดใช้: ชื่อไฟล์เดิม (.pdf/.mp4) → mime ต้นทางของ document เดาส่วนขยาย → รูป = .jpg
    ถ้าส่งไฟล์โยนชื่อไม่มีส่วนขยายขึ้น Telegram มันจะลงไปเป็น "ไฟล์เฉยๆ" ไม่ใช่วีดิโอ/รูป
    (บักจากใช้งานจริง: คลิปต้นทางไม่มี DocumentAttributeFilename แถมขึ้นมาเป็น "ไฟล์")
    """
    name = getattr(getattr(keep, "file", None), "name", None)
    if name and Path(name).suffix:
        return Path(name).suffix
    mime = getattr(getattr(keep, "document", None), "mime_type", None)
    if mime:
        ext = mimetypes.guess_extension(mime)
        if ext:
            return ext
    if media_kind(keep) == "photo":
        return ".jpg"  # รูปของ Telegram เข้ารหัสเป็น JPEG เสมอ
    if media_kind(keep) == "video":
        return ".mp4"  # คลิปไร้ชื่อ/uploads ไม่บอก mime — .mp4 แพร่หมายที่สุดใน telegram
    return ""

def too_big(message) -> bool:
    return exceeds_send_limit(file_size(message))

def to_media_item(message) -> MediaItem:
    return MediaItem(message.id, message.raw_text or "", media_kind(message), file_size(message), _media_suffix(message))
