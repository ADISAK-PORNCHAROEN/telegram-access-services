"""แปลงลิงก์โพสต์ t.me เป็น (chat, message_id) — ไฟล์เดียวที่แปลงลิงก์ (ตาม AGENTS.md).

รูปแบบที่รองรับ:
  - กลุ่มปิด:     https://t.me/c/<raw id>/<message id>  →  chat = -100<raw id>
  - กลุ่มสาธารณะ: https://t.me/<username>/<message id>  →  chat = username
"""

import re
from app.domain.models import ParsedLink


_ANY_TME = re.compile(r"(?:https?://)?(?:t\.me|telegram\.me)/\S+", re.IGNORECASE)
_INVITE = re.compile(r"(?:t\.me|telegram\.me)/(?:\+|joinchat/)", re.IGNORECASE)
_TOPIC = re.compile(r"(?:t\.me|telegram\.me)/c/\d{5,}/\d+/\d+", re.IGNORECASE)
_PRIVATE = re.compile(r"(?:t\.me|telegram\.me)/c/(\d{5,})/(\d+)(?![\d/])", re.IGNORECASE)
_PUBLIC = re.compile(r"(?:t\.me|telegram\.me)/([A-Za-z0-9_]{4,32})/(\d+)(?![\d/])", re.IGNORECASE)
_JOIN_INVITE = re.compile(r"(?:t\.me|telegram\.me)/(?:\+|joinchat/)([A-Za-z0-9_-]{4,})", re.IGNORECASE)
_JOIN_PUBLIC = re.compile(r"(?:t\.me|telegram\.me)/([A-Za-z0-9_]{4,32})(?:/\d+)?(?![\w/])", re.IGNORECASE)


def _convert(url: str) -> ParsedLink:
    # ตัด query (?single) / fragment ออก แล้วเทียบ regex กับ path เท่านั้น
    path = url.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    if _INVITE.search(path):
        raise ValueError(
            "นี่คือลิงก์เชิญเข้ากลุ่ม ไม่ใช่ลิงก์โพสต์ — ใช้ลิงก์ของโพสต์เอง (เปิดโพสต์ที่เป็นข้อความ แล้วก็ Copy Post Link) — "
            "ส่วนการ auto-join ด้วยลิงก์เชิญจะมาใน Phase 2"
        )
    if _TOPIC.search(path):
        raise ValueError("ลิงก์โพสต์ในหัวข้อ (topic) ยังไม่รองรับใน v1 — ใช้ลิงก์โพสต์ปกติก่อนนะ")
    match = _PRIVATE.search(path)
    if match:
        # raw id ชุดเลขหลัง /c/ → entity id จริงคือ "100" + raw แล้วทำเป็นลบ (มาตรฐานของ Telegram)
        return ParsedLink(
            chat=int("-100" + match.group(1)),
            message_id=int(match.group(2)),
            is_private=True,
        )
    match = _PUBLIC.search(path)
    if match:
        return ParsedLink(chat=match.group(1), message_id=int(match.group(2)), is_private=False)
    raise ValueError(
        f"ลิงก์รูปแบบไม่รองรับ: {url} — ต้องเป็นลิงก์โพสต์ เช่น "
        "https://t.me/c/2521xxxx/123 (กลุ่มปิด) หรือ https://t.me/ชื่อช่อง/123 (ช่องสาธารณะ)"
    )


def parse_links(text: str) -> list[ParsedLink]:
    """ดึงลิงก์โพสต์ทุกลิงก์ในข้อความเรียงตามลำดับ; ไม่มี/ผิดรูปแบบ → ValueError พร้อมเหตุผลภาษาไทย."""
    if text is None or not text.strip():
        raise ValueError("ไม่พบลิงก์โพสต์ — วางลิงก์จากกลุ่มมาได้เลย เช่น https://t.me/c/2521xxxx/123")
    candidates = [match.group(0) for match in _ANY_TME.finditer(text)]
    if not candidates:
        raise ValueError("ไม่พบลิงก์ t.me ในข้อความ — คัดลอกลิงก์โพสต์จากกลุ่มแล้ววางที่นี่")
    return [_convert(url) for url in candidates]


def parse_invite_link(text: str) -> tuple[str, str]:
    """สำหรับคำสั่ง /join: ลิงก์เชิญ → ("invite", hash) · ลิงก์กลุ่มสาธารณะ → ("public", username).

    ไม่รู้จักรูปแบบ → ValueError พร้อมเหตุผลภาษาไทย (parse_links ปฏิเสธลิงก์เชิญไว้เพื่องานนี้โดยเฉพาะ)
    """
    if text is None or not text.strip():
        raise ValueError("ไม่พบลิงก์ — ใช้แบบนี้: /join <ลิงก์เชิญ เช่น t.me/+xxxx> หรือ /join <ลิงก์กลุ่ม>")
    match = _JOIN_INVITE.search(text)
    if match:
        return "invite", match.group(1)
    match = _JOIN_PUBLIC.search(text)
    if match:
        return "public", match.group(1)
    raise ValueError(
        "รูปแบบลิงก์ไม่รองรับ — ใช้ลิงก์เชิญ t.me/+xxxx หรือ t.me/joinchat/xxxx "
        "หรือลิงก์กลุ่มสาธารณะ t.me/ชื่อกลุ่ม"
    )
