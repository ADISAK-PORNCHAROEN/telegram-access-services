"""Thai labels, preference arguments and byte formatting."""

KIND_LABELS_TH = {"photo": "รูปภาพ", "video": "วีดิโอ", "file": "ไฟล์"}

_KIND_ARGS: dict[str, str | None] = {
    "photo": "photo", "รูป": "photo", "รูปภาพ": "photo", "ภาพ": "photo",
    "video": "video", "วีดิโอ": "video", "วีดีโอ": "video",
    "file": "file", "ไฟล์": "file", "เอกสาร": "file",
    "off": None, "ปิด": None, "ทุกอย่าง": None,
}

def fmt_size(size: int | float | None) -> str:
    """แปลง byte → ข้อความขนาดอ่านง่าย (GB/MB/KB/B)."""
    if not size:
        return "?"
    for unit, divider in (("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)):
        if size >= divider:
            return f"{size / divider:.1f} {unit}"
    return f"{int(size)} B"

def resolve_kind_arg(arg: str) -> str | None:
    """แปลงคำจากคำสั่ง /only → ชนิด (photo/video/file) หรือ None ถ้า "off" — คำไม่รู้จัก → ValueError ภาษาไทย."""
    kind = _KIND_ARGS.get((arg or "").strip().lower())
    if kind is None and (arg or "").strip().lower() not in {k for k, v in _KIND_ARGS.items() if v is None}:
        raise ValueError(
            "ไม่รู้จักชนิดนี้ — ใช้ได้แค่: /only รูป · /only วีดิโอ · /only ไฟล์ · /only off (ปิดกรอง)"
        )
    return kind
