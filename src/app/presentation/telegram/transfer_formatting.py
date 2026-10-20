"""Audit summaries compatible with existing logs."""

from app.presentation.telegram.formatting import fmt_size

def _transfer_body(stats: dict) -> str:
    """สถิติโอน 1 ลิงก์ → "N ไฟล์ X · โหลด Ys · อัพ Zs" (ขั้นไม่เกิด = ไม่แสดง; ฟ้าว่าง = "")."""
    parts: list[str] = []
    if stats.get("n"):
        parts.append(f"{stats['n']} ไฟล์ {fmt_size(stats['bytes'])}")
    for key, label in (("dl", "โหลด"), ("up", "อัพ")):
        if stats.get(key) is not None:
            parts.append(f"{label} {stats[key]:.1f}s")
    return " · ".join(parts)

def _fmt_transfer(stats: dict) -> str:
    """เพิ่มวงเล็บของสถิติโอนให้พร้อมต่อท้ายบรรทัด log ("..." เมื่อไม่มีข้อมูลก็คือ "")."""
    body = _transfer_body(stats)
    return f" ({body})" if body else ""

def _totals_body(totals: dict) -> str:
    """สถิติรวมทั้งแบตช์ → "N ไฟล์ X · โหลดรวม Ys · อัพรวม Zs" (ค่า 0 หรือไม่มีมีเดีย = "")."""
    parts: list[str] = []
    if totals.get("n"):
        parts.append(f"{totals['n']} ไฟล์ {fmt_size(totals['bytes'])}")
    for key, label in (("dl", "โหลดรวม"), ("up", "อัพรวม")):
        if totals.get(key):
            parts.append(f"{label} {totals[key]:.1f}s")
    return " · ".join(parts)
