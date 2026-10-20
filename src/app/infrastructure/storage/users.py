"""รายชื่อผู้ใช้ที่ได้รับอนุญาตให้ใช้บอท (allowlist) — ไฟล์เดียวที่จัดการสิทธิผู้ใช้ (users.json).

- admin = เจ้าของบอท (id จาก ALLOWED_USER_ID ใน .env — seed ตอนเริ่มด้วย ensure_admin)
- user = คนที่ admin สั่ง /grant ให้ (เก็บถาวรใน users.json)
- คนอื่นนอกรายชื่อ = เมินเงียบ (ตาม guardrails)
"""

import json
from pathlib import Path


class UserStore:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._users: set[int] = set()
        self._admins: set[int] = set()
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise ValueError(
                    f"users.json อ่านไม่ได้ ({exc}) — แก้รูปแบบ JSON หรือลบไฟล์นี้แล้วรันใหม่ "
                    "(admin จะถูกสร้างใหม่อัตโนมัติจาก .env)"
                ) from None
            self._users = {int(x) for x in data.get("users", [])}
            self._admins = {int(x) for x in data.get("admins", [])}

    def ensure_admin(self, uid: int) -> None:
        if uid not in self._admins:
            self._admins.add(uid)
            self._save()

    def grant(self, uid: int) -> bool:
        """เปิดสิทธิ 1 คน — คืน False ถ้าอยู่ในรายชื่อ (admin หรือ user) อยู่แล้ว."""
        if uid in self._users or uid in self._admins:
            return False
        self._users.add(uid)
        self._save()
        return True

    def revoke(self, uid: int) -> bool:
        """ปิดสิทธิ 1 คน — คืน False ถ้าคนนั้นไม่ได้อยู่ในรายชื่อ user."""
        if uid not in self._users:
            return False
        self._users.remove(uid)
        self._save()
        return True

    def is_allowed(self, uid: int) -> bool:
        return uid in self._users or uid in self._admins

    def is_admin(self, uid: int) -> bool:
        return uid in self._admins

    def ids(self) -> list[int]:
        return sorted(self._users)

    def admins(self) -> list[int]:
        return sorted(self._admins)

    def _save(self) -> None:
        """บันทึกแบบไม่พังกลางทาง: เขียนไฟล์ชั่วคราวแล้ว swap ทันที."""
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps({"users": self.ids(), "admins": self.admins()}, ensure_ascii=False),
            encoding="utf-8",
        )
        tmp.replace(self._path)
