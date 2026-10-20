"""กรองชนิดไฟล์ต่อบุคคล (/only) — ไฟล์เดียวที่จัดการค่ากำหนดผู้ใช้ (prefs.json).

- ค่าเริ่มต้น: ไม่กรอง (ทุกชนิด) — ตั้งด้วย /only รูป|วีดิโอ|ไฟล์, ปิดด้วย /only off
- กรองเป็นของ "แต่ละคน" ต่างกันได้ และเก็บถาวรข้ามการรีสตาร์ทบอท
"""

import json
from pathlib import Path


class PrefsStore:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._kind_by_user: dict[int, str] = {}
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise ValueError(
                    f"prefs.json อ่านไม่ได้ ({exc}) — แก้รูปแบบ JSON หรือลบไฟล์นี้แล้วรันใหม่ "
                    "(กรองทั้งหมดจะเริ่มจากไม่กรอง)"
                ) from None
            self._kind_by_user = {int(uid): str(kind) for uid, kind in data.get("kind_by_user", {}).items()}

    def get(self, uid: int) -> str | None:
        """คืนชนิดที่กรองไว้ (photo/video/file) — None = ไม่กรอง."""
        return self._kind_by_user.get(uid)

    def set(self, uid: int, kind: str | None) -> None:
        """ตั้งกรองให้ 1 คน — kind None = ปิดกรอง (ลบออกจากไฟล์)."""
        if kind is None:
            self._kind_by_user.pop(uid, None)
        else:
            self._kind_by_user[uid] = kind
        self._save()

    def _save(self) -> None:
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps({"kind_by_user": {str(k): v for k, v in sorted(self._kind_by_user.items())}},
                       ensure_ascii=False),
            encoding="utf-8",
        )
        tmp.replace(self._path)
