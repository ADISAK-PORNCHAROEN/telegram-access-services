"""Admin authorization belongs to the operation, not just its handler."""

from app.application.ports import AccessStore
from app.domain.errors import AccessDenied


class ManageAccess:
    def __init__(self, store: AccessStore):
        self.store = store

    def execute(self, actor: int, action: str, target: int | None = None):
        if not self.store.is_admin(actor):
            raise AccessDenied()
        if action == "users":
            return self.store.admins(), self.store.ids()
        if target is None:
            raise ValueError("target user id is required")
        if action == "grant":
            return self.store.grant(target)
        if action == "revoke":
            return self.store.revoke(target)
        raise ValueError("unknown access action")
