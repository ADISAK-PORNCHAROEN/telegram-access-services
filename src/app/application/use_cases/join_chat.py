"""Joining a chat is an explicit owner-only operation."""

from app.application.ports import AccessStore, Membership
from app.domain.errors import AccessDenied


class JoinChat:
    def __init__(self, access: AccessStore, membership: Membership):
        self.access, self.membership = access, membership

    async def execute(self, actor: int, kind: str, value: str) -> str:
        if not self.access.is_admin(actor):
            raise AccessDenied()
        return await self.membership.join(kind, value)
