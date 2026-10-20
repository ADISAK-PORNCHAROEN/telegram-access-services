"""Store a user's media preference after checking access."""

from app.application.ports import AccessStore, Preferences
from app.domain.errors import AccessDenied
from app.domain.models import MediaKind


class SetFilter:
    def __init__(self, access: AccessStore, preferences: Preferences):
        self.access, self.preferences = access, preferences

    def execute(self, actor: int, kind: MediaKind | None) -> None:
        if not self.access.is_allowed(actor):
            raise AccessDenied()
        self.preferences.set(actor, kind)
