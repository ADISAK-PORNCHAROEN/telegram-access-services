"""Owner-authorized membership changes using the existing user client."""

from telethon.errors import InviteHashExpiredError, InviteRequestSentError, UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

from app.infrastructure.telegram.errors import telegram_errors


class TelegramMembership:
    def __init__(self, client):
        self.client = client

    async def join(self, kind: str, value: str) -> str:
        with telegram_errors():
            try:
                await self.client(ImportChatInviteRequest(value) if kind == "invite" else JoinChannelRequest(value))
            except UserAlreadyParticipantError:
                return "already"
            except InviteHashExpiredError:
                return "expired"
            except InviteRequestSentError:
                return "requested"
        return "joined"
