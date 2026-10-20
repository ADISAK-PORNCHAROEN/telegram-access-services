"""Temporary HTTP surface for first-run owner authorization."""

import asyncio
import hmac
import ipaddress
import json
import secrets
from pathlib import Path

from aiohttp import web

from app.application.use_cases.setup_owner_session import SetupOwnerSession
from app.domain.owner_setup import OwnerAuthOutcome, OwnerSetupState


STATIC = Path(__file__).with_name("static")
SETUP_TOKEN = web.AppKey("setup_token", object)
SETUP_USE_CASE = web.AppKey("setup_use_case", SetupOwnerSession)
SETUP_COMPLETE = web.AppKey("setup_complete", asyncio.Event)


class OneTimeSetupToken:
    def __init__(self, value: str):
        self._value = value
        self.active = True

    def accepts(self, request: web.Request) -> bool:
        scheme, _, supplied = request.headers.get("Authorization", "").partition(" ")
        return (self.active and scheme.casefold() == "bearer"
                and bool(supplied) and hmac.compare_digest(supplied, self._value))

    def invalidate(self) -> None:
        self.active = False
        self._value = ""


@web.middleware
async def secure_responses(request: web.Request, handler):
    try:
        response = await handler(request)
    except web.HTTPException as exc:
        _add_security_headers(exc)
        raise
    _add_security_headers(response)
    return response


def _add_security_headers(response: web.StreamResponse) -> None:
    response.headers["Cache-Control"] = "no-store"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Content-Security-Policy"] = (
        "default-src 'none'; style-src 'self'; script-src 'self'; "
        "img-src 'self' data:; connect-src 'self'; form-action 'self'; "
        "base-uri 'none'; frame-ancestors 'none'")


def _state_payload(state: OwnerSetupState) -> dict:
    return {
        "phase": state.phase.value,
        "issue": state.issue.value if state.issue else None,
        "retryAfterSeconds": state.retry_after_seconds,
    }


async def _authorized_action(request: web.Request, action):
    token = request.app[SETUP_TOKEN]
    if not token.accepts(request):
        raise web.HTTPUnauthorized(text="Unauthorized")
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return web.json_response({"issue": "invalid_request"}, status=400)
    if not isinstance(payload, dict):
        return web.json_response({"issue": "invalid_request"}, status=400)
    state = await action(request.app[SETUP_USE_CASE], payload)
    if state.phase.value == "complete":
        token.invalidate()
        request.app[SETUP_COMPLETE].set()
    return web.json_response(_state_payload(state))


async def _request_code(setup: SetupOwnerSession, payload: dict) -> OwnerSetupState:
    phone = payload.get("phone")
    if not isinstance(phone, str) or len(phone) > 64:
        return OwnerSetupState(setup.state().phase, issue=OwnerAuthOutcome.INVALID_PHONE)
    return await setup.request_code(phone)


async def _resend_code(setup: SetupOwnerSession, _payload: dict) -> OwnerSetupState:
    return await setup.resend_code()


async def _submit_code(setup: SetupOwnerSession, payload: dict) -> OwnerSetupState:
    code = payload.get("code")
    if not isinstance(code, str) or len(code) > 64:
        return OwnerSetupState(setup.state().phase, issue=OwnerAuthOutcome.INVALID_CODE)
    return await setup.submit_code(code)


async def _submit_password(setup: SetupOwnerSession, payload: dict) -> OwnerSetupState:
    password = payload.get("password")
    if not isinstance(password, str) or len(password) > 1024:
        return OwnerSetupState(setup.state().phase, issue=OwnerAuthOutcome.INVALID_PASSWORD)
    return await setup.submit_password(password)


async def _phone(request: web.Request) -> web.Response:
    return await _authorized_action(request, _request_code)


async def _resend(request: web.Request) -> web.Response:
    return await _authorized_action(request, _resend_code)


async def _code(request: web.Request) -> web.Response:
    return await _authorized_action(request, _submit_code)


async def _password(request: web.Request) -> web.Response:
    return await _authorized_action(request, _submit_password)


async def _index(_request: web.Request) -> web.Response:
    return web.FileResponse(STATIC / "setup.html")


async def _stylesheet(_request: web.Request) -> web.Response:
    return web.FileResponse(STATIC / "setup.css")


async def _script(_request: web.Request) -> web.Response:
    return web.FileResponse(STATIC / "setup.js")


def create_setup_app(setup: SetupOwnerSession, token: OneTimeSetupToken,
                     completed: asyncio.Event) -> web.Application:
    app = web.Application(client_max_size=2048, middlewares=[secure_responses])
    app[SETUP_USE_CASE] = setup
    app[SETUP_TOKEN] = token
    app[SETUP_COMPLETE] = completed
    app.router.add_get("/", _index)
    app.router.add_get("/setup.css", _stylesheet)
    app.router.add_get("/setup.js", _script)
    app.router.add_post("/api/phone", _phone)
    app.router.add_post("/api/resend", _resend)
    app.router.add_post("/api/code", _code)
    app.router.add_post("/api/password", _password)
    return app


class SetupWebServer:
    def __init__(self, setup: SetupOwnerSession, *, host: str, port: int,
                 bind_address: str):
        self._setup = setup
        self._host = host
        self._port = port
        self._bind_address = bind_address

    def _announce_access(self, token_value: str) -> None:
        print(
            "[setup] One-time setup URL: "
            f"http://{self._bind_address}:{self._port}/#{token_value}"
        )
        if ipaddress.ip_address(self._bind_address).is_loopback:
            print(
                "[setup] From your workstation (replace the placeholders), run: "
                f"ssh -L {self._port}:{self._bind_address}:{self._port} "
                "YOUR_USER@YOUR_SERVER"
            )
        else:
            print(
                "[setup] Restrict the host firewall to trusted LAN/VPN clients "
                f"for {self._bind_address}:{self._port}; never expose this HTTP "
                "port to the public internet."
            )

    async def run_until_complete(self) -> None:
        token_value = secrets.token_urlsafe(32)
        token = OneTimeSetupToken(token_value)
        completed = asyncio.Event()
        app = create_setup_app(self._setup, token, completed)
        runner = web.AppRunner(app, access_log=None)
        try:
            await runner.setup()
            site = web.TCPSite(runner, self._host, self._port)
            await site.start()
            self._announce_access(token_value)
            await completed.wait()
        finally:
            token.invalidate()
            await runner.cleanup()
