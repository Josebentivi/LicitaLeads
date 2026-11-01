"""Small HTTP security guards for the local operator application."""

from __future__ import annotations

from urllib.parse import urlsplit

from starlette.datastructures import Headers, MutableHeaders
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_SAFE_FETCH_SITES = frozenset({"same-origin", "none"})
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def _normalized_origin(value: str) -> tuple[str, str, int] | None:
    """Return (scheme, host, port), folding loopback aliases together."""

    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or not parsed.hostname:
        return None
    hostname = parsed.hostname.lower().rstrip(".")
    if hostname in _LOOPBACK_HOSTS:
        hostname = "loopback"
    try:
        port = parsed.port
    except ValueError:
        return None
    return scheme, hostname, port or (443 if scheme == "https" else 80)


def _normalized_host(value: str, *, scheme: str) -> tuple[str, str, int] | None:
    try:
        parsed = urlsplit(f"//{value}")
    except ValueError:
        return None
    if not parsed.hostname:
        return None
    hostname = parsed.hostname.lower().rstrip(".")
    if hostname in _LOOPBACK_HOSTS:
        hostname = "loopback"
    try:
        port = parsed.port
    except ValueError:
        return None
    return scheme, hostname, port or (443 if scheme == "https" else 80)


def _origin_matches_host(origin: str, host: str, *, scheme: str | None = None) -> bool:
    origin_key = _normalized_origin(origin)
    if origin_key is None:
        return False
    if scheme is not None and origin_key[0] != scheme:
        return False
    host_key = _normalized_host(host, scheme=origin_key[0])
    return host_key is not None and origin_key == host_key


def _is_loopback_host(value: str) -> bool:
    try:
        parsed = urlsplit(f"//{value}")
    except ValueError:
        return False
    return (parsed.hostname or "").lower().rstrip(".") in _LOOPBACK_HOSTS


class FormOriginMiddleware:
    """Reject cross-site browser form submissions to the HTML application."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        method = str(scope.get("method", "GET")).upper()
        if method in _UNSAFE_METHODS:
            headers = Headers(scope=scope)
            fetch_site = headers.get("sec-fetch-site")
            origin = headers.get("origin")
            host = headers.get("host")
            if origin:
                if origin.strip().lower() == "null":
                    if fetch_site != "same-origin" or not host or not _is_loopback_host(host):
                        await PlainTextResponse(
                            "Cross-origin form submission blocked", status_code=403
                        )(scope, receive, send)
                        return
                elif not host or not _origin_matches_host(origin, host, scheme=scope.get("scheme")):
                    await PlainTextResponse(
                        "Cross-origin form submission blocked", status_code=403
                    )(scope, receive, send)
                    return
            elif fetch_site and fetch_site not in _SAFE_FETCH_SITES:
                await PlainTextResponse("Cross-site form submission blocked", status_code=403)(
                    scope, receive, send
                )
                return
        await self.app(scope, receive, send)


class SecurityHeadersMiddleware:
    """Apply conservative browser hardening headers to every response."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers.setdefault("X-Content-Type-Options", "nosniff")
                headers.setdefault("X-Frame-Options", "DENY")
                headers.setdefault("Referrer-Policy", "no-referrer")
                headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
                headers.setdefault(
                    "Content-Security-Policy",
                    "default-src 'self'; "
                    "script-src 'self' 'unsafe-inline'; "
                    "style-src 'self' 'unsafe-inline'; "
                    "img-src 'self' data:; "
                    "connect-src 'self'; "
                    "frame-ancestors 'none'; base-uri 'self'; form-action 'self'",
                )
            await send(message)

        await self.app(scope, receive, send_with_headers)
