"""Streaming bounds and public-address checks for untrusted source downloads."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import urljoin, urlsplit

import httpcore
import httpx


class SourceDownloadError(ValueError):
    """A response or destination violates the source download policy."""


async def resolve_addresses(host: str, port: int) -> list[str]:
    answers = await asyncio.to_thread(socket.getaddrinfo, host, port, type=socket.SOCK_STREAM)
    return list(dict.fromkeys(str(answer[4][0]) for answer in answers))


async def public_addresses(host: str, port: int) -> list[str]:
    try:
        addresses = [str(ipaddress.ip_address(host))]
    except ValueError:
        try:
            addresses = await asyncio.wait_for(resolve_addresses(host, port), timeout=10)
        except OSError as exc:
            raise SourceDownloadError("source DNS resolution failed") from exc
    if not addresses or any(
        not ipaddress.ip_address(value).is_global or ipaddress.ip_address(value).is_multicast
        for value in addresses
    ):
        raise SourceDownloadError("source URL resolves to a non-public address")
    return addresses


class PublicNetworkBackend(httpcore.AsyncNetworkBackend):
    """Resolve at connect time and connect to a vetted literal IP, keeping TLS SNI."""

    def __init__(self, backend: httpcore.AsyncNetworkBackend) -> None:
        self.backend = backend

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,  # noqa: ASYNC109 - fixed httpcore backend protocol
        local_address: str | None = None,
        socket_options: Iterable[Any] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        try:
            async with asyncio.timeout(timeout):
                addresses = await public_addresses(host, port)
                addresses.sort(key=lambda value: ipaddress.ip_address(value).version)
                last_error: Exception | None = None
                for address in addresses:
                    try:
                        # httpcore retains the original URL and original TLS hostname.
                        return await self.backend.connect_tcp(
                            address,
                            port,
                            timeout=timeout,
                            local_address=local_address,
                            socket_options=socket_options,
                        )
                    except (httpcore.ConnectError, httpcore.ConnectTimeout) as exc:
                        last_error = exc
                if last_error is not None:
                    raise last_error
        except TimeoutError as exc:
            raise httpcore.ConnectTimeout("public source connect timed out") from exc
        raise SourceDownloadError("no public address available for source connection")

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,  # noqa: ASYNC109 - fixed httpcore backend protocol
        socket_options: Iterable[Any] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        raise SourceDownloadError("local sockets are forbidden for public source downloads")

    async def sleep(self, seconds: float) -> None:
        await self.backend.sleep(seconds)


def pin_public_transport(client: httpx.AsyncClient, url: httpx.URL) -> None:
    # HTTPX exposes no per-request DNS resolver. Wrapping its httpcore backend changes
    # only connect_tcp; certificate validation, SNI, Host and pool origin stay intact.
    transport = client._transport_for_url(url)
    if isinstance(transport, httpx.MockTransport):
        return
    pool = getattr(transport, "_pool", None)
    if pool is None or isinstance(pool, (httpcore.AsyncHTTPProxy, httpcore.AsyncSOCKSProxy)):
        raise SourceDownloadError("public source downloads require a direct HTTP transport")
    if not isinstance(pool._network_backend, PublicNetworkBackend):
        pool._network_backend = PublicNetworkBackend(pool._network_backend)


async def validate_public_url(url: str, *, allow_http: bool = False) -> None:
    try:
        parsed = urlsplit(url)
        if parsed.scheme not in ({"https", "http"} if allow_http else {"https"}):
            raise SourceDownloadError("source URL requires HTTPS (HTTP needs explicit opt-in)")
        if not parsed.hostname or parsed.username or parsed.password:
            raise SourceDownloadError("source URL has no host or contains credentials")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        if port < 1:
            raise SourceDownloadError("invalid port in source URL")
        await public_addresses(parsed.hostname, port)
    except (OSError, ValueError) as exc:
        if isinstance(exc, SourceDownloadError):
            raise
        raise SourceDownloadError("source URL could not be validated") from exc


async def bounded_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    max_bytes: int,
    public_only: bool = False,
    allow_http: bool = False,
    max_redirects: int = 3,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    json_body: Mapping[str, Any] | None = None,
) -> httpx.Response:
    """Buffer at most max_bytes, validating every externally supplied redirect."""
    current = url
    current_headers = dict(headers or {})
    strip_credentials = False
    for redirect in range(max_redirects + 1):
        if public_only:
            await validate_public_url(current, allow_http=allow_http)
        request = client.build_request(
            method,
            current,
            params=params,
            headers=current_headers,
            json=dict(json_body) if json_body is not None else None,
        )
        if strip_credentials:
            for key in ("authorization", "proxy-authorization", "cookie", "chave-api-dados"):
                request.headers.pop(key, None)
        if public_only:
            pin_public_transport(client, request.url)
        response = await client.send(
            request,
            stream=True,
            follow_redirects=False,
            **({"auth": None} if strip_credentials else {}),
        )
        try:
            if response.status_code in {301, 302, 303, 307, 308}:
                location = response.headers.get("location")
                if redirect == max_redirects or not location:
                    raise SourceDownloadError("source redirect limit exceeded or missing Location")
                target = urljoin(str(response.url), location)
                if (urlsplit(target).scheme, urlsplit(target).netloc) != (
                    urlsplit(current).scheme,
                    urlsplit(current).netloc,
                ):
                    strip_credentials = True
                    current_headers = {
                        key: value
                        for key, value in current_headers.items()
                        if key.lower() not in {"authorization", "cookie", "chave-api-dados"}
                    }
                if response.status_code == 303 or (
                    response.status_code in {301, 302} and method == "POST"
                ):
                    method, json_body = "GET", None
                current, params = target, None
                continue
            declared = response.headers.get("content-length")
            if declared:
                try:
                    if int(declared) < 0 or int(declared) > max_bytes:
                        raise SourceDownloadError("source response exceeds size limit")
                except ValueError as exc:
                    if isinstance(exc, SourceDownloadError):
                        raise
                    raise SourceDownloadError("invalid source Content-Length") from exc
            content = bytearray()
            async for chunk in response.aiter_bytes():
                if len(content) + len(chunk) > max_bytes:
                    raise SourceDownloadError("source response exceeds size limit")
                content.extend(chunk)
            response_headers = dict(response.headers)
            # aiter_bytes already decompresses; do not decode the reconstructed body twice.
            response_headers.pop("content-encoding", None)
            response_headers["content-length"] = str(len(content))
            return httpx.Response(
                response.status_code,
                headers=response_headers,
                content=bytes(content),
                request=response.request,
                extensions=response.extensions,
            )
        finally:
            await response.aclose()
    raise SourceDownloadError("source redirect limit exceeded")
