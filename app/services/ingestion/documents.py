"""Safe download and persistence adapters for official source documents."""

from __future__ import annotations

import asyncio
import ipaddress
import json
import os
import socket
import tempfile
import threading
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePath
from urllib.parse import unquote, urljoin, urlparse

import httpx

from app.connectors.bounded_http import pin_public_transport
from app.connectors.http import AsyncHTTPClient, is_retryable_status
from app.connectors.progress import ActivityCallback, ProgressEvent
from app.services.documents.pncp_urls import PNCPDocumentResolution


class UnsafeDocumentURL(ValueError):
    """Raised when a URL can reach an untrusted or local network destination."""


class DocumentDownloadError(RuntimeError):
    """Raised for bounded download failures that should remain auditable."""

    def __init__(
        self,
        message: str,
        *,
        pncp_storage_failure: bool = False,
        status_code: int | None = None,
        retry_after: str | None = None,
    ) -> None:
        super().__init__(message)
        self.pncp_storage_failure = pncp_storage_failure
        self.status_code, self.retry_after = status_code, retry_after


Resolver = Callable[[str, int], Awaitable[list[str]]]
NetworkSlot = Callable[[str], AbstractAsyncContextManager[int]]
_STORE_LOCKS = tuple(threading.Lock() for _ in range(64))


async def _system_resolver(host: str, port: int) -> list[str]:
    def resolve() -> list[str]:
        answers = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        return list(dict.fromkeys(str(answer[4][0]) for answer in answers))

    return await asyncio.to_thread(resolve)


def _public_ip(value: str) -> bool:
    address = ipaddress.ip_address(value.split("%", 1)[0])
    return not any(
        (
            address.is_private,
            address.is_loopback,
            address.is_link_local,
            address.is_multicast,
            address.is_reserved,
            address.is_unspecified,
        )
    )


@dataclass(frozen=True, slots=True)
class DownloadedDocument:
    url: str
    content: bytes
    filename: str
    declared_mime: str | None


class SecureDocumentDownloader:
    """Download only allow-listed public hosts with strict redirect and size bounds."""

    def __init__(
        self,
        *,
        allowed_hosts: set[str],
        max_bytes: int = 50 * 1024 * 1024,
        max_redirects: int = 3,
        timeout: float = 30.0,
        user_agent: str = "LicitaLeadMonitor/0.1",
        client: httpx.AsyncClient | None = None,
        resolver: Resolver | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.allowed_hosts = {host.lower().rstrip(".") for host in allowed_hosts if host}
        self.max_bytes = max_bytes
        self.max_redirects = max_redirects
        self._resolver = resolver or _system_resolver
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            headers={"User-Agent": user_agent, "Accept": "*/*"},
            follow_redirects=False,
            trust_env=False,
        )
        self._owns_client = client is None
        self._sleep = sleep

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def _host_allowed(self, host: str) -> bool:
        normalized = host.lower().rstrip(".")
        return any(
            normalized == allowed or normalized.endswith(f".{allowed}")
            for allowed in self.allowed_hosts
        )

    async def validate_url(self, url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise UnsafeDocumentURL("only HTTP(S) document URLs are accepted")
        if parsed.username or parsed.password:
            raise UnsafeDocumentURL("credentials are forbidden in document URLs")
        if not parsed.hostname or not self._host_allowed(parsed.hostname):
            raise UnsafeDocumentURL("document host is not an allow-listed official host")
        try:
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
        except ValueError as exc:
            raise UnsafeDocumentURL("invalid port in document URL") from exc
        if port not in {80, 443}:
            raise UnsafeDocumentURL("non-standard document ports are forbidden")
        try:
            addresses = [str(ipaddress.ip_address(parsed.hostname))]
        except ValueError:
            addresses = await self._resolver(parsed.hostname, port)
        if not addresses or any(not _public_ip(address) for address in addresses):
            raise UnsafeDocumentURL("document host resolves to a non-public address")

    async def download(
        self,
        url: str,
        *,
        on_activity: ActivityCallback | None = None,
        checkpoint: Callable[[], Awaitable[None]] | None = None,
        http_budget: AsyncHTTPClient | None = None,
        network_slot: NetworkSlot | None = None,
        max_retries: int = 0,
        resolution: PNCPDocumentResolution | None = None,
    ) -> DownloadedDocument:
        delays = (2.0, 5.0)
        attempt = storage_attempt = 0
        while True:
            if checkpoint is not None:
                await checkpoint()
            try:
                return await self._download_once(
                    url,
                    checkpoint=checkpoint,
                    http_budget=http_budget,
                    network_slot=network_slot,
                    resolution=resolution,
                )
            except DocumentDownloadError as exc:
                if exc.status_code is not None and is_retryable_status(exc.status_code):
                    if http_budget is None or attempt >= max_retries:
                        raise
                    delay = http_budget._backoff_delay(attempt, exc.retry_after)
                    attempt += 1
                    await http_budget._retry_activity(attempt - 1, delay, exc.status_code)
                    await self._sleep(delay)
                    continue
                if not exc.pncp_storage_failure:
                    raise
                if storage_attempt == 2:
                    raise DocumentDownloadError(
                        "PNCP: falha temporária ao carregar o arquivo (HTTP 422; 3 tentativas)",
                        pncp_storage_failure=True,
                    ) from exc
                delay = delays[storage_attempt]
                storage_attempt += 1
                if on_activity is not None:
                    await on_activity(
                        ProgressEvent(
                            message=f"PNCP não conseguiu carregar o arquivo; tentativa {storage_attempt + 1} de 3",
                            attempt=storage_attempt + 1,
                            max_attempts=3,
                            retry_at=(datetime.now(UTC) + timedelta(seconds=delay)).isoformat(),
                        )
                    )
                await self._sleep(delay)

    async def _download_once(
        self,
        url: str,
        *,
        checkpoint: Callable[[], Awaitable[None]] | None = None,
        http_budget: AsyncHTTPClient | None = None,
        network_slot: NetworkSlot | None = None,
        resolution: PNCPDocumentResolution | None = None,
    ) -> DownloadedDocument:
        current = url
        visited: set[str] = set()
        for redirect_count in range(self.max_redirects + 1):
            if checkpoint is not None:
                await checkpoint()
            if resolution is not None:
                try:
                    current = resolution.resolve(current)
                except ValueError as exc:
                    raise UnsafeDocumentURL(str(exc)) from exc
                if urlparse(current).scheme != "https":
                    raise UnsafeDocumentURL("resolved PNCP documents require HTTPS at every hop")
            await self.validate_url(current)
            if current in visited:
                raise DocumentDownloadError("document redirect loop")
            visited.add(current)
            pin_public_transport(self._client, httpx.URL(current))
            slot = (
                network_slot(current)
                if network_slot is not None
                else http_budget.request_slot(current)
                if http_budget is not None
                else nullcontext()
            )
            async with slot as generation, self._client.stream("GET", current) as response:
                if response.status_code in {301, 302, 303, 307, 308}:
                    if redirect_count >= self.max_redirects:
                        raise DocumentDownloadError("document redirect limit exceeded")
                    location = response.headers.get("location")
                    if not location:
                        raise DocumentDownloadError("document redirect has no Location header")
                    current = urljoin(current, location)
                    continue
                if response.status_code >= 400:
                    if response.status_code == 429 and http_budget is not None:
                        delay = http_budget._backoff_delay(0, response.headers.get("Retry-After"))
                        await http_budget._throttle(current, delay, request_generation=generation)
                    host = (urlparse(current).hostname or "").lower()
                    if response.status_code == 422 and (
                        host == "pncp.gov.br" or host.endswith(".pncp.gov.br")
                    ):
                        body = bytearray()
                        async for chunk in response.aiter_bytes(chunk_size=4096):
                            body.extend(chunk[: 16384 - len(body)])
                            if len(body) >= 16384:
                                break
                        try:
                            payload = json.loads(body)
                        except (ValueError, UnicodeError):
                            payload = None
                        message = payload.get("message") if isinstance(payload, dict) else None
                        if (
                            isinstance(message, str)
                            and "erro ao tentar carregar arquivo" in message.casefold()
                        ):
                            raise DocumentDownloadError(
                                "PNCP: falha temporária ao carregar o arquivo (HTTP 422)",
                                pncp_storage_failure=True,
                            )
                    raise DocumentDownloadError(
                        f"document source returned HTTP {response.status_code}",
                        status_code=response.status_code,
                        retry_after=response.headers.get("Retry-After"),
                    )
                declared_size = response.headers.get("content-length")
                if declared_size and int(declared_size) > self.max_bytes:
                    raise DocumentDownloadError("document exceeds configured size limit")
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > self.max_bytes:
                        raise DocumentDownloadError("document exceeds configured size limit")
                    chunks.append(chunk)
                content_type = response.headers.get("content-type")
                mime = content_type.split(";", 1)[0].strip().lower() if content_type else None
                return DownloadedDocument(
                    url=str(response.url),
                    content=b"".join(chunks),
                    filename=self._filename(response, current),
                    declared_mime=mime,
                )
        raise DocumentDownloadError("document redirect limit exceeded")

    @staticmethod
    def _filename(response: httpx.Response, fallback_url: str) -> str:
        disposition = response.headers.get("content-disposition", "")
        marker = "filename="
        filename: str | None = None
        if marker in disposition.lower():
            position = disposition.lower().find(marker) + len(marker)
            filename = disposition[position:].split(";", 1)[0].strip().strip('"')
        if not filename:
            filename = unquote(PurePath(urlparse(fallback_url).path).name)
        filename = PurePath(filename or "document.bin").name
        return filename or "document.bin"


def store_by_hash(content: bytes, digest: str, root: Path, filename: str) -> Path:
    """Atomically store immutable bytes under a hash-derived, traversal-safe path."""

    # Concurrent identical downloads must publish only once. Windows refuses
    # replacement while another worker reads the already published file.
    key = (str(root.resolve()).casefold(), digest)
    with _STORE_LOCKS[hash(key) % len(_STORE_LOCKS)]:
        return _store_by_hash(content, digest, root, filename)


def _store_by_hash(content: bytes, digest: str, root: Path, filename: str) -> Path:

    extension = PurePath(filename).suffix.lower()
    if len(extension) > 12 or not extension.replace(".", "").isalnum():
        extension = ""
    directory = root.resolve() / digest[:2] / digest[2:4]
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f"{digest}{extension}"
    if destination.exists():
        return destination
    handle, temporary_name = tempfile.mkstemp(prefix=".download-", dir=directory)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, destination)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)
    return destination
