"""Conservative asynchronous HTTP client used by official-source connectors."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

logger = logging.getLogger(__name__)

RETRYABLE_STATUSES = frozenset({408, 429})


def is_retryable_status(status_code: int) -> bool:
    """Retry request throttling/timeouts and every server-side 5xx response."""

    return status_code in RETRYABLE_STATUSES or 500 <= status_code <= 599


def setting(settings: object | None, name: str, default: Any) -> Any:
    """Read a setting from an object or mapping, tolerating upper-case names."""

    if settings is None:
        return default
    if isinstance(settings, Mapping):
        if name in settings:
            return settings[name]
        return settings.get(name.upper(), default)
    if hasattr(settings, name):
        return getattr(settings, name)
    return getattr(settings, name.upper(), default)


class ConnectorHTTPError(RuntimeError):
    """An HTTP response that cannot be returned as connector data."""

    def __init__(self, *, status_code: int, url: str, body: str = "") -> None:
        self.status_code = status_code
        self.url = url
        self.body = body[:1000]
        super().__init__(f"HTTP {status_code} while requesting {url}")


class ConnectorDecodeError(RuntimeError):
    """A successful response that was not valid JSON."""


class HTTPJSONResponse:
    """Small immutable-ish transport DTO, independent from an open response."""

    __slots__ = ("collected_at", "headers", "payload", "request_parameters", "status_code", "url")

    def __init__(
        self,
        *,
        payload: Any,
        url: str,
        status_code: int,
        request_parameters: dict[str, Any],
        headers: Mapping[str, str],
        collected_at: datetime,
    ) -> None:
        self.payload = payload
        self.url = url
        self.status_code = status_code
        self.request_parameters = request_parameters
        self.headers = dict(headers)
        self.collected_at = collected_at


class AsyncHTTPClient:
    """HTTPX wrapper with bounded concurrency and narrowly scoped retries."""

    def __init__(
        self,
        settings: object | None = None,
        *,
        client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_source: Callable[[], float] = random.random,
    ) -> None:
        timeout = float(setting(settings, "http_timeout_seconds", 30.0))
        user_agent = str(setting(settings, "http_user_agent", "LicitaLeadMonitor/0.1"))
        self.max_retries = int(setting(settings, "http_max_retries", 4))
        self._min_request_interval = max(
            float(setting(settings, "http_min_request_interval_seconds", 0.5)), 0.0
        )
        self._semaphore = asyncio.Semaphore(int(setting(settings, "http_max_concurrency", 4)))
        self._pace_lock = asyncio.Lock()
        self._next_request_at = 0.0
        self._sleep = sleep
        self._random = random_source
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            follow_redirects=True,
            max_redirects=3,
            headers={"Accept": "application/json", "User-Agent": user_agent},
        )

    async def request_json(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HTTPJSONResponse:
        """Request JSON, retrying only transport errors and transient statuses."""

        request_params = {key: value for key, value in (params or {}).items() if value is not None}
        for attempt in range(self.max_retries + 1):
            await self._pace()
            try:
                async with self._semaphore:
                    response = await self._client.request(
                        method,
                        url,
                        params=request_params,
                        json=dict(json_body) if json_body is not None else None,
                        headers=dict(headers) if headers else None,
                    )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                if attempt >= self.max_retries:
                    raise exc
                delay = self._backoff_delay(attempt, None)
                logger.warning(
                    "connector_transport_retry",
                    extra={"url": url, "attempt": attempt + 1, "delay_seconds": delay},
                )
                await self._sleep(delay)
                continue

            if is_retryable_status(response.status_code) and attempt < self.max_retries:
                delay = self._backoff_delay(attempt, response.headers.get("Retry-After"))
                logger.warning(
                    "connector_http_retry",
                    extra={
                        "url": str(response.request.url),
                        "status_code": response.status_code,
                        "attempt": attempt + 1,
                        "delay_seconds": delay,
                    },
                )
                await self._sleep(delay)
                continue

            if response.is_error:
                raise ConnectorHTTPError(
                    status_code=response.status_code,
                    url=str(response.request.url),
                    body=response.text,
                )

            try:
                payload = response.json()
            except ValueError as exc:
                raise ConnectorDecodeError(
                    f"Invalid JSON returned by {response.request.url}"
                ) from exc

            return HTTPJSONResponse(
                payload=payload,
                url=str(response.request.url),
                status_code=response.status_code,
                request_parameters=request_params,
                headers=response.headers,
                collected_at=datetime.now(UTC),
            )

        raise AssertionError("unreachable retry state")

    async def _pace(self) -> None:
        """Space request starts by a configurable minimum interval."""

        if self._min_request_interval <= 0:
            return
        async with self._pace_lock:
            now = time.monotonic()
            start = max(now, self._next_request_at)
            self._next_request_at = start + self._min_request_interval
        delay = start - now
        if delay > 0:
            await self._sleep(delay)

    def _backoff_delay(self, attempt: int, retry_after: str | None) -> float:
        if retry_after:
            parsed = self._parse_retry_after(retry_after)
            if parsed is not None:
                return min(max(parsed, 0.0), 120.0)
        return min((0.5 * (2**attempt)) + (self._random() * 0.25), 30.0)

    @staticmethod
    def _parse_retry_after(value: str) -> float | None:
        try:
            return float(value)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
            except (TypeError, ValueError):
                return None
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            return (retry_at - datetime.now(UTC)).total_seconds()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> AsyncHTTPClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()
