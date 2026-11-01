"""Resume bounded pagination by replaying immutable responses locally.

Connectors still parse the official envelopes and construct their own URLs.
Previously fetched pages are replayed, not downloaded, and do not consume the
new-page budget. This also reconstructs complete detail lists without treating
the last page of a resumed response as a complete independent collection.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from app.connectors.base import RawSourceRecord
from app.connectors.http import ConnectorContractError, HTTPJSONResponse, response_validators


def request_key(url: str, params: dict[str, Any], *, operation: bool = False) -> str:
    parts = urlsplit(url)
    clean = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    parameters = {
        key: value
        for key, value in params.items()
        if value is not None and (not operation or key not in {"pagina", "page"})
    }
    return hashlib.sha256(
        json.dumps([clean, parameters], sort_keys=True, default=str).encode()
    ).hexdigest()


def page_budget_exhausted(
    transport: Any,
    page: int,
    limit: int,
    *,
    url: str | None = None,
    params: dict[str, Any] | None = None,
) -> bool:
    resume = getattr(transport, "collection_resume", None)
    operation = request_key(url, params or {}, operation=True) if url is not None else None
    return (
        resume.exhausted(limit, operation=operation, page=page)
        if resume is not None
        else page >= limit
    )


class ResponseReplay:
    def __init__(
        self,
        transport: Any,
        records: list[RawSourceRecord],
        save: Callable[[RawSourceRecord], Awaitable[None]],
        invalidate: Callable[[], Awaitable[None]],
        *,
        source: str,
        invalidate_partition: Callable[[str], Awaitable[None]] | None = None,
        conditional_lookup: Callable[[str, dict[str, Any]], Awaitable[Any]] | None = None,
        on_revalidated: Callable[[Any, Any, Any], Awaitable[None]] | None = None,
    ) -> None:
        prior = getattr(transport, "collection_resume", None)
        self.original = prior.original if prior is not None else transport.request_json
        self.transport = transport
        self.save = save
        self.invalidate = invalidate
        self.source = source
        self.invalidate_partition = invalidate_partition
        self.conditional_lookup = conditional_lookup
        self.on_revalidated = on_revalidated
        self.cache = {request_key(raw.endpoint, raw.request_parameters): raw for raw in records}
        self.frontiers: dict[str, int] = {}
        for raw in records:
            page = raw.request_parameters.get("pagina", raw.request_parameters.get("page"))
            if page is not None:
                operation = request_key(raw.endpoint, raw.request_parameters, operation=True)
                self.frontiers[operation] = max(self.frontiers.get(operation, 0), int(page))
        self.checked: set[str] = set()
        self.new_pages: dict[str, int] = {}
        self.limited: set[str] = set()
        self._context: ContextVar[tuple[str, int]] = ContextVar("replay_operation", default=("", 0))
        self._inflight: dict[str, asyncio.Task[HTTPJSONResponse]] = {}
        self._waiters: dict[str, int] = {}
        self.invalidated_operations: set[str] = set()
        self.invalidated = False
        transport.collection_resume = self
        transport.request_json = self.request_json

    def exhausted(
        self, limit: int, *, operation: str | None = None, page: int | None = None
    ) -> bool:
        prior_operation, prior_page = self._context.get()
        operation = operation if operation is not None else prior_operation
        page = page if page is not None else prior_page
        stopped = self.new_pages.get(operation, 0) >= limit and page >= self.frontiers.get(
            operation, 0
        )
        if stopped:
            self.limited.add(operation)
        return stopped

    async def request_json(self, method: str, url: str, **kwargs: Any) -> HTTPJSONResponse:
        bypass = bool(kwargs.pop("bypass_replay", False))
        if method.upper() != "GET":
            return await self.original(method, url, **kwargs)
        params = kwargs.get("params") or {}
        key = request_key(url, params)
        flight_key = key + (":verify-current" if bypass else "")
        operation = request_key(url, params, operation=True)
        page = int(params.get("pagina", params.get("page", 0)) or 0)
        self._context.set((operation, page))
        if operation in self.invalidated_operations:
            raise ConnectorContractError(
                "pagination totals changed; collection partition must restart"
            )
        # A request owns its task until all waiters have left. The final waiter
        # cancels and joins it, so no snapshot can arrive after lease release.
        task = self._inflight.get(flight_key)
        if task is None:
            task = asyncio.create_task(
                self._request(method, url, key, operation, page, bypass_replay=bypass, **kwargs)
            )
            self._inflight[flight_key] = task
        self._waiters[flight_key] = self._waiters.get(flight_key, 0) + 1
        try:
            return await asyncio.shield(task)
        finally:
            self._waiters[flight_key] -= 1
            if self._waiters[flight_key] == 0:
                if not task.done():
                    task.cancel()
                self._inflight.pop(flight_key, None)
                self._waiters.pop(flight_key, None)
                await asyncio.gather(task, return_exceptions=True)

    async def _request(
        self,
        method: str,
        url: str,
        key: str,
        operation: str,
        page: int,
        bypass_replay: bool = False,
        **kwargs: Any,
    ) -> HTTPJSONResponse:
        cached = None if bypass_replay else self.cache.get(key)
        frontier = bool(
            page and page == self.frontiers.get(operation) and operation not in self.checked
        )
        if cached is not None and not frontier:
            checkpoint = getattr(self.transport, "checkpoint", None)
            if checkpoint is not None:
                await checkpoint()
            return HTTPJSONResponse(
                payload=cached.raw_payload,
                url=cached.endpoint,
                status_code=cached.http_status,
                request_parameters=cached.request_parameters,
                headers={},
                collected_at=cached.collected_at,
            )
        prior = (
            await self.conditional_lookup(url, kwargs.get("params") or {})
            if self.conditional_lookup
            else None
        )
        if prior is not None:
            _, validators, _ = prior
            kwargs = {**kwargs, "headers": {**(kwargs.get("headers") or {}), **validators}}
        response = await self.original(method, url, **kwargs)
        revalidated = response.status_code == 304
        if revalidated:
            if prior is None or self.on_revalidated is None:
                raise ConnectorContractError("HTTP 304 without a complete saved response")
            previous, _, source_id = prior
            await self.on_revalidated(previous, response, source_id)
            response = HTTPJSONResponse(
                payload=previous.raw_payload,
                url=previous.endpoint,
                status_code=previous.http_status,
                request_parameters=previous.request_parameters,
                headers=response.headers,
                collected_at=previous.collected_at,
            )
        raw = RawSourceRecord.build(
            source=self.source,
            endpoint=response.url,
            request_parameters=response.request_parameters,
            raw_payload=response.payload,
            http_status=response.status_code,
            collected_at=response.collected_at,
            response_headers=response_validators(response.headers),
        )
        if not revalidated:
            await self.save(raw)
        if frontier and cached is not None:
            self.checked.add(operation)
            if isinstance(cached.raw_payload, dict) and isinstance(response.payload, dict):
                for name in ("totalPaginas", "totalPages", "totalRegistros", "totalElements"):
                    old, new = cached.raw_payload.get(name), response.payload.get(name)
                    if old is not None and new is not None and old != new:
                        self.invalidated_operations.add(operation)
                        if self.invalidate_partition is not None:
                            await self.invalidate_partition(operation)
                        else:
                            await self.invalidate()
                        self.cache = {
                            key: raw
                            for key, raw in self.cache.items()
                            if request_key(raw.endpoint, raw.request_parameters, operation=True)
                            != operation
                        }
                        self.invalidated = True
                        raise ConnectorContractError(
                            "pagination totals changed; collection partition must restart"
                        )
        else:
            self.new_pages[operation] = self.new_pages.get(operation, 0) + 1
        self.cache[key] = raw
        return response
