"""Continuously refill bounded ARP requests while consuming pages in order."""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Any

import httpx

from app.connectors.continuation import request_key
from app.connectors.discovery_budget import DiscoveryBudget, DiscoveryLimitReached
from app.connectors.http import (
    ConnectorContractError,
    ConnectorDecodeError,
    ConnectorHTTPError,
    HTTPJSONResponse,
)

_HTTP_ERRORS = (
    ConnectorHTTPError,
    ConnectorContractError,
    ConnectorDecodeError,
    httpx.TimeoutException,
    httpx.TransportError,
)


class PageCredits:
    """Bound unconsumed page results across every operation of one source."""

    def __init__(self, capacity: int) -> None:
        self.available = capacity
        self.changed = asyncio.Event()

    def acquire_nowait(self) -> bool:
        if not self.available:
            return False
        self.available -= 1
        return True

    def release(self) -> None:
        self.available += 1
        self.changed.set()
        self.changed = asyncio.Event()


class PageWindow:
    """A rolling queue with bounded look-ahead and durable response callbacks."""

    def __init__(
        self, *, concurrency: int, max_pages: int, budget: DiscoveryBudget | None = None
    ) -> None:
        self.concurrency = max(1, min(concurrency, 20))
        self.max_pages = max_pages
        self.pending: dict[int, HTTPJSONResponse | Exception] = {}
        self._tasks: dict[asyncio.Task[HTTPJSONResponse], int] = {}
        self._producer: asyncio.Task[None] | None = None
        self._ready = asyncio.Event()
        self._wake = asyncio.Event()
        self._next = 0
        self._last = 0
        self._reserved = 0
        self._stopping = False
        self._fatal: BaseException | None = None
        self._transport: Any = None
        self._endpoint = ""
        self._params: dict[str, Any] = {}
        self._operation = ""
        self._credits: PageCredits | None = None
        self._held = 0
        self.budget = budget

    @property
    def budget_exhausted(self) -> bool:
        return bool(self.budget and self.budget.remaining <= 0 and not self.budget.pending)

    @property
    def has_pending(self) -> bool:
        return bool(self.pending or self._tasks or (self._operation and self._can_schedule()))

    def _can_schedule(self) -> bool:
        if self._stopping or self._next > self._last:
            return False
        resume = getattr(self._transport, "collection_resume", None)
        if resume is not None:
            frontier = resume.frontiers.get(self._operation, 0)
            if frontier and self._operation not in resume.checked:
                if self._next > frontier or (self._next == frontier and self._tasks):
                    return False
            cached = (
                request_key(self._endpoint, {**self._params, "pagina": self._next}) in resume.cache
            )
            if cached:
                return True
        return self._reserved < self.max_pages and (
            self.budget is None or self.budget.remaining > 0
        )

    async def _produce(self) -> None:
        try:
            while True:
                while (
                    self._can_schedule()
                    and len(self._tasks) < self.concurrency
                    and len(self.pending) + len(self._tasks) < self.concurrency * 2
                ):
                    if self._credits is not None and not self._credits.acquire_nowait():
                        break
                    self._held += 1
                    number = self._next
                    params = {**self._params, "pagina": number}
                    resume = getattr(self._transport, "collection_resume", None)
                    if resume is None or request_key(self._endpoint, params) not in resume.cache:
                        if self.budget is not None:
                            self.budget.reserve(
                                request_key(self._endpoint, params), int(params["tamanhoPagina"])
                            )
                        self._reserved += 1
                    task = asyncio.create_task(
                        self._transport.request_json("GET", self._endpoint, params=params)
                    )
                    self._tasks[task] = number
                    self._next += 1
                if self._tasks:
                    done, _ = await asyncio.wait(self._tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        number = self._tasks.pop(task)
                        key = request_key(self._endpoint, {**self._params, "pagina": number})
                        try:
                            self.pending[number] = task.result()
                            if self.budget is not None:
                                self.budget.settle(key, task.result().payload)
                        except _HTTP_ERRORS as exc:
                            if self.budget is not None:
                                self.budget.settle(key)
                            self.pending[number] = exc
                            self._stopping = True
                        except BaseException:
                            if self.budget is not None:
                                self.budget.settle(key)
                            raise
                    self._ready.set()
                elif self._can_schedule() or (
                    not self._stopping
                    and self.budget is not None
                    and self.budget.pending
                    and self._next <= self._last
                ):
                    self._wake.clear()
                    waiters = [asyncio.create_task(self._wake.wait())]
                    if self._credits is not None and not self._credits.available:
                        waiters.append(asyncio.create_task(self._credits.changed.wait()))
                    if self.budget is not None and self.budget.pending:
                        waiters.append(asyncio.create_task(self.budget.changed.wait()))
                    try:
                        await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
                    finally:
                        for waiter in waiters:
                            waiter.cancel()
                        await asyncio.gather(*waiters, return_exceptions=True)
                else:
                    return
        except BaseException as exc:
            self._fatal = exc
            raise
        finally:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            if self.budget is not None:
                for number in self._tasks.values():
                    self.budget.settle(
                        request_key(self._endpoint, {**self._params, "pagina": number})
                    )
            self._tasks.clear()
            self._ready.set()

    async def request(
        self, transport: Any, endpoint: str, params: dict[str, Any], *, total_pages: int
    ) -> HTTPJSONResponse:
        """Wait for this numeric page while later requests keep using free slots."""
        page = int(params["pagina"])
        operation = request_key(endpoint, params, operation=True)
        if not self._operation:
            self._operation, self._transport, self._endpoint = operation, transport, endpoint
            self._credits = getattr(transport, "page_credits", None)
            if self._credits is None:
                self._credits = PageCredits(self.concurrency * 2)
                transport.page_credits = self._credits
            self._params = dict(params)
            self._next = page
            resume = getattr(transport, "collection_resume", None)
            self._reserved = resume.new_pages.get(operation, 0) if resume else 0
        elif operation != self._operation:
            raise ConnectorContractError("a page queue cannot change its source filters")
        self._last = max(self._last, total_pages or page, page)
        if self._producer is None or self._producer.done():
            if self._producer is not None:
                self._producer.result()
            self._producer = asyncio.create_task(self._produce())
        self._wake.set()
        try:
            while page not in self.pending:
                if self._fatal is not None:
                    raise self._fatal
                if self._producer.done():
                    self._producer.result()
                    if self.budget_exhausted:
                        raise DiscoveryLimitReached("ARP header discovery budget reached")
                    raise ConnectorContractError("page queue ended before the requested page")
                self._ready.clear()
                await self._ready.wait()
            result = self.pending.pop(page)
            self._held -= 1
            if self._credits is not None:
                self._credits.release()
            self._wake.set()
            if isinstance(result, Exception):
                raise result
            return result
        except BaseException:
            current = asyncio.current_task()
            if self._fatal is not None or (current is not None and current.cancelling()):
                await self.aclose(cancel=True)
            raise

    async def aclose(self, *, cancel: bool = False) -> None:
        self._stopping = True
        self._wake.set()
        if self._producer is not None:
            if cancel:
                self._producer.cancel()
            results = await asyncio.gather(self._producer, return_exceptions=True)
            if not cancel and isinstance(results[0], BaseException):
                raise results[0]
        if self._credits is not None:
            while self._held:
                self._held -= 1
                self._credits.release()

    async def __aenter__(self) -> PageWindow:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose(cancel=exc_type is not None)

    def unconsumed_responses(self) -> list[HTTPJSONResponse]:
        return [
            result
            for _, result in sorted(self.pending.items())
            if isinstance(result, HTTPJSONResponse)
        ]
