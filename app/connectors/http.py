"""Conservative asynchronous HTTP client used by official-source connectors."""

from __future__ import annotations

import asyncio
import logging
import random
import threading
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager, nullcontext
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from app.connectors.bounded_http import SourceDownloadError, bounded_request
from app.connectors.progress import ActivityCallback, ProgressEvent

logger = logging.getLogger(__name__)

RETRYABLE_STATUSES = frozenset({408, 429})
_MAX_JSON_BYTES = 20 * 1024 * 1024
_RECOVERY_WINDOW_SECONDS = 15.0
_CHECKPOINT_POLL_SECONDS = 5.0
_OBSERVATION_LOCK = threading.Lock()
_last_observation: datetime | None = None


def observation_time() -> datetime:
    """Order request starts even on Windows clocks with coarse UTC resolution."""
    global _last_observation
    with _OBSERVATION_LOCK:
        now = datetime.now(UTC)
        if _last_observation is not None and now <= _last_observation:
            now = _last_observation + timedelta(microseconds=1)
        _last_observation = now
        return now


@dataclass(eq=False)
class _Admission:
    since: float
    timings: dict[str, float]
    phase: str = "rate"
    sequence: int = 0


@dataclass
class _RecoveryWindow:
    successes: int = 0
    started_at: float | None = None

    def observe(self, now: float) -> None:
        self.successes += 1
        if self.started_at is None:
            self.started_at = now

    def healthy(self, now: float) -> bool:
        return (
            self.successes >= 2
            and self.started_at is not None
            and now - self.started_at >= _RECOVERY_WINDOW_SECONDS
        )


@dataclass
class _OriginPacing:
    interval: float
    next_request_at: float = 0.0
    cooldown_until: float = 0.0
    generation: int = 0
    last_request_at: float | None = None
    recovery: _RecoveryWindow | None = None
    recovery_floor: float = 0.0
    probe_from_interval: float | None = None
    waiters: deque[_Admission] = field(default_factory=deque)
    changed: asyncio.Event = field(default_factory=asyncio.Event)

    def wake(self) -> None:
        self.changed.set()
        self.changed = asyncio.Event()

    def invalidate_slots(self) -> None:
        self.generation += 1
        self.wake()


def is_retryable_status(status_code: int) -> bool:
    """Retry request throttling/timeouts and every server-side 5xx response."""

    return status_code in RETRYABLE_STATUSES or 500 <= status_code <= 599


def response_validators(headers: Mapping[str, str]) -> dict[str, str]:
    normalized = {key.lower(): value for key, value in headers.items()}
    policy = normalized.get("cache-control", "").lower()
    if "no-store" in policy or "private" in policy or "*" in normalized.get("vary", ""):
        return {}
    return {key: normalized[key] for key in ("etag", "last-modified") if normalized.get(key)}


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

    def __init__(
        self,
        *,
        status_code: int,
        url: str,
        body: str = "",
        content_type: str = "",
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.url = url
        self.body = body[:1000]
        self.content_type = content_type
        self.headers = dict(headers or {})
        super().__init__(f"HTTP {status_code} while requesting {url}")


class ConnectorContractError(RuntimeError):
    """A successful JSON response whose envelope does not match the contract."""

    def __init__(self, message: str, *, url: str | None = None) -> None:
        self.url = url
        super().__init__(message)


class ConnectorDecodeError(RuntimeError):
    """A successful response that was not valid JSON."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        url: str | None = None,
        content_type: str = "",
        body: str = "",
    ) -> None:
        self.status_code = status_code
        self.url = url
        self.content_type = content_type
        self.body = body[:1000]
        super().__init__(message)


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
        timeout_seconds: float | None = None,
        max_retries: int | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_source: Callable[[], float] = random.random,
        clock: Callable[[], float] = time.monotonic,
        checkpoint: Callable[[], Awaitable[None]] | None = None,
        public_only: bool = False,
        allow_http: bool = False,
        on_activity: ActivityCallback | None = None,
    ) -> None:
        timeout = (
            float(timeout_seconds)
            if timeout_seconds is not None
            else float(setting(settings, "http_timeout_seconds", 30.0))
        )
        user_agent = str(setting(settings, "http_user_agent", "LicitaLeadMonitor/0.1"))
        self.max_retries = (
            int(max_retries)
            if max_retries is not None
            else int(setting(settings, "http_max_retries", 4))
        )
        self._min_request_interval = max(
            float(setting(settings, "http_min_request_interval_seconds", 0.5)), 0.0
        )
        self.concurrency = int(setting(settings, "http_max_concurrency", 4))
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self.active = 0
        self.peak = 0
        self._pace_lock = asyncio.Lock()
        self._origin_pacing: dict[str, _OriginPacing] = {}
        self._scope_pacing: dict[tuple[str, str], _OriginPacing] = {}
        self._waiting: set[_Admission] = set()
        self._admission_sequence = 0
        self._sleep = sleep
        self._random = random_source
        self._clock = clock
        self.checkpoint = checkpoint
        self.on_activity = on_activity
        self.public_only, self.allow_http = public_only, allow_http
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            trust_env=not public_only,
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
        rate_scope: str | None = None,
        bypass_replay: bool = False,
    ) -> HTTPJSONResponse:
        """Request JSON, retrying only transport errors and transient statuses.

        rate_scope names a fixed adapter operation shared by all its records;
        it narrows adaptive pacing, never origin-wide Retry-After or capacity.
        """

        request_params = {key: value for key, value in (params or {}).items() if value is not None}
        timing_page = request_params.get("pagina", request_params.get("page"))
        for attempt in range(self.max_retries + 1):
            await self._notify(
                ProgressEvent(attempt=attempt + 1, max_attempts=self.max_retries + 1)
            )
            try:
                admission_started = self._clock()
                timings: dict[str, float] = {}
                async with self.request_slot(
                    url, timings=timings, rate_scope=rate_scope
                ) as rate_generation:
                    network_started = self._clock()
                    observed_at = observation_time()
                    response = await bounded_request(
                        self._client,
                        method,
                        url,
                        max_bytes=_MAX_JSON_BYTES,
                        public_only=self.public_only,
                        allow_http=self.allow_http,
                        params=request_params,
                        json_body=json_body,
                        headers=dict(headers) if headers else None,
                    )
                    network_finished = self._clock()
                    if response.status_code == 429:
                        delay = self._backoff_delay(attempt, response.headers.get("Retry-After"))
                        await self._throttle(
                            url, delay, request_generation=rate_generation, rate_scope=rate_scope
                        )
            except SourceDownloadError as exc:
                raise ConnectorDecodeError(str(exc), url=url) from exc
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                if attempt >= self.max_retries:
                    raise exc
                delay = self._backoff_delay(attempt, None)
                await self._retry_activity(attempt, delay)
                logger.warning(
                    "connector_transport_retry",
                    extra={
                        "context": {
                            "url": url,
                            "attempt": attempt + 1,
                            "delay_seconds": delay,
                        }
                    },
                )
                await self._retry_sleep(delay)
                continue

            logger.info(
                "connector_http_timing",
                extra={
                    "context": {
                        "host": httpx.URL(url).host,
                        "page": int(str(timing_page)) if str(timing_page).isdigit() else None,
                        "attempt": attempt + 1,
                        "status_code": response.status_code,
                        "rate_scope": rate_scope,
                        "admission_seconds": round(
                            max(0.0, network_started - admission_started), 6
                        ),
                        "network_seconds": round(max(0.0, network_finished - network_started), 6),
                        **{
                            f"{phase}_wait_seconds": round(timings.get(phase, 0.0), 6)
                            for phase in ("rate", "capacity", "checkpoint")
                        },
                    }
                },
            )
            if is_retryable_status(response.status_code) and attempt < self.max_retries:
                if response.status_code != 429:
                    delay = self._backoff_delay(attempt, response.headers.get("Retry-After"))
                await self._retry_activity(attempt, delay, response.status_code)
                logger.warning(
                    "connector_http_retry",
                    extra={
                        "context": {
                            "url": str(response.request.url),
                            "status_code": response.status_code,
                            "attempt": attempt + 1,
                            "delay_seconds": delay,
                        }
                    },
                )
                # Retry-After is already part of the shared origin deadline.
                # Rejoin the FIFO instead of hiding a retry in a second sleep.
                if response.status_code != 429:
                    await self._retry_sleep(delay)
                continue

            if response.is_error:
                raise ConnectorHTTPError(
                    status_code=response.status_code,
                    url=str(response.request.url),
                    body=response.text,
                    content_type=response.headers.get("content-type", ""),
                    headers=response.headers,
                )

            if len(response.content) > _MAX_JSON_BYTES:
                raise ConnectorDecodeError(
                    f"JSON response larger than {_MAX_JSON_BYTES} bytes",
                    status_code=response.status_code,
                    url=str(response.request.url),
                    content_type=response.headers.get("content-type", ""),
                )

            if response.status_code == 304:
                await self._rate_success(url, rate_generation, rate_scope=rate_scope)
                return HTTPJSONResponse(
                    payload=None,
                    url=str(response.request.url),
                    status_code=304,
                    request_parameters=request_params,
                    headers=response.headers,
                    collected_at=observed_at,
                )
            if response.status_code in {204, 205}:
                # The PNCP returns 204 for a query with no matching records.
                # This is a valid empty result, not a malformed JSON payload.
                payload: Any = []
            else:
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise ConnectorDecodeError(
                        f"Invalid JSON returned by {response.request.url}",
                        status_code=response.status_code,
                        url=str(response.request.url),
                        content_type=response.headers.get("content-type", ""),
                        body=response.text,
                    ) from exc

            await self._rate_success(url, rate_generation, rate_scope=rate_scope)

            page = request_params.get("pagina", request_params.get("page"))
            total_pages = payload.get("totalPaginas") if isinstance(payload, dict) else None
            await self._notify(
                ProgressEvent(
                    message="Resposta recebida da fonte oficial",
                    page=int(str(page)) if str(page).isdigit() else None,
                    total_pages=int(str(total_pages)) if str(total_pages).isdigit() else None,
                )
            )
            return HTTPJSONResponse(
                payload=payload,
                url=str(response.request.url),
                status_code=response.status_code,
                request_parameters=request_params,
                headers=response.headers,
                collected_at=observed_at,
            )

        raise AssertionError("unreachable retry state")

    @asynccontextmanager
    async def request_slot(
        self,
        url: str,
        *,
        extra_semaphore: asyncio.Semaphore | None = None,
        timings: dict[str, float] | None = None,
        rate_scope: str | None = None,
    ) -> AsyncIterator[int]:
        """Admit the FIFO head at dispatch time; waiting never books future slots."""
        admission = _Admission(self._clock(), timings if timings is not None else {})
        async with self._pace_lock:
            self._admission_sequence += 1
            admission.sequence = self._admission_sequence
            origin = self._origin(url)
            state = self._pacing_state(url, rate_scope)
            origin_state = self._origin_pacing[origin]
            state.waiters.append(admission)
            self._waiting.add(admission)
        try:
            await self._network_activity()
            while True:
                generation = await self._pace(state, admission, origin_state, origin)
                await self._wait_phase(admission, "capacity")
                async with extra_semaphore if extra_semaphore is not None else nullcontext():
                    await self._semaphore.acquire()
                    try:
                        if self.checkpoint is not None:
                            await self._wait_phase(admission, "checkpoint")
                            await self.checkpoint()
                        async with self._pace_lock:
                            now = self._clock()
                            valid = (
                                generation == state.generation
                                and now + 1e-9
                                >= max(
                                    state.next_request_at,
                                    state.cooldown_until,
                                    origin_state.next_request_at,
                                    origin_state.cooldown_until,
                                )
                                and self._oldest_eligible(origin, now) is admission
                            )
                            if valid:
                                state.last_request_at = now
                                state.next_request_at = now + state.interval
                                origin_state.last_request_at = now
                                origin_state.next_request_at = now + origin_state.interval
                                state.waiters.popleft()
                                self._wake_origin(origin)
                        if not valid:
                            continue
                        self._finish_wait(admission)
                        self.active += 1
                        self.peak = max(self.peak, self.active)
                        try:
                            await self._network_activity()
                            yield generation
                        finally:
                            self.active -= 1
                            await self._network_activity()
                        return
                    finally:
                        self._semaphore.release()
        finally:
            async with self._pace_lock:
                if admission in state.waiters:
                    state.waiters.remove(admission)
                    self._wake_origin(origin)
            self._finish_wait(admission)
            await self._network_activity()

    async def _wait_phase(self, admission: _Admission, phase: str) -> None:
        if admission.phase != phase:
            self._time_phase(admission)
            admission.phase = phase
            await self._network_activity()

    def _time_phase(self, admission: _Admission) -> None:
        now = self._clock()
        admission.timings[admission.phase] = admission.timings.get(admission.phase, 0.0) + max(
            0.0, now - admission.since
        )
        admission.since = now

    def _finish_wait(self, admission: _Admission) -> None:
        if admission in self._waiting:
            self._time_phase(admission)
            self._waiting.remove(admission)

    async def _network_activity(self) -> None:
        await self._notify(
            ProgressEvent(
                http_active=self.active,
                http_limit=self.concurrency,
                http_peak=self.peak,
                http_waiting=len(self._waiting),
                http_waiting_rate=sum(item.phase == "rate" for item in self._waiting),
                http_waiting_capacity=sum(item.phase == "capacity" for item in self._waiting),
                http_waiting_checkpoint=sum(item.phase == "checkpoint" for item in self._waiting),
                http_waiting_retry=sum(item.phase == "retry" for item in self._waiting),
                http_rate_interval_seconds=max(
                    (
                        state.interval
                        for state in (*self._origin_pacing.values(), *self._scope_pacing.values())
                    ),
                    default=0.0,
                ),
                http_cooldown_seconds=max(
                    (
                        max(0.0, state.cooldown_until - self._clock())
                        for state in self._origin_pacing.values()
                    ),
                    default=0.0,
                ),
            )
        )

    async def _notify(self, event: ProgressEvent) -> None:
        if self.on_activity is not None:
            # All transport telemetry uses the pipeline's coalesced resource
            # path, including retry/page signals before or after admission.
            # A SQLite progress flush must not gate a network attempt.
            await self.on_activity(
                replace(event, http_active=self.active, http_limit=self.concurrency)
                if event.http_active is None
                else event
            )

    async def _retry_sleep(self, delay: float) -> None:
        admission = _Admission(self._clock(), {}, phase="retry")
        self._waiting.add(admission)
        try:
            await self._network_activity()
            await self._sleep(delay)
        finally:
            self._finish_wait(admission)
            await self._network_activity()

    async def _retry_activity(self, attempt: int, delay: float, status: int | None = None) -> None:
        reason = f"HTTP {status}" if status is not None else "falha de conexão"
        await self._notify(
            ProgressEvent(
                message=f"Nova tentativa {attempt + 2} de {self.max_retries + 1} após {reason}",
                attempt=attempt + 2,
                max_attempts=self.max_retries + 1,
                retry_at=(datetime.now(UTC) + timedelta(seconds=delay)).isoformat(),
            )
        )

    def _origin(self, url: str) -> str:
        address = httpx.URL(url)
        return str(address.copy_with(path="/", query=None, fragment=None))

    def _pacing_state(self, url: str, rate_scope: str | None) -> _OriginPacing:
        origin = self._origin(url)
        state = self._origin_pacing.setdefault(origin, _OriginPacing(self._min_request_interval))
        if rate_scope is None:
            return state
        return self._scope_pacing.setdefault(
            (origin, rate_scope), _OriginPacing(self._min_request_interval)
        )

    def _wake_origin(self, origin: str) -> None:
        self._origin_pacing[origin].wake()
        for (scope_origin, _), state in self._scope_pacing.items():
            if scope_origin == origin:
                state.wake()

    def _oldest_eligible(self, origin: str, now: float) -> _Admission | None:
        states = [self._origin_pacing[origin]] + [
            state
            for (scope_origin, _), state in self._scope_pacing.items()
            if scope_origin == origin
        ]
        return min(
            (
                state.waiters[0]
                for state in states
                if state.waiters and now + 1e-9 >= max(state.next_request_at, state.cooldown_until)
            ),
            key=lambda admission: admission.sequence,
            default=None,
        )

    async def _pace(
        self,
        state: _OriginPacing,
        admission: _Admission,
        origin_state: _OriginPacing,
        origin: str,
    ) -> int:
        """Only the queue head waits on a deadline; all others wait for their turn."""
        while True:
            await self._wait_phase(admission, "rate")
            async with self._pace_lock:
                now = self._clock()
                head = state.waiters[0] is admission
                delay = max(
                    0.0,
                    max(
                        state.next_request_at,
                        state.cooldown_until,
                        origin_state.next_request_at,
                        origin_state.cooldown_until,
                    )
                    - now,
                )
                eligible = self._oldest_eligible(origin, now) is admission
                if head and delay <= 1e-9 and eligible:
                    return state.generation
                changed = state.changed
            if not head or delay <= 1e-9:
                await changed.wait()
                continue
            # A long Retry-After must still observe cooperative cancellation and
            # fencing. Only the head polls, outside the network semaphore.
            poll = self.checkpoint is not None and delay > _CHECKPOINT_POLL_SECONDS
            sleeper = asyncio.ensure_future(
                self._sleep(min(delay, _CHECKPOINT_POLL_SECONDS) if poll else delay)
            )
            revised = asyncio.create_task(changed.wait())
            try:
                done, _ = await asyncio.wait(
                    {sleeper, revised}, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    task.result()
                if poll and sleeper in done and revised not in done:
                    await self._wait_phase(admission, "checkpoint")
                    assert self.checkpoint is not None
                    await self.checkpoint()
            finally:
                sleeper.cancel()
                revised.cancel()
                await asyncio.gather(sleeper, revised, return_exceptions=True)

    async def _throttle(
        self,
        url: str,
        delay: float,
        *,
        request_generation: int | None = None,
        rate_scope: str | None = None,
    ) -> None:
        async with self._pace_lock:
            state = self._pacing_state(url, rate_scope)
            origin = self._origin(url)
            origin_state = self._origin_pacing[origin]
            now = self._clock()
            # Late responses from before a reduction cannot escalate it again.
            current_policy = request_generation is None or request_generation == state.generation
            if current_policy and now >= state.cooldown_until:
                # If a recovery probe is rejected, remember the preceding
                # healthy rate for this client's lifetime. Unrelated successful
                # operations must not send it back to the same rejected rate.
                if state.probe_from_interval is not None:
                    state.recovery_floor = max(state.recovery_floor, state.probe_from_interval)
                state.probe_from_interval = None
                state.interval = min(30.0, max(state.interval * 2, 0.5))
                logger.info(
                    "connector_rate_reduced",
                    extra={
                        "context": {
                            "host": httpx.URL(url).host,
                            "interval_seconds": state.interval,
                            "rate_scope": rate_scope,
                            "recovery_floor_seconds": state.recovery_floor,
                            "cooldown_seconds": max(delay, 0.5),
                        }
                    },
                )
            state.cooldown_until = max(state.cooldown_until, now + max(delay, 0.5))
            # Retry-After always pauses the entire origin. The longer-lived
            # adaptive interval belongs to the operation that actually failed.
            origin_state.cooldown_until = max(origin_state.cooldown_until, state.cooldown_until)
            state.next_request_at = max(
                state.cooldown_until,
                (state.last_request_at + state.interval)
                if state.last_request_at is not None
                else now,
            )
            state.invalidate_slots()
            self._wake_origin(origin)
            state.recovery = _RecoveryWindow()
        await self._network_activity()

    async def _rate_success(
        self, url: str, generation: int, *, rate_scope: str | None = None
    ) -> None:
        async with self._pace_lock:
            state = self._pacing_state(url, rate_scope)
            origin_state = self._origin_pacing[self._origin(url)]
            # Responses already in flight before the throttle cannot count as recovery.
            if generation != state.generation or self._clock() < max(
                state.cooldown_until, origin_state.cooldown_until
            ):
                return
            now = self._clock()
            window = state.recovery
            if window is None:
                return
            window.observe(now)
            # Only the failed operation's own current-generation responses
            # demonstrate recovery. Items/files do not prove detail is healthy.
            if window.healthy(now):
                previous_interval = state.interval
                recovered_interval = max(
                    self._min_request_interval, state.recovery_floor, previous_interval / 2
                )
                if self._min_request_interval == 0 and recovered_interval < 0.05:
                    recovered_interval = 0.0
                state.interval = recovered_interval
                if recovered_interval < previous_interval:
                    state.probe_from_interval = previous_interval
                    state.next_request_at = max(
                        self._clock(),
                        state.cooldown_until,
                        (state.last_request_at + recovered_interval)
                        if state.last_request_at is not None
                        else self._clock(),
                    )
                    state.invalidate_slots()
                    self._wake_origin(self._origin(url))
                    logger.info(
                        "connector_rate_recovered",
                        extra={
                            "context": {
                                "host": httpx.URL(url).host,
                                "interval_seconds": state.interval,
                                "rate_scope": rate_scope,
                                "recovery_floor_seconds": state.recovery_floor,
                            }
                        },
                    )
                state.recovery = _RecoveryWindow()

    def _backoff_delay(self, attempt: int, retry_after: str | None) -> float:
        if retry_after:
            parsed = self._parse_retry_after(retry_after)
            if parsed is not None:
                return max(parsed, 0.0)
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
