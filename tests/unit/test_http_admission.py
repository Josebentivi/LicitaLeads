"""Deterministic HTTP admission: fairness, backpressure, cancellation and recovery."""

import asyncio
from dataclasses import dataclass, field

import httpx
import pytest

from app.connectors.http import AsyncHTTPClient, ConnectorDecodeError, ConnectorHTTPError


@dataclass
class Clock:
    now: float = 0.0
    timers: dict[asyncio.Future, float] = field(default_factory=dict)

    async def sleep(self, delay):
        future = asyncio.get_running_loop().create_future()
        self.timers[future] = self.now + delay
        try:
            await future
        finally:
            self.timers.pop(future, None)

    def advance(self, seconds):
        self.now += seconds
        for future, deadline in list(self.timers.items()):
            if deadline <= self.now and not future.done():
                future.set_result(None)


async def until(condition):
    async def poll():
        while not condition():  # noqa: ASYNC110 - drain a manually advanced virtual clock
            await asyncio.sleep(0)

    await asyncio.wait_for(poll(), timeout=5)


async def slot(client, url="https://source.test/data"):
    async with client.request_slot(url) as generation:
        return generation


async def test_fifo_dispatch_has_one_deadline_and_no_future_reservation_debt():
    clock, arrivals, events = Clock(), [], []

    async def activity(event):
        events.append(event)

    async with httpx.AsyncClient() as raw:
        client = AsyncHTTPClient(
            {"http_max_concurrency": 20, "http_min_request_interval_seconds": 0.5},
            client=raw,
            clock=lambda: clock.now,
            sleep=clock.sleep,
            on_activity=activity,
        )

        async def request(index):
            async with client.request_slot("https://source.test/data"):
                arrivals.append((index, clock.now))

        tasks = [asyncio.create_task(request(index)) for index in range(20)]
        try:
            await until(lambda: len(arrivals) == 1 and len(clock.timers) == 1)
            state = next(iter(client._origin_pacing.values()))
            assert state.next_request_at == 0.5
            assert len(state.waiters) == 19
            assert events[-1].http_waiting == events[-1].http_waiting_rate == 19
            for count in range(2, 21):
                clock.advance(0.5)
                await until(lambda expected=count: len(arrivals) == expected)
                if count < 20:
                    await until(lambda: len(clock.timers) == 1)
                assert state.next_request_at == clock.now + 0.5
            await asyncio.gather(*tasks)
            assert arrivals == [(index, index * 0.5) for index in range(20)]
            assert not client._waiting and not clock.timers
            assert events[-1].http_active == events[-1].http_waiting == 0
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_cancelling_head_removes_it_without_leaving_an_empty_reserved_slot():
    clock = Clock()
    async with httpx.AsyncClient() as raw:
        client = AsyncHTTPClient(
            {"http_min_request_interval_seconds": 0.5},
            client=raw,
            clock=lambda: clock.now,
            sleep=clock.sleep,
        )
        await slot(client)
        tasks = [asyncio.create_task(slot(client)) for _ in range(3)]
        try:
            await until(lambda: len(client._waiting) == 3 and len(clock.timers) == 1)
            tasks[0].cancel()
            await asyncio.gather(tasks[0], return_exceptions=True)
            await until(lambda: len(clock.timers) == 1)
            clock.advance(0.5)
            await until(tasks[1].done)
            assert not tasks[2].done()
            await until(lambda: len(clock.timers) == 1)
            clock.advance(0.5)
            await tasks[2]
            assert not client._waiting and client.active == 0
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_policy_is_revalidated_after_waiting_for_capacity():
    clock, events = Clock(), []

    async def activity(event):
        events.append(event)

    async with httpx.AsyncClient() as raw:
        client = AsyncHTTPClient(
            {"http_max_concurrency": 1, "http_min_request_interval_seconds": 0},
            client=raw,
            clock=lambda: clock.now,
            sleep=clock.sleep,
            on_activity=activity,
        )
        async with client.request_slot("https://other.test/held"):
            waiting = asyncio.create_task(slot(client))
            await until(lambda: any(event.http_waiting_capacity == 1 for event in events))
            await client._throttle("https://source.test/data", 5)
        try:
            await until(lambda: len(clock.timers) == 1)
            assert not waiting.done() and client.active == 0
            # A rate wait releases the shared capacity for another origin.
            await slot(client, "https://available.test/data")
            clock.advance(5)
            await waiting
            assert clock.now == 5 and not client._waiting
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)


async def test_checkpoint_wait_is_visible_and_failure_does_not_leak_capacity():
    entered, release, events = asyncio.Event(), asyncio.Event(), []

    async def checkpoint():
        entered.set()
        await release.wait()
        raise RuntimeError("lease lost")

    async def activity(event):
        events.append(event)

    async with httpx.AsyncClient() as raw:
        client = AsyncHTTPClient(
            {"http_max_concurrency": 1, "http_min_request_interval_seconds": 0},
            client=raw,
            checkpoint=checkpoint,
            on_activity=activity,
        )
        task = asyncio.create_task(slot(client))
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
            assert events[-1].http_waiting_checkpoint == 1
            assert events[-1].http_active == 0
            release.set()
            with pytest.raises(RuntimeError, match="lease lost"):
                await task
            client.checkpoint = None
            await asyncio.wait_for(slot(client), timeout=5)
            assert not client._waiting and not next(iter(client._origin_pacing.values())).waiters
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_long_retry_after_polls_cooperative_cancellation_outside_network_budget():
    clock, checked = Clock(), []

    async def checkpoint():
        checked.append(clock.now)
        raise asyncio.CancelledError

    async with httpx.AsyncClient() as raw:
        client = AsyncHTTPClient(
            {"http_max_concurrency": 1, "http_min_request_interval_seconds": 0},
            client=raw,
            clock=lambda: clock.now,
            sleep=clock.sleep,
        )
        await client._throttle("https://source.test/data", 60)
        client.checkpoint = checkpoint
        task = asyncio.create_task(slot(client))
        try:
            await until(lambda: len(clock.timers) == 1)
            assert client.active == 0
            clock.advance(5)
            with pytest.raises(asyncio.CancelledError):
                await task
            assert checked == [5] and not client._waiting and not clock.timers
            client.checkpoint = None
            await slot(client, "https://other.test/data")
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_slow_recovery_needs_multiple_current_successes_over_a_healthy_window():
    clock = Clock()
    async with httpx.AsyncClient() as raw:
        client = AsyncHTTPClient(client=raw, clock=lambda: clock.now)
        url = "https://source.test/data"
        await client._throttle(url, 0.5)
        state = client._origin_pacing[client._origin(url)]
        state.interval = state.next_request_at = 16
        generation = state.generation
        clock.advance(16)
        await client._rate_success(url, generation)
        assert state.interval == 16
        clock.advance(16)
        await client._rate_success(url, generation)
        assert state.interval == 8
        assert state.next_request_at == 32  # no previously booked future deadlines
        await client._rate_success(url, generation)  # stale in-flight response
        assert state.recovery.successes == 0
        await client._throttle(url, 5, request_generation=state.generation)
        assert state.interval == 16 and state.recovery.started_at is None


async def test_fast_successes_cannot_trigger_early_repeated_acceleration():
    clock = Clock(now=1)
    async with httpx.AsyncClient() as raw:
        client = AsyncHTTPClient(client=raw, clock=lambda: clock.now)
        url = "https://source.test/data"
        await client._throttle(url, 0.5)
        state = client._origin_pacing[client._origin(url)]
        clock.advance(1)
        for _ in range(8):
            await client._rate_success(url, state.generation)
            clock.advance(1)
        assert state.interval == 1
        clock.advance(7)
        await client._rate_success(url, state.generation)
        assert state.interval == 0.5


@pytest.mark.parametrize("limit", [1, 4, 20])
@pytest.mark.parametrize("scoped", [False, True])
async def test_network_capacity_is_shared_and_can_be_filled_without_rate_limit(limit, scoped):
    entered, release = asyncio.Event(), asyncio.Event()
    active = peak = 0

    async def handler(request):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if peak == limit:
            entered.set()
        try:
            await release.wait()
            return httpx.Response(200, json={"ok": True})
        finally:
            active -= 1

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
        client = AsyncHTTPClient(
            {"http_max_concurrency": limit, "http_min_request_interval_seconds": 0},
            client=raw,
        )
        tasks = [
            asyncio.create_task(
                client.request_json(
                    "GET", f"https://source.test/{i}", rate_scope=f"op{i % 2}" if scoped else None
                )
            )
            for i in range(2 * limit)
        ]
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
            assert active == peak == limit
            release.set()
            await asyncio.gather(*tasks)
            assert client.active == 0 and not client._waiting
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("scoped", [False, True])
async def test_throttle_during_checkpoint_revalidates_start_and_separates_wait_timings(scoped):
    clock, entered, release = Clock(), asyncio.Event(), asyncio.Event()
    timings = {}

    async def checkpoint():
        entered.set()
        await release.wait()

    async with httpx.AsyncClient() as raw:
        client = AsyncHTTPClient(
            {"http_min_request_interval_seconds": 0},
            client=raw,
            clock=lambda: clock.now,
            sleep=clock.sleep,
            checkpoint=checkpoint,
        )

        async def request():
            async with client.request_slot(
                "https://source.test/data", timings=timings, rate_scope="items" if scoped else None
            ):
                return clock.now

        task = asyncio.create_task(request())
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
            clock.advance(2)
            await client._throttle(
                "https://source.test/data", 5, rate_scope="detail" if scoped else None
            )
            release.set()
            await until(lambda: len(clock.timers) == 1)
            assert not task.done() and client.active == 0
            clock.advance(5)
            assert await task == 7
            assert timings == {"rate": 5, "capacity": 0, "checkpoint": 2}
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_unrelated_successes_do_not_accelerate_a_throttled_operation():
    clock, detail_calls = Clock(), 0

    def handler(request):
        nonlocal detail_calls
        if request.url.path.startswith("/detail/"):
            detail_calls += 1
            if detail_calls == 1:
                return httpx.Response(429, headers={"Retry-After": "5"})
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
        client = AsyncHTTPClient(
            {"http_max_retries": 0}, client=raw, clock=lambda: clock.now, sleep=clock.sleep
        )
        with pytest.raises(ConnectorHTTPError):
            await client.request_json("GET", "https://source.test/detail/1", rate_scope="detail")
        state = client._scope_pacing[("https://source.test/", "detail")]
        for index in range(30):
            clock.advance(5 if index == 0 else 1)
            await client.request_json("GET", f"https://source.test/items/{index}")
        assert state.interval == 1 and state.recovery.successes == 0
        for index in range(2):
            clock.advance(16)
            await client.request_json(
                "GET", f"https://source.test/detail/{index + 2}", rate_scope="detail"
            )
        assert state.interval == 0.5  # different record identities share the operation's evidence


async def test_failed_recovery_probe_remembers_the_last_healthy_interval():
    clock, url = Clock(), "https://source.test/detail/1"
    async with httpx.AsyncClient() as raw:
        client = AsyncHTTPClient(client=raw, clock=lambda: clock.now)
        await client._throttle(url, 1, rate_scope="detail")
        state = client._scope_pacing[(client._origin(url), "detail")]
        state.interval = 4
        for _ in range(2):
            clock.advance(16)
            await client._rate_success(url, state.generation, rate_scope="detail")
        assert state.interval == 2 and state.probe_from_interval == 4
        await client._throttle(url, 1, request_generation=state.generation, rate_scope="detail")
        assert state.interval == state.recovery_floor == 4
        for _ in range(8):
            clock.advance(16)
            await client._rate_success(url, state.generation, rate_scope="detail")
        assert state.interval == 4  # never repeat the already-rejected 2-second probe
        assert client._origin_pacing[client._origin(url)].interval == 0.5
        await client._throttle(url, 1, request_generation=state.generation, rate_scope="detail")
        assert state.interval == 8  # a worsening source can still slow it further


async def test_scoped_backoff_keeps_global_retry_after_without_blocking_healthy_operations():
    clock, arrivals = Clock(), []
    async with httpx.AsyncClient() as raw:
        client = AsyncHTTPClient(client=raw, clock=lambda: clock.now, sleep=clock.sleep)
        url = "https://source.test/detail/1"
        await client._throttle(url, 5, rate_scope="detail")
        detail = client._scope_pacing[(client._origin(url), "detail")]
        detail.interval = detail.next_request_at = 8

        async def request(scope):
            async with client.request_slot(url, rate_scope=scope):
                arrivals.append((scope, clock.now))

        tasks = [asyncio.create_task(request(scope)) for scope in ["detail", None]]
        try:
            await until(lambda: len(clock.timers) == 2)
            clock.advance(4)
            await asyncio.sleep(0)
            assert not arrivals and client.active == 0
            await slot(client, "https://other.test/data")
            clock.advance(1)
            await until(tasks[1].done)
            assert arrivals == [(None, 5)] and not tasks[0].done()
            await until(lambda: len(clock.timers) == 1)
            clock.advance(3)
            await tasks[0]
            assert arrivals == [(None, 5), ("detail", 8)]
            assert not client._waiting and not clock.timers
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_eligible_operation_heads_are_fair_and_share_the_origin_start_interval():
    clock, arrivals = Clock(), []
    async with httpx.AsyncClient() as raw:
        client = AsyncHTTPClient(client=raw, clock=lambda: clock.now, sleep=clock.sleep)

        async def request(index):
            async with client.request_slot(
                "https://source.test/data", rate_scope=[None, "detail", "items"][index % 3]
            ):
                arrivals.append((index, clock.now))

        tasks = [asyncio.create_task(request(index)) for index in range(9)]
        try:
            await until(lambda: len(arrivals) == 1 and len(clock.timers) == 3)
            for count in range(2, 10):
                clock.advance(0.5)
                await until(lambda expected=count: len(arrivals) == expected)
                if count < 9:
                    await until(lambda: len(clock.timers) == min(3, 9 - len(arrivals)))
            await asyncio.gather(*tasks)
            assert arrivals == [(index, index * 0.5) for index in range(9)]
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_malformed_success_cannot_be_used_as_recovery_evidence():
    clock = Clock()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text="not-json"))
    ) as raw:
        client = AsyncHTTPClient(client=raw, clock=lambda: clock.now, sleep=clock.sleep)
        url = "https://source.test/detail/1"
        await client._throttle(url, 1, rate_scope="detail")
        state = client._scope_pacing[(client._origin(url), "detail")]
        for _ in range(2):
            clock.advance(16)
            with pytest.raises(ConnectorDecodeError):
                await client.request_json("GET", url, rate_scope="detail")
        assert state.interval == 1 and state.recovery.successes == 0


@pytest.mark.parametrize("status", [429, 503])
async def test_retry_wait_remains_visible_and_other_origins_can_continue(status):
    clock, events, arrivals = Clock(), [], []

    async def activity(event):
        if event.http_waiting is not None:
            events.append(event)

    def handler(request):
        if request.url.host == "source.test":
            arrivals.append(clock.now)
            if len(arrivals) == 1:
                return httpx.Response(status, headers={"Retry-After": "30"})
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
        client = AsyncHTTPClient(
            {
                "http_max_concurrency": 1,
                "http_min_request_interval_seconds": 0,
                "http_max_retries": 1,
            },
            client=raw,
            clock=lambda: clock.now,
            sleep=clock.sleep,
            on_activity=activity,
        )
        task = asyncio.create_task(client.request_json("GET", "https://source.test/data"))
        try:
            await until(lambda: len(clock.timers) == 1)
            assert events[-1].http_active == 0 and events[-1].http_waiting == 1
            assert events[-1].http_waiting_rate == int(status == 429)
            assert events[-1].http_waiting_retry == int(status != 429)
            await client.request_json("GET", "https://other.test/data")
            clock.advance(29)
            assert not task.done() and arrivals == [0]
            clock.advance(1)
            assert (await task).payload == {"ok": True}
            assert arrivals == [0, 30]
            assert not client._waiting and not clock.timers
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
