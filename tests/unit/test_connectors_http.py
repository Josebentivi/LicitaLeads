from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
import pytest

from app.connectors.http import AsyncHTTPClient, ConnectorDecodeError, ConnectorHTTPError


async def _take_slot(client, url):
    async with client.request_slot(url) as generation:
        return generation


async def test_429_cooldown_is_shared_by_origin_and_recovers_gradually():
    now = 0.0
    arrivals = []

    async def sleep(delay):
        nonlocal now
        now += delay
        await asyncio.sleep(0)

    def handler(request):
        arrivals.append((request.url.host, now))
        if len(arrivals) == 1:
            return httpx.Response(429, headers={"Retry-After": "5"})
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw_client:
        client = AsyncHTTPClient(
            {"http_max_retries": 0, "http_min_request_interval_seconds": 0.5},
            client=raw_client,
            sleep=sleep,
            clock=lambda: now,
        )
        with pytest.raises(ConnectorHTTPError):
            await client.request_json("GET", "https://source.test/first")
        await client.request_json("GET", "https://other.test/independent")
        assert arrivals[-1] == ("other.test", 0)
        for index in range(20):
            await client.request_json("GET", f"https://source.test/page/{index}")
    times = [at for host, at in arrivals if host == "source.test"]
    assert times[1:17] == list(range(5, 21))
    assert times[17:] == [20.5, 21, 21.5, 22]


async def test_429_invalidates_a_request_already_waiting_for_its_start_slot():
    now = 0.0
    sleeps = []
    queued, release = asyncio.Event(), asyncio.Event()

    async def sleep(delay):
        nonlocal now
        target = now + delay
        sleeps.append(delay)
        if len(sleeps) == 1:
            queued.set()
            await release.wait()
        now = max(now, target)

    async with httpx.AsyncClient() as raw_client:
        client = AsyncHTTPClient(
            {"http_min_request_interval_seconds": 0.5},
            client=raw_client,
            sleep=sleep,
            clock=lambda: now,
        )
        url = "https://source.test/data"
        await _take_slot(client, url)
        waiting = asyncio.create_task(_take_slot(client, url))
        await asyncio.wait_for(queued.wait(), timeout=2)
        now = 0.1
        await client._throttle(url, 5)
        release.set()
        generation = await asyncio.wait_for(waiting, timeout=2)
        assert generation == 1 and now == pytest.approx(5.1)
        assert sleeps == pytest.approx([0.5, 4.6])


async def test_simultaneous_429_responses_extend_pause_without_repeated_rate_reduction():
    now = 0.0
    async with httpx.AsyncClient() as raw_client:
        client = AsyncHTTPClient(
            {"http_min_request_interval_seconds": 0.5}, client=raw_client, clock=lambda: now
        )
        url = "https://source.test/atas"
        await client._throttle(url, 5)
        now = 0.2
        await client._throttle(url, 8)
        state = client._origin_pacing[client._origin(url)]
        assert state.interval == 1 and state.cooldown_until == 8.2
        now = 8.3
        await client._throttle(url, 1)
        assert state.interval == 2 and state.cooldown_until == 9.3


@pytest.mark.asyncio
async def test_http_client_retries_transient_status_and_honors_retry_after() -> None:
    calls = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(501, headers={"Retry-After": "2"}, request=request)
        return httpx.Response(200, json={"ok": True}, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    client = AsyncHTTPClient(
        SimpleNamespace(
            http_max_retries=2,
            http_max_concurrency=1,
            http_min_request_interval_seconds=0,
        ),
        client=raw_client,
        sleep=fake_sleep,
        random_source=lambda: 0,
    )
    response = await client.request_json("GET", "https://official.example/data")

    assert response.payload == {"ok": True}
    assert calls == 2
    assert sleeps == [2.0]
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_http_client_does_not_retry_non_transient_client_error() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(400, text="invalid filter", request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncHTTPClient(
        {"HTTP_MAX_RETRIES": 4},
        client=raw_client,
        sleep=lambda _delay: _no_sleep(),
    )

    with pytest.raises(ConnectorHTTPError) as error:
        await client.request_json("GET", "https://official.example/data")

    assert error.value.status_code == 400
    assert calls == 1
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_http_client_paces_consecutive_requests() -> None:
    calls = 0
    now = 0.0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"ok": True}, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def fake_sleep(delay: float) -> None:
        nonlocal now
        sleeps.append(delay)
        now += delay

    client = AsyncHTTPClient(
        SimpleNamespace(
            http_max_retries=0,
            http_max_concurrency=1,
            http_min_request_interval_seconds=0.5,
        ),
        client=raw_client,
        sleep=fake_sleep,
        clock=lambda: now,
    )
    await client.request_json("GET", "https://official.example/one")
    await client.request_json("GET", "https://official.example/two")

    assert calls == 2
    assert sleeps == [pytest.approx(0.5, abs=0.05)]
    await raw_client.aclose()


async def test_429_backoff_releases_network_slot_for_another_origin():
    retrying, block = asyncio.Event(), asyncio.Event()

    async def sleep(delay):
        retrying.set()
        await block.wait()

    def handler(request):
        return (
            httpx.Response(429, headers={"Retry-After": "5"})
            if request.url.host == "limited.test"
            else httpx.Response(200, json={"ok": True})
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
        client = AsyncHTTPClient(
            {
                "http_max_concurrency": 1,
                "http_min_request_interval_seconds": 0,
                "http_max_retries": 1,
            },
            client=raw,
            sleep=sleep,
        )
        first = asyncio.create_task(client.request_json("GET", "https://limited.test/records"))
        try:
            await asyncio.wait_for(retrying.wait(), timeout=2)
            result = await asyncio.wait_for(
                client.request_json("GET", "https://available.test/records"), timeout=2
            )
            assert result.payload == {"ok": True} and client.active == 0
        finally:
            first.cancel()
            await asyncio.gather(first, return_exceptions=True)


async def test_observation_time_is_request_start_even_when_response_finishes_later():
    first_entered, release = asyncio.Event(), asyncio.Event()

    async def handler(request):
        if request.url.path == "/earlier":
            first_entered.set()
            await release.wait()
        return httpx.Response(200, json={"value": request.url.path})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
        client = AsyncHTTPClient(
            {"http_max_concurrency": 2, "http_min_request_interval_seconds": 0}, client=raw
        )
        earlier = asyncio.create_task(client.request_json("GET", "https://source.test/earlier"))
        try:
            await first_entered.wait()
            later = await client.request_json("GET", "https://source.test/later")
            release.set()
            result = await earlier
            assert result.collected_at < later.collected_at
        finally:
            earlier.cancel()
            await asyncio.gather(earlier, return_exceptions=True)


@pytest.mark.asyncio
async def test_http_client_paces_retries_after_throttling() -> None:
    calls = 0
    now = 0.0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, request=request)
        return httpx.Response(200, json={"ok": True}, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def fake_sleep(delay: float) -> None:
        nonlocal now
        sleeps.append(delay)
        now += delay

    client = AsyncHTTPClient(
        SimpleNamespace(
            http_max_retries=1,
            http_max_concurrency=1,
            http_min_request_interval_seconds=0.5,
        ),
        client=raw_client,
        sleep=fake_sleep,
        random_source=lambda: 0,
        clock=lambda: now,
    )
    response = await client.request_json("GET", "https://official.example/data")

    assert response.payload == {"ok": True}
    assert calls == 2
    assert sleeps == [pytest.approx(1, abs=0.05)]
    await raw_client.aclose()


async def test_late_429_from_an_old_request_does_not_escalate_the_rate_again():
    now = 0.0
    async with httpx.AsyncClient() as raw_client:
        client = AsyncHTTPClient(
            {"http_min_request_interval_seconds": 0.5}, client=raw_client, clock=lambda: now
        )
        url = "https://source.test/atas"
        original_generation = await _take_slot(client, url)
        await client._throttle(url, 0.5, request_generation=original_generation)
        state = client._origin_pacing[client._origin(url)]
        assert state.interval == 1
        now = 3
        await client._throttle(url, 0.5, request_generation=original_generation)
        assert state.interval == 1 and state.cooldown_until == 3.5
        now = 4
        fresh_generation = await _take_slot(client, url)
        await client._throttle(url, 0.5, request_generation=fresh_generation)
        assert state.interval == 2


async def test_recovery_wakes_fifo_head_without_twenty_future_reservations():
    now = 0.0
    sleeps = []
    cancelled_old = []
    all_queued, rebooked, block = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def activity(event):
        if event.http_waiting == 20:
            all_queued.set()

    async def sleep(delay):
        position = len(sleeps)
        sleeps.append(delay)
        if len(sleeps) == 2:
            rebooked.set()
        try:
            await block.wait()
        except asyncio.CancelledError:
            if position == 0:
                cancelled_old.append(position)
            raise

    async with httpx.AsyncClient() as raw_client:
        client = AsyncHTTPClient(
            {"http_min_request_interval_seconds": 0.5},
            client=raw_client,
            clock=lambda: now,
            sleep=sleep,
            on_activity=activity,
        )
        url = "https://source.test/atas"
        await _take_slot(client, url)
        await client._throttle(url, 0.5)
        state = client._origin_pacing[client._origin(url)]
        state.interval = state.next_request_at = 30
        tasks = [asyncio.create_task(_take_slot(client, url)) for _ in range(20)]
        try:
            await asyncio.wait_for(all_queued.wait(), timeout=2)
            for _ in range(7):
                await client._rate_success(url, state.generation)
                now += 15
                await client._rate_success(url, state.generation)
            await asyncio.wait_for(rebooked.wait(), timeout=2)
            assert state.interval == 0.5
            assert cancelled_old == [0]
            assert sleeps == [30, 0.5]
            assert state.next_request_at == 105.5
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_http_timing_records_admission_and_network_without_request_secrets(caplog):
    now = 0.0

    def handler(request):
        nonlocal now
        now += 2
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw_client:
        client = AsyncHTTPClient(
            {"http_min_request_interval_seconds": 0}, client=raw_client, clock=lambda: now
        )
        with caplog.at_level("INFO", logger="app.connectors.http"):
            await client.request_json(
                "GET",
                "https://source.test/atas",
                params={"pagina": 7},
                headers={"Authorization": "Bearer private-test-token"},
            )
    metrics = [
        record.context for record in caplog.records if record.message == "connector_http_timing"
    ]
    assert len(metrics) == 1
    assert metrics[0]["network_seconds"] == 2
    assert metrics[0]["admission_seconds"] == 0
    assert metrics[0]["host"] == "source.test" and metrics[0]["page"] == 7
    assert "private-test-token" not in str(metrics)


async def _no_sleep() -> None:
    return None


@pytest.mark.asyncio
async def test_http_client_treats_204_no_content_as_empty() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(204, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncHTTPClient(
        {"HTTP_MAX_RETRIES": 3},
        client=raw_client,
        sleep=lambda _delay: _no_sleep(),
    )

    response = await client.request_json("GET", "https://pncp.test/empty")

    assert response.status_code == 204
    assert response.payload == []
    assert calls == 1
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_http_client_reports_html_instead_of_json() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text="<html>maintenance</html>",
            headers={"content-type": "text/html"},
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncHTTPClient(
        {"HTTP_MAX_RETRIES": 0},
        client=raw_client,
        sleep=lambda _delay: _no_sleep(),
    )

    with pytest.raises(ConnectorDecodeError) as error:
        await client.request_json("GET", "https://pncp.test/maintenance")

    assert error.value.status_code == 200
    assert error.value.content_type == "text/html"
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_http_client_records_content_type_on_server_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            text="<html>unavailable</html>",
            headers={"content-type": "text/html"},
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncHTTPClient(
        {"HTTP_MAX_RETRIES": 0},
        client=raw_client,
        sleep=lambda _delay: _no_sleep(),
    )

    with pytest.raises(ConnectorHTTPError) as error:
        await client.request_json("GET", "https://pncp.test/unavailable")

    assert error.value.status_code == 503
    assert error.value.content_type == "text/html"
    await raw_client.aclose()
