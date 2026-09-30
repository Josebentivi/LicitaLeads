from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

from app.connectors.http import AsyncHTTPClient, ConnectorHTTPError


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
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"ok": True}, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    client = AsyncHTTPClient(
        SimpleNamespace(
            http_max_retries=0,
            http_max_concurrency=1,
            http_min_request_interval_seconds=0.5,
        ),
        client=raw_client,
        sleep=fake_sleep,
    )
    await client.request_json("GET", "https://official.example/one")
    await client.request_json("GET", "https://official.example/two")

    assert calls == 2
    assert sleeps == [pytest.approx(0.5, abs=0.05)]
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_http_client_paces_retries_after_throttling() -> None:
    calls = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, request=request)
        return httpx.Response(200, json={"ok": True}, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    client = AsyncHTTPClient(
        SimpleNamespace(
            http_max_retries=1,
            http_max_concurrency=1,
            http_min_request_interval_seconds=0.5,
        ),
        client=raw_client,
        sleep=fake_sleep,
        random_source=lambda: 0,
    )
    response = await client.request_json("GET", "https://official.example/data")

    assert response.payload == {"ok": True}
    assert calls == 2
    assert sleeps == [
        pytest.approx(0.5, abs=0.05),
        pytest.approx(0.5, abs=0.05),
    ]
    await raw_client.aclose()


async def _no_sleep() -> None:
    return None
