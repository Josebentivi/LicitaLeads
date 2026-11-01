"""Cross-origin form guard, including loopback aliases."""

from __future__ import annotations

import httpx
import pytest
from starlette.responses import PlainTextResponse

from app.security import FormOriginMiddleware, _origin_matches_host


async def _inner(scope, receive, send) -> None:
    await PlainTextResponse("ok")(scope, receive, send)


@pytest.fixture
def guarded_app():
    return FormOriginMiddleware(_inner)


async def _post(headers: dict[str, str], guarded_app: FormOriginMiddleware) -> httpx.Response:
    transport = httpx.ASGITransport(app=guarded_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8000") as client:
        return await client.post("/", headers=headers)


@pytest.mark.asyncio
async def test_equal_origin_and_host_are_allowed(guarded_app: FormOriginMiddleware) -> None:
    response = await _post(
        {"host": "127.0.0.1:8000", "origin": "http://127.0.0.1:8000"},
        guarded_app,
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_localhost_and_ipv4_loopback_are_equivalent(
    guarded_app: FormOriginMiddleware,
) -> None:
    localhost_origin = await _post(
        {"host": "127.0.0.1:8000", "origin": "http://localhost:8000"},
        guarded_app,
    )
    ip_origin = await _post(
        {"host": "localhost:8000", "origin": "http://127.0.0.1:8000"},
        guarded_app,
    )
    assert localhost_origin.status_code == 200
    assert ip_origin.status_code == 200


@pytest.mark.asyncio
async def test_port_and_external_origins_are_blocked(guarded_app: FormOriginMiddleware) -> None:
    wrong_port = await _post(
        {"host": "127.0.0.1:8000", "origin": "http://127.0.0.1:9000"},
        guarded_app,
    )
    external = await _post(
        {"host": "127.0.0.1:8000", "origin": "https://evil.test"},
        guarded_app,
    )
    assert wrong_port.status_code == 403
    assert external.status_code == 403


@pytest.mark.asyncio
async def test_null_origin_requires_same_origin_loopback(
    guarded_app: FormOriginMiddleware,
) -> None:
    accepted = await _post(
        {
            "host": "127.0.0.1:8000",
            "origin": "null",
            "sec-fetch-site": "same-origin",
        },
        guarded_app,
    )
    cross_site = await _post(
        {
            "host": "127.0.0.1:8000",
            "origin": "null",
            "sec-fetch-site": "cross-site",
        },
        guarded_app,
    )
    non_loopback = await _post(
        {
            "host": "evil.test:8000",
            "origin": "null",
            "sec-fetch-site": "same-origin",
        },
        guarded_app,
    )
    assert accepted.status_code == 200
    assert cross_site.status_code == 403
    assert non_loopback.status_code == 403


@pytest.mark.asyncio
async def test_missing_origin_with_cross_site_is_blocked(
    guarded_app: FormOriginMiddleware,
) -> None:
    response = await _post(
        {"host": "127.0.0.1:8000", "sec-fetch-site": "cross-site"},
        guarded_app,
    )
    assert response.status_code == 403


def test_origin_helper_normalizes_loopback_and_default_ports() -> None:
    assert _origin_matches_host("http://localhost:8000", "127.0.0.1:8000")
    assert _origin_matches_host("http://127.0.0.1:8000", "localhost:8000")
    assert _origin_matches_host("http://localhost", "localhost:80", scheme="http")
    assert not _origin_matches_host("https://localhost:8000", "localhost:8000", scheme="http")
    assert not _origin_matches_host("http://localhost:8001", "localhost:8000", scheme="http")
    assert not _origin_matches_host("null", "localhost:8000", scheme="http")


@pytest.mark.asyncio
async def test_api_mutations_are_guarded(guarded_app: FormOriginMiddleware) -> None:
    transport = httpx.ASGITransport(app=guarded_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8000") as client:
        response = await client.post(
            "/api/crawls/00000000-0000-0000-0000-000000000000/cancel",
            headers={
                "host": "127.0.0.1:8000",
                "origin": "https://evil.test",
                "sec-fetch-site": "cross-site",
            },
        )

    assert response.status_code == 403
