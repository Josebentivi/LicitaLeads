import asyncio
from unittest.mock import AsyncMock

import httpx
import pytest

from app.connectors.http import AsyncHTTPClient
from app.services.ingestion.documents import DocumentDownloadError, SecureDocumentDownloader


async def resolver(_host, _port):
    return ["93.184.216.34"]


async def test_pncp_storage_failure_recovers_with_short_retry_without_leaking_body():
    calls, events = [], []

    async def transport(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(
                422, json={"message": "Erro ao tentar carregar arquivo secret-internal-bucket"}
            )
        return httpx.Response(200, content=b"%PDF-1.7 content")

    sleep, checkpoint = AsyncMock(), AsyncMock()

    async def activity(event):
        events.append(event)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        downloader = SecureDocumentDownloader(
            allowed_hosts={"pncp.gov.br"}, client=client, resolver=resolver, sleep=sleep
        )
        document = await downloader.download(
            "https://pncp.gov.br/arquivo", on_activity=activity, checkpoint=checkpoint
        )
    assert document.content.startswith(b"%PDF") and len(calls) == 2
    sleep.assert_awaited_once_with(2.0)
    assert checkpoint.await_count >= 2
    assert len(events) == 1 and events[0].attempt == 2 and events[0].retry_at
    assert "secret-internal-bucket" not in repr(events)


async def test_pncp_storage_failure_is_bounded_and_explained_after_three_requests():
    calls = []

    def transport(request):
        calls.append(request)
        return httpx.Response(
            422, json={"message": "Erro ao tentar carregar arquivo storage-secret"}
        )

    sleep = AsyncMock()
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        downloader = SecureDocumentDownloader(
            allowed_hosts={"pncp.gov.br"}, client=client, resolver=resolver, sleep=sleep
        )
        with pytest.raises(DocumentDownloadError, match="3 tentativas") as error:
            await downloader.download("https://pncp.gov.br/arquivo")
    assert len(calls) == 3
    assert [call.args[0] for call in sleep.await_args_list] == [2.0, 5.0]
    assert error.value.pncp_storage_failure and "storage-secret" not in str(error.value)


@pytest.mark.parametrize(
    "host,status,body",
    [
        ("pncp.gov.br", 422, {"message": "Parâmetro inválido"}),
        ("pncp.gov.br", 422, ["Erro ao tentar carregar arquivo"]),
        ("pncp.gov.br", 422, "not-json"),
        ("official.test", 422, {"message": "Erro ao tentar carregar arquivo"}),
        ("pncp.gov.br", 404, {"message": "Erro ao tentar carregar arquivo"}),
        ("pncp.gov.br", 503, {"message": "Erro ao tentar carregar arquivo"}),
    ],
)
async def test_other_failures_are_not_blindly_retried(host, status, body):
    calls = []

    def transport(request):
        calls.append(request)
        return (
            httpx.Response(status, text=body)
            if isinstance(body, str)
            else httpx.Response(status, json=body)
        )

    sleep = AsyncMock()
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        downloader = SecureDocumentDownloader(
            allowed_hosts={host}, client=client, resolver=resolver, sleep=sleep
        )
        with pytest.raises(DocumentDownloadError, match=str(status)):
            await downloader.download(f"https://{host}/arquivo")
    assert len(calls) == 1
    sleep.assert_not_awaited()


async def test_cancellation_during_retry_wait_is_propagated():
    async def cancelled(_delay):
        raise asyncio.CancelledError

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(422, json={"message": "Erro ao tentar carregar arquivo"})
        )
    ) as client:
        downloader = SecureDocumentDownloader(
            allowed_hosts={"pncp.gov.br"}, client=client, resolver=resolver, sleep=cancelled
        )
        with pytest.raises(asyncio.CancelledError):
            await downloader.download("https://pncp.gov.br/arquivo")


async def test_document_retry_releases_shared_download_capacity_for_other_origin():
    retrying, block = asyncio.Event(), asyncio.Event()
    slots = asyncio.Semaphore(1)

    async def sleep(delay):
        retrying.set()
        await block.wait()

    def handler(request):
        return (
            httpx.Response(429, headers={"Retry-After": "5"})
            if request.url.host == "limited.test"
            else httpx.Response(200, content=b"official document")
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        budget = AsyncHTTPClient(
            {"http_max_concurrency": 2, "http_min_request_interval_seconds": 0}, client=client
        )
        downloader = SecureDocumentDownloader(
            allowed_hosts={"limited.test", "available.test"},
            client=client,
            resolver=resolver,
            sleep=sleep,
        )

        def slot(url):
            return budget.request_slot(url, extra_semaphore=slots)

        first = asyncio.create_task(
            downloader.download(
                "https://limited.test/a.txt", http_budget=budget, network_slot=slot, max_retries=1
            )
        )
        try:
            await asyncio.wait_for(retrying.wait(), timeout=2)
            result = await asyncio.wait_for(
                downloader.download(
                    "https://available.test/b.txt",
                    http_budget=budget,
                    network_slot=slot,
                    max_retries=1,
                ),
                timeout=2,
            )
            assert result.content == b"official document" and budget.active == 0
        finally:
            first.cancel()
            await asyncio.gather(first, return_exceptions=True)
