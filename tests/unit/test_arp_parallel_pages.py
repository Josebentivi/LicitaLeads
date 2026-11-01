"""ARP pages overlap on the network while consumption and continuation stay ordered."""

import asyncio
from datetime import UTC, date, datetime

import httpx
import pytest

from app.config import Settings
from app.connectors.base import DataAvailability, ProcurementFilters, RawSourceRecord
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.continuation import ResponseReplay
from app.connectors.http import AsyncHTTPClient, HTTPJSONResponse
from app.connectors.page_window import PageWindow
from app.connectors.pncp import PNCPConnector


@pytest.mark.parametrize("source", ["pncp", "compras_gov"])
@pytest.mark.parametrize("status", [200, 204])
async def test_empty_arp_list_and_http_204_are_successful_empty_results(source, status):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(status, json=[] if status == 200 else None, request=request)

    settings = Settings(_env_file=None, http_min_request_interval_seconds=0, http_max_retries=0)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    cls = PNCPConnector if source == "pncp" else ComprasGovConnector
    connector = cls(settings, http_client=AsyncHTTPClient(settings, client=client))
    try:
        result = await connector.discover_price_registries(
            ProcurementFilters(agency_cnpj="06307102000130", unit_code="2124", days=2)
        )
    finally:
        await connector.aclose()
    assert result.availability is DataAvailability.EMPTY
    assert result.data == [] and result.is_complete
    assert len(requests) == 1
    assert result.raw_records[0].http_status == status
    assert result.raw_records[0].raw_payload == []


def registry_page(page, total, *, source="pncp"):
    row = {
        "numeroControlePNCPAta": f"00000000000191-1-{page:06}/2026",
        "numeroControlePncpAta": f"00000000000191-1-{page:06}/2026",
        "numeroAtaRegistroPreco": str(page),
        "anoAta": 2026,
        "anoCompra": 2026,
        "cnpjOrgao": "00000000000191",
        "objetoContratacao": f"Objeto {page}",
    }
    return {
        "data" if source == "pncp" else "resultado": [row],
        "totalPaginas": total,
        "totalRegistros": total,
        "paginasRestantes": total - page,
    }


@pytest.mark.parametrize("source", ["pncp", "compras_gov"])
@pytest.mark.parametrize("concurrency", [1, 2, 4, 8])
async def test_arp_pages_run_in_bounded_parallel_windows(source, concurrency):
    active, peak = 0, 0
    requests, finished = [], []
    release = asyncio.Event()
    max_pages = max(5, concurrency + 1)
    finish_allowed = {page: asyncio.Event() for page in range(2, max_pages + 1)}
    finish_allowed[max_pages].set()

    async def handler(request):
        nonlocal active, peak
        page = int(request.url.params["pagina"])
        requests.append(page)
        active += 1
        peak = max(peak, active)
        try:
            if page > 1:
                if active == concurrency:
                    release.set()
                await release.wait()
                if concurrency >= 4:
                    await finish_allowed[page].wait()
            finished.append(page)
            if concurrency >= 4 and page > 2:
                finish_allowed[page - 1].set()
            return httpx.Response(200, json=registry_page(page, max_pages + 3, source=source))
        finally:
            active -= 1

    settings = Settings(
        _env_file=None,
        http_max_concurrency=concurrency,
        http_min_request_interval_seconds=0,
        http_max_retries=0,
    )
    connector_type = PNCPConnector if source == "pncp" else ComprasGovConnector
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = connector_type(settings, http_client=AsyncHTTPClient(settings, client=client))
        result = await asyncio.wait_for(
            connector.discover_price_registries(
                ProcurementFilters(
                    start_date=date(2026, 1, 1), end_date=date(2026, 1, 2), max_pages=max_pages
                )
            ),
            timeout=5,
        )
    assert peak == concurrency
    assert requests == list(range(1, max_pages + 1))
    assert finished[0] == 1
    if concurrency >= 4:
        assert finished[1:] == list(range(max_pages, 1, -1))
    assert [row.registry_number for row in result.data] == [
        str(page) for page in range(1, max_pages + 1)
    ]
    assert [raw.request_parameters["pagina"] for raw in result.raw_records] == list(
        range(1, max_pages + 1)
    )
    assert result.pagination.fetched_pages == max_pages and result.pagination.truncated
    assert all(
        row.source_record_ref == raw.record_key
        for row, raw in zip(result.data, result.raw_records, strict=True)
    )


@pytest.mark.parametrize("source", ["pncp", "compras_gov"])
async def test_failed_page_retains_other_received_responses(source):
    async def handler(request):
        page = int(request.url.params["pagina"])
        await asyncio.sleep(0)
        if page == 3:
            return httpx.Response(503)
        return httpx.Response(200, json=registry_page(page, 8, source=source))

    settings = Settings(_env_file=None, http_min_request_interval_seconds=0, http_max_retries=0)
    connector_type = PNCPConnector if source == "pncp" else ComprasGovConnector
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = connector_type(settings, http_client=AsyncHTTPClient(settings, client=client))
        result = await connector.discover_price_registries(ProcurementFilters(max_pages=5))
    assert result.partial and not result.is_complete
    assert [row.registry_number for row in result.data] == ["1", "2"]
    assert [raw.request_parameters["pagina"] for raw in result.raw_records] == [1, 2, 4, 5]


@pytest.mark.parametrize("source", ["pncp", "compras_gov"])
async def test_rolling_queue_refills_slots_while_an_earlier_page_is_waiting(source):
    requests = []
    slow_page = asyncio.Event()
    later_page = asyncio.Event()
    buffer_filled = asyncio.Event()

    async def handler(request):
        page = int(request.url.params["pagina"])
        requests.append(page)
        if page == 3:
            await slow_page.wait()
        if page == 10:
            later_page.set()
        if page == 18:
            buffer_filled.set()
        await asyncio.sleep(0)
        return httpx.Response(200, json=registry_page(page, 60, source=source))

    settings = Settings(_env_file=None, http_max_concurrency=8, http_min_request_interval_seconds=0)
    connector_type = PNCPConnector if source == "pncp" else ComprasGovConnector
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = connector_type(settings, http_client=AsyncHTTPClient(settings, client=client))
        collection = asyncio.create_task(
            connector.discover_price_registries(ProcurementFilters(max_pages=60))
        )
        try:
            await asyncio.wait_for(later_page.wait(), timeout=3)
            assert not collection.done() and not slow_page.is_set()
            await asyncio.wait_for(buffer_filled.wait(), timeout=3)
            for _ in range(10):
                await asyncio.sleep(0)
            # Bound ready results while the earliest missing page cannot be consumed.
            assert max(requests) == 18
            slow_page.set()
            result = await asyncio.wait_for(collection, timeout=5)
        finally:
            collection.cancel()
            await asyncio.gather(collection, return_exceptions=True)
    assert result.is_complete
    assert [row.registry_number for row in result.data] == [str(page) for page in range(1, 61)]


async def test_429_retry_does_not_hold_the_next_pages_after_shared_cooldown():
    now = 0.0
    attempts, arrivals = {}, {}
    retry_release, later_page = asyncio.Event(), asyncio.Event()

    async def sleep(delay):
        nonlocal now
        now += delay
        await asyncio.sleep(0)

    async def handler(request):
        page = int(request.url.params["pagina"])
        attempts[page] = attempts.get(page, 0) + 1
        arrivals.setdefault(page, now)
        await asyncio.sleep(0)
        if page == 3:
            if attempts[page] == 1:
                return httpx.Response(429, headers={"Retry-After": "1"})
            await retry_release.wait()
        if page == 10:
            later_page.set()
        return httpx.Response(200, json=registry_page(page, 18))

    settings = Settings(
        _env_file=None,
        http_max_concurrency=8,
        http_min_request_interval_seconds=0,
        http_max_retries=1,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transport = AsyncHTTPClient(
            settings, client=client, clock=lambda: now, sleep=sleep, random_source=lambda: 0
        )
        collection = asyncio.create_task(
            PNCPConnector(settings, http_client=transport).discover_price_registries(
                ProcurementFilters(max_pages=18)
            )
        )
        try:
            await asyncio.wait_for(later_page.wait(), timeout=3)
            assert not collection.done() and not retry_release.is_set()
            assert arrivals[10] >= 1
            retry_release.set()
            result = await asyncio.wait_for(collection, timeout=5)
        finally:
            collection.cancel()
            await asyncio.gather(collection, return_exceptions=True)
    assert result.is_complete and len(result.data) == 18
    assert attempts[3] == 2


class ReplayTransport:
    def __init__(self, total=7):
        self.total = total
        self.requests = []

    async def request_json(self, _method, url, **kwargs):
        page = kwargs["params"]["pagina"]
        self.requests.append(page)
        await asyncio.sleep(0)
        return HTTPJSONResponse(
            payload=registry_page(page, self.total),
            url=url,
            status_code=200,
            request_parameters=kwargs["params"],
            headers={},
            collected_at=datetime.now(UTC),
        )


async def test_parallel_replay_preserves_budget_and_checks_frontier_before_new_pages():
    previous, requests = [], []

    async def save(raw):
        pass

    async def invalidate():
        raise AssertionError("unchanged totals")

    settings = Settings(_env_file=None)
    for expected in ([1, 2, 3], [3, 4, 5, 6], [6, 7]):
        transport = ReplayTransport()
        replay = ResponseReplay(transport, previous, save, invalidate, source="pncp")
        result = await PNCPConnector(settings, http_client=transport).discover_price_registries(
            ProcurementFilters(max_pages=3)
        )
        assert transport.requests == expected
        requests.extend(transport.requests)
        previous = list(replay.cache.values())
    assert len(result.data) == 7 and result.is_complete
    assert requests == [1, 2, 3, 3, 4, 5, 6, 6, 7]


async def test_changed_frontier_does_not_launch_later_pages():
    endpoint = "https://pncp.gov.br/api/consulta/v1/atas"
    filters = ProcurementFilters(start_date=date(2026, 1, 1), end_date=date(2026, 1, 2))
    previous = [
        RawSourceRecord.build(
            source="pncp",
            endpoint=endpoint,
            request_parameters={
                "dataInicial": "20260101",
                "dataFinal": "20260102",
                "pagina": page,
                "tamanhoPagina": 50,
            },
            raw_payload=registry_page(page, 7),
            http_status=200,
            collected_at=datetime.now(UTC),
        )
        for page in range(1, 4)
    ]
    saved, invalidated = [], []

    async def save(raw):
        saved.append(raw)

    async def invalidate():
        invalidated.append(True)

    transport = ReplayTransport(total=8)
    replay = ResponseReplay(transport, previous, save, invalidate, source="pncp")
    result = await PNCPConnector(
        Settings(_env_file=None), http_client=transport
    ).discover_price_registries(filters)
    assert transport.requests == [3]
    assert replay.invalidated and invalidated
    assert saved[0].request_parameters["pagina"] == 3
    assert result.partial


@pytest.mark.parametrize("failure", ["cancel", "lease"])
async def test_page_window_drains_inflight_tasks_on_cancellation_or_lost_lease(failure):
    started, cancelled = set(), set()
    all_started, block = asyncio.Event(), asyncio.Event()

    class LeaseLost(RuntimeError):
        pass

    class Transport:
        async def request_json(self, _method, _url, **kwargs):
            page = kwargs["params"]["pagina"]
            started.add(page)
            if len(started) == 4:
                all_started.set()
            try:
                await all_started.wait()
                if failure == "lease" and page == 2:
                    raise LeaseLost
                await block.wait()
            except asyncio.CancelledError:
                cancelled.add(page)
                raise

    task = asyncio.create_task(
        PageWindow(concurrency=4, max_pages=4).request(
            Transport(), "https://official.test/atas", {"pagina": 1}, total_pages=4
        )
    )
    await asyncio.wait_for(all_started.wait(), timeout=2)
    if failure == "cancel":
        task.cancel()
    with pytest.raises(asyncio.CancelledError if failure == "cancel" else LeaseLost):
        await asyncio.wait_for(task, timeout=2)
    assert cancelled == ({1, 2, 3, 4} if failure == "cancel" else {1, 3, 4})
