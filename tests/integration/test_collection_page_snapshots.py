"""Exercise the real connector with the durable HTTP snapshot callback."""

import asyncio
from datetime import UTC, datetime

import httpx
import pytest
from sqlalchemy import select

from app.config import Settings
from app.connectors.http import AsyncHTTPClient, HTTPJSONResponse
from app.connectors.pncp import PNCPConnector
from app.models import CollectionBatch, CollectionWorkItem, PriceRegistry, SourceRecord
from app.services.ingestion.continuation import create_continuation, pipeline_request_from_filters
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest


def _arp_payload(page, total):
    return {
        "data": [
            {
                "numeroControlePNCPAta": f"00000000000191-1-{page:06}/2026",
                "anoAta": 2026,
                "numeroAtaRegistroPreco": str(page),
                "cnpjOrgao": "00000000000191",
                "objetoContratacao": f"Objeto {page}",
            }
        ],
        "totalPaginas": total,
        "totalRegistros": total,
    }


async def test_real_connector_replays_saved_pages_after_worker_restart(database):
    settings = Settings(_env_file=None, crawl_max_pages=1)
    previous = None
    batch_id = None
    all_requests = []

    class Transport:
        checkpoint = None
        on_activity = None

        def __init__(self):
            self.requests = []

        async def request_json(self, _method, url, **kwargs):
            page = kwargs["params"]["pagina"]
            self.requests.append(page)
            row = {
                "numeroControlePNCPAta": f"00000000000191-1-{page:06}/2026",
                "anoAta": 2026,
                "numeroAtaRegistroPreco": str(page),
                "cnpjOrgao": "00000000000191",
                "objetoContratacao": f"Objeto {page}",
            }
            return HTTPJSONResponse(
                payload={"data": [row], "totalPaginas": 3, "totalRegistros": 3},
                url=url,
                status_code=200,
                request_parameters=kwargs["params"],
                headers={},
                collected_at=datetime.now(UTC),
            )

        async def aclose(self):
            pass

    for _ in range(3):
        transport = Transport()
        connector = PNCPConnector(settings, http_client=transport)
        request = PipelineRequest(
            connector="pncp", mode="price_registries", process_documents=False
        )
        run_id = None
        if previous:
            async with database.sessions() as session:
                child, filters = await create_continuation(session, previous)
                await session.commit()
            request, run_id, batch_id = (
                pipeline_request_from_filters(filters),
                child.id,
                child.batch_id,
            )
        summary = await IngestionPipeline(
            settings, session_factory=database.sessions, connectors={"pncp": connector}
        ).run(request, run_id=run_id)
        assert summary.records_created == 1, summary.diagnostics
        previous = summary.run_id
        all_requests.append(transport.requests)
    assert all_requests == [[1], [1, 2], [2, 3]]
    async with database.sessions() as session:
        assert len((await session.scalars(select(PriceRegistry))).all()) == 3
        batch = await session.get(CollectionBatch, batch_id)
        work = (
            await session.scalars(
                select(CollectionWorkItem).where(CollectionWorkItem.kind == "record")
            )
        ).all()
        assert batch.status == "complete", (
            batch.cursors,
            [(row.payload.get("external_id"), row.status) for row in work],
        )
        snapshot = await session.scalar(
            select(SourceRecord).where(SourceRecord.endpoint.like("%atas"))
        )
        assert snapshot.raw_payload["totalPaginas"] == 3


@pytest.mark.parametrize("failed_page", [None, 3])
@pytest.mark.parametrize("concurrency", [4, 8])
async def test_parallel_snapshots_survive_limit_or_page_failure_and_worker_restart(
    database, failed_page, concurrency, caplog
):
    caplog.set_level("INFO", logger="app.services.ingestion.collection_batches")
    max_pages = concurrency * 2 + 1
    total_pages = max_pages + 2
    settings = Settings(
        _env_file=None,
        crawl_max_pages=max_pages,
        http_max_concurrency=concurrency,
        http_min_request_interval_seconds=0,
        http_max_retries=0,
    )
    active, peak = 0, 0
    release = asyncio.Event()
    requests = []
    retry = False

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
            if not retry and page == failed_page:
                return httpx.Response(503)
            return httpx.Response(200, json=_arp_payload(page, total_pages))
        finally:
            active -= 1

    async def run(request, run_id=None):
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            connector = PNCPConnector(
                settings, http_client=AsyncHTTPClient(settings, client=client)
            )
            return await asyncio.wait_for(
                IngestionPipeline(
                    settings, session_factory=database.sessions, connectors={"pncp": connector}
                ).run(request, run_id=run_id),
                timeout=30,
            )

    request = PipelineRequest(connector="pncp", mode="price_registries", process_documents=False)
    first = await run(request)
    assert first.records_created == (2 if failed_page else max_pages), first.diagnostics
    assert peak == concurrency and requests[0] == 1
    initial_end = concurrency + 1 if failed_page else max_pages
    assert sorted(requests) == list(range(1, initial_end + 1))
    async with database.sessions() as session:
        child, filters = await create_continuation(session, first.run_id)
        batch = await session.get(CollectionBatch, child.batch_id)
        assert batch.status == ("failed" if failed_page else "limited")
        await session.commit()
        child_id, batch_id = child.id, child.batch_id
    requests.clear()
    retry = True
    second = await run(pipeline_request_from_filters(filters), child_id)
    assert second.records_created == (total_pages - 2 if failed_page else 2), second.diagnostics
    boundary_and_new = list(range(initial_end, total_pages + 1))
    assert sorted(requests) == ([3, *boundary_and_new] if failed_page else boundary_and_new)
    assert requests[:2] == [3, initial_end] if failed_page else requests[0] == initial_end
    async with database.sessions() as session:
        assert (await session.get(CollectionBatch, batch_id)).status == "complete"
        assert len((await session.scalars(select(PriceRegistry))).all()) == total_pages
        responses = (
            await session.scalars(
                select(CollectionWorkItem).where(
                    CollectionWorkItem.batch_id == batch_id, CollectionWorkItem.kind == "response"
                )
            )
        ).all()
        assert len(responses) == total_pages
        for response in responses:
            source = await session.get(SourceRecord, response.source_record_id)
            assert source.raw_payload == response.payload["raw_payload"]
            assert source.raw_payload["totalRegistros"] == total_pages
    timings = [
        record.context for record in caplog.records if record.message == "crawl_snapshot_timing"
    ]
    assert {entry["crawl_run_id"] for entry in timings} == {str(first.run_id), str(child_id)}
    assert all(entry["snapshot_seconds"] >= 0 and entry["source"] == "pncp" for entry in timings)
