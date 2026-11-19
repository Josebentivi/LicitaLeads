"""Exercise the real connector with the durable HTTP snapshot callback."""

import asyncio
from datetime import UTC, date, datetime

import httpx
import pytest
from sqlalchemy import select

from app.config import Settings
from app.connectors.base import ConnectorResult, DataAvailability, RawSourceRecord
from app.connectors.http import AsyncHTTPClient, HTTPJSONResponse
from app.connectors.pncp import PNCPConnector
from app.models import (
    CollectionBatch,
    CollectionWorkItem,
    CrawlRun,
    CrawlRunStatus,
    FieldState,
    PriceRegistry,
    Procurement,
    SourceRecord,
)
from app.models.entities import ProcurementItem
from app.repositories.procurements import ProcurementRepository
from app.services.ingestion.continuation import create_continuation, pipeline_request_from_filters
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest
from app.services.procurement_search import ObjectScope


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


@pytest.mark.parametrize("max_records", [0, 1])
async def test_historical_month_discovery_continues_both_truncated_partitions(
    database, max_records
):
    settings = Settings(
        _env_file=None,
        crawl_max_pages=1,
        crawl_max_records_per_source=max_records,
        http_min_request_interval_seconds=0,
    )
    requests = []

    def handler(request):
        month = int(request.url.params["dataInicial"][4:6])
        page = int(request.url.params["pagina"])
        requests.append((month, page))
        sequence = month * 10 + page
        row = {
            "numeroControlePNCP": f"00000000000191-1-{sequence:06}/2026",
            "sequencialCompra": sequence,
            "anoCompra": 2026,
            "numeroCompra": str(sequence),
            "modalidadeNome": "Pregão eletrônico",
            "objetoCompra": "Aquisição de material de limpeza",
            "orgaoEntidade": {"cnpj": "00000000000191", "esferaId": "M"},
            "unidadeOrgao": {"codigoUnidade": "980921", "ufSigla": "MA"},
            "dataPublicacaoPncp": f"2026-{month:02}-20T10:00:00",
        }
        return httpx.Response(200, json={"data": [row], "totalPaginas": 2, "totalRegistros": 2})

    async def collect(request, run_id=None):
        async def empty(_identifier):
            return ConnectorResult(data=[], availability=DataAvailability.EMPTY)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            connector = PNCPConnector(
                settings, http_client=AsyncHTTPClient(settings, client=client)
            )
            connector.fetch_procurement = empty
            connector.fetch_items = empty
            connector.fetch_documents = empty
            connector.fetch_results = empty
            pipeline = IngestionPipeline(
                settings, session_factory=database.sessions, connectors={"pncp": connector}
            )
            return await pipeline.run(request, run_id=run_id)

    request = PipelineRequest(
        connector="pncp",
        days=None,
        start_date=date(2026, 1, 15),
        end_date=date(2026, 2, 22),
        calendar_months=True,
        object_scope=ObjectScope.build(object_presets=["material-limpeza"], government_sphere="M"),
        process_documents=False,
    )
    first = await collect(request)
    assert first.records_created == (1 if max_records else 2), first.diagnostics
    assert requests == [(2, 1), (1, 1)]

    current = first
    expected_additions = [1, 1, 1] if max_records else [2]
    for expected_created in expected_additions:
        assert current.status is CrawlRunStatus.PARTIAL
        async with database.sessions.begin() as session:
            parent = await session.get(CrawlRun, current.run_id)
            assert parent.status is CrawlRunStatus.PARTIAL
            child, filters = await create_continuation(session, current.run_id)
            batch = await session.get(CollectionBatch, child.batch_id)
            assert batch.status == "limited"
            child_id, batch_id = child.id, child.batch_id
        continued = pipeline_request_from_filters(filters)
        assert continued.object_scope == request.object_scope
        current = await collect(continued, child_id)
        assert current.records_created == expected_created, current.diagnostics
    assert current.status is CrawlRunStatus.COMPLETED
    assert requests[2:] == [(2, 1), (2, 2), (1, 1), (1, 2)]
    async with database.sessions() as session:
        assert (await session.get(CollectionBatch, batch_id)).status == "complete"
        assert len((await session.scalars(select(Procurement))).all()) == 4


@pytest.mark.parametrize("failed_first", [False, True])
async def test_scoped_collection_checks_items_before_other_details_and_resumes_pending(
    database, monkeypatch, failed_first
):
    monkeypatch.setattr("app.connectors.pncp._MAX_DETAIL_PAGES", 1)

    async def stale_version(*_args, **_kwargs):
        raise AssertionError("The header cannot establish eligibility for a reused version")

    monkeypatch.setattr("app.services.ingestion.pipeline.find_reusable", stale_version)
    settings = Settings(_env_file=None, http_min_request_interval_seconds=0, http_max_retries=0)
    item_requests = []
    detailed = []
    resuming = False

    def row(number):
        return {
            "numeroControlePNCP": f"00000000000191-1-{number:06}/2026",
            "anoCompra": 2026,
            "numeroCompra": str(number),
            "sequencialCompra": number,
            "modalidadeNome": "Pregão eletrônico",
            "objetoCompra": "Aquisição de bens para a unidade",
            "informacaoComplementar": "Entrega refrigerada",
            "orgaoEntidade": {"cnpj": "00000000000191", "esferaId": "M"},
            "unidadeOrgao": {"codigoUnidade": "980921", "ufSigla": "MA"},
        }

    def handler(request):
        if request.url.path.endswith("/publicacao"):
            return httpx.Response(
                200, json={"data": [row(1), row(2), row(3)], "totalPaginas": 1, "totalRegistros": 3}
            )
        assert request.url.path.endswith("/itens")
        number = int(request.url.path.split("/")[-2])
        page = int(request.url.params["pagina"])
        item_requests.append((number, page))
        if failed_first and number == 2 and not resuming:
            return httpx.Response(503)
        matched = (number, page) in {(1, 1), (2, 1 if failed_first else 2)}
        item = {
            "numeroItem": page,
            "descricao": "Detergente" if matched else "Equipamento",
            "informacaoComplementar": "Frasco de 1 litro"
            if matched
            else "Acessório de informática",
        }
        total = 2 if number == 2 and not failed_first else 1
        return httpx.Response(
            200, json={"data": [item], "totalPaginas": total, "totalRegistros": total}
        )

    async def collect(request, run_id=None):
        async def details(identifier):
            number = int(identifier.split("-")[-1].split("/")[0])
            detailed.append(number)
            endpoint = (
                f"https://pncp.gov.br/api/consulta/v1/orgaos/00000000000191/compras/2026/{number}"
            )
            payload = row(number)
            payload.pop("informacaoComplementar")
            return ConnectorResult(
                data=connector._map_procurement(payload, endpoint, detail=True),
                availability=DataAvailability.AVAILABLE,
                raw_records=[
                    RawSourceRecord(
                        source="pncp",
                        endpoint=endpoint,
                        raw_payload=payload,
                        response_hash="b" * 64,
                        http_status=200,
                        collected_at=datetime.now(UTC),
                    )
                ],
            )

        async def empty(_identifier):
            return ConnectorResult(data=[], availability=DataAvailability.EMPTY)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            connector = PNCPConnector(
                settings, http_client=AsyncHTTPClient(settings, client=client)
            )
            connector.fetch_procurement = details
            connector.fetch_documents = empty
            connector.fetch_results = empty
            pipeline = IngestionPipeline(
                settings, session_factory=database.sessions, connectors={"pncp": connector}
            )
            return await pipeline.run(request, run_id=run_id)

    request = PipelineRequest(
        connector="pncp",
        object_scope=ObjectScope.build("frasco", ["material-limpeza"], "bens", "M"),
        process_documents=False,
    )
    first = await collect(request)
    assert (first.records_created, first.records_updated) == (1, 0), first.diagnostics
    assert first.source_results["pncp"]["scope_pending"] == 1
    assert first.source_results["pncp"]["outside_scope"] == 1
    assert sorted(item_requests) == [(1, 1), (2, 1), (3, 1)]
    assert detailed == [1]
    async with database.sessions.begin() as session:
        child, filters = await create_continuation(session, first.run_id)
        batch = await session.get(CollectionBatch, child.batch_id)
        assert batch.status == "limited"
        work = (
            await session.scalars(
                select(CollectionWorkItem).where(CollectionWorkItem.kind == "record")
            )
        ).all()
        assert {item.payload["purchase_number"]: item.status for item in work} == {
            "1": "succeeded",
            "2": "details_pending",
            "3": "outside_scope",
        }
        snapshots = (
            await session.scalars(
                select(SourceRecord).where(SourceRecord.endpoint.like("%/itens%"))
            )
        ).all()
        assert len(snapshots) == (2 if failed_first else 3)
        child_id, batch_id = child.id, child.batch_id

    resuming = True
    second = await collect(pipeline_request_from_filters(filters), child_id)
    assert (second.records_created, second.records_updated) == (1, 0), second.diagnostics
    assert detailed == [1, 2]
    assert item_requests[3:] == ([(2, 1)] if failed_first else [(2, 1), (2, 2)])
    async with database.sessions() as session:
        assert (await session.get(CollectionBatch, batch_id)).status == "complete"
        processes = (await session.scalars(select(Procurement.purchase_number))).all()
        assert sorted(processes) == ["1", "2"]
        items = (await session.scalars(select(ProcurementItem.description))).all()
        expected = ["Frasco de 1 litro", "Frasco de 1 litro"]
        if not failed_first:
            expected.insert(0, "Acessório de informática")
        assert sorted(items) == expected
        matches = await ProcurementRepository(session).list_filtered(
            object_scope=request.object_scope
        )
        assert sorted(procurement.purchase_number for procurement in matches.items) == ["1", "2"]
        header_only = await ProcurementRepository(session).list_filtered(
            object_scope=ObjectScope.build("entrega refrigerada")
        )
        assert header_only.total == 2
        origins = await session.scalars(
            select(SourceRecord.endpoint)
            .join(FieldState, FieldState.source_record_id == SourceRecord.id)
            .where(FieldState.field_name == "object_complement")
        )
        assert len(saved_origins := list(origins)) == 2
        assert all("/publicacao" in endpoint for endpoint in saved_origins)


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
