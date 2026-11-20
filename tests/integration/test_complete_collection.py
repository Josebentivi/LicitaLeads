"""The manual collection shares one ticket and preserves independent source results."""

import asyncio
from datetime import UTC, date, datetime
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from bs4 import BeautifulSoup
from sqlalchemy import func, select

from app.config import Settings
from app.connectors.base import ConnectorResult, PaginationInfo, RawPriceRegistry, RawSourceRecord
from app.connectors.base import DataAvailability as ConnectorAvailability
from app.connectors.http import AsyncHTTPClient
from app.connectors.pncp import PNCPConnector
from app.connectors.tce_ma import SincContrataResource, SincContrataRow, TCEMAConnector
from app.models import (
    CollectionBatch,
    CollectionWorkItem,
    CrawlRun,
    CrawlRunStatus,
    JobLease,
    Lead,
    PriceRegistry,
    Procurement,
    ProcurementEvent,
    ProcurementSource,
    SourceRecord,
)
from app.models.enums import DataAvailability
from app.services.ingestion.collection_batches import retained_response_fingerprint
from app.services.ingestion.complete_collection import (
    COMPLETE_COLLECTION_LEASE,
    CollectionScope,
    CompleteCollection,
    StepResult,
)
from app.services.ingestion.tce_ma_observations import tce_observations_for

SCOPE = CollectionScope("MA", date(2026, 5, 1), date(2026, 7, 31))


@pytest.fixture
def collection_with_registries(database, monkeypatch):
    settings = Settings(
        _env_file=None,
        crawl_max_pages=1,
        crawl_max_records_per_source=0,
        http_max_concurrency=1,
        http_min_request_interval_seconds=0,
        http_max_retries=0,
    )
    collection = CompleteCollection(settings, sessions=database.sessions)

    async def skip(ticket, scope):
        return StepResult("skipped")

    for stage in ("_procurements", "_tce", "_contracts", "_municipal", "_federal"):
        monkeypatch.setattr(collection, stage, skip)
    return collection


async def _seed_arp_unit(database):
    async with database.sessions.begin() as db:
        db.add(
            Procurement(
                source="pncp",
                external_id="arp-unit-anchor",
                agency_cnpj="00000000000191",
                agency_name="Órgão de Exemplo",
                uf="MA",
                uasg="980921",
                fingerprint="arp-unit-anchor",
            )
        )


@pytest.mark.parametrize(
    "source_outcome", ["complete", "record_limit", "new_total", "failed", "cancelled"]
)
async def test_general_collection_continues_the_same_arp_batch(
    database, monkeypatch, collection_with_registries, source_outcome
):
    collection = collection_with_registries
    if source_outcome == "record_limit":
        monkeypatch.setattr(collection.settings, "crawl_max_records_per_source", 1)
    startup = asyncio.Event()
    requests: list[int] = []
    await _seed_arp_unit(database)

    async def handler(request):
        page = int(request.url.params["pagina"])
        assert request.url.params["dataInicial"] == "20260501"
        assert request.url.params["dataFinal"] == "20260731"
        assert request.url.params["codigoUnidadeAdministrativa"] == "980921"
        requests.append(page)
        if source_outcome == "failed" and page == 2:
            return httpx.Response(503)
        if source_outcome == "cancelled" and len(requests) == 2:
            startup.set()
            await asyncio.Event().wait()
        total = 2 if source_outcome == "new_total" and len(requests) == 1 else 3
        serials = range(1, 4) if source_outcome == "record_limit" else (page,)
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "numeroControlePNCPAta": f"00000000000191-1-{serial:06}/2026",
                        "anoAta": 2026,
                        "numeroAtaRegistroPreco": str(serial),
                        "cnpjOrgao": "00000000000191",
                        "codigoUnidadeOrgao": "980921",
                    }
                    for serial in serials
                ],
                "totalPaginas": 1 if source_outcome == "record_limit" else total,
                "totalRegistros": total,
            },
        )

    def make_connectors(source):
        raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        http = AsyncHTTPClient(collection.settings, client=raw_client)
        connector = PNCPConnector(collection.settings, http_client=http)
        monkeypatch.setattr(connector, "aclose", raw_client.aclose)
        return [connector]

    monkeypatch.setattr(collection.pipeline, "_selected_connectors", make_connectors)
    ticket = await collection.enqueue(SCOPE)
    worker = asyncio.create_task(collection.run(ticket, SCOPE))
    try:
        if source_outcome == "cancelled":
            await asyncio.wait_for(startup.wait(), 10)
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker
        else:
            await asyncio.wait_for(worker, 10)
    finally:
        if not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

    expected_requests = {
        "complete": [1, 1, 2, 2, 3],
        "record_limit": [1],
        "new_total": [1, 1, 1, 1, 2, 2, 3],
        "failed": [1, 1, 2],
        "cancelled": [1, 1],
    }
    assert requests == expected_requests[source_outcome]
    async with database.sessions() as db:
        root = await db.get_one(CrawlRun, ticket.run_id)
        batches = (await db.scalars(select(CollectionBatch))).all()
        (batch,) = batches
        children = (
            await db.scalars(
                select(CrawlRun).where(CrawlRun.batch_id == batch.id).order_by(CrawlRun.created_at)
            )
        ).all()
        assert all(row.filters["parent_run_id"] == str(ticket.run_id) for row in children)
        assert all(row.filters["start_date"] == "2026-05-01" for row in children)
        assert all(row.filters["end_date"] == "2026-07-31" for row in children)
        assert len({row.filters["price_registry_scopes"][0]["key"] for row in children}) == 1
        assert await db.scalar(select(func.count()).select_from(JobLease)) == 0
        step = next(s for s in root.cursor["complete_stages"] if s["key"] == "price_registries")
        if source_outcome in ("complete", "record_limit", "new_total"):
            assert root.status is CrawlRunStatus.COMPLETED
            assert batch.status == "complete"
            assert step["status"] == "completed" and "3 atas novas" in step["note"]
            expected_registries = 3
        else:
            assert root.status.value == (
                "cancelled" if source_outcome == "cancelled" else "partial"
            )
            assert batch.status == ("suspended" if source_outcome == "cancelled" else "failed")
            expected_registries = 1
        if source_outcome != "cancelled":
            assert step["run_ids"] == [str(row.id) for row in children]
        assert (
            await db.scalar(select(func.count()).select_from(PriceRegistry)) == expected_registries
        )


async def test_general_collection_stops_arp_continuation_when_checkpoint_repeats(
    database, monkeypatch, collection_with_registries
):
    collection = collection_with_registries
    calls = 0
    await _seed_arp_unit(database)

    async def discover(filters):
        nonlocal calls
        calls += 1
        assert calls <= 2
        return ConnectorResult(
            data=[
                RawPriceRegistry(
                    source="pncp",
                    source_url="https://pncp.gov.br/api/consulta/v1/atas",
                    external_id="same-ata",
                    pncp_control_number="same-ata",
                )
            ],
            availability=ConnectorAvailability.AVAILABLE,
            pagination=PaginationInfo(truncated=True),
            partial=True,
        )

    async def close():
        pass

    async def no_items(external_id):
        return ConnectorResult(data=[], availability=ConnectorAvailability.NOT_SUPPORTED)

    monkeypatch.setattr(
        collection.pipeline,
        "_selected_connectors",
        lambda source: [
            SimpleNamespace(
                name="pncp",
                discover_price_registries=discover,
                fetch_price_registry_items=no_items,
                aclose=close,
            )
        ],
    )
    ticket = await collection.enqueue(SCOPE)
    await asyncio.wait_for(collection.run(ticket, SCOPE), 10)

    async with database.sessions() as db:
        run = await db.get_one(CrawlRun, ticket.run_id)
        assert run.status is CrawlRunStatus.PARTIAL
        assert (await db.scalar(select(CollectionBatch))).status == "limited"
        assert await db.scalar(select(func.count()).select_from(PriceRegistry)) == 1
        step = next(s for s in run.cursor["complete_stages"] if s["key"] == "price_registries")
        assert calls == len(step["run_ids"]) == 2
        assert "continuação sem progresso" in step["note"]


@pytest.mark.parametrize("cancel", [False, True])
async def test_manual_collection_is_fenced_and_keeps_financial_progress(
    database, monkeypatch, cancel
):
    collection = CompleteCollection(Settings(_env_file=None), sessions=database.sessions)
    pncp_waiting = asyncio.Event()
    municipal_finished = asyncio.Event()
    release_procurements = asyncio.Event()
    child_id = uuid4()

    async def procurements(ticket, scope):
        pncp_waiting.set()
        await release_procurements.wait()
        return StepResult("partial", "Limite atingido na fonte", [child_id])

    async def municipal(ticket, scope):
        await pncp_waiting.wait()
        municipal_finished.set()
        return StepResult("completed", "Pagamentos consultados")

    async def failed_tce(ticket, scope):
        raise OSError("source unavailable")

    async def completed(ticket, scope):
        return StepResult("completed")

    async def federal(ticket, scope):
        return StepResult("skipped", "Chave não configurada")

    monkeypatch.setattr(collection, "_procurements", procurements)
    monkeypatch.setattr(collection, "_municipal", municipal)
    monkeypatch.setattr(collection, "_tce", failed_tce)
    monkeypatch.setattr(collection, "_registries", completed)
    monkeypatch.setattr(collection, "_contracts", completed)
    monkeypatch.setattr(collection, "_federal", federal)
    ticket = await collection.enqueue(SCOPE)
    duplicate = await collection.enqueue(SCOPE)
    assert ticket.generation is not None
    assert duplicate.generation is None and duplicate.run_id == ticket.run_id

    worker = asyncio.create_task(collection.run(ticket, SCOPE))
    try:
        await asyncio.wait_for(municipal_finished.wait(), 10)
        assert not worker.done()
        if cancel:
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker
        else:
            release_procurements.set()
            await asyncio.wait_for(worker, 10)
    finally:
        if not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

    async with database.sessions() as db:
        run = await db.get_one(CrawlRun, ticket.run_id)
        assert await db.get(JobLease, COMPLETE_COLLECTION_LEASE) is None
        steps = {step["key"]: step for step in run.cursor["complete_stages"]}
        assert all(step["status"] not in {"pending", "running"} for step in steps.values())
        if cancel:
            assert run.status is CrawlRunStatus.CANCELLED
            assert steps["procurements"]["status"] == "cancelled"
            assert steps["tce_ma"]["status"] == steps["contracts"]["status"] == "not_started"
        else:
            assert run.status is CrawlRunStatus.PARTIAL
            assert steps["procurements"]["run_ids"] == [str(child_id)]
            assert steps["tce_ma"]["status"] == "failed"
            assert (
                steps["price_registries"]["status"] == steps["contracts"]["status"] == "completed"
            )
            assert steps["municipal"]["status"] == "completed"
            assert steps["federal"]["status"] == "skipped"


async def test_cancelled_collection_shows_interrupted_stages_and_saved_batches(
    api_client, database
):
    now = datetime.now(UTC)
    root = CrawlRun(
        id=uuid4(),
        connector="complete",
        status=CrawlRunStatus.CANCELLED,
        finished_at=now,
        filters={"mode": "complete"},
        cursor={
            "complete_stages": [
                {
                    "key": "procurements",
                    "label": "Contratações e documentos",
                    "status": "cancelled",
                    "started_at": now.isoformat(),
                    "run_ids": [],
                },
                {"key": "tce_ma", "label": "TCE-MA", "status": "cancelled", "run_ids": []},
            ]
        },
    )
    active = CrawlRun(id=uuid4(), connector="pncp", status=CrawlRunStatus.RUNNING)
    async with database.sessions.begin() as db:
        db.add_all([root, active])
        saved = []
        for connector, mode, has_records in (
            ("all", "procurements", True),
            ("municipal:imperatriz", "municipal_payments", True),
            ("municipal:sao_luis", "municipal_payments", False),
        ):
            uid = uuid4()
            batch = CollectionBatch(
                id=uuid4(),
                stream_key=uuid4().hex,
                status="suspended",
                filters={"mode": mode, "connector": connector},
                last_run_id=uid,
            )
            db.add(batch)
            await db.flush()
            db.add(
                CrawlRun(
                    id=uid,
                    batch_id=batch.id,
                    connector=connector,
                    status=CrawlRunStatus.CANCELLED,
                    filters=batch.filters,
                )
            )
            if has_records:
                saved.append(f"/crawls/{uid}/continue")
                db.add(
                    CollectionWorkItem(
                        batch_id=batch.id,
                        source=connector,
                        kind="record",
                        identity="record",
                        status="pending",
                        payload={},
                    )
                )
                db.add(
                    CrawlRun(
                        batch_id=batch.id,
                        connector=connector,
                        status=CrawlRunStatus.CANCELLED,
                        filters=batch.filters,
                    )
                )
    for url in ("/crawls", "/crawls/progress"):
        response = await api_client.get(url)
        assert response.status_code == 200
        page = BeautifulSoup(response.text, "html.parser")
        active_section = page.select_one('section[aria-labelledby="crawl-active-title"]')
        finished = page.select_one('section[aria-labelledby="crawl-finished-title"]')
        assert active_section.select_one(f'[data-crawl-id="{active.id}"]')
        assert active_section.select_one(f'[data-crawl-id="{root.id}"]') is None
        card = finished.select_one(f'[data-crawl-id="{root.id}"]')
        assert card.select_one(".status-cancelled").get_text(strip=True) == "Cancelada"
        assert [
            item.get_text(" ", strip=True) for item in card.select(".complete-stage-list li")
        ] == [
            "Contratações e documentos Interrompida",
            "TCE-MA Não iniciada",
        ]
        assert card.select_one('a[href="#lotes-para-continuar"]')
        assert card.select_one('a[href^="/leads"]') is None
        actions = page.select("#lotes-para-continuar form")
        assert sorted(form["action"] for form in actions) == sorted(saved)


async def test_classifying_retained_page_reuses_record_and_preserves_raw_response(database):
    collection = CompleteCollection(Settings(_env_file=None), sessions=database.sessions)
    ticket = await collection.enqueue(SCOPE)
    raw = RawSourceRecord.build(
        source="pncp",
        endpoint="https://pncp.gov.br/api/pncp/v1/consulta",
        request_parameters={"pagina": 1},
        raw_payload={"data": [{"id": "item"}]},
        http_status=200,
        collected_at=datetime.now(UTC),
    )
    previous_id = uuid4()
    async with database.sessions.begin() as db:
        db.add(
            SourceRecord(
                id=previous_id,
                crawl_run_id=ticket.run_id,
                source=raw.source,
                endpoint=raw.endpoint,
                request_parameters=raw.request_parameters,
                response_hash=raw.response_hash,
                raw_payload=raw.raw_payload,
                http_status=200,
                availability=DataAvailability.AVAILABLE,
                collected_at=raw.collected_at,
                fingerprint=retained_response_fingerprint(ticket.run_id, raw),
            )
        )
    result = ConnectorResult(
        data=[{"id": "item"}], raw_records=[raw], availability=ConnectorAvailability.AVAILABLE
    )
    for _ in range(2):
        ids = await collection.pipeline._persist_source_records(ticket.run_id, result)
        assert ids == [previous_id]
    async with database.sessions() as db:
        record = (await db.scalars(select(SourceRecord))).one()
        assert record.collection_complete is True
        assert record.raw_payload == raw.raw_payload
        assert record.response_hash == raw.response_hash


async def test_tce_attaches_scoped_observations_without_creating_events_or_leads(
    api_client, database, monkeypatch
):
    collection = CompleteCollection(Settings(_env_file=None), sessions=database.sessions)
    ticket = await collection.enqueue(SCOPE)
    cnpj, control = "06307102000130", "06307102000130-1-000115/2026"
    procurement = Procurement(
        id=uuid4(),
        source="pncp",
        external_id=control,
        pncp_control_number=control,
        agency_cnpj=cnpj,
        uf="MA",
        uasg="980921",
        purchase_number="90024",
        purchase_year=2026,
        modality="Pregão - Eletrônico",
        publication_at=datetime(2026, 5, 1, 12, tzinfo=UTC),
        fingerprint="test-pregao-tce",
    )
    ambiguous_control = "06307102000130-1-000116/2026"
    ambiguous = Procurement(
        id=uuid4(),
        source="pncp",
        external_id=ambiguous_control,
        pncp_control_number=ambiguous_control,
        agency_cnpj=cnpj,
        uf="MA",
        uasg="980921",
        purchase_number="59",
        purchase_year=2026,
        modality="Pregão - Eletrônico",
        publication_at=procurement.publication_at,
        fingerprint="test-pregao-tce-ambiguous",
    )
    origin_url = "https://cnetmobile.estaleiro.serpro.gov.br/comprasnet-web/public/landing?destino=acompanhamento-compra&compra=98092105900242026"
    raw_pncp = RawSourceRecord.build(
        source="pncp",
        endpoint="https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao",
        request_parameters={},
        raw_payload={
            "data": [
                {
                    "numeroControlePNCP": "06307102000130-1-000116/2026",
                    "linkSistemaOrigem": "https://example.org/other",
                },
                {"numeroControlePNCP": control, "linkSistemaOrigem": origin_url},
            ]
        },
        http_status=200,
        collected_at=datetime.now(UTC),
    )
    async with database.sessions.begin() as db:
        source = SourceRecord(
            source=raw_pncp.source,
            endpoint=raw_pncp.endpoint,
            request_parameters={},
            response_hash=raw_pncp.response_hash,
            raw_payload=raw_pncp.raw_payload,
            http_status=200,
            availability=DataAvailability.AVAILABLE,
            collected_at=raw_pncp.collected_at,
        )
        db.add_all([procurement, ambiguous, source])
        await db.flush()
        db.add(
            ProcurementSource(
                procurement_id=procurement.id,
                source="pncp",
                external_id=control,
                source_record_id=source.id,
                fingerprint="test-pregao-pncp-source",
            )
        )
    exact = {"cnpjProcedimento": cnpj, "idProcedimento": "PE900242026CPL", "enteIdEnvio": 2111300}
    data = {
        SincContrataResource.PROCEDURES: [
            {
                **exact,
                "id": "confirmed",
                "idContratacaoPncp": control,
                "numeroProcedimento": 90024,
                "anoProcedimento": 2026,
                "tipoProcedimento": "PE",
            },
            {
                **exact,
                "id": "different-agency",
                "idProcedimento": "PE900422026FMS",
                "cnpjProcedimento": "13816886000198",
                "idContratacaoPncp": ambiguous_control,
                "numeroProcedimento": 90042,
                "anoProcedimento": 2026,
                "tipoProcedimento": "PE",
            },
        ],
        SincContrataResource.CONTRACTS: [
            {**exact, "id": "contract", "numeroContrato": "366", "anoContrato": 2025},
            {**exact, "id": "conflict", "idContratacaoPncp": "06307102000130-1-000116/2026"},
        ],
    }

    async def fetch(_self, resource, *, ente_id):
        assert ente_id == 2111300
        rows = data.get(resource, [])
        raw = RawSourceRecord.build(
            source="tce_ma",
            endpoint=f"https://tce-api.apps.tcema.tc.br/sinccontrata/{resource.value}",
            request_parameters={"enteId": ente_id},
            raw_payload={"number": 0, "totalPages": 1, "totalElements": len(rows), "content": rows},
            http_status=200,
            collected_at=datetime.now(UTC),
        )
        return ConnectorResult(
            data=[SincContrataRow(row, raw.record_key) for row in rows],
            raw_records=[raw],
            availability=ConnectorAvailability.AVAILABLE if rows else ConnectorAvailability.EMPTY,
        )

    monkeypatch.setattr(TCEMAConnector, "fetch", fetch)
    result = await collection._tce(ticket, SCOPE)
    assert result.status == "partial" and "1 registros com controle divergente" in result.note
    assert "1 pregões com identidade para revisão" in result.note
    async with database.sessions() as db:
        observations = await tce_observations_for(db, procurement.id)
        assert {observation.value["id"] for observation in observations} == {
            "confirmed",
            "contract",
        }
        for observation in observations:
            record = await db.get_one(SourceRecord, observation.source_record_id)
            assert record.crawl_run_id == ticket.run_id
            assert observation.value in record.raw_payload["content"]
        review = await tce_observations_for(db, ambiguous.id)
        assert len(review) == 1 and review[0].field_name == "tce_ma.identidade_para_revisao"
        assert review[0].value["cnpjProcedimento"] == "13816886000198"
        record = await db.get_one(SourceRecord, review[0].source_record_id)
        assert review[0].value in record.raw_payload["content"]
        assert await db.scalar(select(func.count()).select_from(ProcurementEvent)) == 0
        assert await db.scalar(select(func.count()).select_from(Lead)) == 0
    page = await api_client.get(f"/procurements/{procurement.id}")
    assert page.status_code == 200
    assert "TCE-MA · dados complementares" in page.text
    assert "Ano anterior ao do pregão" in page.text
    assert (
        origin_url.replace("&", "&amp;") in page.text
        and "https://example.org/other" not in page.text
    )
    review_page = await api_client.get(f"/procurements/{ambiguous.id}")
    assert "Mesmo controle PNCP, identificação divergente" in review_page.text
    assert "13816886000198" in review_page.text and "Contrato informado" not in review_page.text
