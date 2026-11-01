"""Regressions from the annual unscoped ARP jobs and their stored failures."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.connectors.base import DataAvailability
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.http import AsyncHTTPClient
from app.jobs import scheduler
from app.models import (
    CollectionBatch,
    CollectionSyncState,
    CollectionWorkItem,
    CrawlRun,
    JobLease,
    PriceRegistry,
    PriceRegistryAdhesion,
)
from app.services.ingestion.continuation import create_continuation, pipeline_request_from_filters
from app.services.ingestion.pipeline import IngestionPipeline, PipelineBusy, PipelineRequest
from app.services.ingestion.scheduled_collections import prepare_scheduled

from .test_price_registry_multi_scope import connector, scope
from .test_price_registry_pipeline import PriceRegistryConnector, _registry, _registry_adhesion


async def test_duplicate_adhesions_in_same_transaction_are_one_fact_with_audit(database):
    from app.models import FieldChange

    raw = _registry()
    adhesion = _registry_adhesion()
    cfg = Settings(_env_file=None)
    pipeline = IngestionPipeline(
        cfg,
        session_factory=database.sessions,
        connectors={"compras_gov": PriceRegistryConnector(raw, [], adhesions=[adhesion, adhesion])},
    )
    result = await pipeline.run(
        PipelineRequest(connector="compras_gov", mode="price_registries", process_documents=False)
    )
    assert result.records_created == 1 and result.records_failed == 0
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(PriceRegistryAdhesion)) == 1
        assert await session.scalar(select(func.count()).select_from(FieldChange)) > 0


async def test_differing_duplicate_adhesions_preserve_transitions_without_summing(database):
    from decimal import Decimal

    from app.models import FieldChange

    raw = _registry()
    row = _registry_adhesion()
    observations = [
        row.model_copy(update={"approved_quantity": Decimal(value)}) for value in ("3", "4", "3")
    ]
    cfg = Settings(_env_file=None)
    pipeline = IngestionPipeline(
        cfg,
        session_factory=database.sessions,
        connectors={"compras_gov": PriceRegistryConnector(raw, [], adhesions=observations)},
    )
    result = await pipeline.run(
        PipelineRequest(connector="compras_gov", mode="price_registries", process_documents=False)
    )
    assert result.records_failed == 0
    async with database.sessions() as session:
        fact = await session.scalar(select(PriceRegistryAdhesion))
        assert fact.approved_quantity == Decimal("3")
        changes = (
            await session.scalars(
                select(FieldChange)
                .where(
                    FieldChange.entity_id == fact.id, FieldChange.field_name == "approved_quantity"
                )
                .order_by(FieldChange.recorded_at)
            )
        ).all()
        assert [Decimal(change.received_value) for change in changes] == [3, 4, 3]


@pytest.mark.parametrize("catalog", [13768, 0, "0013768"])
async def test_compras_catalog_code_and_empty_item_dependent_operations(catalog):
    cfg = Settings(_env_file=None, http_min_request_interval_seconds=0, http_max_retries=0)
    calls = []

    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(200, json={"resultado": [], "totalRegistros": 0, "totalPaginas": 0})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        con = ComprasGovConnector(cfg, http_client=AsyncHTTPClient(cfg, client=client))
        row = {"numeroItem": "1", "codigoPdm": catalog}
        item = con._map_price_registry_item(row, "official-arp", "https://official.test")
        unit = con._map_arp_unit(row, "official-arp", "https://official.test")
        assert item.catalog_code == unit.catalog_code == str(catalog)
        assert item.raw_payload["codigoPdm"] == catalog
        registry = _registry()
        await con.fetch_price_registry_items(registry.external_id)
        units = await con.fetch_price_registry_units(registry)
        adhesions = await con.fetch_price_registry_adhesions(registry)
    assert len(calls) == 1
    for result in (units, adhesions):
        assert result.availability is DataAvailability.NOT_PUBLISHED
        assert not result.partial and not result.is_complete and result.raw_records == []


async def test_scheduled_busy_admission_leaves_no_run_or_coverage(database):
    cfg = Settings(_env_file=None)
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        session.add(
            JobLease(
                name="crawl:pncp:MA",
                owner_id="other-worker",
                acquired_at=now,
                heartbeat_at=now,
                expires_at=now + timedelta(hours=1),
            )
        )
    request = PipelineRequest(connector="pncp", resume_automatically=True, process_documents=False)
    planned = await prepare_scheduled(database.sessions, cfg, request)
    pipeline = IngestionPipeline(cfg, session_factory=database.sessions)
    with pytest.raises(PipelineBusy):
        await pipeline.run(planned)
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(CrawlRun)) == 0
        state = await session.scalar(select(CollectionSyncState))
        assert state.covered_until is None and state.pending_batch_id is None
        assert (await session.get(JobLease, "crawl:pncp:MA")).owner_id == "other-worker"


async def test_arp_pending_rows_are_bounded_and_survive_last_page_and_restart(database):
    from tests.unit.test_arp_discovery_budget import page_payload

    requests = []

    def handler(request):
        page = int(request.url.params["pagina"])
        requests.append(page)
        return httpx.Response(200, json=page_payload(page, 10, "pncp", total=2))

    cfg = Settings(
        _env_file=None,
        crawl_max_records_per_source=3,
        http_min_request_interval_seconds=0,
        http_max_retries=0,
    )
    original = PipelineRequest(
        connector="pncp",
        mode="price_registries",
        process_documents=False,
        price_registry_scopes=(scope("1"),),
    )

    def pipeline():
        return IngestionPipeline(
            cfg,
            session_factory=database.sessions,
            connectors={"pncp": connector("pncp", cfg, handler)},
        )

    result = await pipeline().run(original)
    async with database.sessions() as session:
        run = await session.get(CrawlRun, result.run_id)
        batch_id = run.batch_id
        assert (
            await session.scalar(
                select(func.count())
                .select_from(CollectionWorkItem)
                .where(CollectionWorkItem.batch_id == batch_id, CollectionWorkItem.kind == "record")
            )
            == 10
        )
    assert result.records_created == 3 and requests == [1]
    for _ in range(6):
        async with database.sessions() as session:
            child, filters = await create_continuation(session, result.run_id)
            await session.commit()
        result = await pipeline().run(pipeline_request_from_filters(filters), run_id=child.id)
        assert result.records_found <= 3 and result.records_updated == 0
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(PriceRegistry)) == 20
        assert (await session.get(CollectionBatch, batch_id)).status == "complete"
    # Only the persisted boundary can be rechecked; pages are not rediscovered in bulk.
    assert requests == [1, 1, 2]


async def test_scheduler_no_scopes_never_starts_national_crawl(database, monkeypatch):
    monkeypatch.setattr(scheduler, "async_session_factory", database.sessions)
    monkeypatch.setattr(scheduler, "get_settings", lambda: Settings(_env_file=None))

    class Never:
        def __init__(self, _):
            pytest.fail("no units must not construct a collection pipeline")

    monkeypatch.setattr(scheduler, "IngestionPipeline", Never)
    await scheduler.discover_price_registries()
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(CollectionSyncState)) == 0


async def test_automatic_scopes_start_new_coverage_and_leave_annual_batches_intact(
    database, monkeypatch
):
    from app.models import Procurement, ProcurementSource
    from app.services.ingestion.collection_batches import stream_key

    cfg = Settings(_env_file=None)
    annual = PipelineRequest(
        connector="pncp",
        mode="price_registries",
        days=365,
        modalities=(),
        process_documents=False,
        resume_automatically=True,
    )
    frozen = {**annual.as_json(), "start_date": "2025-10-31", "end_date": "2026-10-30"}
    async with database.sessions() as session, session.begin():
        old = CollectionBatch(
            stream_key=stream_key(annual.as_json()), filters=frozen, status="limited", cursors={}
        )
        session.add(old)
        for uf, unit in (("MA", "1"), ("PI", "2")):
            row = Procurement(
                source="pncp",
                external_id=unit,
                uf=uf,
                uasg=unit,
                agency_cnpj="00000000000191",
                fingerprint=unit,
            )
            session.add(row)
            await session.flush()
            if uf == "MA":
                session.add(
                    ProcurementSource(
                        procurement_id=row.id,
                        source="compras_gov",
                        external_id=unit,
                        fingerprint="source-1",
                    )
                )
    calls = []

    class Pipeline:
        def __init__(self, _):
            pass

        async def run(self, request):
            calls.append(request)

    monkeypatch.setattr(scheduler, "get_settings", lambda: cfg)
    monkeypatch.setattr(scheduler, "async_session_factory", database.sessions)
    monkeypatch.setattr(scheduler, "IngestionPipeline", Pipeline)
    await scheduler.discover_price_registries()
    assert len(calls) == 2
    for request in calls:
        assert request.days == 30 and request.continuation_batch_id is None
        assert (request.end_date - request.start_date).days == 29
        assert [s.unit_code for s in request.price_registry_scopes] == ["1"]
    async with database.sessions() as session:
        stored = await session.get(CollectionBatch, old.id)
        assert stored.filters == frozen and stored.status == "limited"


async def test_busy_recheck_does_not_advance_rotation(database):
    from app.models import CollectionRecheckState, Procurement, ProcurementSource
    from app.services.ingestion.collection_rechecks import recheck_compras

    cfg = Settings(_env_file=None)
    async with database.sessions() as session, session.begin():
        row = Procurement(source="compras_gov", external_id="official", uf="MA", fingerprint="p")
        session.add(row)
        await session.flush()
        source = ProcurementSource(
            procurement_id=row.id, source="compras_gov", external_id="official", fingerprint="s"
        )
        session.add(source)

    class Busy:
        async def run(self, request):
            raise PipelineBusy

    await recheck_compras(Busy(), PipelineRequest(connector="compras_gov"), cfg, database.sessions)
    async with database.sessions() as session:
        assert (await session.get(ProcurementSource, source.id)).last_refresh_attempt_at is None
        assert await session.scalar(select(func.count()).select_from(CollectionRecheckState)) == 0


async def test_failed_details_take_precedence_over_a_limited_page(database):
    from app.connectors.base import ConnectorResult
    from app.services.ingestion.collection_batches import latest_limited_batch
    from tests.unit.test_arp_discovery_budget import page_payload

    cfg = Settings(
        _env_file=None,
        crawl_max_records_per_source=3,
        http_min_request_interval_seconds=0,
        http_max_retries=0,
    )
    con = connector(
        "pncp", cfg, lambda request: httpx.Response(200, json=page_payload(1, 10, "pncp", total=2))
    )

    async def unavailable(_registry):
        return ConnectorResult(data=[], availability=DataAvailability.TEMPORARY_ERROR)

    con.fetch_price_registry_units = unavailable
    request = PipelineRequest(connector="pncp", mode="price_registries", process_documents=False)
    result = await IngestionPipeline(
        cfg, session_factory=database.sessions, connectors={"pncp": con}
    ).run(request)
    async with database.sessions() as session:
        run = await session.get(CrawlRun, result.run_id)
        assert (await session.get(CollectionBatch, run.batch_id)).status == "failed"
    assert await latest_limited_batch(database.sessions, request.as_json()) is None
