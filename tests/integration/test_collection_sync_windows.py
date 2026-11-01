"""Coverage is a fenced completion fact, not the greatest timestamp in a response."""

import asyncio
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.connectors.base import ConnectorResult, DataAvailability
from app.models import CollectionBatch, CollectionSyncState, CrawlRunStatus
from app.services.ingestion.continuation import create_continuation, pipeline_request_from_filters
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest
from app.services.ingestion.scheduled_collections import prepare_scheduled

from .test_collection_continuation import CountingConnector, records


class Empty(CountingConnector):
    def __init__(self, _records):
        super().__init__(records()[:1])

    async def discover_procurements(self, filters):
        self.discoveries += 1
        return ConnectorResult(data=[], availability=DataAvailability.EMPTY)


def settings(**kwargs):
    return Settings(_env_file=None, timezone="America/Sao_Paulo", **kwargs)


async def test_empty_success_advances_window_with_two_day_overlap_and_weekly_review(database):
    cfg = settings()
    now = datetime(2026, 10, 29, 1, 30, tzinfo=UTC)  # still Oct 28 at the source
    request = PipelineRequest(
        connector="pncp", discovery_kind="update", days=30, process_documents=False
    )
    pipeline = IngestionPipeline(
        cfg, session_factory=database.sessions, connectors={"pncp": Empty([])}
    )
    initial = await prepare_scheduled(database.sessions, cfg, request, now=now)
    assert initial.end_date == date(2026, 10, 28)
    assert initial.start_date == date(2026, 9, 29)
    result = await pipeline.run(initial)
    assert result.status is CrawlRunStatus.COMPLETED
    async with database.sessions() as session:
        state = await session.scalar(select(CollectionSyncState))
        assert state.covered_until == initial.end_date
        completed_at = state.last_reconciliation_at
    next_plan = await prepare_scheduled(
        database.sessions, cfg, request, now=completed_at + timedelta(days=1)
    )
    assert next_plan.sync_context["strategy"] == "incremental"
    assert next_plan.start_date == initial.end_date - timedelta(days=1)
    await pipeline.run(next_plan)
    reviewed = await prepare_scheduled(
        database.sessions, cfg, request, now=completed_at + timedelta(days=8)
    )
    assert reviewed.sync_context["strategy"] == "reconciliation"
    assert (reviewed.end_date - reviewed.start_date).days == 29


async def test_invalid_pncp_contract_row_keeps_coverage_pending_and_retains_valid_contract(
    database,
):
    import httpx

    from app.connectors.http import AsyncHTTPClient
    from app.connectors.pncp import PNCPConnector
    from app.models import Contract, SourceRecord

    cfg = settings(
        pncp_base_url="https://pncp.test/api/consulta/v1",
        pncp_integration_base_url="https://pncp.test/api/pncp/v1",
        http_max_retries=0,
        http_min_request_interval_seconds=0,
    )
    payload = {
        "data": [
            {
                "numeroControlePNCP": "00000000000191-2-000001/2026",
                "numeroContratoEmpenho": "0001",
                "anoContrato": 2026,
                "sequencialContrato": 1,
                "orgaoEntidade": {"cnpj": "00000000000191"},
            },
            {"numeroContratoEmpenho": "sem-identidade"},
        ],
        "totalRegistros": 2,
        "totalPaginas": 1,
        "numeroPagina": 1,
        "paginasRestantes": 0,
    }

    def response(request):
        return httpx.Response(200, json=payload if "consulta" in request.url.path else [])

    original = PipelineRequest(
        connector="pncp",
        mode="contracts",
        discovery_kind="update",
        agency_cnpj="00000000000191",
        process_documents=False,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        connector = PNCPConnector(cfg, http_client=AsyncHTTPClient(cfg, client=client))
        pipeline = IngestionPipeline(
            cfg, session_factory=database.sessions, connectors={"pncp": connector}
        )
        planned = await prepare_scheduled(database.sessions, cfg, original)
        summary = await pipeline.run(planned)
    assert summary.status is not CrawlRunStatus.COMPLETED
    async with database.sessions() as session:
        state = await session.scalar(select(CollectionSyncState))
        assert state.covered_until is None
        assert (await session.get(CollectionBatch, state.pending_batch_id)).status == "failed"
        assert await session.scalar(select(func.count()).select_from(Contract)) == 1
        stored = list((await session.scalars(select(SourceRecord))).all())
        assert any(
            record.raw_payload == payload and record.collection_complete is not True
            for record in stored
        )
    assert await prepare_scheduled(database.sessions, cfg, original) is None


async def test_limited_work_freezes_window_and_only_final_confirmation_advances_it(database):
    cfg = settings(crawl_max_records_per_source=1)
    original = PipelineRequest(
        connector="pncp", discovery_kind="update", days=30, process_documents=False
    )
    pipeline = IngestionPipeline(
        cfg, session_factory=database.sessions, connectors={"pncp": CountingConnector(records())}
    )
    first = await prepare_scheduled(database.sessions, cfg, original)
    summary = await pipeline.run(first)
    assert summary.status is CrawlRunStatus.PARTIAL
    async with database.sessions() as session:
        state = await session.scalar(select(CollectionSyncState))
        batch_id = state.pending_batch_id
        assert state.covered_until is None
        assert (await session.get(CollectionBatch, batch_id)).status == "limited"
    for _ in range(2):
        resumed = await prepare_scheduled(
            database.sessions, cfg, original, now=datetime.now(UTC) + timedelta(days=10)
        )
        assert resumed.continuation_batch_id == batch_id
        assert (resumed.start_date, resumed.end_date) == (first.start_date, first.end_date)
        await pipeline.run(resumed)
    async with database.sessions() as session:
        state = await session.scalar(select(CollectionSyncState))
        assert state.covered_until == first.end_date and state.pending_request is None


async def test_failed_batch_requires_explicit_continuation_and_manual_does_not_advance(database):
    cfg = settings()
    connector = CountingConnector(records()[:1])
    connector.broken = next(iter(connector.records))
    pipeline = IngestionPipeline(
        cfg, session_factory=database.sessions, connectors={"pncp": connector}
    )
    original = PipelineRequest(connector="pncp", discovery_kind="update", process_documents=False)
    planned = await prepare_scheduled(database.sessions, cfg, original)
    failed = await pipeline.run(planned)
    assert await prepare_scheduled(database.sessions, cfg, original) is None
    async with database.sessions() as session:
        state = await session.scalar(select(CollectionSyncState))
        assert state.covered_until is None
    connector.broken = None
    await pipeline.run(original)  # a new manual run cannot certify an automatic window
    async with database.sessions() as session:
        assert (await session.scalar(select(CollectionSyncState))).covered_until is None
        child, filters = await create_continuation(session, failed.run_id)
        await session.commit()
    await pipeline.run(pipeline_request_from_filters(filters), run_id=child.id)
    async with database.sessions() as session:
        assert (await session.scalar(select(CollectionSyncState))).covered_until == planned.end_date


@pytest.mark.parametrize(
    "changes",
    [
        {"uf": "PI"},
        {"modalities": ("concorrencia",)},
        {"unit_code": "42"},
        {"agency_cnpj": "00000000000191"},
        {"days": 60},
    ],
)
async def test_widened_or_changed_filters_create_a_new_bootstrap(database, changes):
    cfg = settings()
    original = PipelineRequest(connector="pncp", discovery_kind="update", process_documents=False)
    await prepare_scheduled(database.sessions, cfg, original)
    other = await prepare_scheduled(database.sessions, cfg, replace(original, **changes))
    assert other.sync_context["strategy"] == "bootstrap"
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(CollectionSyncState)) == 2


async def test_pncp_bootstrap_by_publication_requires_update_catchup(database):
    cfg = settings()
    original = PipelineRequest(connector="pncp", days=30, process_documents=False)
    pipeline = IngestionPipeline(
        cfg, session_factory=database.sessions, connectors={"pncp": Empty([])}
    )
    beginning = datetime.now(UTC) - timedelta(days=10)
    initial = await prepare_scheduled(database.sessions, cfg, original, now=beginning)
    await pipeline.run(initial)
    async with database.sessions() as session:
        state = await session.scalar(select(CollectionSyncState))
        assert state.covered_until is None and state.pending_request["discovery_kind"] == "update"
    catchup = await prepare_scheduled(database.sessions, cfg, original)
    assert catchup.sync_context["phase"] == "catchup"
    assert catchup.start_date == beginning.astimezone(ZoneInfo(cfg.timezone)).date() - timedelta(
        days=1
    )
    assert catchup.start_date > initial.start_date
    await pipeline.run(catchup)
    async with database.sessions() as session:
        assert (await session.scalar(select(CollectionSyncState))).covered_until == catchup.end_date


async def test_cancellation_keeps_coverage_unconfirmed_until_explicit_continuation(database):
    entered = asyncio.Event()
    blocker = asyncio.Event()

    class Paused(CountingConnector):
        async def fetch_procurement(self, identifier):
            entered.set()
            await blocker.wait()
            return await super().fetch_procurement(identifier)

    cfg = settings()
    connector = Paused(records()[:1])
    pipeline = IngestionPipeline(
        cfg, session_factory=database.sessions, connectors={"pncp": connector}
    )
    original = PipelineRequest(connector="pncp", discovery_kind="update", process_documents=False)
    planned = await prepare_scheduled(database.sessions, cfg, original)
    worker = asyncio.create_task(pipeline.run(planned))
    await asyncio.wait_for(entered.wait(), 5)
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)
    async with database.sessions() as session:
        state = await session.scalar(select(CollectionSyncState))
        assert state.covered_until is None
        assert (await session.get(CollectionBatch, state.pending_batch_id)).status == "suspended"
    assert await prepare_scheduled(database.sessions, cfg, original) is None
