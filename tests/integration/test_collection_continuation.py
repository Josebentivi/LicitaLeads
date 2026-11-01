"""Batches survive execution boundaries without dropping a fetched page tail."""

from datetime import date

import pytest
from sqlalchemy import func, select

from app.api.routes import crawls
from app.config import Settings
from app.connectors.base import (
    ConnectorResult,
    DataAvailability,
    PaginationInfo,
    RawProcurementItem,
)
from app.models import (
    CollectionBatch,
    CollectionWorkItem,
    CrawlRun,
    CrawlRunStatus,
    Procurement,
    ProcurementItem,
)
from app.services.ingestion.collection_batches import latest_limited_batch
from app.services.ingestion.continuation import create_continuation, pipeline_request_from_filters
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest

from .test_pipeline import MultiRecordConnector, _procurement


class CountingConnector(MultiRecordConnector):
    def __init__(self, records):
        super().__init__("pncp", records)
        self.discoveries = 0
        self.visits = []
        self.broken = None

    async def discover_procurements(self, filters):
        self.discoveries += 1
        return await super().discover_procurements(filters)

    async def fetch_procurement(self, external_id):
        self.visits.append(external_id)
        if external_id == self.broken:
            raise ValueError("test record failure")
        return await super().fetch_procurement(external_id)


def records():
    return [
        _procurement("pncp", f"id-{index}").model_copy(
            update={
                "pncp_control_number": f"00000000000191-1-{index:06}/2026",
                "purchase_number": str(index),
            }
        )
        for index in range(1, 4)
    ]


async def test_three_bounded_executions_consume_one_saved_page_and_keep_dates(database):
    settings = Settings(_env_file=None, crawl_max_records_per_source=1)
    connector = CountingConnector(records())
    request = PipelineRequest(connector="pncp", process_documents=False, days=365)
    first = await IngestionPipeline(
        settings, session_factory=database.sessions, connectors={"pncp": connector}
    ).run(request)
    assert first.records_created == 1
    previous = first.run_id
    batch_id = None
    fixed_dates = None
    for _ in range(2):
        async with database.sessions() as session:
            child, filters = await create_continuation(session, previous)
            await session.commit()
        resumed = pipeline_request_from_filters(filters)
        assert isinstance(resumed.start_date, date) and isinstance(resumed.end_date, date)
        assert fixed_dates in {None, (resumed.start_date, resumed.end_date)}
        fixed_dates = (resumed.start_date, resumed.end_date)
        batch_id = child.batch_id
        # A new service object simulates restarting the worker.
        summary = await IngestionPipeline(
            settings, session_factory=database.sessions, connectors={"pncp": connector}
        ).run(resumed, run_id=child.id)
        assert summary.records_created == 1
        previous = summary.run_id
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Procurement)) == 3
        assert (await session.get(CollectionBatch, batch_id)).status == "complete"
        work = (
            await session.scalars(
                select(CollectionWorkItem).where(CollectionWorkItem.kind == "record")
            )
        ).all()
        assert all(row.status == "succeeded" for row in work)
    assert connector.discoveries == 1
    assert connector.visits == ["id-1", "id-2", "id-3"]


async def test_failed_record_requires_explicit_continuation_and_only_it_repeats(database):
    connector = CountingConnector(records())
    connector.broken = "id-2"
    settings = Settings(_env_file=None)
    request = PipelineRequest(connector="pncp", process_documents=False)
    pipeline = IngestionPipeline(
        settings, session_factory=database.sessions, connectors={"pncp": connector}
    )
    first = await pipeline.run(request)
    assert first.status is CrawlRunStatus.PARTIAL
    assert await latest_limited_batch(database.sessions, request.as_json()) is None
    connector.broken = None
    async with database.sessions() as session:
        child, filters = await create_continuation(session, first.run_id)
        await session.commit()
    second = await pipeline.run(pipeline_request_from_filters(filters), run_id=child.id)
    assert second.records_created == 1
    assert sorted(connector.visits[:3]) == ["id-1", "id-2", "id-3"]
    assert connector.visits[3:] == ["id-2"]


@pytest.mark.parametrize("failure", ["temporary_error", "truncated"])
async def test_detail_failure_keeps_valid_items_and_can_continue(
    api_client, database, monkeypatch, failure
):
    class DetailConnector(CountingConnector):
        retry = False

        async def fetch_procurement(self, external_id):
            result = await super().fetch_procurement(external_id)
            if external_id != "id-2" or self.retry:
                return result
            if failure == "temporary_error":
                return ConnectorResult(
                    data=None,
                    availability=DataAvailability.TEMPORARY_ERROR,
                    diagnostic="official detail temporarily unavailable",
                )
            return result.model_copy(
                update={"partial": True, "pagination": PaginationInfo(truncated=True)}
            )

        async def fetch_items(self, external_id):
            return ConnectorResult(
                data=[
                    RawProcurementItem(
                        source=self.name,
                        external_id=f"{external_id}-item",
                        procurement_external_id=external_id,
                        item_number="1",
                        description="Item oficial preservado",
                    )
                ],
                availability=DataAvailability.AVAILABLE,
            )

    connector = DetailConnector(records())
    settings = Settings(_env_file=None)
    pipeline = IngestionPipeline(
        settings, session_factory=database.sessions, connectors={"pncp": connector}
    )
    request = PipelineRequest(connector="pncp", process_documents=False)
    first = await pipeline.run(request)
    assert first.status is CrawlRunStatus.PARTIAL
    async with database.sessions() as session:
        run = await session.get_one(CrawlRun, first.run_id)
        batch_id = run.batch_id
        batch = await session.get_one(CollectionBatch, batch_id)
        assert batch.status == ("failed" if failure == "temporary_error" else "limited")
        work = (
            await session.scalars(
                select(CollectionWorkItem).where(CollectionWorkItem.kind == "record")
            )
        ).all()
        assert {row.payload["external_id"]: row.status for row in work} == {
            "id-1": "succeeded",
            "id-2": "failed" if failure == "temporary_error" else "details_pending",
            "id-3": "succeeded",
        }
        assert await session.scalar(select(func.count()).select_from(ProcurementItem)) == 3
    if failure == "temporary_error":
        assert await latest_limited_batch(database.sessions, request.as_json()) is None

    launched = []
    monkeypatch.setattr(crawls, "launch_continuation", lambda *args: launched.append(args))
    response = await api_client.post(f"/api/crawls/{first.run_id}/continue")
    assert response.status_code == 202 and len(launched) == 1
    async with database.sessions() as session:
        child = await session.get_one(CrawlRun, response.json()["id"])
        assert child.batch_id == batch_id
        resumed = pipeline_request_from_filters(child.filters)
        child_id = child.id
    connector.retry = True
    second = await IngestionPipeline(
        settings, session_factory=database.sessions, connectors={"pncp": connector}
    ).run(resumed, run_id=child_id)
    assert second.status is CrawlRunStatus.COMPLETED
    assert second.records_updated == 1 and second.records_created == 0
    assert sorted(connector.visits[:3]) == ["id-1", "id-2", "id-3"]
    assert connector.visits[3:] == ["id-2"]
    async with database.sessions() as session:
        assert (await session.get_one(CollectionBatch, batch_id)).status == "complete"
        assert await session.scalar(select(func.count()).select_from(ProcurementItem)) == 3


async def test_continue_api_is_async_and_rejects_duplicate_admission(
    api_client, database, monkeypatch
):
    settings = Settings(_env_file=None, crawl_max_records_per_source=1)
    pipeline = IngestionPipeline(
        settings,
        session_factory=database.sessions,
        connectors={"pncp": CountingConnector(records())},
    )
    first = await pipeline.run(PipelineRequest(connector="pncp", process_documents=False))
    launched = []
    monkeypatch.setattr(crawls, "launch_continuation", lambda *args: launched.append(args))
    response = await api_client.post(f"/api/crawls/{first.run_id}/continue")
    assert response.status_code == 202 and len(launched) == 1
    duplicate = await api_client.post(f"/api/crawls/{first.run_id}/continue")
    assert duplicate.status_code == 409
    async with database.sessions() as session:
        run = await session.get(CrawlRun, first.run_id)
        child = await session.get(CrawlRun, response.json()["id"])
        assert run.batch_id == child.batch_id
    assert (
        await api_client.post("/api/crawls/00000000-0000-0000-0000-000000000001/continue")
    ).status_code == 404


async def test_scheduler_automatically_resumes_only_limited_matching_stream(database):
    connector = CountingConnector(records())
    settings = Settings(_env_file=None, crawl_max_records_per_source=1)
    request = PipelineRequest(connector="pncp", process_documents=False, resume_automatically=True)
    pipeline = IngestionPipeline(
        settings, session_factory=database.sessions, connectors={"pncp": connector}
    )
    first, second = await pipeline.run(request), await pipeline.run(request)
    async with database.sessions() as session:
        a, b = await session.get(CrawlRun, first.run_id), await session.get(CrawlRun, second.run_id)
        assert a.batch_id == b.batch_id
    assert connector.discoveries == 1


async def test_crashed_batch_becomes_explicitly_resumable_after_recovery(database):
    from datetime import UTC, datetime, timedelta

    from app.services.ingestion.recovery import reconcile_stale_crawls

    old = datetime.now(UTC) - timedelta(hours=6)
    async with database.sessions() as session, session.begin():
        batch = CollectionBatch(
            stream_key="a" * 64,
            filters={"connector": "pncp", "mode": "procurements", "uf": "MA"},
            status="running",
            cursors={},
        )
        session.add(batch)
        await session.flush()
        run = CrawlRun(
            connector="pncp",
            batch_id=batch.id,
            status=CrawlRunStatus.RUNNING,
            started_at=old,
            heartbeat_at=old,
            created_at=old,
        )
        session.add(run)
        await session.flush()
        batch.last_run_id = run.id
        run_id, batch_id = run.id, batch.id
    assert (
        await reconcile_stale_crawls(session_factory=database.sessions, stale_after_minutes=5) == 1
    )
    async with database.sessions() as session:
        assert (await session.get(CollectionBatch, batch_id)).status == "failed"
        child, _filters = await create_continuation(session, run_id)
        assert child.batch_id == batch_id
