"""Real replay/snapshot/write admission with concurrent official-source transports."""

import asyncio

import httpx
import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.connectors.http import AsyncHTTPClient
from app.database import write_session
from app.models import CollectionWorkItem, CrawlRun, CrawlRunStatus, JobLease, Procurement
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest

from .test_pipeline import MultiRecordConnector, _procurement


@pytest.mark.parametrize("concurrency", [1, 4, 20])
async def test_sources_fill_independent_http_budgets_and_commit_exact_work(database, concurrency):
    active = {"pncp": 0, "compras_gov": 0}
    peak = dict(active)
    full, release = asyncio.Event(), asyncio.Event()

    async def handler(request):
        source = request.url.host.split(".")[0]
        active[source] += 1
        peak[source] = max(peak[source], active[source])
        if all(count == concurrency for count in active.values()):
            full.set()
        try:
            await release.wait()
            return httpx.Response(200, json={"official": str(request.url)})
        finally:
            active[source] -= 1

    settings = Settings(
        _env_file=None,
        http_max_concurrency=concurrency,
        http_min_request_interval_seconds=0,
        http_max_retries=0,
    )
    clients = []

    class Connector(MultiRecordConnector):
        async def fetch_procurement(self, external_id):
            await self._http.request_json("GET", f"https://{self.name}.test/detail/{external_id}")
            return await super().fetch_procurement(external_id)

        async def fetch_items(self, external_id):
            await self._http.request_json("GET", f"https://{self.name}.test/items/{external_id}")
            return await super().fetch_items(external_id)

        async def fetch_documents(self, external_id):
            await self._http.request_json(
                "GET", f"https://{self.name}.test/documents/{external_id}"
            )
            return await super().fetch_documents(external_id)

    connectors = {}
    for offset, source in enumerate(active):
        rows = [
            _procurement(source, f"{source}-{index}").model_copy(
                update={
                    "pncp_control_number": f"00000000000191-1-{offset * 100 + index:06}/2026",
                    "purchase_number": str(offset * 100 + index),
                }
            )
            for index in range(1, concurrency + 1)
        ]
        connector = Connector(source, rows)
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        clients.append(client)
        connector._http = AsyncHTTPClient(settings, client=client)
        connectors[source] = connector
    pipeline = IngestionPipeline(settings, session_factory=database.sessions, connectors=connectors)
    running = asyncio.create_task(pipeline.run(PipelineRequest(process_documents=False)))
    try:
        await asyncio.wait_for(full.wait(), timeout=30)
        assert sum(active.values()) == 2 * concurrency
        # HTTP overlap occurs before any domain transaction, through the real
        # SQLite checkpoint queue, rather than an isolated semaphore test.
        async with database.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(Procurement)) == 0
        release.set()
        result = await asyncio.wait_for(running, timeout=60)
    finally:
        release.set()
        if not running.done():
            running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        for client in clients:
            await client.aclose()
    assert peak == {"pncp": concurrency, "compras_gov": concurrency}
    assert active == {"pncp": 0, "compras_gov": 0}
    assert result.status is CrawlRunStatus.COMPLETED
    assert result.records_created == 2 * concurrency
    assert {key: value["records_created"] for key, value in result.source_results.items()} == peak
    async with database.sessions() as session:
        work = (
            await session.scalars(
                select(CollectionWorkItem).where(
                    CollectionWorkItem.kind == "record",
                )
            )
        ).all()
        assert len(work) == 2 * concurrency and all(row.status == "succeeded" for row in work)
        run = await session.get_one(CrawlRun, result.run_id)
        assert all(run.cursor["parallel"][source]["http_peak"] == concurrency for source in active)


@pytest.mark.parametrize("ending", ["cancel", "lease_lost"])
async def test_cancel_parallel_preparations_joins_requests_and_keeps_pending_work(database, ending):
    entered, block = asyncio.Event(), asyncio.Event()
    active = 0

    async def handler(request):
        nonlocal active
        active += 1
        if active == 4:
            entered.set()
        try:
            await block.wait()
            return httpx.Response(200, json={})
        finally:
            active -= 1

    settings = Settings(_env_file=None, http_max_concurrency=4, http_min_request_interval_seconds=0)
    rows = [
        _procurement("pncp", str(index)).model_copy(
            update={
                "pncp_control_number": f"00000000000191-1-{index:06}/2026",
                "purchase_number": str(index),
            }
        )
        for index in range(1, 9)
    ]

    class Connector(MultiRecordConnector):
        async def fetch_procurement(self, external_id):
            await self._http.request_json("GET", f"https://pncp.test/{external_id}")
            return await super().fetch_procurement(external_id)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = Connector("pncp", rows)
        connector._http = AsyncHTTPClient(settings, client=client)
        pipeline = IngestionPipeline(
            settings, session_factory=database.sessions, connectors={"pncp": connector}
        )
        request = PipelineRequest(connector="pncp", process_documents=False)
        run = await pipeline.create_run(request)
        task = asyncio.create_task(pipeline.run(request, run_id=run.id))
        try:
            await asyncio.wait_for(entered.wait(), timeout=15)
            async with write_session(database.sessions) as session:
                stored = await session.get_one(CrawlRun, run.id)
                if ending == "cancel":
                    stored.cancel_requested = True
                else:
                    lease = await session.get_one(JobLease, "crawl:pncp:MA")
                    lease.version += 1
                    lease.owner_id = "replacement"
                    stored.status = CrawlRunStatus.COMPLETED
            if ending == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                block.set()
                await asyncio.wait_for(task, timeout=15)
            assert active == 0
            async with database.sessions() as session:
                assert await session.scalar(select(func.count()).select_from(Procurement)) == 0
                assert (await session.get_one(CrawlRun, run.id)).status is (
                    CrawlRunStatus.CANCELLED if ending == "cancel" else CrawlRunStatus.COMPLETED
                )
                if ending == "lease_lost":
                    assert (
                        await session.get_one(JobLease, "crawl:pncp:MA")
                    ).owner_id == "replacement"
                statuses = (
                    await session.scalars(
                        select(CollectionWorkItem.status).where(
                            CollectionWorkItem.kind == "record",
                        )
                    )
                ).all()
                assert statuses and set(statuses) == {"pending"}
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
