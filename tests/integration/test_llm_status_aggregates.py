"""Queue summaries preserve accounting while transferring only grouped rows."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import event, insert, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import OperationalError

from app.config import Settings
from app.database import write_session
from app.models import (
    CrawlRun,
    CrawlRunStatus,
    LLMAttempt,
    LLMBatch,
    LLMDemand,
    LLMProviderState,
    LLMWorkItem,
)
from app.services.llm.queue import config_key
from app.services.llm.status import queue_statuses

from .test_document_processing import _pending_document


async def test_scopes_shared_work_costs_and_fresh_cancellation(database):
    document_id = await _pending_document(database)
    settings = Settings(_env_file=None)
    now = datetime.now(UTC)
    async with write_session(database.sessions) as session:
        runs = [CrawlRun(connector="pncp", status=CrawlRunStatus.COMPLETED) for _ in range(3)]
        session.add_all(runs)
        await session.flush()
        batches = [LLMBatch(crawl_run_id=r.id, label=str(i)) for i, r in enumerate(runs[:2])]
        session.add_all(batches)
        await session.flush()
        works = []
        for i, (status, config) in enumerate(
            [("succeeded", config_key(settings)), ("pending", "blocked"), ("failed", "old")]
        ):
            work = LLMWorkItem(
                job_key=uuid4().hex,
                cache_key=uuid4().hex,
                config_key=config,
                document_id=document_id,
                snapshot={},
                status=status,
                reused=i == 0,
                next_attempt_at=now + timedelta(minutes=5) if i == 1 else None,
            )
            session.add(work)
            works.append(work)
        await session.flush()
        session.add_all(
            [
                LLMDemand(batch_id=batches[0].id, work_id=works[0].id),
                LLMDemand(batch_id=batches[1].id, work_id=works[0].id),
                LLMDemand(batch_id=batches[0].id, work_id=works[1].id),
                LLMDemand(batch_id=batches[1].id, work_id=works[2].id),
                LLMProviderState(
                    config_key="blocked", blocked_at=now, error={"code": "invalid_model"}
                ),
            ]
        )
        for batch, cost in [
            (batches[0], Decimal("0.000000000000001")),
            (batches[0], Decimal("0.000000000000001")),
            (batches[0], None),
            (batches[1], Decimal("0.6")),
        ]:
            session.add(
                LLMAttempt(
                    batch_id=batch.id,
                    model="test",
                    input_price=Decimal("0.1"),
                    output_price=Decimal("0.5"),
                    cost_usd=cost,
                )
            )
        run_ids = [r.id for r in runs]
        pending_id = works[1].id
    async with database.sessions() as session:
        result = await queue_statuses(session, settings, run_ids=run_ids, include_global=True)
        total, first, second = result[None], result[run_ids[0]], result[run_ids[1]]
        assert result[run_ids[2]] is None
        assert total["total"] == 3 and total["reused"] == 2
        assert total["succeeded"] == total["pending"] == total["failed"] == 1
        assert first["total"] == 2 and second["total"] == 2
        assert first["blocked"] == first["configuration_changed"] == 1
        assert total["diagnostics"] == [{"code": "invalid_model"}]
        assert first["unknown_cost_attempts"] == 1 and first["requests"] == 3
        assert Decimal(first["known_cost_usd"]) == Decimal("0.000000000000002")
        assert Decimal(total["known_cost_usd"]) == Decimal("0.600000000000002")
        assert datetime.fromisoformat(first["next_request_at"]) == now + timedelta(minutes=5)
        assert total["retry_available"]
        assert not any(isinstance(obj, LLMWorkItem) for obj in session.identity_map.values())
    async with write_session(database.sessions) as session:
        work = await session.get_one(LLMWorkItem, pending_id)
        work.status = "cancelled"
    async with database.sessions() as session:
        result = await queue_statuses(session, settings, include_global=True)
        assert result[None]["pending"] == 0 and result[None]["cancelled"] == 1


async def test_empty_batches_are_available_but_missing_runs_are_not(database):
    settings = Settings(_env_file=None)
    async with database.sessions() as session:
        assert await queue_statuses(session, settings, include_global=True) == {None: None}
    async with write_session(database.sessions) as session:
        batch = LLMBatch(label="Empty", cancelled=True)
        session.add(batch)
    async with database.sessions() as session:
        result = await queue_statuses(session, settings, run_ids=[uuid4()], include_global=True)
        assert result[None]["total"] == result[None]["requests"] == 0
        assert result[None]["known_cost_usd"] == "0"
        assert result[None]["batches"][0]["cancelled"]


async def test_twelve_thousand_jobs_and_twenty_batches_use_constant_queries(database):
    document_id = await _pending_document(database)
    settings = Settings(_env_file=None)
    run_ids, batch_ids = [uuid4() for _ in range(20)], [uuid4() for _ in range(20)]
    async with write_session(database.sessions) as session:
        await session.execute(
            insert(CrawlRun),
            [dict(id=i, connector="pncp", status=CrawlRunStatus.COMPLETED) for i in run_ids],
        )
        await session.execute(
            insert(LLMBatch),
            [
                dict(id=b, crawl_run_id=r, label=str(i))
                for i, (b, r) in enumerate(zip(batch_ids, run_ids, strict=True))
            ],
        )
        jobs, demands = [], []
        for i in range(12_000):
            work_id = uuid4()
            jobs.append(
                dict(
                    id=work_id,
                    job_key=uuid4().hex,
                    cache_key=uuid4().hex,
                    config_key=config_key(settings),
                    document_id=document_id,
                    snapshot={"large_field": "x" * 100},
                    status="cancelled",
                )
            )
            demands.append(dict(id=uuid4(), batch_id=batch_ids[i % 20], work_id=work_id))
        await session.execute(insert(LLMWorkItem), jobs)
        await session.execute(insert(LLMDemand), demands)
    statements = []

    def observed(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)
        # Every query uses SQLAlchemy constructs valid for PostgreSQL as well.
        if context.compiled is not None:
            context.compiled.statement.compile(dialect=postgresql.dialect())

    event.listen(database.engine.sync_engine, "before_cursor_execute", observed)
    try:
        async with database.sessions() as session:
            one = await queue_statuses(session, settings, run_ids=run_ids[:1], include_global=True)
            count = len(statements)
            statements.clear()
            twenty = await queue_statuses(session, settings, run_ids=run_ids, include_global=True)
            assert len(statements) == count == 8
            assert one[None]["cancelled"] == twenty[None]["cancelled"] == 12_000
            assert len(twenty[None]["batches"]) == 20
            assert all(twenty[run_id]["cancelled"] == 600 for run_id in run_ids)
            assert not any(isinstance(obj, LLMWorkItem) for obj in session.identity_map.values())
            assert not any("llm_work_items.snapshot" in sql for sql in statements)
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", observed)


async def test_list_and_monitor_fetch_all_scopes_once(api_client, database, monkeypatch):
    from app.api.routes import crawls
    from app.services.llm import queue

    async with write_session(database.sessions) as session:
        for _ in range(3):
            run = CrawlRun(connector="pncp", status=CrawlRunStatus.RUNNING)
            session.add(run)
            await session.flush()
            session.add(LLMBatch(crawl_run_id=run.id, label="Active"))
    actual = queue.queue_statuses
    calls = []

    async def summarize(*args, **kwargs):
        calls.append(kwargs)
        return await actual(*args, **kwargs)

    monkeypatch.setattr(crawls, "queue_statuses", summarize)
    monkeypatch.setattr(queue, "queue_statuses", summarize)
    response = await api_client.get("/api/crawls")
    assert response.status_code == 200 and len(response.json()["items"]) == 3
    assert len(calls) == 1 and len(calls[0]["run_ids"]) == 3
    calls.clear()
    response = await api_client.get("/crawls/progress")
    assert response.status_code == 200
    assert len(calls) == 1 and len(calls[0]["run_ids"]) == 3
    assert calls[0]["include_global"]


async def test_database_errors_hide_bound_document_contents(database):
    async with database.engine.connect() as connection:
        with pytest.raises(OperationalError) as error:
            await connection.execute(
                text("SELECT :document FROM missing_table"),
                {"document": "Bearer api-private-secret document contents"},
            )
    assert "api-private-secret" not in str(error.value)
    assert "document contents" not in str(error.value)
