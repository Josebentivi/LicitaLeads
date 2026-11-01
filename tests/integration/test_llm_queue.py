"""Durable optional analysis, evidence guards, cancellation and accounting."""

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import delete, func, select

from app.config import Settings
from app.database import write_session
from app.models import (
    CrawlRun,
    CrawlRunStatus,
    Document,
    DocumentChunk,
    ExtractionStatus,
    JobLease,
    LLMAttempt,
    LLMBatch,
    LLMDemand,
    LLMProviderState,
    LLMResult,
    LLMWorkItem,
    ProcurementEvent,
)
from app.services.llm.providers import LLMReply
from app.services.llm.queue import (
    LLM_LEASE,
    LLMQueueService,
    cancel_batch,
    cancel_for_run,
    enqueue_document,
    next_request_at,
    queue_status,
    retry_for_run,
)

from .test_document_processing import _pending_document

TEXT = "Ata oficial: a proposta foi desclassificada por ausência de documento."
ANALYSIS = {
    "event_type": "DISQUALIFIED",
    "evidence_quotes": [{"text": TEXT}],
    "confidence": 0.9,
    "requires_manual_review": True,
}


@pytest.fixture
def settings(tmp_path):
    return Settings(
        app_env="test",
        llm_enabled=True,
        llm_provider="openai",
        llm_model="gpt-6-luna",
        llm_api_key="test",
        llm_min_chunk_characters=1,
        llm_max_output_tokens=2000,
        document_storage_path=tmp_path / "docs",
        raw_data_storage_path=tmp_path / "raw",
    )


class Provider:
    def __init__(self, reply=None, action=None):
        self.reply = reply or LLMReply(
            input_tokens=1_000_000, output_tokens=1_000_000, analyses=[ANALYSIS]
        )
        self.calls = 0
        self.action = action

    async def request(self, text, pages):
        self.calls += 1
        if self.action:
            await self.action()
        return self.reply

    async def aclose(self):
        pass


async def seed(database, settings, *, count=1, run=True):
    document_id = await _pending_document(database)
    async with write_session(database.sessions) as session:
        doc = await session.get_one(Document, document_id)
        doc.file_hash = hashlib.sha256(TEXT.encode()).hexdigest()
        doc.extraction_status = ExtractionStatus.EXTRACTED
        doc.extracted_text = TEXT
        parent = (
            CrawlRun(
                connector="pncp", filters={"mode": "procurements"}, status=CrawlRunStatus.COMPLETED
            )
            if run
            else None
        )
        if parent:
            session.add(parent)
        for i in range(count):
            session.add(
                DocumentChunk(
                    document_id=doc.id,
                    sequence=i,
                    text=TEXT,
                    start_offset=i * 100,
                    end_offset=i * 100 + len(TEXT),
                )
            )
        await session.flush()
        await enqueue_document(session, doc, settings, run_id=parent.id if parent else None)
        return document_id, parent.id if parent else None


async def test_persisted_queue_cost_evidence_and_idempotency(database, settings):
    doc_id, run_id = await seed(database, settings)
    provider = Provider()
    assert (
        await LLMQueueService(settings, session_factory=database.sessions, provider=provider).drain(
            wait=False
        )
        == 1
    )
    async with database.sessions() as session:
        work = await session.scalar(select(LLMWorkItem))
        attempt = await session.scalar(select(LLMAttempt))
        event = await session.scalar(select(ProcurementEvent))
        assert work.status == "succeeded" and not work.reused
        assert attempt.cost_usd == Decimal("0.60")
        assert event.source == "llm" and event.requires_manual_review
        status = await queue_status(session, settings, run_id=run_id)
        assert status["succeeded"] == 1 and Decimal(status["known_cost_usd"]) == Decimal("0.6")
        doc = await session.get_one(Document, doc_id)
        assert doc.extraction_attempts == 0 and doc.extraction_status is ExtractionStatus.EXTRACTED
    provider2 = Provider()
    assert (
        await LLMQueueService(
            settings, session_factory=database.sessions, provider=provider2
        ).drain(wait=False)
        == 0
    )
    assert provider2.calls == 0


async def test_systemic_error_blocks_backlog_and_explicit_retry_releases(database, settings):
    _, run_id = await seed(database, settings, count=8)
    reply = LLMReply(
        diagnostic={
            "error": True,
            "status_http": 400,
            "code": "unsupported_parameter",
            "param": "max_tokens",
        },
        systemic=True,
    )
    provider = Provider(reply)
    assert (
        await LLMQueueService(settings, session_factory=database.sessions, provider=provider).drain(
            wait=False
        )
        == 1
    )
    assert provider.calls == 1
    async with database.sessions() as session:
        assert (await session.scalar(select(LLMProviderState))).blocked_at
        status = await queue_status(session, settings, run_id=run_id)
        assert status["blocked"] == 7 and status["failed"] == 1
        assert status["unknown_cost_attempts"] == 1
    async with write_session(database.sessions) as session:
        assert await retry_for_run(session, run_id, settings) == 8
        assert (await session.scalar(select(LLMProviderState))).blocked_at is None


@pytest.mark.parametrize("valid", [True, False])
async def test_empty_result_cached_invalid_evidence_still_costs(database, settings, valid):
    doc_id, _ = await seed(database, settings)
    reply = LLMReply(
        analyses=[]
        if valid
        else [{**ANALYSIS, "evidence_quotes": [{"text": "Frase que não existe no documento"}]}],
        input_tokens=100,
        output_tokens=20,
    )
    await LLMQueueService(
        settings, session_factory=database.sessions, provider=Provider(reply)
    ).drain(wait=False)
    async with write_session(database.sessions) as session:
        attempt = await session.scalar(select(LLMAttempt))
        work = await session.scalar(select(LLMWorkItem))
        assert attempt.cost_usd == Decimal("0.00002")
        assert work.status == ("succeeded" if valid else "failed")
        assert await session.scalar(select(func.count()).select_from(LLMResult)) == int(valid)
        if valid:
            # A fresh extraction/chunk identity can reuse the same validated empty response.
            await session.execute(delete(DocumentChunk).where(DocumentChunk.document_id == doc_id))
            session.add(
                DocumentChunk(
                    document_id=doc_id, sequence=0, text=TEXT, start_offset=0, end_offset=len(TEXT)
                )
            )
            await session.flush()
            await enqueue_document(session, await session.get_one(Document, doc_id), settings)
    if valid:
        provider = Provider()
        await LLMQueueService(settings, session_factory=database.sessions, provider=provider).drain(
            wait=False
        )
        assert provider.calls == 0
        async with database.sessions() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(LLMWorkItem)
                    .where(LLMWorkItem.reused.is_(True))
                )
                == 1
            )


@pytest.mark.parametrize("change", ["inactive", "content", "ancestor"])
async def test_version_and_ancestor_rechecked_after_network(database, settings, change):
    doc_id, _ = await seed(database, settings)

    async def alter():
        async with write_session(database.sessions) as session:
            doc = await session.get_one(Document, doc_id)
            if change == "inactive":
                doc.is_active = False
            elif change == "content":
                doc.file_hash = "b" * 64
            else:
                parent = Document(
                    procurement_id=doc.procurement_id,
                    original_url="https://pncp.gov.br/archive.zip",
                    is_active=False,
                )
                session.add(parent)
                await session.flush()
                doc.parent_document_id = parent.id

    await LLMQueueService(
        settings, session_factory=database.sessions, provider=Provider(action=alter)
    ).drain(wait=False)
    async with database.sessions() as session:
        assert (await session.scalar(select(LLMWorkItem))).status == "obsolete"
        assert await session.scalar(select(func.count()).select_from(ProcurementEvent)) == 0
        assert (await session.scalar(select(LLMAttempt))).cost_usd == Decimal("0.6")


async def test_cancel_inflight_and_shared_demand(database, settings):
    doc_id, run_id = await seed(database, settings)
    async with write_session(database.sessions) as session:
        batch = LLMBatch(label="Outra coleta", cancelled=False)
        session.add(batch)
        await session.flush()
        work = await session.scalar(select(LLMWorkItem))
        session.add(LLMDemand(batch_id=batch.id, work_id=work.id))
        second = batch.id
        assert await cancel_for_run(session, run_id)
    # The shared request continues for the remaining live demand.
    await LLMQueueService(settings, session_factory=database.sessions, provider=Provider()).drain(
        wait=False
    )
    async with database.sessions() as session:
        assert (await session.scalar(select(LLMWorkItem))).status == "succeeded"
    async with write_session(database.sessions) as session:
        await cancel_batch(session, await session.get_one(LLMBatch, second))


async def test_cancelling_live_request_interrupts_transport(database, settings):
    _, run_id = await seed(database, settings)
    started, stopped = asyncio.Event(), asyncio.Event()

    async def hold():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    task = asyncio.create_task(
        LLMQueueService(
            settings, session_factory=database.sessions, provider=Provider(action=hold)
        ).drain(wait=False)
    )
    await asyncio.wait_for(started.wait(), 2)
    async with write_session(database.sessions) as session:
        await cancel_for_run(session, run_id)
    await asyncio.wait_for(task, 3)
    assert stopped.is_set()
    async with database.sessions() as session:
        assert (await session.scalar(select(LLMWorkItem))).status == "cancelled"
        assert await session.scalar(select(func.count()).select_from(ProcurementEvent)) == 0


async def test_hourly_budget_is_global_rolling_and_durable(database, settings):
    now = datetime.now(UTC)
    async with write_session(database.sessions) as session:
        session.add_all(
            [
                LLMAttempt(
                    started_at=now - timedelta(seconds=3590 - i / 1000),
                    model="other-model",
                    input_price=Decimal("0.1"),
                    output_price=Decimal("0.5"),
                    status="failed",
                )
                for i in range(500)
            ]
        )
    async with database.sessions() as session:
        due = await next_request_at(session, settings, now)
        assert due > now and due <= now + timedelta(seconds=10)
        assert await next_request_at(
            session, settings, now + timedelta(seconds=11)
        ) == now + timedelta(seconds=11)
    # Pacing still applies when below 500 in a fresh worker/session.
    async with write_session(database.sessions) as session:
        session.add(
            LLMAttempt(
                started_at=now + timedelta(seconds=12),
                model=settings.llm_model,
                input_price=Decimal("0.1"),
                output_price=Decimal("0.5"),
            )
        )
    async with database.sessions() as session:
        assert await next_request_at(
            session, settings, now + timedelta(seconds=12)
        ) == now + timedelta(seconds=19.2)


async def test_transient_retry_restart_and_prices_are_snapshotted(database, settings):
    await seed(database, settings, count=2)
    now = [datetime.now(UTC)]
    first = LLMReply(
        diagnostic={"error": True, "status_http": 429}, transient=True, retry_seconds=60
    )
    first_provider = Provider(first)
    await LLMQueueService(
        settings, session_factory=database.sessions, provider=first_provider, clock=lambda: now[0]
    ).drain(wait=False)
    assert first_provider.calls == 1
    async with database.sessions() as session:
        work = await session.scalar(select(LLMWorkItem))
        assert work.status == "pending" and work.next_attempt_at == now[0] + timedelta(seconds=60)
    now[0] += timedelta(seconds=61)
    changed = settings.model_copy(
        update={
            "llm_input_price_per_million_usd": Decimal("1"),
            "llm_output_price_per_million_usd": Decimal("2"),
        }
    )
    await LLMQueueService(
        changed, session_factory=database.sessions, provider=Provider(), clock=lambda: now[0]
    ).drain(wait=False)
    async with database.sessions() as session:
        attempts = list(
            (await session.scalars(select(LLMAttempt).order_by(LLMAttempt.started_at))).all()
        )
        assert attempts[0].input_price == Decimal("0.1") and attempts[0].cost_usd is None
        assert attempts[1].cost_usd == Decimal("3")


async def test_lost_lease_cannot_apply_or_release_new_owner(database, settings):
    await seed(database, settings)

    async def steal():
        async with write_session(database.sessions) as session:
            lease = await session.get_one(JobLease, LLM_LEASE)
            lease.owner_id = "new-owner"
            lease.version += 1

    with pytest.raises(asyncio.CancelledError):
        await LLMQueueService(
            settings, session_factory=database.sessions, provider=Provider(action=steal)
        ).drain(wait=False)
    async with database.sessions() as session:
        assert (await session.get_one(JobLease, LLM_LEASE)).owner_id == "new-owner"
        assert await session.scalar(select(func.count()).select_from(ProcurementEvent)) == 0


async def test_backfill_is_prospective_bounded_and_api_retry(
    database, settings, api_client, monkeypatch
):
    _, run_id = await seed(database, settings, count=3)
    monkeypatch.setattr("app.api.routes.crawls.get_settings", lambda: settings)
    service = LLMQueueService(
        settings.model_copy(update={"llm_max_chunks_per_document": 2}),
        session_factory=database.sessions,
    )
    result = await service.backfill()
    assert result["documents"] == 1 and result["analyses"] == 2
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(LLMAttempt)) == 0
    assert (await api_client.get(f"/api/crawls/{run_id}")).json()["llm"]["total"] == 3
    # Healthy pending work does not need a retry.
    assert (await api_client.post(f"/api/crawls/{run_id}/retry-llm")).status_code == 409
    async with write_session(database.sessions) as session:
        (await session.scalar(select(LLMWorkItem))).status = "failed"
    assert (await api_client.post(f"/api/crawls/{run_id}/retry-llm")).status_code == 202
    # Reconfiguring a large batch prepares each document once, rather than
    # selecting every chunk again for each old work item (quadratic SQL).
    from app.services.llm import queue

    original_enqueue, calls = queue.enqueue_document, []

    async def tracked_enqueue(*args, **kwargs):
        calls.append(True)
        return await original_enqueue(*args, **kwargs)

    monkeypatch.setattr(queue, "enqueue_document", tracked_enqueue)
    async with write_session(database.sessions) as session:
        selected = settings.model_copy(update={"llm_model": "updated-model"})
        assert await retry_for_run(session, run_id, selected) == 3
    assert len(calls) == 1


async def test_single_worker_and_restart_after_interruption(database, settings):
    await seed(database, settings)
    started = asyncio.Event()

    async def hold():
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(
        LLMQueueService(
            settings, session_factory=database.sessions, provider=Provider(action=hold)
        ).drain(wait=False)
    )
    await asyncio.wait_for(started.wait(), 2)
    other = Provider()
    assert (
        await LLMQueueService(settings, session_factory=database.sessions, provider=other).drain(
            wait=False
        )
        == 0
    )
    assert other.calls == 0
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    def clock():
        return datetime.now(UTC) + timedelta(seconds=10)

    assert (
        await LLMQueueService(
            settings, session_factory=database.sessions, provider=Provider(), clock=clock
        ).drain(wait=False)
        == 1
    )
    async with database.sessions() as session:
        assert (await session.scalar(select(LLMWorkItem))).status == "succeeded"
        assert (
            await session.scalar(select(LLMAttempt).order_by(LLMAttempt.started_at))
        ).status == "interrupted"


async def test_501st_request_is_not_sent(database, settings):
    await seed(database, settings)
    now = datetime.now(UTC)
    async with write_session(database.sessions) as session:
        session.add_all(
            [
                LLMAttempt(
                    started_at=now - timedelta(minutes=30),
                    model="test",
                    input_price=Decimal("0.1"),
                    output_price=Decimal("0.5"),
                    finished_at=now,
                    status="failed",
                )
                for _ in range(500)
            ]
        )
    provider = Provider()
    assert (
        await LLMQueueService(settings, session_factory=database.sessions, provider=provider).drain(
            wait=False
        )
        == 0
    )
    assert provider.calls == 0


async def test_activity_transition_a_b_a_invalidates_pending_analysis(database, settings):
    from app.models import FieldChange

    doc_id, _ = await seed(database, settings)
    async with write_session(database.sessions) as session:
        doc = await session.get_one(Document, doc_id)
        session.add_all(
            [
                FieldChange(
                    entity_type="document",
                    entity_id=doc_id,
                    parent_type="procurement",
                    parent_id=doc.procurement_id,
                    field_name="is_active",
                    previous_value=a,
                    received_value=b,
                    decision="applied",
                    applied=True,
                    source="pncp",
                )
                for a, b in [(True, False), (False, True)]
            ]
        )
    provider = Provider()
    assert (
        await LLMQueueService(settings, session_factory=database.sessions, provider=provider).drain(
            wait=False
        )
        == 0
    )
    async with database.sessions() as session:
        assert (await session.scalar(select(LLMWorkItem))).status == "obsolete"


async def test_web_queue_and_cancel_preserve_extraction(
    database, settings, api_client, monkeypatch
):
    doc_id, run_id = await seed(database, settings)
    monkeypatch.setattr("app.web.get_settings", lambda: settings)
    monkeypatch.setattr("app.api.routes.crawls.get_settings", lambda: settings)
    response = await api_client.get("/crawls/progress")
    assert response.status_code == 200 and "fila independente" in response.text
    assert "US$" in response.text and "500 chamadas/hora" in response.text
    assert (await api_client.post(f"/api/crawls/{run_id}/cancel")).status_code == 202
    async with database.sessions() as session:
        assert (
            await session.get_one(Document, doc_id)
        ).extraction_status is ExtractionStatus.EXTRACTED
        assert (await session.scalar(select(LLMWorkItem))).status == "cancelled"


async def test_demand_reuse_and_new_prompt_or_model_requires_new_work(database, settings):
    doc_id, _ = await seed(database, settings)
    await LLMQueueService(
        settings, session_factory=database.sessions, provider=Provider(LLMReply(analyses=[]))
    ).drain(wait=False)
    async with write_session(database.sessions) as session:
        batch = LLMBatch(label="Repetição", cancelled=False)
        session.add(batch)
        await session.flush()
        assert (
            await enqueue_document(
                session, await session.get_one(Document, doc_id), settings, batch=batch
            )
            == 1
        )
        await session.flush()
        assert (
            await session.scalar(select(LLMDemand).where(LLMDemand.batch_id == batch.id))
        ).reused
        changed = settings.model_copy(update={"llm_model": "other-model"})
        assert (
            await enqueue_document(
                session, await session.get_one(Document, doc_id), changed, batch=batch
            )
            == 1
        )
        assert await session.scalar(select(func.count()).select_from(LLMWorkItem)) == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("llm_max_requests_per_hour", 0),
        ("llm_input_price_per_million_usd", "-0.1"),
        ("llm_output_price_per_million_usd", "NaN"),
    ],
)
def test_invalid_budget_configuration_rejected(field, value):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(**{field: value})
