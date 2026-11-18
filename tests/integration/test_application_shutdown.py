"""Shutdown admission, durable cancellations and exact lease recovery."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.config import Settings
from app.database import write_session
from app.main import app
from app.models import CollectionBatch, CollectionWorkItem, CrawlRun, CrawlRunStatus, JobLease
from app.models.llm_queue import LLMAttempt, LLMBatch, LLMDemand, LLMWorkItem
from app.repositories.crawls import JobLeaseRepository
from app.runtime_control import OwnedRuntime, read_json
from app.services.application_shutdown import ApplicationShutdown
from app.services.scheduler_control import control_state
from tests.integration.test_llm_queue import seed


@pytest.fixture
def managed(database, tmp_path, monkeypatch):
    settings = Settings(
        _env_file=None,
        database_url=str(database.engine.url),
        raw_data_storage_path=tmp_path / "raw",
        document_storage_path=tmp_path / "docs",
        llm_enabled=True,
        llm_provider="openai",
        llm_model="test",
        llm_api_key="fake",
        llm_min_chunk_characters=1,
    )
    runtime = OwnedRuntime(tmp_path / "application.shutdown.json", settings.database_url)
    for name, value in runtime.environment.items():
        monkeypatch.setenv(name, value)
    service = ApplicationShutdown(settings, factory=database.sessions)
    monkeypatch.setattr(app.state, "shutdown", service, raising=False)
    try:
        yield runtime, service, settings
    finally:
        runtime.close()


async def test_cancel_shared_manual_queue_keeps_results_and_saved_pages(database, managed):
    runtime, service, settings = managed
    doc_id, _ = await seed(database, settings)
    now = datetime.now(UTC)
    async with write_session(database.sessions) as session:
        work = await session.scalar(select(LLMWorkItem))
        work.status = "running"
        manual = LLMBatch(label="Demanda manual compartilhada")
        batch = CollectionBatch(stream_key="frozen", filters={"days": 7}, status="limited")
        run = CrawlRun(
            connector="pncp",
            status=CrawlRunStatus.RUNNING,
            records_created=3,
            errors=[{"message": "diagnóstico anterior da fonte"}],
        )
        session.add_all([manual, batch, run])
        await session.flush()
        run.batch_id, batch.last_run_id = batch.id, run.id
        saved = CollectionWorkItem(
            batch_id=batch.id,
            source="pncp",
            kind="record",
            identity="official",
            payload={"kept": True},
        )
        session.add_all(
            [
                saved,
                LLMDemand(batch_id=manual.id, work_id=work.id),
                LLMAttempt(
                    work_id=work.id,
                    model="test",
                    input_price=Decimal("0.1"),
                    output_price=Decimal("0.5"),
                ),
            ]
        )
        assert await JobLeaseRepository(session).acquire(
            name="scheduler:test",
            owner_id="ours",
            now=now,
            expires_at=now + timedelta(minutes=5),
        )
        session.add(
            JobLease(
                name="other", owner_id="external", version=1, expires_at=now + timedelta(minutes=5)
            )
        )
        run_id, batch_id, work_id = run.id, batch.id, work.id
    replies = await asyncio.gather(service.request(), service.request())
    assert replies[0]["id"] == replies[1]["id"] == runtime.channel.instance
    assert control_state(settings) == {"collections": True, "llm": True}
    await service.cancel_persisted()
    await service.finalize()
    await service.cancel_persisted()
    await service.finalize()
    async with database.sessions() as session:
        run = await session.get_one(CrawlRun, run_id)
        assert run.status is CrawlRunStatus.CANCELLED and run.records_created == 3
        assert run.errors[0]["message"] == "diagnóstico anterior da fonte"
        assert run.cursor["shutdown_id"] == runtime.channel.instance
        assert (await session.get_one(CollectionBatch, batch_id)).status == "suspended"
        assert (await session.scalar(select(CollectionWorkItem))).payload == {"kept": True}
        assert (await session.get_one(LLMWorkItem, work_id)).status == "cancelled"
        assert all(batch.cancelled for batch in await session.scalars(select(LLMBatch)))
        attempt = await session.scalar(select(LLMAttempt))
        assert attempt.status == "interrupted" and attempt.cost_usd is None
        assert await session.get(JobLease, "scheduler:test") is None
        assert (await session.get_one(JobLease, "other")).owner_id == "external"
    assert read_json(runtime.channel.receipt_path)["cleanup_complete"]


async def test_exact_generation_and_recovery_before_admission(database, managed):
    runtime, service, settings = managed
    now = datetime.now(UTC)
    async with write_session(database.sessions) as session:
        leases = JobLeaseRepository(session)
        assert await leases.acquire(
            name="mine", owner_id="first", now=now, expires_at=now + timedelta(seconds=1)
        )
        assert await leases.acquire(
            name="mine",
            owner_id="second",
            now=now + timedelta(seconds=2),
            expires_at=now + timedelta(minutes=5),
        )
    await service.request()
    await service.cancel_persisted()
    async with write_session(database.sessions) as session:
        lease = await session.get_one(JobLease, "mine")
        assert lease.version == 2
        lease.version, lease.owner_id = 3, "new-worker"
    await service.recover()
    async with database.sessions() as session:
        lease = await session.get_one(JobLease, "mine")
        assert lease.owner_id == "new-worker" and lease.version == 3
        assert not await JobLeaseRepository(session).acquire(
            name="new",
            owner_id="blocked",
            now=now,
            expires_at=now + timedelta(minutes=1),
        )
    assert control_state(settings)["llm"]


async def test_local_confirmation_token_idempotency_and_http_admission(api_client, managed):
    runtime, service, _ = managed
    origin = {"Origin": "http://testserver"}
    token = runtime.channel.read("manifest")["token"]
    page = await api_client.get("/application/shutdown")
    assert page.status_code == 200 and token in page.text
    for headers, value in [
        ({}, token),
        ({"Origin": "https://evil.test"}, token),
        (origin, "invalid"),
    ]:
        response = await api_client.post(
            "/api/application/shutdown", json={"token": value}, headers=headers
        )
        assert response.status_code == 403
    assert not runtime.channel.requested
    response = await api_client.post(
        "/api/application/shutdown", json={"token": token}, headers=origin
    )
    assert response.status_code == 202
    again = await api_client.post("/application/shutdown", data={"token": token}, headers=origin)
    assert again.status_code == 202 and "Encerramento solicitado" in again.text
    blocked = await api_client.post("/api/crawls/run", json={"connector": "pncp"}, headers=origin)
    assert blocked.status_code == 503
    assert (await api_client.get("/api/application/shutdown")).json()[
        "id"
    ] == runtime.channel.instance


async def test_remote_request_rejected_without_proxy_header_trust(managed):
    import httpx

    runtime, _, _ = managed
    token = runtime.channel.read("manifest")["token"]
    transport = httpx.ASGITransport(app=app, client=("192.0.2.14", 45123))
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/api/application/shutdown",
            json={"token": token},
            headers={
                "Origin": "http://testserver",
                "X-Forwarded-For": "127.0.0.1",
            },
        )
    assert response.status_code == 403 and not runtime.channel.requested


async def test_unmanaged_status_and_confirmation_page(api_client):
    assert (await api_client.get("/api/application/shutdown")).json()["available"] is False
    page = await api_client.get("/application/shutdown")
    assert "iniciar.bat" in page.text and 'name="token"' not in page.text


async def test_recovery_refuses_different_database(managed):
    runtime, service, _ = managed
    await service.request()
    service.settings = service.settings.model_copy(update={"database_url": "sqlite:///other.db"})
    with pytest.raises(RuntimeError, match="banco efetivo"):
        await service.recover()


async def test_shutdown_interrupts_paid_request_without_inventing_usage(database, managed):
    from app.services.llm.queue import LLMQueueService, queue_status

    _, service, settings = managed
    await seed(database, settings)
    entered, interrupted = asyncio.Event(), asyncio.Event()

    class Provider:
        async def request(self, text, pages):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                interrupted.set()

        async def aclose(self):
            pass

    task = asyncio.create_task(
        LLMQueueService(
            settings,
            session_factory=database.sessions,
            provider=Provider(),
        ).drain(wait=False)
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await service.request()
        await service.cancel_persisted()
        await asyncio.wait_for(task, 5)
        await service.finalize()
        assert interrupted.is_set()
        async with database.sessions() as session:
            work = await session.scalar(select(LLMWorkItem))
            attempt = await session.scalar(select(LLMAttempt))
            assert work.status == "cancelled"
            assert attempt.status != "succeeded" and attempt.cost_usd is None
            status = await queue_status(session, settings)
            assert status["unknown_cost_attempts"] == 1
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_stale_supervisor_cannot_accept_shutdown(api_client, managed):
    runtime, _, _ = managed
    runtime.stop_event.set()
    runtime.thread.join(timeout=2)
    runtime.channel.write("heartbeat", {"at": 0})
    token = runtime.channel.read("manifest")["token"]
    response = await api_client.post(
        "/api/application/shutdown", json={"token": token}, headers={"Origin": "http://testserver"}
    )
    assert response.status_code == 409 and not runtime.channel.requested


async def test_opaque_browser_origin_requires_same_origin_and_loopback(api_client, managed):
    runtime, _, _ = managed
    token = runtime.channel.read("manifest")["token"]
    blocked = await api_client.post(
        "/application/shutdown",
        data={"token": token},
        headers={
            "Origin": "null",
            "Host": "127.0.0.1",
        },
    )
    assert blocked.status_code == 403 and not runtime.channel.requested
    accepted = await api_client.post(
        "/application/shutdown",
        data={"token": token},
        headers={
            "Origin": "null",
            "Sec-Fetch-Site": "same-origin",
            "Host": "127.0.0.1",
        },
    )
    assert accepted.status_code == 202 and runtime.channel.requested
