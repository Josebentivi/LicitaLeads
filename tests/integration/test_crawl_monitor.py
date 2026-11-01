import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select, update

from app.config import Settings
from app.connectors.progress import ProgressEvent
from app.models import CrawlRun, CrawlRunStatus, JobLease, Payment
from app.services.ingestion.municipal_payments import MunicipalPaymentIngestionService
from app.services.ingestion.payments import PaymentIngestionService
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest

from .test_municipal_payments import FakeMunicipalConnector
from .test_pipeline import StaticConnector, _procurement


async def test_monitor_shows_parallel_budgets_queues_and_documents(database, api_client):
    async with database.sessions() as session, session.begin():
        session.add(
            CrawlRun(
                connector="all",
                status=CrawlRunStatus.RUNNING,
                filters={},
                started_at=datetime.now(UTC),
                heartbeat_at=datetime.now(UTC),
                cursor={
                    "parallel": {
                        "pncp": {
                            "http_active": 12,
                            "http_limit": 20,
                            "http_peak": 20,
                            "http_waiting": 1,
                            "http_waiting_rate": 1,
                            "http_waiting_capacity": 0,
                            "http_waiting_checkpoint": 0,
                            "http_rate_interval_seconds": 8.0,
                            "http_cooldown_seconds": 2.5,
                        },
                        "documents": {
                            "download_active": 3,
                            "download_limit": 4,
                            "process_active": 2,
                            "process_limit": 2,
                            "queue_ready": 1,
                        },
                        "<script>source</script>": {"http_limit": 4},
                    },
                    "sources": {"pncp": {"status": "running", "progress": {"ready": 7}}},
                },
            )
        )
    result = await api_client.get("/crawls/progress")
    assert result.status_code == 200
    assert "12/20 requisições ativas · pico 20" in result.text
    assert "7 resultado(s) aguardando gravação" in result.text
    assert (
        "1 aguardando envio (1 fila/frequência · 0 capacidade HTTP · 0 validação no banco · 0 nova tentativa)"
        in result.text
    )
    assert "intervalo por operação: até 8 s" in result.text
    assert "pausa da fonte: 2,5 s" in result.text
    assert "3/4 downloads · 2/2 processos de extração/OCR" in result.text
    assert "1 resultado(s) aguardando gravação" in result.text
    assert "<script>source</script>" not in result.text


async def test_monitor_refresh_history_recent_limit_and_active_outside_history(
    database, api_client
):
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        active = CrawlRun(
            connector="all",
            status=CrawlRunStatus.RUNNING,
            created_at=now - timedelta(days=3),
            started_at=now,
            heartbeat_at=now,
            filters={},
            records_found=10,
            cursor={
                "progress": {
                    "stage": "details",
                    "source": "pncp",
                    "processed": 4,
                    "total": 10,
                    "subject": "<script>unsafe</script>",
                },
                "sources": {"pncp": {"status": "running"}, "compras_gov": {"status": "pending"}},
            },
        )
        session.add(active)
        for _index in range(205):
            session.add(
                CrawlRun(
                    connector="pncp",
                    status=CrawlRunStatus.COMPLETED,
                    filters={},
                    created_at=now - timedelta(minutes=10),
                    finished_at=now - timedelta(minutes=10),
                )
            )
        for status in [
            CrawlRunStatus.COMPLETED,
            CrawlRunStatus.PARTIAL,
            CrawlRunStatus.FAILED,
            CrawlRunStatus.CANCELLED,
        ]:
            session.add(CrawlRun(connector="pncp", status=status, filters={}, finished_at=now))
    result = await api_client.get("/crawls/progress")
    assert result.status_code == 200 and result.headers["cache-control"] == "no-store"
    assert 'hx-trigger="every 2s"' in result.text
    assert result.text.count("data-crawl-id=") == 4  # all active + only three recent terminal runs
    assert str(active.id) in result.text
    assert "40% desta etapa" in result.text
    assert "&lt;script&gt;unsafe&lt;/script&gt;" in result.text
    assert "<script>unsafe</script>" not in result.text
    assert "Aguardando" in result.text
    page = await api_client.get("/crawls")
    assert "Histórico de coletas" in page.text and 'id="crawl-monitor"' in page.text
    assert "crawls:changed from:body" in page.text


async def test_idle_monitor_polls_for_background_jobs_and_handles_legacy_run(database, api_client):
    result = await api_client.get("/crawls/progress")
    assert 'hx-trigger="every 15s"' in result.text

    assert "Nenhuma coleta em andamento" in result.text
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        run = CrawlRun(connector="pncp", status=CrawlRunStatus.PENDING, filters={})
        session.add(run)
    result = await api_client.get("/crawls/progress")
    assert "Aguardando início" in result.text
    async with database.sessions() as session, session.begin():
        run = await session.get(CrawlRun, run.id)
        run.status = CrawlRunStatus.CANCELLED
        run.finished_at = now
    result = await api_client.get("/crawls/progress")
    assert "Coleta cancelada" in result.text and "Encerrar coleta" not in result.text
    assert 'hx-trigger="every 15s"' in result.text


@pytest.mark.parametrize("lose_ownership", [False, True])
async def test_pipeline_reports_while_discovery_waits_and_fences_old_progress(
    database, lose_ownership
):
    entered, resume = asyncio.Event(), asyncio.Event()

    class SlowConnector(StaticConnector):
        async def discover_procurements(self, filters):
            await self._http.on_activity(ProgressEvent(page=1, message="Página recebida"))
            entered.set()
            await resume.wait()
            return await super().discover_procurements(filters)

    connector = SlowConnector("pncp", _procurement("pncp", "monitor"))
    connector._http = SimpleNamespace()
    service = IngestionPipeline(
        Settings(_env_file=None), session_factory=database.sessions, connectors={"pncp": connector}
    )
    task = asyncio.create_task(
        service.run(PipelineRequest(connector="pncp", process_documents=False))
    )
    await asyncio.wait_for(entered.wait(), timeout=5)
    # The final page of a burst is flushed even without another event.
    await asyncio.sleep(2.1)
    async with database.sessions() as session:
        run = await session.scalar(select(CrawlRun))
        assert run.cursor["progress"]["stage"] == "discovery"
        assert run.cursor["progress"]["page"] == 1
        assert run.records_found == 0 and run.records_created == 0
        run_id = run.id
    if lose_ownership:
        async with database.sessions() as session, session.begin():
            await session.execute(
                update(JobLease).values(owner_id="recovery", version=JobLease.version + 1)
            )
            run = await session.get(CrawlRun, run_id)
            run.status = CrawlRunStatus.FAILED
            run.cursor = {"sentinel": "recovered"}
    resume.set()
    await task
    async with database.sessions() as session:
        run = await session.get(CrawlRun, run_id)
        if lose_ownership:
            assert run.cursor == {"sentinel": "recovered"} and run.status == CrawlRunStatus.FAILED
        else:
            assert run.status == CrawlRunStatus.COMPLETED
            assert run.cursor["progress"]["stage"] == "completed"
            assert run.records_created == 1
            assert run.cursor["activity"][-1]["message"] == "Coleta concluída"


async def test_municipal_progress_never_publishes_rows_before_commit_and_cancel_rolls_back(
    database,
):
    entered, resume = asyncio.Event(), asyncio.Event()
    settings = Settings(_env_file=None, allow_insecure_http_sources=True)

    class SlowPayments(PaymentIngestionService):
        async def persist_raw_payments(self, payments, **kwargs):
            result = await super().persist_raw_payments(payments, **kwargs)
            entered.set()
            await resume.wait()
            return result

    service = MunicipalPaymentIngestionService(
        settings,
        session_factory=database.sessions,
        connector=FakeMunicipalConnector(),
        payment_ingestion=SlowPayments(settings, session_factory=database.sessions),
    )
    events = []
    task = asyncio.create_task(
        service.run("imperatriz", year=2026, progress_callback=events.append)
    )
    await asyncio.wait_for(entered.wait(), timeout=5)
    async with database.sessions() as session:
        run = await session.scalar(select(CrawlRun))
        assert run.cursor["progress"]["stage"] == "persisting"
        assert run.records_created == 0 and run.records_found == 1
        assert await session.scalar(select(func.count(Payment.id))) == 0
    assert all(event["processed"] == 0 for event in events if event["stage"] == "persisting")
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with database.sessions() as session:
        run = await session.scalar(select(CrawlRun))
        assert run.status == CrawlRunStatus.CANCELLED and run.records_created == 0
        assert run.cursor["progress"]["stage"] == "cancelled"
        assert await session.scalar(select(func.count(Payment.id))) == 0
