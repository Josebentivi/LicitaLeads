"""Tracking preferences feed manual and scheduled federal collections."""

from datetime import date
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import func, select

from app import web
from app.config import Settings
from app.connectors.base import DataAvailability
from app.jobs import scheduler
from app.models import Company, CrawlRun, CrawlRunStatus, PaymentTrackingTarget
from app.services.ingestion import complete_collection
from app.services.ingestion.complete_collection import (
    CollectionScope,
    CompleteCollection,
    StepResult,
)
from app.services.payment_tracking import set_payment_tracking, tracked_payment_cnpjs

REGISTERED = "12345678000195"
LEGACY = "00000000000191"


async def test_payments_page_manages_tracking_without_starting_collection(
    api_client, database, monkeypatch
):
    settings = Settings(_env_file=None, payment_poll_cnpjs=[LEGACY])
    monkeypatch.setattr(web, "get_settings", lambda: settings)
    async with database.sessions.begin() as db:
        db.add(
            Company(
                cnpj=REGISTERED,
                legal_name="Empresa acompanhada",
                normalized_name="EMPRESA ACOMPANHADA",
                fingerprint="empresa-acompanhada",
            )
        )

    page = await api_client.get("/pagamentos")
    assert page.status_code == 200
    assert 'id="acompanhamento-federal"' in page.text
    assert LEGACY in page.text
    dossier = await api_client.get(f"/empresas/{REGISTERED}")
    assert dossier.status_code == 200
    assert 'action="/pagamentos/acompanhamento"' in dossier.text
    assert "Acompanhar pagamentos federais" in dossier.text

    for cnpj in ("12.345.678/0001-95", REGISTERED):
        added = await api_client.post(
            "/pagamentos/acompanhamento", data={"cnpj": cnpj, "active": "true"}
        )
        assert added.status_code == 303
        assert added.headers["location"].endswith("#acompanhamento-federal")

    paused = await api_client.post(
        "/pagamentos/acompanhamento", data={"cnpj": LEGACY, "active": "false"}
    )
    assert paused.status_code == 303
    page = await api_client.get(paused.headers["location"])
    assert page.status_code == 200
    assert "Empresa acompanhada" in page.text
    assert LEGACY in page.text
    assert "Retomar" in page.text

    bad = await api_client.post(
        "/pagamentos/acompanhamento", data={"cnpj": "123", "active": "true"}
    )
    assert bad.status_code == 303
    invalid = await api_client.get(bad.headers["location"])
    assert invalid.status_code == 422
    assert "Informe um CNPJ válido." in invalid.text

    async with database.sessions() as db:
        assert await tracked_payment_cnpjs(db, settings.payment_poll_cnpjs) == [REGISTERED]
        assert await db.scalar(select(func.count()).select_from(PaymentTrackingTarget)) == 2
        assert await db.scalar(select(func.count()).select_from(CrawlRun)) == 0

    resumed = await api_client.post(
        "/pagamentos/acompanhamento", data={"cnpj": LEGACY, "active": "true"}
    )
    assert resumed.status_code == 303
    async with database.sessions() as db:
        assert set(await tracked_payment_cnpjs(db, settings.payment_poll_cnpjs)) == {
            REGISTERED,
            LEGACY,
        }


async def test_general_collection_uses_active_targets_for_each_year(database, monkeypatch):
    settings = Settings(
        _env_file=None,
        transparencia_api_key="test-only-key",
        payment_sources=["transparencia"],
        payment_poll_cnpjs=[LEGACY, LEGACY],
    )
    collection = CompleteCollection(settings, sessions=database.sessions)
    scope = CollectionScope("MA", date(2025, 12, 20), date(2026, 1, 20))
    queries = []

    class FederalService:
        def __init__(self, chosen, *, session_factory):
            assert chosen is settings
            self.sessions = session_factory

        async def run(self, *, company_cnpj, year):
            queries.append((company_cnpj, year))
            run_id = uuid4()
            async with self.sessions.begin() as db:
                db.add(
                    CrawlRun(id=run_id, connector="transparencia", status=CrawlRunStatus.COMPLETED)
                )
            return SimpleNamespace(crawl_run_id=run_id)

        async def aclose(self):
            pass

    async def skip(ticket, selected_scope):
        return StepResult("skipped")

    monkeypatch.setattr(complete_collection, "PaymentIngestionService", FederalService)
    for stage in ("_procurements", "_tce", "_registries", "_contracts", "_municipal"):
        monkeypatch.setattr(collection, stage, skip)
    async with database.sessions.begin() as db:
        await set_payment_tracking(db, LEGACY, active=False)

    empty_ticket = await collection.enqueue(scope)
    await collection.run(empty_ticket, scope)
    async with database.sessions() as db:
        empty_run = await db.get_one(CrawlRun, empty_ticket.run_id)
        empty_stage = next(s for s in empty_run.cursor["complete_stages"] if s["key"] == "federal")
        assert empty_stage["status"] == "skipped"
        assert "Nenhum CNPJ ativo" in empty_stage["note"]

    async with database.sessions.begin() as db:
        await set_payment_tracking(db, REGISTERED, active=True)

    ticket = await collection.enqueue(scope)
    await collection.run(ticket, scope)

    assert queries == [(REGISTERED, 2025), (REGISTERED, 2026)]
    async with database.sessions() as db:
        run = await db.get_one(CrawlRun, ticket.run_id)
        federal = next(s for s in run.cursor["complete_stages"] if s["key"] == "federal")
        assert federal["status"] == "completed"
        assert len(federal["run_ids"]) == 2


async def test_scheduler_reads_tracking_changes_at_each_cycle(database, monkeypatch):
    settings = Settings(
        _env_file=None,
        transparencia_api_key="test-only-key",
        payment_sources=["transparencia"],
        payment_poll_cnpjs=[],
    )
    queries = []

    class FederalService:
        def __init__(self, chosen):
            assert chosen is settings

        async def run(self, **kwargs):
            queries.append(kwargs)
            return SimpleNamespace(availability=DataAvailability.EMPTY)

        async def aclose(self):
            pass

    monkeypatch.setattr(scheduler, "get_settings", lambda: settings)
    monkeypatch.setattr(scheduler, "async_session_factory", database.sessions)
    monkeypatch.setattr(scheduler, "PaymentIngestionService", FederalService)
    job = scheduler.build_scheduler().get_job("poll_configured_payments")
    assert job is not None
    await scheduler.poll_configured_payments()
    assert queries == []

    async with database.sessions.begin() as db:
        await set_payment_tracking(db, REGISTERED, active=True)
    await scheduler.poll_configured_payments()
    assert len(queries) == 1
    assert queries[0]["company_cnpj"] == REGISTERED
    assert queries[0]["resume_automatically"] is True

    async with database.sessions.begin() as db:
        await set_payment_tracking(db, REGISTERED, active=False)
    await scheduler.poll_configured_payments()
    assert len(queries) == 1
