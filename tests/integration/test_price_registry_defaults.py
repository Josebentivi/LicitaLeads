"""ARP collection is automatic and accessible without JSON or a terminal."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import select

from app import web
from app.config import Settings
from app.jobs import scheduler
from app.models import JobLease, Procurement, ProcurementSource
from app.services.price_registry_scopes import PriceRegistryScope


async def known_scopes(_session, uf):
    return [
        PriceRegistryScope(
            key=f"{uf}:00000000000191:980921",
            uf=uf,
            agency_cnpj="00000000000191",
            unit_code="980921",
            agency_name="Órgão de Exemplo",
            sources=("pncp", "compras_gov"),
        )
    ]


@pytest.mark.parametrize("paused", [False, True])
async def test_default_arp_job_starts_immediately_and_respects_pause(database, monkeypatch, paused):
    settings = Settings(_env_file=None)
    requests = []

    class Pipeline:
        def __init__(self, _settings):
            pass

        async def run(self, request):
            requests.append(request)

    monkeypatch.setattr(scheduler, "get_settings", lambda: settings)
    monkeypatch.setattr(scheduler, "async_session_factory", database.sessions)
    monkeypatch.setattr(scheduler, "scheduler_paused", lambda: paused)
    monkeypatch.setattr(scheduler, "IngestionPipeline", Pipeline)
    monkeypatch.setattr(scheduler, "known_price_registry_scopes", known_scopes)
    before = datetime.now(UTC)
    job = scheduler.build_scheduler().get_job("discover_price_registries")

    assert job is not None
    assert before <= job.next_run_time <= datetime.now(UTC)
    assert job.trigger.interval == timedelta(hours=24)
    assert job.max_instances == 1
    await job.func()

    assert len(requests) == (0 if paused else 2)
    if not paused:
        assert {request.connector for request in requests} == {"pncp", "compras_gov"}
    for request in requests:
        assert request.mode == "price_registries"
        assert request.days == 30
        assert request.price_registry_scopes
        assert request.process_documents is False
        assert request.modalities == ()
        assert request.sync_context["strategy"] == "bootstrap"
    async with database.sessions() as session:
        assert (await session.scalars(select(JobLease))).all() == []


async def test_arp_schedule_uses_configured_window_and_interval(database, monkeypatch):
    settings = Settings(
        _env_file=None,
        scheduler_price_registry_hours=12,
        price_registry_lookback_days=90,
        default_uf="PI",
    )
    requests = []

    class Pipeline:
        def __init__(self, _settings):
            pass

        async def run(self, request):
            requests.append(request)

    monkeypatch.setattr(scheduler, "get_settings", lambda: settings)
    monkeypatch.setattr(scheduler, "IngestionPipeline", Pipeline)
    monkeypatch.setattr(scheduler, "async_session_factory", database.sessions)
    monkeypatch.setattr(scheduler, "known_price_registry_scopes", known_scopes)
    job = scheduler.build_scheduler().get_job("discover_price_registries")

    assert job.trigger.interval == timedelta(hours=12)
    await scheduler.discover_price_registries()
    assert requests[0].days == 90
    assert requests[0].uf == "PI"


async def test_atas_form_starts_scoped_arp_asynchronously_with_selected_window(
    api_client, database, monkeypatch
):
    settings = Settings(_env_file=None, price_registry_lookback_days=90, default_uf="PI")
    run_id = uuid4()
    created = []
    launched = []
    async with database.sessions() as session, session.begin():
        procurement = Procurement(
            source="pncp",
            external_id="scope",
            agency_cnpj="00000000000191",
            agency_name="Órgão de Exemplo",
            uf="PI",
            uasg="980921",
            fingerprint="scope",
        )
        session.add(procurement)
        await session.flush()
        session.add(
            ProcurementSource(
                procurement_id=procurement.id,
                source="compras_gov",
                external_id="scope",
                fingerprint="scope-source",
            )
        )

    async def create_run(_self, request):
        created.append(request)
        return SimpleNamespace(id=run_id)

    monkeypatch.setattr(web, "get_settings", lambda: settings)
    monkeypatch.setattr(web.IngestionPipeline, "create_run", create_run)
    monkeypatch.setattr(web, "launch_crawl", lambda *args: launched.append(args))
    page = await api_client.get("/atas")
    assert page.status_code == 200
    assert created == []
    form = BeautifulSoup(page.text, "html.parser").find("form", action="/atas/collect")
    assert form is not None and form["method"] == "post"
    assert form.find("input", {"name": "days"})["value"] == "7"
    assert form.find("button", {"type": "submit"}).has_attr("disabled")

    response = await api_client.post(
        form["action"],
        data={
            "uf": "PI",
            "registry_scope": "PI:00000000000191:980921",
            "days": "14",
            "connector": "all",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/crawls"
    assert len(created) == 1
    request = created[0]
    assert request.mode == "price_registries"
    assert request.connector == "all"
    assert request.uf == "PI"
    assert request.days == 14
    assert request.agency_cnpj == "00000000000191"
    assert request.unit_code == "980921"
    assert request.process_documents is False
    assert request.modalities == ()
    assert launched == [(run_id, request)]
