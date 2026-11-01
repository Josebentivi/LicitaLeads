"""Manual ARP selectors cannot silently turn a regional choice into a national crawl."""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from bs4 import BeautifulSoup

from app import web
from app.api.routes.crawls import pipeline_request
from app.config import Settings
from app.models import Procurement, ProcurementSource
from app.schemas.api import CrawlRunRequest
from app.services.ingestion.continuation import pipeline_request_from_filters
from app.services.ingestion.pipeline import PipelineRequest
from app.services.price_registry_scopes import known_price_registry_scopes


async def seed_scope(
    database,
    *,
    uf="MA",
    source="pncp",
    unit="980921",
    cnpj="00000000000191",
    name="Órgão Exemplo",
    linked=(),
):
    async with database.sessions() as session, session.begin():
        entity = Procurement(
            source=source,
            external_id=str(uuid4()),
            fingerprint=str(uuid4()),
            uf=uf,
            agency_cnpj=cnpj,
            agency_name=name,
            uasg=unit,
        )
        session.add(entity)
        await session.flush()
        for origin in linked:
            session.add(
                ProcurementSource(
                    procurement_id=entity.id,
                    source=origin,
                    external_id=str(uuid4()),
                    fingerprint=str(uuid4()),
                )
            )


async def test_scope_options_deduplicate_exact_units_and_exclude_other_states_or_missing_identity(
    database,
):
    await seed_scope(database)
    await seed_scope(database, source="compras_gov")
    await seed_scope(database, uf="PI")
    await seed_scope(database, unit=None)
    await seed_scope(database, cnpj=None)
    await seed_scope(database, cnpj="123")
    await seed_scope(database, source="manual", unit="other")
    async with database.sessions() as session:
        scopes = await known_price_registry_scopes(session, "MA")
    assert len(scopes) == 1
    assert scopes[0].key == "MA:00000000000191:980921"
    assert scopes[0].sources == ("pncp", "compras_gov")


async def test_scope_lookup_and_form_escape_names_and_explain_partial_coverage(
    api_client, database, monkeypatch
):
    monkeypatch.setattr(web, "get_settings", lambda: Settings(_env_file=None))
    await seed_scope(database, name='<script>alert("x")</script>', linked=("compras_gov",))
    response = await api_client.get("/atas")
    assert response.status_code == 200
    assert '<script>alert("x")</script>' not in response.text
    assert "&lt;script&gt;" in response.text
    soup = BeautifulSoup(response.text, "html.parser")
    form = soup.find("form", {"data-registry-collection": True})
    assert form.find("select", {"name": "uf"})
    assert form.find("select", {"name": "registry_scope"})
    assert form.find("select", {"name": "registry_scope"}).find("option", {"value": "all"})
    assert "Todas as fontes disponíveis" in response.text
    assert not form.find("input", {"name": "process_documents"})
    assert "pode estar incompleta" in response.text
    options = await api_client.get("/atas/collection-scopes", params={"uf": "ma"})
    assert options.status_code == 200
    assert options.json()["scopes"][0]["sources"] == ["pncp", "compras_gov"]
    assert (await api_client.get("/atas/collection-scopes?uf=PI")).json()["scopes"] == []
    assert (await api_client.get("/atas/collection-scopes?uf=ZZ")).status_code == 422


@pytest.mark.parametrize(
    "invalid",
    [
        {},
        {"uf": "PI", "registry_scope": "MA:00000000000191:980921"},
        {"registry_scope": "MA:00000000000191:unknown"},
        {"registry_scope": "all", "uf": "PI"},
        {"connector": "compras_gov"},
        {"connector": "unknown"},
        {"days": "0"},
        {"days": "3651"},
    ],
)
async def test_invalid_scope_never_launches_an_unfiltered_collection(
    api_client, database, monkeypatch, invalid
):
    await seed_scope(database)
    launched = []
    monkeypatch.setattr(web, "launch_crawl", lambda *args: launched.append(args))
    data = {
        "uf": "MA",
        "registry_scope": "MA:00000000000191:980921",
        "connector": "all",
        "days": "7",
    }
    if invalid:
        data.update(invalid)
    else:
        data = {}
    response = await api_client.post("/atas/collect", data=data, follow_redirects=False)
    assert response.status_code == 422
    assert launched == []


async def test_auto_source_uses_only_verified_source_and_preserves_scope_for_retries(
    api_client, database, monkeypatch
):
    await seed_scope(database, unit="001")
    created = []
    run_id = uuid4()

    async def create_run(_self, request):
        created.append(request)
        return SimpleNamespace(id=run_id)

    monkeypatch.setattr(web.IngestionPipeline, "create_run", create_run)
    monkeypatch.setattr(web, "launch_crawl", lambda *args: None)
    response = await api_client.post(
        "/atas/collect",
        data={
            "uf": "MA",
            "registry_scope": "MA:00000000000191:001",
            "connector": "all",
            "days": "30",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    request = created[0]
    assert request.connector == "pncp"
    assert request.filters().unit_code == "001"
    assert request.filters().agency_cnpj == "00000000000191"
    assert request.days == 30 and request.process_documents is False
    assert pipeline_request_from_filters(request.as_json()).unit_code == "001"
    assert web._retry_request(request.as_json(), connector="pncp").unit_code == "001"


def test_optional_unit_preserves_legacy_filter_shape_and_public_api_translation():
    assert "unit_code" not in PipelineRequest().as_json()
    request = pipeline_request(CrawlRunRequest(mode="price_registries", unit_code="980921"))
    assert request.filters().unit_code == "980921"
    assert request.as_json()["unit_code"] == "980921"


@pytest.mark.parametrize("source", ["all", "pncp", "compras_gov"])
async def test_all_units_freezes_only_known_state_units_and_preserves_source_selection(
    api_client, database, monkeypatch, source
):
    await seed_scope(database, linked=("compras_gov",))
    await seed_scope(database, unit="980922")
    await seed_scope(database, uf="PI", unit="980923")
    created = []

    async def create_run(_self, request):
        created.append(request)
        return SimpleNamespace(id=uuid4())

    monkeypatch.setattr(web.IngestionPipeline, "create_run", create_run)
    monkeypatch.setattr(web, "launch_crawl", lambda *args: None)
    response = await api_client.post(
        "/atas/collect",
        data={"uf": "MA", "registry_scope": "all", "connector": source, "days": "2"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    request = created[0]
    assert request.connector == source
    assert request.agency_cnpj is None and request.unit_code is None
    assert {item.unit_code for item in request.price_registry_scopes} == {"980921", "980922"}
    assert all(item.uf == "MA" for item in request.price_registry_scopes)
    assert (
        pipeline_request_from_filters(request.as_json()).price_registry_scopes
        == request.price_registry_scopes
    )
    assert (
        web._retry_request(request.as_json(), connector=source).price_registry_scopes
        == request.price_registry_scopes
    )
