"""Manual collection forms accept an explicit inclusive calendar window."""

from datetime import date, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from bs4 import BeautifulSoup

from app import web
from app.services.collection_periods import collection_today, earliest_collection_day
from app.services.price_registry_scopes import PriceRegistryScope


@pytest.mark.parametrize(
    "page",
    ["/dashboard", "/grupos/operacao", "/atas", "/contratos", "/pagamentos", "/leads", "/crawls"],
)
async def test_pages_offer_one_collection_action(api_client, page):
    response = await api_client.get(page)
    assert response.status_code == 200
    soup = BeautifulSoup(response.text, "html.parser")
    forms = soup.select("form[action='/crawls/collect']")
    assert len(forms) == 1
    assert forms[0].get_text(" ", strip=True) == "Coletar dados"
    assert not soup.select(
        "form[action='/crawls/run'], form[action='/atas/collect'], form[action='/contratos/collect'], form[action='/pagamentos/coletar']"
    )
    if page == "/crawls":
        start = date.fromisoformat(
            soup.select_one("[name='start_date'][form='complete-collection-form']")["value"]
        )
        end = date.fromisoformat(
            soup.select_one("[name='end_date'][form='complete-collection-form']")["value"]
        )
        assert (end - start).days == web.get_settings().default_lookback_days - 1
        assert soup.select_one('[data-period="two-years"]') is not None
        assert len(soup.select("select[name='object_preset'] option")) == 30


async def test_complete_form_sends_explicit_scope_and_refresh(api_client, monkeypatch):
    calls = []
    run_id = uuid4()
    end = collection_today(web.get_settings().timezone) - timedelta(days=1)
    start = end - timedelta(days=9)

    async def enqueue(_service, scope):
        return SimpleNamespace(run_id=run_id, generation=1)

    monkeypatch.setattr(web.CompleteCollection, "enqueue", enqueue)
    monkeypatch.setattr(web, "launch_complete_collection", lambda *args: calls.append(args))
    response = await api_client.post(
        "/crawls/collect",
        data={
            "uf": "PI",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "force_refresh": "true",
        },
        follow_redirects=False,
    )
    assert response.headers["location"] == f"/crawls#coleta-{run_id}"
    assert calls[0][2] == web.CollectionScope("PI", start, end, True)


async def test_historical_collection_form_preserves_full_scope(api_client, monkeypatch):
    launches = []
    today = collection_today(web.get_settings().timezone)
    earliest = earliest_collection_day(today)

    async def enqueue(_service, scope):
        return SimpleNamespace(run_id=uuid4(), generation=1)

    monkeypatch.setattr(web.CompleteCollection, "enqueue", enqueue)
    monkeypatch.setattr(web, "launch_complete_collection", lambda *args: launches.append(args))
    valid = {
        "uf": "MA",
        "start_date": earliest.isoformat(),
        "end_date": today.isoformat(),
        "modalities": ["concorrencia_eletronica", "concorrencia_presencial"],
        "object_query": "reforma de escola",
        "object_group": "obras",
        "government_sphere": "M",
        "procurements_only": "true",
    }
    accepted = await api_client.post("/crawls/collect", data=valid, follow_redirects=False)
    rejected = await api_client.post(
        "/crawls/collect",
        data={**valid, "start_date": (earliest - timedelta(days=1)).isoformat()},
        follow_redirects=False,
    )
    assert accepted.status_code == 303 and rejected.status_code == 422
    assert "reforma de escola" in rejected.text
    assert len(launches) == 1
    scope = launches[0][2]
    assert scope.start_date == earliest and scope.end_date == today
    assert scope.procurements_only
    assert scope.modalities == ("concorrencia_eletronica", "concorrencia_presencial")
    assert scope.object_scope.matches("M", "Reforma de escola municipal")
    assert not scope.object_scope.matches("F", "Reforma de escola municipal")


def _capture_crawl(monkeypatch):
    created = []
    launched = []
    run_id = uuid4()

    async def create_run(_self, request):
        created.append(request)
        return SimpleNamespace(id=run_id)

    monkeypatch.setattr(web.IngestionPipeline, "create_run", create_run)
    monkeypatch.setattr(web, "launch_crawl", lambda *args: launched.append(args))
    return created, launched


async def test_general_form_sends_explicit_dates(api_client, monkeypatch):
    created, launched = _capture_crawl(monkeypatch)
    end = collection_today(web.get_settings().timezone) - timedelta(days=1)
    start = end - timedelta(days=30)
    response = await api_client.post(
        "/crawls/run",
        data={
            "connector": "all",
            "uf": "MA",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "batch_size": "30",
            "process_documents": "true",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert len(created) == 1 and len(launched) == 1
    request = created[0]
    assert request.mode == "procurements"
    assert request.start_date == start
    assert request.end_date == end
    assert request.days is None
    assert request.process_documents is True


async def test_general_form_blank_dates_fall_back_to_days(api_client, monkeypatch):
    created, _launched = _capture_crawl(monkeypatch)
    response = await api_client.post(
        "/crawls/run",
        data={
            "connector": "all",
            "uf": "MA",
            "start_date": "",
            "end_date": "",
            "days": "7",
            "batch_size": "30",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    request = created[0]
    assert request.start_date is None and request.end_date is None
    assert request.days == 7


async def test_atas_form_sends_explicit_dates(api_client, monkeypatch):
    created, _launched = _capture_crawl(monkeypatch)

    async def scopes(_session, uf):
        return [
            PriceRegistryScope(
                key=f"{uf}:00000000000191:980921",
                uf=uf,
                agency_cnpj="00000000000191",
                unit_code="980921",
                agency_name="Orgao de Exemplo",
                sources=("pncp",),
            )
        ]

    monkeypatch.setattr(web, "known_price_registry_scopes", scopes)
    response = await api_client.post(
        "/atas/collect",
        data={
            "uf": "MA",
            "registry_scope": "MA:00000000000191:980921",
            "connector": "pncp",
            "start_date": "2026-01-01",
            "end_date": "2026-01-31",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    request = created[0]
    assert request.mode == "price_registries"
    assert request.start_date == date(2026, 1, 1)
    assert request.end_date == date(2026, 1, 31)
    assert request.days is None


async def test_contracts_form_sends_explicit_dates(api_client, monkeypatch):
    created, _launched = _capture_crawl(monkeypatch)
    response = await api_client.post(
        "/contratos/collect",
        data={
            "connector": "pncp",
            "agency_cnpj": "00000000000191",
            "start_date": "2026-01-01",
            "end_date": "2026-01-31",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    request = created[0]
    assert request.mode == "contracts"
    assert request.start_date == date(2026, 1, 1)
    assert request.end_date == date(2026, 1, 31)
    assert request.days is None


@pytest.mark.parametrize(
    "data",
    [
        {"start_date": "2026-01-01"},
        {"end_date": "2026-01-31"},
        {"start_date": "2026-02-01", "end_date": "2026-01-01"},
    ],
)
@pytest.mark.parametrize("path", ["/crawls/run", "/crawls/collect"])
async def test_incoherent_dates_are_rejected(api_client, monkeypatch, data, path):
    created, _launched = _capture_crawl(monkeypatch)
    response = await api_client.post(
        path,
        data={"connector": "all", "uf": "MA", **data},
        follow_redirects=False,
    )

    assert response.status_code == 422
    assert created == []
