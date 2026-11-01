"""Ordinary repetitions reuse proofs; forced refresh remains an explicit choice."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.database import write_session
from app.models import CrawlRun, CrawlRunStatus
from app.web import _retry_request


@pytest.mark.parametrize("mode", ["procurements", "price_registries", "contracts"])
@pytest.mark.parametrize("endpoint", ["retry-all", "retry"])
@pytest.mark.parametrize("force", [False, True])
async def test_repeat_requires_explicit_force_refresh(
    api_client, database, monkeypatch, mode, endpoint, force
):
    from app import web

    filters = {
        "connector": "pncp",
        "mode": mode,
        "uf": "MA",
        "days": 60,
        "start_date": "2026-08-31",
        "end_date": "2026-10-29",
        "modalities": ["pregao_eletronico"],
        "agency_cnpj": "00394544000185",
        "unit_code": "123",
        "keyword": "escola",
        "max_pages": 3,
        "external_ids": ["official-id"],
        "process_documents": True,
        "document_batch_size": 100,
        "contract_details": False,
        "force_refresh": True,
    }
    async with write_session(database.sessions) as session:
        run = CrawlRun(
            connector="pncp",
            status=CrawlRunStatus.PARTIAL,
            filters=filters,
            cursor={"sources": {"pncp": {"status": "failed", "diagnostics": ["HTTP 500"]}}},
        )
        session.add(run)
        await session.flush()
        run_id = run.id
    captured = []

    async def create(self, request):
        captured.append(request)
        return SimpleNamespace(id=uuid4())

    monkeypatch.setattr(web.IngestionPipeline, "create_run", create)
    monkeypatch.setattr(web, "launch_crawl", lambda *args: None)
    payload = {"connector": "pncp"} if endpoint == "retry" else {}
    if force:
        payload["force_refresh"] = "true"
    response = await api_client.post(
        f"/crawls/{run_id}/{endpoint}", data=payload, follow_redirects=False
    )
    assert response.status_code == 303, response.text
    request = captured[0]
    assert request.force_refresh is force
    assert (
        request.as_json()
        == _retry_request(filters, connector="pncp", force_refresh=force).as_json()
    )
    assert request.days == 60 and request.start_date.isoformat() == "2026-08-31"
    assert request.mode == mode and request.unit_code == "123"
    assert request.external_ids == ("official-id",)
    assert request.process_documents and request.document_batch_size == 100
    assert not request.contract_details and request.max_pages == 3
    async with database.sessions() as session:
        assert (await session.get_one(CrawlRun, run_id)).filters == filters
    page = await api_client.get("/crawls/table")
    from bs4 import BeautifulSoup

    form = BeautifulSoup(page.text, "html.parser").find(
        "form", action=f"/crawls/{run_id}/{endpoint}"
    )
    checkbox = form.find("input", attrs={"name": "force_refresh"})
    assert checkbox is not None and not checkbox.has_attr("checked")
    assert form.has_attr("hx-preserve") and str(run_id) in form["id"]
