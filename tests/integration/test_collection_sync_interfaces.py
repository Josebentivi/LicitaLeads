"""The opt-out reaches API/forms and remains attached to a resumed batch."""

import pytest

from app.config import Settings
from app.models import CrawlRun
from app.services.ingestion.continuation import create_continuation, pipeline_request_from_filters
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest

from .test_collection_continuation import CountingConnector, records


async def test_forced_collection_continuation_preserves_choice(database):
    cfg = Settings(_env_file=None, crawl_max_records_per_source=1)
    pipeline = IngestionPipeline(
        cfg, session_factory=database.sessions, connectors={"pncp": CountingConnector(records())}
    )
    first = await pipeline.run(
        PipelineRequest(connector="pncp", process_documents=False, force_refresh=True)
    )
    async with database.sessions() as session, session.begin():
        child, filters = await create_continuation(session, first.run_id)
        request = pipeline_request_from_filters(filters)
        assert request.force_refresh is True and request.continuation_batch_id
        assert (await session.get_one(CrawlRun, child.id)).filters["force_refresh"] is True


@pytest.mark.parametrize(
    "url,payload",
    [
        (
            "/api/crawls/run",
            {"connector": "pncp", "force_refresh": True, "process_documents": False},
        ),
        ("/crawls/run", {"force_refresh": "true"}),
        ("/contratos/collect", {"agency_cnpj": "00000000000191", "force_refresh": "true"}),
    ],
)
async def test_api_and_forms_forward_force_refresh(api_client, monkeypatch, url, payload):
    calls = []
    monkeypatch.setattr(
        "app.api.routes.crawls.launch_crawl", lambda run_id, request: calls.append(request)
    )
    monkeypatch.setattr("app.web.launch_crawl", lambda run_id, request: calls.append(request))
    response = await api_client.post(
        url,
        **({"json": payload} if url.startswith("/api/") else {"data": payload}),
        follow_redirects=False,
    )
    assert response.status_code in {202, 303}, response.text
    assert calls and calls[0].force_refresh is True


@pytest.mark.parametrize("page", ["/", "/atas", "/contratos"])
async def test_forms_offer_forced_refresh(api_client, page):
    response = await api_client.get(page, follow_redirects=True)
    assert response.status_code == 200
    assert 'name="force_refresh"' in response.text
