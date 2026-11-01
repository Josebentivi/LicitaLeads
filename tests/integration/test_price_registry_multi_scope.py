"""Known UF units share durable ARP pagination, audit records and explicit retry."""

import httpx
import pytest
from sqlalchemy import select

from app.config import Settings
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.http import AsyncHTTPClient
from app.connectors.pncp import PNCPConnector
from app.models import CollectionBatch, CrawlRun, CrawlRunStatus, PriceRegistry, SourceRecord
from app.services.ingestion.collection_batches import latest_limited_batch
from app.services.ingestion.continuation import create_continuation, pipeline_request_from_filters
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest
from app.services.price_registry_scopes import PriceRegistryScope


def scope(unit, sources=("pncp",)):
    return PriceRegistryScope(
        key=f"MA:00000000000191:{unit}",
        uf="MA",
        agency_cnpj="00000000000191",
        agency_name="Órgão Exemplo",
        unit_code=unit,
        sources=sources,
    )


def connector(source, settings, handler):
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    cls = PNCPConnector if source == "pncp" else ComprasGovConnector
    return cls(settings, http_client=AsyncHTTPClient(settings, client=client))


async def test_all_units_and_sources_never_issue_unscoped_queries_and_audit_http_204(database):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(204, request=request)

    settings = Settings(_env_file=None, http_min_request_interval_seconds=0, http_max_retries=0)
    request = PipelineRequest(
        mode="price_registries",
        process_documents=False,
        days=2,
        price_registry_scopes=(scope("980921", ("pncp", "compras_gov")), scope("980922")),
    )
    summary = await IngestionPipeline(
        settings,
        session_factory=database.sessions,
        connectors={name: connector(name, settings, handler) for name in ("pncp", "compras_gov")},
    ).run(request)
    assert summary.status is CrawlRunStatus.COMPLETED
    assert summary.records_found == 0 and summary.diagnostics == []
    assert len(requests) == 3
    pncp_requests = [r for r in requests if "codigoUnidadeAdministrativa" in r.url.params]
    compras_requests = [r for r in requests if "codigoUnidadeGerenciadora" in r.url.params]
    assert sorted(r.url.params["codigoUnidadeAdministrativa"] for r in pncp_requests) == [
        "980921",
        "980922",
    ]
    assert (
        len(compras_requests) == 1
        and compras_requests[0].url.params["codigoUnidadeGerenciadora"] == "980921"
    )
    assert all(r.url.params["cnpj"] == "00000000000191" for r in pncp_requests)
    async with database.sessions() as session:
        run = await session.get(CrawlRun, summary.run_id)
        batch = await session.get(CollectionBatch, run.batch_id)
        assert batch.status == "complete"
        assert len(batch.filters["price_registry_scopes"]) == 2
        records = (await session.scalars(select(SourceRecord))).all()
        assert records and all(record.http_status == 204 for record in records)


@pytest.mark.parametrize("failure", ["temporary_error", "truncated"])
async def test_multi_unit_partial_collection_resumes_frozen_units_without_reprocessing_successes(
    database, failure
):
    recovered = False
    requests = []

    def handler(request):
        unit = request.url.params["codigoUnidadeAdministrativa"]
        page = int(request.url.params["pagina"])
        requests.append((unit, page))
        if unit == "980922" and not recovered and failure == "temporary_error":
            return httpx.Response(503, request=request)
        total = 2 if unit == "980922" and failure == "truncated" else 1
        serial = (1 if unit == "980921" else 2) + page - 1
        return httpx.Response(
            200,
            request=request,
            json={
                "data": [
                    {
                        "numeroControlePNCPAta": f"00000000000191-1-{serial:06}/2026",
                        "numeroAtaRegistroPreco": str(serial),
                        "cnpjOrgao": "00000000000191",
                        "codigoUnidadeOrgao": unit,
                        "anoAta": 2026,
                    }
                ],
                "totalRegistros": total,
                "totalPaginas": total,
            },
        )

    settings = Settings(_env_file=None, http_min_request_interval_seconds=0, http_max_retries=0)
    request = PipelineRequest(
        connector="pncp",
        mode="price_registries",
        process_documents=False,
        max_pages=1,
        price_registry_scopes=(scope("980921"), scope("980922")),
    )

    def pipeline():
        return IngestionPipeline(
            settings,
            session_factory=database.sessions,
            connectors={"pncp": connector("pncp", settings, handler)},
        )

    first = await pipeline().run(request)
    assert first.status is CrawlRunStatus.PARTIAL
    assert first.records_created == (1 if failure == "temporary_error" else 2)
    if failure == "temporary_error":
        assert await latest_limited_batch(database.sessions, request.as_json()) is None
    async with database.sessions() as session:
        child, filters = await create_continuation(session, first.run_id)
        await session.commit()
    resumed = pipeline_request_from_filters(filters)
    assert resumed.price_registry_scopes == request.price_registry_scopes
    assert resumed.start_date is not None and resumed.end_date is not None
    recovered = True
    second = await pipeline().run(resumed, run_id=child.id)
    assert second.status is CrawlRunStatus.COMPLETED
    assert second.records_created == 1 and second.records_updated == 0
    async with database.sessions() as session:
        assert (
            len((await session.scalars(select(PriceRegistry))).all()) == first.records_created + 1
        )
        assert (await session.get(CollectionBatch, child.batch_id)).status == "complete"
    assert all(unit in {"980921", "980922"} for unit, _ in requests)
