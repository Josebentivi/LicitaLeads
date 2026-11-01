"""Every numeric procurement/contract/detail pager uses the same bounded window."""

import asyncio

import httpx
import pytest

from app.config import Settings
from app.connectors.base import ProcurementFilters
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.http import AsyncHTTPClient
from app.connectors.pncp import PNCPConnector


def page_payload(page, total, source):
    row = {
        "numeroControlePNCP": f"00000000000191-1-{page:06}/2026",
        "numeroControlePncpContrato": f"00000000000191-2-{page:06}/2026",
        "idCompra": f"98092105{page:05}2026",
        "anoCompra": 2026,
        "sequencialCompra": page,
        "numeroCompra": str(page),
        "anoContrato": 2026,
        "sequencialContrato": page,
        "numeroContrato": str(page),
        "modalidadeId": 6,
        "modalidadeIdPncp": 6,
        "codigoModalidade": 5,
        "orgaoEntidade": {"cnpj": "00000000000191"},
        "orgaoEntidadeCnpj": "00000000000191",
        "unidadeOrgao": {"codigoUnidade": "980921", "ufSigla": "MA"},
        "unidadeOrgaoUfSigla": "MA",
        "numeroItem": page,
    }
    return {
        "data" if source == "pncp" else "resultado": [row],
        "totalPaginas": total,
        "totalRegistros": total,
    }


@pytest.mark.parametrize("source", ["pncp", "compras_gov"])
@pytest.mark.parametrize("kind", ["procurements", "contracts", "details"])
async def test_discovery_and_detail_pages_overlap_but_are_consumed_in_order(source, kind):
    active = peak = 0
    full, release = asyncio.Event(), asyncio.Event()
    requests = []

    async def handler(request):
        nonlocal active, peak
        page = int(request.url.params["pagina"])
        requests.append(page)
        if page > 1:
            active += 1
            peak = max(peak, active)
            if active == 4:
                full.set()
            try:
                await release.wait()
            finally:
                active -= 1
        return httpx.Response(200, json=page_payload(page, 5, source))

    settings = Settings(_env_file=None, http_max_concurrency=4, http_min_request_interval_seconds=0)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        cls = PNCPConnector if source == "pncp" else ComprasGovConnector
        connector = cls(settings, http_client=AsyncHTTPClient(settings, client=client))
        if kind == "details":
            call = (
                connector._collect_integration("records")
                if source == "pncp"
                else connector._collect_result_pages(
                    "https://official.test/records", {}, operation="test"
                )
            )
        else:
            call = getattr(connector, f"discover_{kind}")(
                ProcurementFilters(max_pages=5, agency_cnpj="00000000000191")
            )
        task = asyncio.create_task(call)
        try:
            await asyncio.wait_for(full.wait(), timeout=10)
            release.set()
            result = await asyncio.wait_for(task, timeout=10)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert peak == 4 and requests == [1, 2, 3, 4, 5]
    raw = result[2] if kind == "details" else result.raw_records
    assert [record.request_parameters["pagina"] for record in raw] == requests
    assert result[4] is None if kind == "details" else result.is_complete


@pytest.mark.parametrize("source", ["pncp", "compras_gov"])
async def test_modalities_start_independently_with_exact_filters(source):
    seen = set()
    both = asyncio.Event()

    async def handler(request):
        field = "codigoModalidadeContratacao" if source == "pncp" else "codigoModalidade"
        seen.add(request.url.params[field])
        if len(seen) == 2:
            both.set()
        await both.wait()
        return httpx.Response(204)

    settings = Settings(_env_file=None, http_max_concurrency=4, http_min_request_interval_seconds=0)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        cls = PNCPConnector if source == "pncp" else ComprasGovConnector
        connector = cls(settings, http_client=AsyncHTTPClient(settings, client=client))
        result = await asyncio.wait_for(
            connector.discover_procurements(
                ProcurementFilters(
                    modalities=["pregao_eletronico", "concorrencia_eletronica"],
                    max_pages=1,
                )
            ),
            timeout=5,
        )
    assert result.is_complete and not result.data and len(seen) == 2
