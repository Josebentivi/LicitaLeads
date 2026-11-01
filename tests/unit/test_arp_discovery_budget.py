"""Source totals do not authorize an unbounded ARP header discovery."""

import asyncio
from datetime import date

import httpx
import pytest

from app.config import Settings
from app.connectors.base import ProcurementFilters
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.discovery_budget import DiscoveryBudget
from app.connectors.http import AsyncHTTPClient
from app.connectors.pncp import PNCPConnector


def page_payload(page, size, source, *, unit="1", total=10000, rows=None):
    return {
        "data" if source == "pncp" else "resultado": [
            {
                "numeroControlePNCPAta": f"00000000000191-1-{unit}{page:05}{n:03}/2026",
                "numeroControlePncpAta": f"00000000000191-1-{unit}{page:05}{n:03}/2026",
                "numeroAtaRegistroPreco": f"{page}-{n}",
                "cnpjOrgao": "00000000000191",
                "anoAta": 2026,
            }
            for n in range(size if rows is None else rows)
        ],
        "totalRegistros": total * size,
        "totalPaginas": total,
    }


@pytest.mark.parametrize("source,size,pages", [("pncp", 50, 40), ("compras_gov", 500, 4)])
async def test_large_totals_reserve_only_2000_headers_before_sending(source, size, pages):
    requested = []

    async def handler(request):
        page = int(request.url.params["pagina"])
        requested.append(page)
        await asyncio.sleep(0)
        return httpx.Response(200, json=page_payload(page, size, source), request=request)

    cfg = Settings(
        _env_file=None,
        crawl_max_records_per_source=2000,
        http_max_concurrency=20,
        http_min_request_interval_seconds=0,
    )
    cls = PNCPConnector if source == "pncp" else ComprasGovConnector
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = cls(cfg, http_client=AsyncHTTPClient(cfg, client=client))
        result = await asyncio.wait_for(
            connector.discover_price_registries(
                ProcurementFilters(start_date=date(2026, 1, 1), end_date=date(2026, 1, 30))
            ),
            timeout=10,
        )
    assert sorted(requested) == list(range(1, pages + 1))
    assert len(result.data) == 2000
    assert result.pagination.truncated and result.partial
    assert result.pagination.total_records == 10000 * size


async def test_units_share_credits_and_short_pages_refund_unused_capacity():
    cfg = Settings(_env_file=None, http_max_concurrency=8, http_min_request_interval_seconds=0)
    requested = []

    async def handler(request):
        page = int(request.url.params["pagina"])
        unit = request.url.params["codigoUnidadeAdministrativa"]
        requested.append((unit, page))
        await asyncio.sleep(0)
        return httpx.Response(200, json=page_payload(page, 50, "pncp", unit=unit, rows=10))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transport = AsyncHTTPClient(cfg, client=client)
        transport.arp_discovery_budget = DiscoveryBudget(100)
        connector = PNCPConnector(cfg, http_client=transport)
        results = await asyncio.wait_for(
            asyncio.gather(
                *(
                    connector.discover_price_registries(ProcurementFilters(unit_code=unit))
                    for unit in ("1", "2", "3")
                )
            ),
            timeout=10,
        )
    # Last-page credit fragments may preserve one extra complete page.
    assert 100 <= sum(len(result.data) for result in results) <= 140
    assert len(requested) <= 14
    assert all(result.pagination.truncated for result in results)


def test_optional_arp_period_inherits_general_setting_and_keeps_explicit_override():
    assert Settings(_env_file=None).effective_price_registry_lookback_days == 30
    assert (
        Settings(_env_file=None, default_lookback_days=14).public_view()[
            "price_registry_lookback_days"
        ]
        == 14
    )
    assert (
        Settings(
            _env_file=None, default_lookback_days=14, price_registry_lookback_days=90
        ).effective_price_registry_lookback_days
        == 90
    )
