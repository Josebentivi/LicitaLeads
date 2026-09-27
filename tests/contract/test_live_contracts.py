"""Opt-in smoke contracts against the current official public endpoints."""

from __future__ import annotations

import os

import pytest

from app.config import Settings
from app.connectors.base import DataAvailability, ProcurementFilters
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.pncp import PNCPConnector

pytestmark = [
    pytest.mark.contract,
    pytest.mark.live,
    pytest.mark.skipif(
        os.getenv("RUN_LIVE_CONTRACT_TESTS", "").lower() != "true",
        reason="set RUN_LIVE_CONTRACT_TESTS=true to query official APIs",
    ),
]


@pytest.mark.parametrize("connector_type", [PNCPConnector, ComprasGovConnector])
async def test_official_discovery_contract_is_reachable(connector_type: type) -> None:
    """Fetch one small page without persisting or downloading documents."""
    settings = Settings(http_max_retries=2, http_timeout_seconds=60)
    connector = connector_type(settings)
    try:
        result = await connector.discover_procurements(
            ProcurementFilters(
                days=1,
                uf="MA",
                modalities=["pregao_eletronico"],
                page_size=10,
                max_pages=1,
            )
        )
    finally:
        await connector.aclose()

    if result.availability is DataAvailability.TEMPORARY_ERROR:
        pytest.skip(f"official source is temporarily unavailable: {result.diagnostic}")
    assert result.availability in {DataAvailability.AVAILABLE, DataAvailability.EMPTY}
    assert result.source_urls
