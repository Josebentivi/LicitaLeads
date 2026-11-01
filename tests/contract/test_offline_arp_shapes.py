"""Offline shape regressions, separate from optional official live contracts.

Samples preserve the envelope/types implicated in stored failures; identities
and descriptions are synthetic. Complete/partial semantics remain explicit.
"""

import json
from pathlib import Path

import httpx
import pytest

from app.config import Settings
from app.connectors.base import DataAvailability
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.http import AsyncHTTPClient

pytestmark = pytest.mark.contract
SAMPLES = json.loads(
    (Path(__file__).parents[1] / "fixtures/responses/arp_regressions.json").read_text()
)


def test_numeric_codes_duplicates_and_child_only_updates_keep_official_identity():
    connector = ComprasGovConnector(Settings(_env_file=None))
    first = connector._map_price_registry_item(
        SAMPLES["numeric_code"], "official-arp", "https://dadosabertos.compras.gov.br/sample"
    )
    assert first.catalog_code == "13768" and first.raw_payload["codigoPdm"] == 13768
    duplicates = [
        connector._map_price_registry_item(
            row, "official-arp", "https://dadosabertos.compras.gov.br/sample"
        )
        for row in SAMPLES["duplicates"]
    ]
    assert duplicates[0].external_id == duplicates[1].external_id
    before, after = [
        connector._map_price_registry_item(
            row, "official-arp", "https://dadosabertos.compras.gov.br/sample"
        )
        for row in SAMPLES["child_update"]
    ]
    assert before.external_id == after.external_id and before.catalog_code != after.catalog_code


@pytest.mark.parametrize("shape,complete", [("empty", True), ("incomplete", False)])
async def test_offline_empty_and_incomplete_pages_are_distinct(shape, complete):
    cfg = Settings(_env_file=None, http_min_request_interval_seconds=0, http_max_retries=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=SAMPLES[shape]))
    ) as client:
        con = ComprasGovConnector(cfg, http_client=AsyncHTTPClient(cfg, client=client))
        result = await con.fetch_price_registry_items("official-arp")
        assert result.partial == (not complete)
        assert result.raw_records
        if complete:
            assert result.availability is DataAvailability.NOT_PUBLISHED
            assert result.pagination.fetched_pages == 1 and not result.pagination.truncated
        else:
            assert result.partial and result.data
