"""Frozen contract checks for the audited PNCP and Compras.gov.br shapes."""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import respx

from app.connectors import DataAvailability, ProcurementFilters
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.pncp import PNCPConnector

FIXTURES = Path(__file__).parents[1] / "fixtures"


def _load(relative_path: str) -> dict[str, object]:
    return json.loads((FIXTURES / relative_path).read_text(encoding="utf-8"))


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        pncp_base_url="https://pncp.contract.test/api/consulta/v1",
        pncp_integration_base_url="https://pncp.contract.test/api/pncp/v1",
        compras_gov_base_url="https://compras.contract.test",
        default_modalities=["pregao_eletronico"],
        default_lookback_days=30,
        timezone="America/Sao_Paulo",
        http_timeout_seconds=5,
        http_max_retries=0,
        http_max_concurrency=2,
        http_min_request_interval_seconds=0,
        http_user_agent="LicitaLeadContractTest/1",
    )


@pytest.mark.contract
def test_frozen_openapi_snapshots_contain_every_ingestion_path() -> None:
    pncp = _load("contracts/pncp_openapi_snapshot.json")
    compras = _load("contracts/compras_openapi_snapshot.json")

    assert pncp["captured_at"] == "2026-09-16"
    assert set(pncp["paths"]) >= {
        "/api/consulta/v1/contratacoes/publicacao",
        "/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens",
        "/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/arquivos",
        "/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{item}/resultados",
    }
    assert compras["captured_at"] == "2026-09-16"
    assert set(compras["paths"]) >= {
        "/modulo-contratacoes/1_consultarContratacoes_PNCP_14133",
        "/modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id",
        "/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id",
        "/modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id",
    }


@pytest.mark.asyncio
@pytest.mark.contract
async def test_pncp_discovery_accepts_frozen_official_response_shape() -> None:
    payload = _load("responses/pncp_discovery.json")
    pattern = re.compile(
        r"^https://pncp\.contract\.test/api/consulta/v1/contratacoes/publicacao(?:\?.*)?$"
    )
    with respx.mock(assert_all_called=True) as router:
        router.route(method="GET", url__regex=pattern).mock(
            return_value=httpx.Response(200, json=payload)
        )
        connector = PNCPConnector(_settings())
        try:
            result = await connector.discover_procurements(
                ProcurementFilters(
                    start_date=date(2026, 9, 15),
                    end_date=date(2026, 9, 16),
                    uf="MA",
                    modalities=["pregao_eletronico"],
                )
            )
        finally:
            await connector.aclose()

    assert result.availability is DataAvailability.AVAILABLE
    assert len(result.data) == 1
    row = result.data[0]
    assert row.external_id == "00000000000191-1-000001/2026"
    assert row.pncp_control_number == "00000000000191-1-000001/2026"
    assert row.modality_code == 6
    assert row.uasg == "980921"
    assert row.uf == "MA"
    assert result.raw_records[0].raw_payload == payload


@pytest.mark.asyncio
@pytest.mark.contract
async def test_compras_discovery_accepts_frozen_official_response_shape() -> None:
    payload = _load("responses/compras_discovery.json")
    pattern = re.compile(
        r"^https://compras\.contract\.test/"
        r"modulo-contratacoes/1_consultarContratacoes_PNCP_14133(?:\?.*)?$"
    )
    with respx.mock(assert_all_called=True) as router:
        router.route(method="GET", url__regex=pattern).mock(
            return_value=httpx.Response(200, json=payload)
        )
        connector = ComprasGovConnector(_settings())
        try:
            result = await connector.discover_procurements(
                ProcurementFilters(
                    start_date=date(2026, 9, 15),
                    end_date=date(2026, 9, 16),
                    uf="MA",
                    modalities=["pregao_eletronico"],
                )
            )
        finally:
            await connector.aclose()

    assert result.availability is DataAvailability.AVAILABLE
    assert len(result.data) == 1
    row = result.data[0]
    assert row.external_id == "98092105900012026"
    assert row.pncp_control_number == "00000000000191-1-000001/2026"
    assert row.modality_code == 5
    assert row.uasg == "980921"
    assert row.uf == "MA"
    assert result.raw_records[0].raw_payload == payload
