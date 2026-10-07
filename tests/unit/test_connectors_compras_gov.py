from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import httpx
import pytest

from app.connectors import DataAvailability, ProcurementFilters
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.http import AsyncHTTPClient


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        compras_gov_base_url="https://compras.test",
        default_modalities=["pregao_eletronico"],
        default_lookback_days=30,
        timezone="America/Sao_Paulo",
        http_max_retries=0,
        http_max_concurrency=2,
        http_min_request_interval_seconds=0,
    )


@pytest.mark.asyncio
async def test_compras_discovery_uses_own_modality_domain_and_paginates() -> None:
    requests: list[httpx.Request] = []
    row = {
        "idCompra": "98092105900012026",
        "numeroControlePNCP": "ABC123450001ZZ-1-000001/2026",
        "anoCompraPncp": 2026,
        "sequencialCompraPncp": 1,
        "orgaoEntidadeCnpj": "ABC123450001ZZ",
        "orgaoEntidadeRazaoSocial": "Município de São Luís",
        "unidadeOrgaoCodigoUnidade": "980921",
        "unidadeOrgaoNomeUnidade": "Prefeitura de São Luís",
        "unidadeOrgaoUfSigla": "MA",
        "unidadeOrgaoMunicipioNome": "São Luís",
        "numeroCompra": "90001",
        "modalidadeIdPncp": 6,
        "codigoModalidade": 5,
        "modalidadeNome": "Pregão - Eletrônico",
        "objetoCompra": "Aquisição de cadeiras",
        "srp": False,
        "amparoLegalNome": "Lei 14.133/2021, art. 74, inciso III",
        "dataPublicacaoPncp": "2026-09-15T10:00:00",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        page = int(request.url.params["pagina"])
        return httpx.Response(
            200,
            json={
                "resultado": [row],
                "totalRegistros": 2,
                "totalPaginas": 2,
                "paginasRestantes": 2 - page,
            },
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = ComprasGovConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    result = await connector.discover_procurements(
        ProcurementFilters(
            start_date=date(2026, 9, 15),
            end_date=date(2026, 9, 16),
            uf="MA",
            modalities=["pregao_eletronico"],
            page_size=1,
        )
    )

    assert result.availability is DataAvailability.AVAILABLE
    assert len(result.data) == 1
    assert result.data[0].modality_code == 5
    assert result.data[0].is_srp is False
    assert result.data[0].legal_basis == "Lei 14.133/2021, art. 74, inciso III"
    assert result.data[0].pncp_control_number == "ABC123450001ZZ-1-000001/2026"
    assert len(requests) == 2
    assert all(request.url.params["codigoModalidade"] == "5" for request in requests)
    assert requests[0].url.params["dataPublicacaoPncpInicial"] == "2026-09-15"
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_compras_arp_discovery_and_items_map_official_fields() -> None:
    requests: list[httpx.Request] = []
    registry_row = {
        "numeroControlePncpAta": "12345678000199-1-000001/2026",
        "numeroControlePncpCompra": "12345678000199-1-000002/2026",
        "numeroAtaRegistroPreco": "0001",
        "anoCompra": "2026",
        "codigoUnidadeGerenciadora": "980921",
        "nomeOrgao": "Órgão de Exemplo",
        "objeto": "Registro de preços de cadeiras",
        "statusAta": "Vigente",
        "dataAssinatura": "2026-01-01",
        "dataVigenciaInicial": "2026-01-01",
        "dataVigenciaFinal": "2026-12-31",
        "valorTotal": 100000.5,
        "ataExcluido": False,
        "linkAtaPNCP": "https://pncp.gov.br/app/atas/12345678000199/2026/1",
    }
    item_row = {
        "numeroItem": "1",
        "descricaoItem": "Cadeira escolar",
        "quantidadeHomologadaItem": 100,
        "valorUnitario": 150.25,
        "valorTotal": 15025.0,
        "maximoAdesao": 200,
        "niFornecedor": "12345678000199",
        "nomeRazaoSocialFornecedor": "Empresa Registrada Ltda.",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if "2.1_consultarARPItem_Id" in str(request.url):
            return httpx.Response(
                200, json={"resultado": [item_row], "totalPaginas": 1}, request=request
            )
        return httpx.Response(
            200,
            json={"resultado": [registry_row], "totalPaginas": 1, "totalRegistros": 1},
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = ComprasGovConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    result = await connector.discover_price_registries(
        ProcurementFilters(start_date=date(2026, 1, 1), end_date=date(2026, 1, 31))
    )

    assert result.availability is DataAvailability.AVAILABLE
    registry = result.data[0]
    assert registry.external_id == "12345678000199-1-000001/2026"
    assert registry.registry_number == "0001"
    assert registry.year == 2026
    assert registry.uasg == "980921"
    assert registry.total_value is not None
    assert registry.source_url == "https://pncp.gov.br/app/atas/12345678000199/2026/1"
    assert requests[0].url.params["dataVigenciaInicialMin"] == "2026-01-01"

    items = await connector.fetch_price_registry_items(registry.external_id)
    assert items.availability is DataAvailability.AVAILABLE
    item = items.data[0]
    assert item.item_number == "1"
    assert item.quantity is not None and item.unit_value is not None
    assert item.supplier_cnpj == "12345678000199"
    assert item.supplier_name == "Empresa Registrada Ltda."
    assert requests[-1].url.params["numeroControlePncpAta"] == "12345678000199-1-000001/2026"
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_compras_participants_exclude_people_and_cancelled_results() -> None:
    rows = [
        {
            "idCompraItem": "item-1",
            "numeroItemPncp": 1,
            "sequencialResultado": 1,
            "niFornecedor": "ABC123450001ZZ",
            "tipoPessoa": "PJ",
            "nomeRazaoSocialFornecedor": "Empresa válida Ltda.",
            "valorTotalHomologado": 100,
            "situacaoCompraItemResultadoNome": "Informado",
        },
        {
            "idCompraItem": "item-2",
            "numeroItemPncp": 2,
            "sequencialResultado": 1,
            "niFornecedor": "12345678901",
            "tipoPessoa": "PF",
            "nomeRazaoSocialFornecedor": "Pessoa física",
            "situacaoCompraItemResultadoNome": "Informado",
        },
        {
            "idCompraItem": "item-3",
            "numeroItemPncp": 3,
            "sequencialResultado": 1,
            "niFornecedor": "99999999000199",
            "tipoPessoa": "PJ",
            "nomeRazaoSocialFornecedor": "Resultado cancelado Ltda.",
            "situacaoCompraItemResultadoNome": "Cancelado",
            "dataCancelamentoPncp": "2026-09-16T09:00:00",
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["tipo"] == "idCompra"
        return httpx.Response(
            200,
            json={
                "resultado": rows,
                "totalRegistros": 3,
                "totalPaginas": 1,
                "paginasRestantes": 0,
            },
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = ComprasGovConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    result = await connector.fetch_participants("98092105900012026")

    assert result.availability is DataAvailability.AVAILABLE
    assert [participant.company_name for participant in result.data] == ["Empresa válida Ltda."]
    assert result.data[0].participation_role == "awarded"
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_compras_documents_are_explicitly_not_supported_without_http_call() -> None:
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(500, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = ComprasGovConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    result = await connector.fetch_documents("id")

    assert result.availability is DataAvailability.NOT_SUPPORTED
    assert not called
    await raw_client.aclose()
