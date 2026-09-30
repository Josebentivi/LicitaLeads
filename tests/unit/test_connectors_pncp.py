from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import httpx
import pytest

from app.connectors import DataAvailability, ProcurementFilters
from app.connectors.http import AsyncHTTPClient
from app.connectors.pncp import PNCPConnector, parse_pncp_external_id


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        pncp_base_url="https://pncp.test/api/consulta/v1",
        pncp_integration_base_url="https://pncp.test/api/pncp/v1",
        default_modalities=["pregao_eletronico"],
        default_lookback_days=30,
        timezone="America/Sao_Paulo",
        http_max_retries=0,
        http_max_concurrency=2,
        http_min_request_interval_seconds=0,
    )


@pytest.mark.asyncio
async def test_pncp_discovery_paginates_filters_and_deduplicates() -> None:
    requests: list[httpx.Request] = []
    row = {
        "numeroControlePNCP": "ABC123450001ZZ-1-000011/2026",
        "anoCompra": 2026,
        "sequencialCompra": 11,
        "numeroCompra": "90008",
        "processo": "0302/2026",
        "objetoCompra": "Aquisição de cadeiras escolares",
        "orgaoEntidade": {
            "cnpj": "ABC123450001ZZ",
            "razaoSocial": "Município de São Luís",
            "esferaId": "M",
            "poderId": "N",
        },
        "unidadeOrgao": {
            "codigoUnidade": "980921",
            "ufSigla": "MA",
            "municipioNome": "São Luís",
            "codigoIbge": "2111300",
        },
        "modalidadeId": 6,
        "modalidadeNome": "Pregão - Eletrônico",
        "valorTotalEstimado": 1000.25,
        "dataPublicacaoPncp": "2026-09-15T10:00:00",
        "dataAtualizacaoGlobal": "2026-09-15T11:00:00",
        "situacaoCompraNome": "Divulgada no PNCP",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        page = int(request.url.params["pagina"])
        payload = {
            "data": [row],
            "totalRegistros": 2,
            "totalPaginas": 2,
            "numeroPagina": page,
            "paginasRestantes": 2 - page,
            "empty": False,
        }
        return httpx.Response(200, json=payload, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    http = AsyncHTTPClient(_settings(), client=raw_client)
    connector = PNCPConnector(_settings(), http_client=http)
    result = await connector.discover_procurements(
        ProcurementFilters(
            start_date=date(2026, 9, 15),
            end_date=date(2026, 9, 16),
            uf="ma",
            agency="SAO LUIS",
            keyword="cadeira",
            municipality="sao luis",
        )
    )

    assert result.availability is DataAvailability.AVAILABLE
    assert len(result.data) == 1
    assert result.data[0].agency_cnpj == "ABC123450001ZZ"
    assert result.data[0].modality_code == 6
    assert result.data[0].publication_at is not None
    assert result.data[0].publication_at.utcoffset().total_seconds() == 0
    assert len(result.raw_records) == 2
    assert result.pagination is not None and result.pagination.fetched_pages == 2
    assert all(request.url.params["codigoModalidadeContratacao"] == "6" for request in requests)
    assert requests[0].url.params["dataInicial"] == "20260915"
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_pncp_partial_pagination_does_not_claim_empty_data() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["pagina"])
        if page == 1:
            return httpx.Response(
                200,
                json={
                    "data": [],
                    "totalRegistros": 1,
                    "totalPaginas": 2,
                    "numeroPagina": 1,
                    "paginasRestantes": 1,
                },
                request=request,
            )
        return httpx.Response(503, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = PNCPConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    result = await connector.discover_procurements(
        ProcurementFilters(
            start_date=date(2026, 9, 15),
            end_date=date(2026, 9, 16),
            uf="MA",
        )
    )

    assert result.availability is DataAvailability.TEMPORARY_ERROR
    assert len(result.raw_records) == 1
    assert result.data == []
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_pncp_returns_only_awarded_legal_entities_as_participants() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/itens"):
            return httpx.Response(
                200,
                json=[
                    {"numeroItem": 1, "descricao": "Item", "temResultado": True},
                    {"numeroItem": 2, "descricao": "Sem resultado", "temResultado": False},
                ],
                request=request,
            )
        if path.endswith("/itens/1/resultados"):
            return httpx.Response(
                200,
                json=[
                    {
                        "numeroItem": 1,
                        "sequencialResultado": 1,
                        "niFornecedor": "ABC123450001ZZ",
                        "tipoPessoaId": "PJ",
                        "nomeRazaoSocialFornecedor": "Empresa vencedora Ltda.",
                        "valorTotalHomologado": 900,
                        "situacaoCompraItemResultadoNome": "Informado",
                    },
                    {
                        "numeroItem": 1,
                        "sequencialResultado": 2,
                        "niFornecedor": "12345678901",
                        "tipoPessoaId": "PF",
                        "nomeRazaoSocialFornecedor": "Pessoa física",
                        "situacaoCompraItemResultadoNome": "Informado",
                    },
                ],
                request=request,
            )
        raise AssertionError(f"unexpected request {request.url}")

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = PNCPConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    participants = await connector.fetch_participants("ABC123450001ZZ-1-000011/2026")

    assert participants.availability is DataAvailability.AVAILABLE
    assert len(participants.data) == 1
    assert participants.data[0].company_cnpj == "ABC123450001ZZ"
    assert participants.data[0].participation_role == "awarded"
    assert "not a complete bidder list" in (participants.diagnostic or "")
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_pncp_events_are_not_confused_with_technical_history() -> None:
    http = AsyncHTTPClient(_settings())
    connector = PNCPConnector(_settings(), http_client=http)
    result = await connector.fetch_events("ABC123450001ZZ-1-000011/2026")

    assert result.availability is DataAvailability.NOT_SUPPORTED
    assert result.data == []
    await http.aclose()


def test_parse_pncp_external_id_accepts_alphanumeric_cnpj() -> None:
    reference = parse_pncp_external_id("ABC123450001ZZ-1-000011/2026")

    assert reference.cnpj == "ABC123450001ZZ"
    assert reference.year == 2026
    assert reference.sequence == 11
