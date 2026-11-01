from __future__ import annotations

import asyncio
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


async def test_detail_throttle_is_scoped_across_records_without_throttling_items():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(429))
    ) as raw:
        http = AsyncHTTPClient(_settings(), client=raw)
        connector = PNCPConnector(_settings(), http_client=http)
        result = await connector.fetch_procurement("12345678000199-1-000001/2026")
        assert result.availability is DataAvailability.TEMPORARY_ERROR
        state = http._scope_pacing[("https://pncp.test/", "pncp.procurement_detail")]
        assert state.interval == 0.5
        assert http._origin_pacing["https://pncp.test/"].interval == 0
        assert http._origin_pacing["https://pncp.test/"].cooldown_until > 0


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
        "srp": True,
        "amparoLegal": {"codigo": 1, "nome": "Lei 14.133/2021, art. 75, inciso I"},
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
    assert result.data[0].is_srp is True
    assert result.data[0].legal_basis == "Lei 14.133/2021, art. 75, inciso I"
    assert result.data[0].publication_at is not None
    assert result.data[0].publication_at.utcoffset().total_seconds() == 0
    assert result.data[0].source_record_ref == result.raw_records[0].record_key
    assert len(result.raw_records) == 2
    assert result.pagination is not None and result.pagination.fetched_pages == 2
    assert all(request.url.params["codigoModalidadeContratacao"] == "6" for request in requests)
    assert requests[0].url.params["dataInicial"] == "20260915"
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_pncp_price_registry_discovery_maps_audited_fields() -> None:
    requests: list[httpx.Request] = []
    row = {
        "numeroControlePNCPAta": "12345678000199-1-000001/2026",
        "numeroControlePNCPCompra": "12345678000199-1-000002/2026",
        "numeroAtaRegistroPreco": "0001",
        "anoAta": 2026,
        "cnpjOrgao": "12345678000199",
        "nomeOrgao": "Órgão de Exemplo",
        "codigoUnidadeOrgao": "980921",
        "objetoContratacao": "Registro de preços de cadeiras",
        "vigenciaInicio": "2026-01-01",
        "vigenciaFim": "2026-12-31",
        "dataAssinatura": "2026-01-01",
        "dataPublicacaoPncp": "2026-01-02T10:00:00",
        "possibilidadeAdesao": True,
        "cancelado": False,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={"data": [row], "totalPaginas": 1, "totalRegistros": 1},
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = PNCPConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    result = await connector.discover_price_registries(
        ProcurementFilters(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 31),
            agency_cnpj="12345678000199",
            unit_code="980921",
        )
    )

    assert result.availability is DataAvailability.AVAILABLE
    assert len(result.data) == 1
    registry = result.data[0]
    assert registry.external_id == "12345678000199-1-000001/2026"
    assert registry.pncp_control_number == "12345678000199-1-000001/2026"
    assert registry.linked_pncp_control_number == "12345678000199-1-000002/2026"
    assert registry.registry_number == "0001"
    assert registry.agency_cnpj == "12345678000199"
    assert registry.uasg == "980921"
    assert registry.valid_from is not None and registry.valid_until is not None
    assert registry.allows_adhesion is True
    assert registry.status is None
    assert registry.source_record_ref == result.raw_records[0].record_key
    assert requests[0].url.params["dataInicial"] == "20260101"
    assert requests[0].url.params["cnpj"] == "12345678000199"
    assert requests[0].url.params["codigoUnidadeAdministrativa"] == "980921"
    assert "codigoModalidadeContratacao" not in requests[0].url.params

    items = await connector.fetch_price_registry_items(registry.external_id)
    assert items.availability is DataAvailability.NOT_SUPPORTED
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
async def test_pncp_no_content_is_empty_not_a_decode_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = PNCPConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    result = await connector.discover_procurements(
        ProcurementFilters(
            start_date=date(2026, 10, 26),
            end_date=date(2026, 10, 26),
            uf="MA",
            modalities=["pregao_eletronico"],
        )
    )

    assert result.availability is DataAvailability.EMPTY
    assert result.data == []
    assert (
        result.diagnostic == "no procurements matched the official response and client-side filters"
    )
    assert len(result.raw_records) == 1
    assert result.raw_records[0].http_status == 204
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_pncp_html_service_error_is_temporary() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            text="<html>No server is available</html>",
            headers={"content-type": "text/html"},
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = PNCPConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    result = await connector.discover_procurements(
        ProcurementFilters(
            start_date=date(2026, 10, 26),
            end_date=date(2026, 10, 26),
            uf="MA",
            modalities=["pregao_eletronico"],
        )
    )

    assert result.availability is DataAvailability.TEMPORARY_ERROR
    assert result.data == []
    assert "temporary official source error" in (result.diagnostic or "")
    assert "503" in (result.diagnostic or "")
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
async def test_pncp_negative_values_and_zero_rank_are_treated_as_unavailable() -> None:
    """Invalid source values never violate the canonical nonnegative invariants."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/contratacoes/publicacao"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "numeroControlePNCP": "12345678000195-1-000001/2026",
                            "anoCompra": 2026,
                            "sequencialCompra": 1,
                            "orgaoEntidade": {"cnpj": "12345678000195"},
                            "unidadeOrgao": {"ufSigla": "MA"},
                            "modalidadeId": 6,
                            "modalidadeNome": "Pregão - Eletrônico",
                            "valorTotalEstimado": -0.0001,
                        }
                    ],
                    "totalPaginas": 1,
                    "totalRegistros": 1,
                },
                request=request,
            )
        if path.endswith("/itens"):
            return httpx.Response(
                200,
                json=[
                    {
                        "numeroItem": 1,
                        "descricao": "Item com valores inválidos",
                        "quantidade": -1,
                        "valorUnitarioEstimado": -0.0001,
                        "valorTotal": -0.0001,
                        "temResultado": True,
                    }
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
                        "niFornecedor": "12345678000195",
                        "tipoPessoaId": "PJ",
                        "nomeRazaoSocialFornecedor": "Empresa Exemplo Ltda.",
                        "valorTotalHomologado": -5,
                        "ordemClassificacaoSrp": 0,
                        "situacaoCompraItemResultadoNome": "Informado",
                    }
                ],
                request=request,
            )
        raise AssertionError(f"unexpected request {request.url}")

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = PNCPConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    discovery = await connector.discover_procurements(
        ProcurementFilters(start_date=date(2026, 1, 1), end_date=date(2026, 1, 31))
    )
    items = await connector.fetch_items("12345678000195-1-000001/2026")
    results = await connector.fetch_results("12345678000195-1-000001/2026")

    assert discovery.data[0].estimated_value is None
    assert items.data[0].quantity is None
    assert items.data[0].estimated_unit_value is None
    assert items.data[0].estimated_total_value is None
    assert results.data[0].total_value is None
    assert results.data[0].rank is None
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


@pytest.mark.asyncio
async def test_pncp_contract_discovery_maps_contract_and_provenance() -> None:
    row = {
        "numeroControlePNCP": "contract-control-1",
        "numeroControlePncpCompra": "12345678000199-1-000001/2026",
        "numeroControlePncpAta": "ata-control-1",
        "numeroContratoEmpenho": "0001",
        "anoContrato": 2026,
        "sequencialContrato": 1,
        "processo": "90001/2026",
        "orgaoEntidade": {"cnpj": "12345678000199", "razaoSocial": "Orgao de Exemplo"},
        "unidadeOrgao": {"codigoUnidade": "980921"},
        "objetoContrato": "Contrato de cadeiras",
        "dataAssinatura": "2026-09-15",
        "dataPublicacaoPncp": "2026-09-15T10:00:00",
        "dataVigenciaInicio": "2026-09-15",
        "dataVigenciaFim": "2027-09-15",
        "valorInicial": 1000,
        "valorGlobal": 1000,
        "valorAcumulado": 800,
        "frutoAdesao": False,
        "niFornecedor": "00000000000191",
        "nomeRazaoSocialFornecedor": "Empresa Contratada Ltda.",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": [row],
                "totalRegistros": 1,
                "totalPaginas": 1,
                "numeroPagina": 1,
                "paginasRestantes": 0,
            },
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = PNCPConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    result = await connector.discover_contracts(
        ProcurementFilters(
            start_date=date(2026, 9, 15), end_date=date(2026, 9, 16), agency_cnpj="12345678000199"
        )
    )

    assert result.availability is DataAvailability.AVAILABLE
    assert result.data[0].contract_number == "0001"
    assert result.data[0].agency_name == "Orgao de Exemplo"
    assert result.data[0].procurement_control_number == "12345678000199-1-000001/2026"
    assert result.data[0].price_registry_control_number == "ata-control-1"
    assert result.data[0].supplier_cnpj == "00000000000191"
    assert result.data[0].global_value == 1000
    assert result.data[0].source_record_ref == result.raw_records[0].record_key
    await raw_client.aclose()


@pytest.mark.parametrize("discovery_kind", ["publication", "update"])
@pytest.mark.parametrize("valid_count,invalid_count", [(1, 1), (0, 2), (1, 0), (0, 0)])
async def test_contract_discovery_retains_rows_but_rejects_incomplete_coverage(
    discovery_kind, valid_count, invalid_count
):
    valid = {
        "numeroControlePNCP": "00000000000191-2-000001/2026",
        "numeroContratoEmpenho": "0001",
        "anoContrato": 2026,
        "sequencialContrato": 1,
        "orgaoEntidade": {"cnpj": "00000000000191"},
    }
    rows = [valid] * valid_count + [{"numeroContratoEmpenho": "sem-identidade"}] * invalid_count
    payload = {
        "data": rows,
        "totalRegistros": len(rows),
        "totalPaginas": 1,
        "numeroPagina": 1,
        "paginasRestantes": 0,
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    ) as raw_client:
        connector = PNCPConnector(
            _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
        )
        result = await connector.discover_contracts(
            ProcurementFilters(
                start_date=date(2026, 9, 15),
                end_date=date(2026, 9, 16),
                agency_cnpj="00000000000191",
                discovery_kind=discovery_kind,
            )
        )
    assert len(result.data) == valid_count
    assert result.raw_records[0].raw_payload == payload
    assert result.partial is bool(invalid_count)
    assert result.is_complete is (not bool(invalid_count))
    assert not result.pagination.truncated  # invalid source rows are not a collection limit
    assert result.availability is (
        DataAvailability.AVAILABLE
        if valid_count
        else DataAvailability.TEMPORARY_ERROR
        if invalid_count
        else DataAvailability.EMPTY
    )
    if valid_count:
        assert result.data[0].source_record_ref == result.raw_records[0].record_key
    if invalid_count:
        assert f"{invalid_count} contract row(s) without official identity" in result.diagnostic


@pytest.mark.asyncio
async def test_pncp_contract_terms_are_mapped() -> None:
    from app.connectors.base import RawContract

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "sequencialTermoContrato": 1,
                        "numeroTermoContrato": "1",
                        "tipoTermoContratoNome": "Aditivo de valor",
                        "dataAssinatura": "2026-09-15",
                        "dataPublicacaoPncp": "2026-09-15T10:00:00",
                        "valorAcrescido": 100,
                        "dataVigenciaFim": "2027-09-15",
                        "objetoTermoContrato": "Acrescimo",
                    }
                ],
                "totalPaginas": 1,
            },
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = PNCPConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    result = await connector.fetch_contract_terms(
        RawContract(
            source="pncp",
            external_id="contract-control-1",
            agency_cnpj="12345678000199",
            year=2026,
            sequence=1,
        )
    )

    assert result.availability is DataAvailability.AVAILABLE
    assert result.data[0].term_type == "Aditivo de valor"
    assert result.data[0].value_delta == 100
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_pncp_unexpected_json_is_not_empty() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"error": "maintenance"}, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = PNCPConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    try:
        result = await connector.discover_procurements(
            ProcurementFilters(start_date=date(2026, 1, 1), end_date=date(2026, 1, 2))
        )
    finally:
        await connector.aclose()

    assert result.availability is DataAvailability.TEMPORARY_ERROR
    assert result.data == []
    assert "contract" in (result.diagnostic or "")


@pytest.mark.asyncio
async def test_pncp_repeated_detail_page_is_bounded() -> None:
    row = {"numeroItem": 1, "descricao": "Item"}
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=[row] * 50, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = PNCPConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    try:
        result = await asyncio.wait_for(
            connector.fetch_items("12345678000199-1-000001/2026"), timeout=5
        )
    finally:
        await connector.aclose()

    assert calls == 2
    assert result.availability is DataAvailability.AVAILABLE
    assert result.partial is True
    assert len(result.data) == 50
    assert "partial" in (result.diagnostic or "")


@pytest.mark.asyncio
async def test_pncp_empty_object_with_200_is_contract_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={}, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    connector = PNCPConnector(
        _settings(), http_client=AsyncHTTPClient(_settings(), client=raw_client)
    )
    try:
        result = await connector.discover_procurements(
            ProcurementFilters(start_date=date(2026, 1, 1), end_date=date(2026, 1, 2))
        )
    finally:
        await connector.aclose()

    assert result.availability is DataAvailability.TEMPORARY_ERROR
