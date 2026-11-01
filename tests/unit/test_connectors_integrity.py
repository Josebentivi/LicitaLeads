"""Offline regressions for identities, completeness and untrusted downloads."""

import base64
from datetime import date

import httpx
import pytest

from app.config import Settings
from app.connectors.base import DataAvailability, ProcurementFilters, RawContract, RawPriceRegistry
from app.connectors.bounded_http import SourceDownloadError, bounded_request
from app.connectors.capabilities import EndpointDescription
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.http import AsyncHTTPClient
from app.connectors.municipal_payments.fiorilli import FiorilliPaymentConnector
from app.connectors.municipal_payments.sao_luis import SaoLuisCsvConnector
from app.connectors.municipal_payments.sources import get_municipal_source
from app.connectors.pncp import PNCPConnector
from app.services.ingestion.audit import _audit_operation


def settings(**changes):
    return Settings(
        _env_file=None,
        app_env="test",
        http_max_retries=0,
        http_min_request_interval_seconds=0,
        **changes,
    )


async def collect(kind, payload, method, *args, **kwargs):
    calls = []

    def handler(request):
        calls.append(request)
        value = payload(request) if callable(payload) else payload
        return httpx.Response(
            value if isinstance(value, int) else 200, json=None if isinstance(value, int) else value
        )

    selected = settings()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = kind(selected, http_client=AsyncHTTPClient(selected, client=client))
        result = await getattr(connector, method)(*args, **kwargs)
    return result, calls


async def test_compras_fallback_preserves_distinct_procurements_and_item_numbers():
    rows = [
        {
            "orgaoEntidadeCnpj": "12345678000199",
            "numeroCompra": str(number),
            "anoCompraPncp": 2026,
            "unidadeOrgaoCodigoUnidade": f"U{number}",
            "modalidadeNome": "Pregão eletrônico",
        }
        for number in (1, 2)
    ]
    result, _ = await collect(
        ComprasGovConnector,
        {"resultado": rows, "totalPaginas": 1},
        "discover_procurements",
        ProcurementFilters(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 2),
            modalities=["pregao_eletronico"],
        ),
    )
    assert len({row.external_id for row in result.data}) == 2
    assert all("None" not in row.external_id for row in result.data)
    result, _ = await collect(
        ComprasGovConnector,
        {"resultado": [{"numeroItem": 1}, {"numeroItem": 2}]},
        "fetch_items",
        "purchase-1",
    )
    assert {row.item_number for row in result.data} == {"1", "2"}
    assert len({row.external_id for row in result.data}) == 2


async def test_compras_adhesion_accepts_the_current_official_schema_without_item_number():
    registry = RawPriceRegistry(
        source="compras_gov",
        source_url="https://source.test",
        raw_payload={},
        external_id="registry",
        registry_number="1",
        uasg="980921",
    )
    result, calls = await collect(
        ComprasGovConnector,
        lambda request: (
            {"resultado": [{"numeroItem": "1"}]}
            if "consultarARPItem_Id" in request.url.path
            else {
                "resultado": [
                    {
                        "numeroAta": "1",
                        "unidadeGerenciadora": "980921",
                        "unidadeNaoParticipante": "123",
                        "dataAprovacaoAnalise": "2026-01-01",
                        "quantidadeAprovadaAdesao": 10,
                    }
                ]
            }
        ),
        "_fetch_arp_adhesions",
        registry,
    )
    assert result.is_complete and len(result.data) == 1
    assert result.data[0].item_number == "1"
    assert calls[-1].url.params["numeroItem"] == "1"
    assert result.raw_records[0].request_parameters["numeroItem"] == "1"
    assert result.data[0].requesting_unit == "123"
    assert result.data[0].approved_quantity == 10


@pytest.mark.parametrize("kind,key", [(PNCPConnector, "data"), (ComprasGovConnector, "resultado")])
async def test_positive_total_with_empty_discovery_is_a_contract_failure(kind, key):
    result, _ = await collect(
        kind,
        {key: [], "totalRegistros": 50, "totalPaginas": 1},
        "discover_procurements",
        ProcurementFilters(modalities=["pregao_eletronico"]),
    )
    assert result.availability is DataAvailability.TEMPORARY_ERROR
    assert not result.is_complete
    assert result.raw_records


async def test_pncp_detail_follows_remaining_and_rejects_contradictions():
    def pages(request):
        page = int(request.url.params["pagina"])
        return {"data": [{"numeroItem": page}], "paginasRestantes": 2 - page, "totalRegistros": 2}

    result, calls = await collect(PNCPConnector, pages, "fetch_items", "12345678000199-1-1/2026")
    assert len(calls) == 2 and result.is_complete and len(result.data) == 2
    result, _ = await collect(
        PNCPConnector,
        {"data": [{"numeroItem": 1}], "totalPaginas": 1, "paginasRestantes": 5},
        "fetch_items",
        "12345678000199-1-1/2026",
    )
    assert result.partial and not result.is_complete


async def test_contract_terms_keep_earlier_pages_and_bound_repeated_responses():
    contract = RawContract(
        source="pncp",
        source_url="https://source.test",
        raw_payload={},
        external_id="contract",
        agency_cnpj="12345678000199",
        year=2026,
        sequence=1,
    )
    result, _ = await collect(
        PNCPConnector,
        lambda request: (
            {"data": [{"sequencialTermoContrato": 1}], "totalPaginas": 2}
            if request.url.params["pagina"] == "1"
            else 503
        ),
        "fetch_contract_terms",
        contract,
    )
    assert result.partial and len(result.data) == 1 and len(result.raw_records) == 1
    result, calls = await collect(
        PNCPConnector,
        {"data": [{"sequencialTermoContrato": 1}], "totalPaginas": 10000},
        "fetch_contract_terms",
        contract,
    )
    assert 2 <= len(calls) <= 2 * settings().http_max_concurrency + 1
    assert len(result.data) == 1 and not result.is_complete


@pytest.mark.parametrize("link", ["http://127.0.0.1:8000/2026.csv", "https://127.0.0.1/2026.csv"])
async def test_sao_luis_blocks_discovered_private_or_http_urls(link):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, text=f'<a href="{link}">Pagamentos CSV</a>')

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = SaoLuisCsvConnector(
            get_municipal_source("sao_luis"),
            settings(sao_luis_payments_url="https://source.test/index"),
            client=client,
        )
        result = await connector.discover_payments(year=2026)
    assert len(calls) == 1 and not result.is_complete


async def test_redirect_checks_dns_again(monkeypatch):
    async def resolve(host, _port):
        return ["8.8.8.8"] if host == "source.test" else ["10.0.0.1"]

    monkeypatch.setattr("app.connectors.bounded_http.resolve_addresses", resolve)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(302, headers={"Location": "https://private.test/export"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(SourceDownloadError, match="non-public"):
            await bounded_request(
                client, "GET", "https://source.test", max_bytes=10, public_only=True
            )
    assert len(calls) == 1


async def test_stream_stops_before_consuming_the_complete_response():
    class Body(httpx.AsyncByteStream):
        sent = 0
        closed = False

        async def __aiter__(self):
            for _ in range(5):
                self.sent += 6
                yield b"123456"

        async def aclose(self):
            self.closed = True

    body = Body()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=body))
    ) as client:
        with pytest.raises(SourceDownloadError, match="size"):
            await bounded_request(client, "GET", "https://source.test", max_bytes=10)
    assert body.sent == 12 and body.closed


@pytest.mark.parametrize("csv", [False, True])
async def test_sao_luis_keeps_integral_official_response(csv):
    original = {
        "data": [{"Credor": "Empresa", "Valor": "10,00", "Data": "01/02/2026"}],
        "totalRegistros": 1,
        "metadata": {"official": True},
    }
    content = b"Credor;Valor;Data\r\nEmpresa;10,00;01/02/2026\r\n"

    def handler(request):
        return (
            httpx.Response(200, content=content, headers={"Content-Type": "text/csv"})
            if csv
            else httpx.Response(200, json=original)
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await SaoLuisCsvConnector(
            get_municipal_source("sao_luis"), settings(), client=client
        ).discover_payments(year=2026)
    raw = result.raw_records[0].raw_payload
    assert base64.b64decode(raw["content_base64"]) == content if csv else raw == original


async def test_fiorilli_incomplete_details_keep_the_aggregate_and_entity_is_required():
    parent = {"PKEMP": "7", "CODIGO": "100", "PAGO": "100,00", "NOMEFOR": "Empresa"}
    details = [
        {"LIQ": "1", "PAR": "1", "EMPENHO": "100", "VALOR": "20,00", "DATAE": "01/02/2026"},
        {"VALOR": "invalid"},
    ]

    def handler(request):
        return httpx.Response(
            200, json=[parent] if request.url.params["Listagem"] == "DespesasGerais" else details
        )

    selected = settings()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transport = AsyncHTTPClient(selected, client=client)
        result = await FiorilliPaymentConnector(
            get_municipal_source("imperatriz"), selected, http_client=transport
        ).discover_payments(year=2026, include_details=True)
        assert result.partial and len(result.data) == 1
        assert result.data[0].status == "agregado_por_empenho"
        assert result.data[0].commitment_external_id == "imperatriz:e2:2026:7"
        assert not result.data[0].commitment_details_complete
        result = await FiorilliPaymentConnector(
            get_municipal_source("timon"), selected, http_client=transport
        ).discover_payments(year=2026)
        assert result.availability is DataAvailability.TEMPORARY_ERROR


def test_audit_requires_the_correct_envelope_and_item_identity():
    endpoint = EndpointDescription(path="/" + ComprasGovConnector.ITEM_ID_PATH)

    def audit(schema):
        return _audit_operation(
            {},
            {"get": {"responses": {"200": {"content": {"application/json": {"schema": schema}}}}}},
            endpoint,
        )[1]

    assert not audit({"type": "object", "properties": {"numeroControlePNCP": {"type": "string"}}})
    assert not audit(
        {
            "type": "array",
            "items": {"type": "object", "properties": {"numeroControlePNCP": {"type": "string"}}},
        }
    )
    assert audit(
        {
            "type": "array",
            "items": {"type": "object", "properties": {"numeroItem": {"type": "integer"}}},
        }
    )
