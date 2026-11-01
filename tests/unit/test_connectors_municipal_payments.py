"""Municipal payment adapters keep source limits and provenance explicit."""

from __future__ import annotations

from datetime import UTC
from decimal import Decimal

import httpx
import pytest

from app.config import Settings
from app.connectors.base import DataAvailability
from app.connectors.http import AsyncHTTPClient
from app.connectors.municipal_payments import (
    build_municipal_connector,
    list_municipal_sources,
    parse_brazilian_date,
    parse_brazilian_money,
)
from app.connectors.municipal_payments.sources import selected_municipal_sources
from app.connectors.payment_sources import list_payment_sources

SOURCE_URL = "https://transparencia.test/VersaoJson/Despesas/"


@pytest.mark.parametrize("payload", [["maintenance"], [{}], {"error": "maintenance"}])
async def test_fiorilli_contract_error_preserves_raw_and_never_claims_empty(payload):
    settings = _settings()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    ) as client:
        connector = build_municipal_connector(
            "imperatriz", settings, http_client=AsyncHTTPClient(settings, client=client)
        )
        result = await connector.discover_payments(year=2026)
    assert result.availability is DataAvailability.TEMPORARY_ERROR
    assert result.raw_records[0].raw_payload == payload


def _settings(**overrides: object) -> Settings:
    return Settings(
        app_env="test",
        http_min_request_interval_seconds=0,
        http_max_retries=0,
        **overrides,
    )


def _row(
    *,
    pkemp: str = "1",
    codigo: str = "100",
    cnpj: str = "52.246.769/0001-98",
    paid: str = "1.234,56",
) -> dict[str, str]:
    return {
        "EMPRESA": "1",
        "PKEMP": pkemp,
        "CODIGO": codigo,
        "TPEM": "OR",
        "CPFFORMATADO": cnpj,
        "NOMEFOR": "Empresa Teste Ltda",
        "NOMEEMPRESA": "Prefeitura Municipal",
        "PAGO": paid,
        "DATAE": "01/02/2026 00:00:00",
    }


@pytest.mark.asyncio
async def test_fiorilli_aggregate_maps_paid_commitment_without_inventing_date() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[_row()], request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = _settings()
    connector = build_municipal_connector(
        "imperatriz",
        settings,
        http_client=AsyncHTTPClient(settings, client=raw_client, max_retries=0),
    )

    result = await connector.discover_payments(year=2026)

    assert result.availability is DataAvailability.AVAILABLE
    assert len(result.data) == 1
    payment = result.data[0]
    assert payment.source == "municipal:imperatriz"
    assert payment.amount == Decimal("1234.56")
    assert payment.supplier_cnpj == "52246769000198"
    assert payment.payment_date is None
    assert payment.status == "agregado_por_empenho"
    assert payment.commitment_number == "100"
    assert payment.external_id.startswith("imperatriz:e2:2026:")
    assert payment.source_record_ref == result.raw_records[0].record_key
    assert result.raw_records and result.raw_records[0].http_status == 200
    assert requests[0].url.params["Listagem"] == "DespesasGerais"
    assert requests[0].url.params["Empresa"] == "2"
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_timon_filters_multi_entity_response_on_the_client() -> None:
    first = _row(pkemp="18-1", codigo="18001")
    first["EMPRESA"] = "18"
    second = _row(pkemp="3-1", codigo="3001")
    second["EMPRESA"] = "3"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["Empresa"] == "18"
        return httpx.Response(200, json=[first, second], request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = _settings()
    connector = build_municipal_connector(
        "timon",
        settings,
        http_client=AsyncHTTPClient(settings, client=raw_client, max_retries=0),
    )

    result = await connector.discover_payments(year=2026)

    assert result.availability is DataAvailability.AVAILABLE
    assert len(result.data) == 1
    assert result.data[0].commitment_number == "18001"
    assert result.data[0].external_id.startswith("timon:e18:2026:")
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_fiorilli_filters_supplier_and_reports_empty() -> None:
    rows = [
        _row(pkemp="1", codigo="100", cnpj="52.246.769/0001-98"),
        _row(pkemp="2", codigo="200", cnpj="11.222.333/0001-81"),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=rows, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = _settings()
    connector = build_municipal_connector(
        "bacabal",
        settings,
        http_client=AsyncHTTPClient(settings, client=raw_client, max_retries=0),
    )

    filtered = await connector.discover_payments(year=2026, company_cnpj="52.246.769/0001-98")
    missing = await connector.discover_payments(year=2026, company_cnpj="00.000.000/0001-91")

    assert filtered.availability is DataAvailability.AVAILABLE
    assert len(filtered.data) == 1
    assert missing.availability is DataAvailability.EMPTY
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_fiorilli_details_are_optional_and_fall_back_to_aggregate() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("Listagem") == "DespesasGerais":
            return httpx.Response(
                200,
                json=[_row(pkemp="1", codigo="100"), _row(pkemp="2", codigo="200")],
                request=request,
            )
        return httpx.Response(
            200,
            json=[
                {
                    "PKEMP": "1",
                    "EMPENHO": "100",
                    "TIPO": "OR",
                    "LIQ": "1",
                    "PAR": "1",
                    "VALOR": "600,00",
                    "DATAE": "10/03/2026 00:00:00",
                    "FORNECEDOR": "Empresa Teste Ltda",
                },
                {
                    "PKEMP": "1",
                    "EMPENHO": "100",
                    "TIPO": "OR",
                    "LIQ": "2",
                    "PAR": "2",
                    "VALOR": "634,56",
                    "DATAE": "10/04/2026 00:00:00",
                    "FORNECEDOR": "Empresa Teste Ltda",
                },
            ],
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = _settings(municipal_payment_detail_limit=1)
    connector = build_municipal_connector(
        "caxias",
        settings,
        http_client=AsyncHTTPClient(settings, client=raw_client, max_retries=0),
    )

    result = await connector.discover_payments(year=2026, include_details=True)

    assert result.availability is DataAvailability.AVAILABLE
    assert len(result.data) == 3
    detailed = [item for item in result.data if item.status == "detalhe_empenho"]
    aggregate = [item for item in result.data if item.status == "agregado_por_empenho"]
    assert len(detailed) == 2
    assert all(item.payment_date is not None for item in detailed)
    assert all(item.external_id.startswith("caxias:2026:") for item in detailed)
    assert detailed[0].document_number == "1/1"
    assert detailed[0].source_record_ref == result.raw_records[1].record_key
    assert len(aggregate) == 1
    assert aggregate[0].source_record_ref == result.raw_records[0].record_key
    assert "limitado a 1" in (result.diagnostic or "")
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_fiorilli_detail_rejects_other_entity_rows() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("Listagem") == "DespesasGerais":
            return httpx.Response(200, json=[_row(pkemp="1", codigo="100")], request=request)
        return httpx.Response(
            200,
            json=[
                {
                    "PKEMP": "1",
                    "EMPENHO": "200",
                    "CPFFORMATADO": "11.222.333/0001-81",
                    "LIQ": "1",
                    "PAR": "1",
                    "VALOR": "600,00",
                    "DATAE": "10/03/2026 00:00:00",
                }
            ],
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = _settings(municipal_payment_detail_limit=1)
    connector = build_municipal_connector(
        "caxias",
        settings,
        http_client=AsyncHTTPClient(settings, client=raw_client, max_retries=0),
    )

    result = await connector.discover_payments(year=2026, include_details=True)

    assert result.availability is DataAvailability.AVAILABLE
    assert len(result.data) == 1
    assert result.data[0].status == "agregado_por_empenho"
    assert result.data[0].supplier_cnpj == "52246769000198"
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_fiorilli_403_and_500_are_not_empty_success() -> None:
    def forbidden(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text="blocked", request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(forbidden))
    settings = _settings()
    connector = build_municipal_connector(
        "imperatriz",
        settings,
        http_client=AsyncHTTPClient(settings, client=raw_client, max_retries=0),
    )
    result = await connector.discover_payments(year=2026)
    assert result.availability is DataAvailability.ACCESS_RESTRICTED
    assert result.data == []
    await raw_client.aclose()

    def broken(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="erro", request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(broken))
    connector = build_municipal_connector(
        "timon",
        settings,
        http_client=AsyncHTTPClient(settings, client=raw_client, max_retries=0),
    )
    result = await connector.discover_payments(year=2026)
    assert result.availability is DataAvailability.TEMPORARY_ERROR
    assert result.raw_records and result.raw_records[0].http_status == 500
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_fiorilli_rejects_invalid_cnpj_without_http() -> None:
    settings = _settings()
    connector = build_municipal_connector("imperatriz", settings)
    result = await connector.discover_payments(year=2026, company_cnpj="123")
    assert result.availability is DataAvailability.NOT_SUPPORTED
    assert "CNPJ" in (result.diagnostic or "")
    await connector.aclose()


@pytest.mark.asyncio
async def test_sao_luis_csv_parses_rows_without_requiring_cnpj() -> None:
    body = (
        "Data;Fornecedor;CNPJ;Valor;Empenho;Orgao\n"
        "01/02/2026;Fornecedor Sem CNPJ;;1.234,56;2026NE0001;SECRETARIA\n"
        "01/02/2025;Fornecedor Antigo;;99,00;2025NE0001;SECRETARIA\n"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text=body,
            headers={"content-type": "text/csv; charset=utf-8"},
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = _settings(sao_luis_payments_url="https://saoluis.test/dados-abertos")
    connector = build_municipal_connector("sao_luis", settings, http_client=raw_client)

    result = await connector.discover_payments(year=2026)

    assert result.availability is DataAvailability.AVAILABLE
    assert len(result.data) == 1
    payment = result.data[0]
    assert payment.supplier_cnpj is None
    assert payment.supplier_name == "Fornecedor Sem CNPJ"
    assert payment.amount == Decimal("1234.56")
    assert payment.payment_date is not None
    assert payment.source == "municipal:sao_luis"
    assert payment.source_record_ref == result.raw_records[0].record_key
    assert "fora do ano 2026" in (result.diagnostic or "")
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_sao_luis_follows_csv_link_and_reports_blocking() -> None:
    csv_body = "Data;Credor;Valor\n01/02/2026;Empresa;10,00\n"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith(".csv"):
            return httpx.Response(
                200,
                text=csv_body,
                headers={"content-type": "text/csv"},
                request=request,
            )
        return httpx.Response(
            200,
            text='<html><a href="/arquivos/despesas.csv">Despesas</a></html>',
            headers={"content-type": "text/html"},
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = _settings(sao_luis_payments_url="https://saoluis.test/dados-abertos")
    connector = build_municipal_connector("sao_luis", settings, http_client=raw_client)
    result = await connector.discover_payments(year=2026)
    assert result.availability is DataAvailability.AVAILABLE
    assert result.data[0].amount == Decimal("10.00")
    assert result.source_urls == ["https://saoluis.test/arquivos/despesas.csv"]
    await raw_client.aclose()

    def blocked(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text="cloudflare", request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(blocked))
    connector = build_municipal_connector("sao_luis", settings, http_client=raw_client)
    result = await connector.discover_payments(year=2026)
    assert result.availability is DataAvailability.ACCESS_RESTRICTED
    assert "exportacao manual" in (result.diagnostic or "")
    await raw_client.aclose()


@pytest.mark.parametrize("date_column", ["Data Empenho", "Data da Despesa"])
@pytest.mark.parametrize("file_format", ["csv", "json"])
async def test_sao_luis_keeps_payment_date_unknown_for_commitment_or_expense_dates(
    date_column: str, file_format: str
) -> None:
    row = {
        "Fornecedor": "Empresa Exemplo",
        "Valor Pago": "100,00",
        date_column: "01/02/2026",
        "Empenho": "123",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if file_format == "json":
            return httpx.Response(200, json=[row], request=request)
        return httpx.Response(
            200,
            text=";".join(row) + "\n" + ";".join(row.values()) + "\n",
            headers={"content-type": "text/csv; charset=utf-8"},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = build_municipal_connector("sao_luis", _settings(), http_client=client)
        result = await connector.discover_payments(year=2026)

    assert result.availability is DataAvailability.AVAILABLE
    assert len(result.data) == 1
    payment = result.data[0]
    assert payment.payment_date is None
    assert payment.status == "periodo_nao_confirmado"
    assert payment.amount == Decimal("100.00")
    assert payment.commitment_number == "123"
    assert payment.raw_payload[date_column.lower().replace(" ", "_")] == "01/02/2026"
    assert payment.source_record_ref == result.raw_records[0].record_key
    assert result.partial
    assert "sem data; periodo nao confirmado" in (result.diagnostic or "")
    payload = result.raw_records[0].raw_payload
    assert (payload if file_format == "json" else payload["rows"]) == [row]


@pytest.mark.parametrize("payment_date_column", ["Data Pagamento", "Data"])
@pytest.mark.parametrize("file_format", ["csv", "json"])
async def test_sao_luis_uses_payment_date_when_commitment_and_expense_dates_differ(
    payment_date_column: str, file_format: str
) -> None:
    row = {
        "Fornecedor": "Empresa Exemplo",
        "Valor Pago": "100,00",
        "Data Empenho": "10/12/2025",
        "Data da Despesa": "15/12/2025",
        payment_date_column: "01/02/2026",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if file_format == "json":
            return httpx.Response(200, json=[row], request=request)
        return httpx.Response(
            200,
            text=";".join(row) + "\n" + ";".join(row.values()) + "\n",
            headers={"content-type": "text/csv; charset=utf-8"},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = build_municipal_connector("sao_luis", _settings(), http_client=client)
        result = await connector.discover_payments(year=2026)

    assert result.availability is DataAvailability.AVAILABLE
    assert len(result.data) == 1
    assert result.data[0].payment_date == parse_brazilian_date("01/02/2026")
    assert result.data[0].status == "pagamento_municipal"
    assert not result.partial
    assert result.diagnostic is None


@pytest.mark.asyncio
async def test_sao_luis_page_without_csv_is_not_supported() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text="<html><body>Sem link</body></html>",
            headers={"content-type": "text/html"},
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = _settings(sao_luis_payments_url="https://saoluis.test/dados-abertos")
    connector = build_municipal_connector("sao_luis", settings, http_client=raw_client)
    result = await connector.discover_payments(year=2026)
    assert result.availability is DataAvailability.NOT_SUPPORTED
    assert "SAO_LUIS_PAYMENTS_URL" in (result.diagnostic or "")
    await raw_client.aclose()


def test_catalog_and_money_date_helpers() -> None:
    catalog = {source.id: source for source in list_municipal_sources()}
    assert set(catalog) == {
        "sao_luis",
        "imperatriz",
        "timon",
        "caxias",
        "bacabal",
    }
    assert catalog["imperatriz"].entity_code == "2"
    assert catalog["bacabal"].entity_code == "2"
    assert catalog["timon"].entity_code == "18"
    assert catalog["caxias"].entity_code == "1"
    assert catalog["imperatriz"].portal_url == (
        "https://transparencia.imperatriz.ma.gov.br/portal-transparencia"
    )
    assert catalog["imperatriz"].export_url == (
        "http://scpi3.adtrcloud.com.br:8079/transparencia/"
        "?AcessoIndividual=lnkDespesasPor_NotaEmpenho"
    )
    assert catalog["timon"].export_url == (
        "http://177.10.118.5:8079/Transparencia_Despesa/"
        "?AcessoIndividual=lnkDespesasPor_NotaEmpenho"
    )
    assert catalog["caxias"].portal_url == (
        "https://www.transparencia.caxias.ma.gov.br/transparencia?filtro=despesas"
    )
    assert catalog["bacabal"].export_url == (
        "https://transparenciapublica.cloud/bacabal/"
        "Default.aspx?AcessoIndividual=lnkDespesasPor_NotaEmpenho"
    )
    assert catalog["sao_luis"].open_data_url == "https://www.saoluis.ma.gov.br/dados-abertos"
    assert all(source.import_steps for source in catalog.values())
    settings = Settings(
        app_env="test",
        municipal_payment_sources=["imperatriz", "timon", "imperatriz"],
    )
    assert [source.id for source in selected_municipal_sources(settings)] == [
        "imperatriz",
        "timon",
    ]
    assert parse_brazilian_money("1.234,56") == Decimal("1234.56")
    assert parse_brazilian_money("1.234") == Decimal("1234")
    parsed = parse_brazilian_date("01/02/2026 00:00:00")
    assert parsed is not None and parsed.tzinfo is UTC

    sources = {
        item.id: item
        for item in list_payment_sources(
            Settings(
                app_env="test",
                payment_sources=["transparencia", "manual", "municipal"],
                municipal_payment_sources=["imperatriz", "imperatris"],
            )
        )
    }
    assert sources["municipal"].configured is True
    assert "Imperatriz" in sources["municipal"].note
    assert "ignoradas: imperatris" in sources["municipal"].note


@pytest.mark.asyncio
async def test_sao_luis_downloads_all_monthly_csvs_for_requested_year() -> None:
    page = """
    <html><body>
      <a href="/arquivos/despesas-2025-12.csv">Despesas dezembro 2025</a>
      <a href="/arquivos/despesas-2026-01.csv">Despesas janeiro 2026</a>
      <a href="/arquivos/despesas-2026-02.csv">Despesas fevereiro 2026</a>
    </body></html>
    """
    downloaded: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/dados-abertos"):
            return httpx.Response(200, text=page, request=request)
        downloaded.append(request.url.path)
        month = "01" if "2026-01" in request.url.path else "02"
        body = f"Data;Credor;Valor\n{month}/02/2026;Empresa;10,00\n"
        return httpx.Response(200, text=body, headers={"content-type": "text/csv"}, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = _settings(sao_luis_payments_url="https://saoluis.test/dados-abertos")
    connector = build_municipal_connector("sao_luis", settings, http_client=raw_client)
    result = await connector.discover_payments(year=2026)

    assert result.availability is DataAvailability.AVAILABLE
    assert len(result.data) == 2
    assert len(result.raw_records) == 2
    assert {path.rsplit("-", 1)[-1] for path in downloaded} == {"01.csv", "02.csv"}
    assert all("2025" not in path for path in downloaded)
    await raw_client.aclose()
