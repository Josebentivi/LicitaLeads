"""Unit tests for the federal payment connector's safe defaults."""

from __future__ import annotations

from decimal import Decimal

import httpx
import pytest

from app.config import Settings
from app.connectors.base import DataAvailability
from app.connectors.http import AsyncHTTPClient
from app.connectors.transparencia import TransparenciaPaymentsConnector


@pytest.mark.asyncio
async def test_missing_token_is_access_restricted() -> None:
    connector = TransparenciaPaymentsConnector(Settings(app_env="test", transparencia_api_key=""))

    result = await connector.discover_payments(company_cnpj="00000000000191", year=2026)

    assert result.availability is DataAvailability.ACCESS_RESTRICTED
    assert result.data == []
    assert "TRANSPARENCIA_API_KEY" in (result.diagnostic or "")


def test_brazilian_amount_is_parsed_without_float_rounding() -> None:
    connector = TransparenciaPaymentsConnector(
        Settings(app_env="test", transparencia_api_key="token")
    )

    payment = connector._map_payment(
        {
            "documento": "OB-1",
            "fase": "Pagamento",
            "valor": "1.234,56",
            "codigoFavorecido": "00000000000191",
            "nomeFavorecido": "Empresa",
        },
        "https://portaldatransparencia.test",
    )

    assert payment.amount == Decimal("1234.56")
    assert payment.supplier_cnpj == "00000000000191"


@pytest.mark.asyncio
async def test_discovery_maps_payment_documents_and_keeps_token_header() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json=[
                {
                    "documento": "OB-1",
                    "fase": "Pagamento",
                    "valor": "250,00",
                    "codigoFavorecido": "00000000000191",
                    "nomeFavorecido": "Empresa",
                    "orgao": "Orgao",
                    "data": "2026-02-03",
                }
            ],
            request=request,
        )

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = Settings(app_env="test", transparencia_api_key="secret-token")
    connector = TransparenciaPaymentsConnector(
        settings,
        http_client=AsyncHTTPClient(settings, client=raw_client),
    )

    result = await connector.discover_payments(company_cnpj="00000000000191", year=2026)

    assert result.availability is DataAvailability.AVAILABLE
    assert result.data[0].document_number == "OB-1"
    assert result.data[0].amount == Decimal("250.00")
    assert result.data[0].payment_date is not None
    assert result.data[0].payment_date.year == 2026
    assert result.data[0].source_record_ref == result.raw_records[0].record_key
    assert requests[0].headers["chave-api-dados"] == "secret-token"
    assert requests[0].url.params["codigoPessoa"] == "00000000000191"
    assert requests[0].url.params["fase"] == "3"
    await raw_client.aclose()


def test_payment_source_registry_reports_missing_token_and_unknown_adapter() -> None:
    from app.connectors.payment_sources import list_payment_sources

    settings = Settings(
        app_env="test",
        transparencia_api_key="",
        payment_sources=["transparencia", "manual", "portal_x"],
    )
    sources = {item.id: item for item in list_payment_sources(settings)}

    assert sources["transparencia"].configured is False
    assert sources["manual"].configured is True
    assert sources["portal_x"].configured is False
    assert "nao implementado" in sources["portal_x"].note


def test_phase_names_map_to_api_codes() -> None:
    connector = TransparenciaPaymentsConnector(
        Settings(app_env="test", transparencia_api_key="token")
    )

    assert connector._phase_code(None) == 3
    assert connector._phase_code("empenho") == 1
    assert connector._phase_code("liquidacao") == 2
    assert connector._phase_code("Pagamento") == 3
    assert connector._phase_code("3") == 3
    assert connector._phase_code("invalida") is None


@pytest.mark.asyncio
async def test_invalid_phase_returns_clear_diagnostic_without_http() -> None:
    connector = TransparenciaPaymentsConnector(
        Settings(app_env="test", transparencia_api_key="token")
    )

    result = await connector.discover_payments(
        company_cnpj="00000000000191", year=2026, phase="invalida"
    )

    assert result.availability is DataAvailability.NOT_SUPPORTED
    assert "empenho" in (result.diagnostic or "")


@pytest.mark.asyncio
async def test_http_400_is_not_reported_as_temporary() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, text="fase invalida", request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = Settings(app_env="test", transparencia_api_key="token")
    connector = TransparenciaPaymentsConnector(
        settings, http_client=AsyncHTTPClient(settings, client=raw_client)
    )

    result = await connector.discover_payments(company_cnpj="00000000000191", year=2026)

    assert result.availability is DataAvailability.NOT_SUPPORTED
    assert "rejected" in (result.diagnostic or "")
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_multipage_payments_reference_the_page_that_contains_them() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["pagina"])
        rows = {
            1: [
                {
                    "documento": "OB-1",
                    "fase": "Pagamento",
                    "valor": "10,00",
                    "data": "2026-01-01",
                    "codigoFavorecido": "00000000000191",
                }
            ],
            2: [
                {
                    "documento": "OB-2",
                    "fase": "Pagamento",
                    "valor": "20,00",
                    "data": "2026-01-02",
                    "codigoFavorecido": "00000000000191",
                }
            ],
        }.get(page, [])
        return httpx.Response(200, json=rows, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = Settings(app_env="test", transparencia_api_key="token")
    connector = TransparenciaPaymentsConnector(
        settings, http_client=AsyncHTTPClient(settings, client=raw_client, max_retries=0)
    )

    result = await connector.discover_payments(company_cnpj="00000000000191", year=2026)

    assert len(result.data) == 2
    assert result.data[0].source_record_ref == result.raw_records[0].record_key
    assert result.data[1].source_record_ref == result.raw_records[1].record_key
    await raw_client.aclose()


@pytest.mark.asyncio
async def test_wrong_json_envelope_is_not_empty() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"error": "maintenance"}, request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = Settings(app_env="test", transparencia_api_key="token")
    connector = TransparenciaPaymentsConnector(
        settings, http_client=AsyncHTTPClient(settings, client=raw_client, max_retries=0)
    )

    result = await connector.discover_payments(company_cnpj="00000000000191", year=2026)

    assert result.availability is DataAvailability.TEMPORARY_ERROR
    assert result.data == []


@pytest.mark.asyncio
async def test_payment_identity_keeps_date_and_ignores_amount_changes() -> None:
    rows = [
        {
            "documento": "OB-1",
            "fase": "Pagamento",
            "valor": "10,00",
            "data": "2026-01-01",
            "codigoFavorecido": "00000000000191",
            "codigoOrgao": "11111111111111",
        },
        {
            "documento": "OB-1",
            "fase": "Pagamento",
            "valor": "10,00",
            "data": "2026-01-02",
            "codigoFavorecido": "00000000000191",
            "codigoOrgao": "11111111111111",
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["pagina"])
        return httpx.Response(200, json=rows if page == 1 else [], request=request)

    raw_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = Settings(app_env="test", transparencia_api_key="token")
    connector = TransparenciaPaymentsConnector(
        settings, http_client=AsyncHTTPClient(settings, client=raw_client, max_retries=0)
    )

    result = await connector.discover_payments(company_cnpj="00000000000191", year=2026)

    assert len(result.data) == 2
    assert result.data[0].external_id != result.data[1].external_id
    assert "10.00" not in result.data[0].external_id
    corrected = connector._map_payment({**rows[0], "valor": "99,00"}, "https://source.test")
    assert corrected.external_id == result.data[0].external_id
