"""A responding homepage is not proof that the payment API works."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from app.config import Settings
from app.connectors.base import DataAvailability
from app.services.source_probes import probe_connectivity


@pytest.mark.parametrize("municipal_status", [200, 403, 503])
async def test_probes_use_payment_endpoints_and_recognize_content(municipal_status):
    year = datetime.now(UTC).year
    settings = Settings(
        _env_file=None,
        app_env="test",
        pncp_base_url="https://pncp.test",
        compras_gov_base_url="https://compras.test",
        http_min_request_interval_seconds=0,
        municipal_payment_sources=["imperatriz", "sao_luis"],
        allow_insecure_http_sources=True,
        sao_luis_payments_url="https://saoluis.test/data",
        transparencia_api_key="test-token",
        payment_poll_cnpjs=["00000000000191"],
    )
    urls = []

    def response(request):
        urls.append(request.url)
        host = request.url.host
        if host == "pncp.test":
            return httpx.Response(200, json={"data": []})
        if host == "compras.test":
            return httpx.Response(200, json={"resultado": []})
        if host == "saoluis.test":
            if request.url.path == "/data":
                return httpx.Response(200, text=f'<a href="/{year}-01.csv">Pagamentos CSV</a>')
            return httpx.Response(
                200, text=f"Fornecedor;CNPJ;Valor;Data\nEmpresa;00000000000191;1,00;01/01/{year}\n"
            )
        if "portaldatransparencia" in host:
            assert request.headers["chave-api-dados"] == "test-token"
            return httpx.Response(
                200, json=[{"documento": "OB-1", "codigoFavorecido": "00000000000191"}]
            )
        assert request.url.path.endswith("/VersaoJson/Despesas/")
        assert request.url.params["Listagem"] == "DespesasGerais"
        return httpx.Response(municipal_status, json=[])

    with respx.mock(assert_all_called=False) as router:
        router.get(url__regex=r".*").mock(side_effect=response)
        outcomes = {
            name: (status, detail) for name, status, detail in await probe_connectivity(settings)
        }
    expected = {
        200: DataAvailability.EMPTY,
        403: DataAvailability.ACCESS_RESTRICTED,
        503: DataAvailability.TEMPORARY_ERROR,
    }[municipal_status]
    assert outcomes["Imperatriz"][0] is expected
    assert outcomes["São Luís"][0] is DataAvailability.AVAILABLE
    assert outcomes["Transparência"][0] is DataAvailability.AVAILABLE
    assert any(url.path.endswith(".csv") for url in urls)


async def test_html_and_json_errors_with_200_are_not_healthy():
    settings = Settings(
        _env_file=None,
        app_env="test",
        pncp_base_url="https://pncp.test",
        compras_gov_base_url="https://compras.test",
        http_min_request_interval_seconds=0,
        municipal_payment_sources=["imperatriz", "sao_luis"],
        allow_insecure_http_sources=True,
        sao_luis_payments_url="https://saoluis.test/data",
    )
    with respx.mock(assert_all_called=False) as router:
        router.get(url__regex=r".*").mock(
            return_value=httpx.Response(200, text="<html>maintenance</html>")
        )
        outcomes = await probe_connectivity(settings)
    assert all(status is DataAvailability.TEMPORARY_ERROR for _, status, _ in outcomes)


async def test_probe_respects_http_opt_in():
    settings = Settings(
        _env_file=None,
        app_env="test",
        http_min_request_interval_seconds=0,
        municipal_payment_sources=["imperatriz"],
        allow_insecure_http_sources=False,
    )
    with respx.mock(assert_all_called=False) as router:
        router.get(url__regex=r"https://pncp.*").mock(
            return_value=httpx.Response(200, json={"data": []})
        )
        router.get(url__regex=r"https://dadosabertos.*").mock(
            return_value=httpx.Response(200, json={"resultado": []})
        )
        outcomes = await probe_connectivity(settings)
    assert outcomes[-1][1] is DataAvailability.NOT_SUPPORTED
