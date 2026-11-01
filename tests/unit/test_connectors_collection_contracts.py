"""Collections may replace facts only after complete, auditable responses."""

from dataclasses import replace
from datetime import date

import httpx
import pytest
import respx

from app.config import Settings
from app.connectors.base import DataAvailability, ProcurementFilters, RawPriceRegistry
from app.connectors.capabilities import EndpointDescription
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.http import AsyncHTTPClient
from app.connectors.municipal_payments.fiorilli import FiorilliPaymentConnector
from app.connectors.municipal_payments.sao_luis import SaoLuisCsvConnector
from app.connectors.municipal_payments.sources import get_municipal_source
from app.connectors.pncp import PNCPConnector
from app.services.ingestion.audit import _audit_operation, audit_sources


def settings(**extra):
    return Settings(
        _env_file=None,
        app_env="test",
        http_max_retries=0,
        http_min_request_interval_seconds=0,
        **extra,
    )


def official_row(kind, operation, page):
    if operation == "contracts":
        if kind is PNCPConnector:
            return {
                "numeroControlePNCP": f"contract-{page}",
                "anoContrato": 2026,
                "sequencialContrato": page,
                "orgaoEntidade": {"cnpj": "12345678000199"},
                "numeroContratoEmpenho": str(page),
                "niFornecedor": "00000000000191",
            }
        return {
            "numeroControlePncpContrato": f"contract-{page}",
            "codigoOrgao": "12345678000199",
            "numeroContrato": str(page),
            "dataVigenciaInicial": "2026-01-01",
            "dataVigenciaFinal": "2026-12-31",
            "valorGlobal": 100,
        }
    if kind is PNCPConnector:
        return {
            "numeroControlePNCPAta": f"registry-{page}",
            "cnpjOrgao": "12345678000199",
            "anoAta": 2026,
            "numeroAtaRegistroPreco": str(page),
        }
    return {
        "numeroControlePncpAta": f"registry-{page}",
        "anoCompra": 2026,
        "numeroAtaRegistroPreco": str(page),
        "codigoUnidadeGerenciadora": "980921",
    }


@pytest.mark.parametrize("kind", [PNCPConnector, ComprasGovConnector])
@pytest.mark.parametrize("operation", ["contracts", "price_registries"])
@pytest.mark.parametrize(
    "scenario",
    [
        "complete",
        "late_503",
        "repeated",
        "limit",
        "empty",
        "wrong_envelope",
        "invalid_identity",
        "bad_metadata",
    ],
)
async def test_collection_contracts_preserve_partial_pages_and_their_provenance(
    kind, operation, scenario
):
    calls = []
    key = "data" if kind is PNCPConnector else "resultado"

    def handler(request):
        page = int(request.url.params["pagina"])
        calls.append(request)
        if scenario == "late_503" and page == 2:
            return httpx.Response(503)
        if scenario == "wrong_envelope":
            return httpx.Response(200, json={"maintenance": True})
        row = official_row(
            kind,
            "contracts" if operation == "contracts" else "registries",
            1 if scenario == "repeated" else page,
        )
        rows = [] if scenario == "empty" else [{}] if scenario == "invalid_identity" else [row]
        total = 0 if scenario == "empty" else 1 if scenario == "invalid_identity" else 2
        payload = {key: rows, "totalRegistros": total, "totalPaginas": total}
        if scenario == "bad_metadata":
            payload["totalPaginas"] = "maintenance"
        return httpx.Response(200, json=payload)

    selected = settings()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = kind(selected, http_client=AsyncHTTPClient(selected, client=client))
        result = await getattr(connector, f"discover_{operation}")(
            ProcurementFilters(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 2),
                agency_cnpj="12345678000199",
                modalities=["pregao_eletronico"],
                max_pages=1 if scenario == "limit" else 3,
                page_size=1,
            )
        )
    assert all(int(request.url.params["tamanhoPagina"]) >= 10 for request in calls)
    assert result.raw_records
    if scenario == "empty":
        assert result.availability is DataAvailability.EMPTY and result.is_complete
    elif scenario in {"wrong_envelope", "invalid_identity"}:
        assert result.availability is DataAvailability.TEMPORARY_ERROR
        assert not result.is_complete and not result.data
    else:
        assert result.availability is DataAvailability.AVAILABLE
        assert result.is_complete is (scenario == "complete")
        assert len(result.data) == (2 if scenario == "complete" else 1)
        for row in result.data:
            record = next(
                raw for raw in result.raw_records if raw.record_key == row.source_record_ref
            )
            assert row.raw_payload in record.raw_payload[key]
        if scenario == "complete":
            assert result.data[0].source_record_ref != result.data[1].source_record_ref


@pytest.mark.parametrize("failure", ["html", "403", "503", "transport", "oversize"])
async def test_sao_luis_preserves_good_month_when_another_export_fails(failure):
    def handler(request):
        if request.url.path == "/index":
            return httpx.Response(
                200, text='<a href="/2026-01.csv">CSV</a><a href="/2026-02.csv">CSV</a>'
            )
        if "02.csv" in request.url.path:
            if failure == "transport":
                raise httpx.ConnectError("offline test", request=request)
            if failure == "oversize":
                return httpx.Response(200, headers={"Content-Length": str(21 * 1024 * 1024)})
            if failure in {"403", "503"}:
                return httpx.Response(int(failure))
            return httpx.Response(200, text="<html>maintenance</html>")
        return httpx.Response(200, text="Credor;Valor;Data\nEmpresa;10,00;01/01/2026\n")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = SaoLuisCsvConnector(
            get_municipal_source("sao_luis"),
            settings(sao_luis_payments_url="https://source.test/index"),
            client=client,
        )
        result = await adapter.discover_payments(year=2026)
    assert result.availability is DataAvailability.AVAILABLE
    assert result.partial and not result.is_complete
    assert len(result.data) == 1 and result.data[0].amount == 10
    assert result.data[0].source_record_ref == result.raw_records[0].record_key


@pytest.mark.parametrize(
    "content,content_type,expected",
    [
        (b"", "text/csv", DataAvailability.TEMPORARY_ERROR),
        (b"{broken", "application/json", DataAvailability.TEMPORARY_ERROR),
        (b'{"error":"maintenance"}', "application/json", DataAvailability.TEMPORARY_ERROR),
        (b'{"data":[],"totalRegistros":50}', "application/json", DataAvailability.TEMPORARY_ERROR),
        (b'[{"Credor":"Empresa","Valor":"10,00"}]', "application/json", DataAvailability.AVAILABLE),
    ],
)
async def test_sao_luis_invalid_or_undated_exports_never_confirm_absence(
    content, content_type, expected
):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, content=content, headers={"Content-Type": content_type}
            )
        )
    ) as client:
        result = await SaoLuisCsvConnector(
            get_municipal_source("sao_luis"), settings(), client=client
        ).discover_payments(year=2026)
    assert result.availability is expected
    assert not result.is_complete
    assert result.raw_records


@pytest.mark.parametrize(
    "scenario",
    [
        "complete",
        "bad_amount",
        "duplicate",
        "wrong_key",
        "bad_date",
        "missing_identity",
        "wrong_envelope",
        "late_failure",
        "cut_group",
    ],
)
async def test_fiorilli_supersession_metadata_requires_a_complete_commitment_group(scenario):
    parent = {"PKEMP": "7", "CODIGO": "100", "PAGO": "100,00", "NOMEFOR": "Empresa"}
    detail = {
        "PKEMP": "7",
        "EMPENHO": "100",
        "LIQ": "1",
        "PAR": "1",
        "DATAE": "01/02/2026",
        "VALOR": "40,00",
    }
    second = {**detail, "LIQ": "2", "VALOR": "60,00"}
    if scenario == "bad_amount":
        second["VALOR"] = "invalid"
    if scenario == "duplicate":
        second = detail.copy()
    if scenario == "wrong_key":
        second["PKEMP"] = "99"
    if scenario == "bad_date":
        second["DATAE"] = "invalid"
    if scenario == "missing_identity":
        second.pop("LIQ")

    def handler(request):
        if request.url.params["Listagem"] == "DespesasGerais":
            return httpx.Response(200, json=[parent])
        if scenario == "late_failure":
            return httpx.Response(503)
        return httpx.Response(
            200, json={"error": "maintenance"} if scenario == "wrong_envelope" else [detail, second]
        )

    selected = settings(municipal_payment_max_records=1 if scenario == "cut_group" else 100)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        source = replace(get_municipal_source("imperatriz"), base_url="https://source.test")
        result = await FiorilliPaymentConnector(
            source, selected, http_client=AsyncHTTPClient(selected, client=client)
        ).discover_payments(year=2026, include_details=True)
    assert all(row.commitment_external_id == "imperatriz:e2:2026:7" for row in result.data)
    if scenario == "complete":
        assert result.is_complete and len(result.data) == 2
        assert all(row.commitment_details_complete for row in result.data)
        assert all(row.source_record_ref == result.raw_records[1].record_key for row in result.data)
    else:
        assert result.partial and len(result.data) == 1
        assert result.data[0].status == "agregado_por_empenho"
        assert not result.data[0].commitment_details_complete
        assert result.data[0].amount == 100


@pytest.mark.parametrize(
    "scenario",
    [
        "missing_method",
        "missing_parameter",
        "new_required_parameter",
        "wrong_parameter_type",
        "missing_schema",
        "scalar_rows",
        "wrong_envelope",
    ],
)
def test_openapi_operation_changes_are_reported_as_unverified(scenario):
    endpoint = EndpointDescription(path="/modulo-contratacoes/items", parameters=["pagina"])
    operation = {
        "parameters": [{"name": "pagina", "in": "query", "schema": {"type": "integer"}}],
        "responses": {
            "200": {
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "resultado": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {"numeroItem": {"type": "integer"}},
                                    },
                                }
                            },
                        }
                    }
                }
            }
        },
    }
    if scenario == "missing_parameter":
        operation["parameters"] = []
    if scenario == "new_required_parameter":
        operation["parameters"].append({"name": "newKey", "in": "query", "required": True})
    if scenario == "wrong_parameter_type":
        operation["parameters"][0]["schema"]["type"] = "string"
    if scenario == "missing_schema":
        operation["responses"] = {}
    if scenario == "scalar_rows":
        operation["responses"]["200"]["content"]["application/json"]["schema"] = {
            "type": "array",
            "items": {"type": "string"},
        }
    if scenario == "wrong_envelope":
        operation["responses"]["200"]["content"]["application/json"]["schema"]["properties"] = {
            "data": {"type": "array"}
        }
    issues, verified = _audit_operation(
        {}, {} if scenario == "missing_method" else {"get": operation}, endpoint
    )
    assert not verified and issues


async def test_unavailable_openapi_is_an_explicit_report_failure(tmp_path):
    with respx.mock(assert_all_called=False) as router:
        router.get(url__regex=r"https://.*").mock(return_value=httpx.Response(503))
        result = await audit_sources(settings=settings(), output_directory=tmp_path)
    assert result.checked_sources == 4
    assert len(result.warnings) == 4 and result.validated_operations == 0
    assert result.report_path.read_text(encoding="utf-8").count("não foi possível auditar") == 4


@pytest.mark.parametrize(
    "scenario", ["complete", "late_failure", "missing_numbers", "bad_item_envelope", "limit"]
)
async def test_arp_units_are_queried_for_official_items_with_separate_provenance(
    scenario, monkeypatch
):
    requests = []
    if scenario == "limit":
        monkeypatch.setattr("app.connectors.compras_gov._MAX_DETAIL_PAGES", 1)

    def handler(request):
        requests.append(request)
        if "consultarARPItem_Id" in request.url.path:
            if scenario == "bad_item_envelope":
                return httpx.Response(200, json={"error": "maintenance"})
            items = [{"numeroItem": "1"}, {"numeroItem": "2"}]
            if scenario == "missing_numbers":
                items = [{"codigoItem": "12345"}]
            return httpx.Response(200, json={"resultado": items, "totalPaginas": 1})
        item = request.url.params["numeroItem"]
        if item == "2" and scenario == "late_failure":
            return httpx.Response(503)
        return httpx.Response(
            200,
            json={
                "resultado": [
                    {
                        "codigoUnidade": "980921",
                        "numeroItem": item,
                        "quantidadeRegistrada": 10,
                        "saldoAdesoes": 3,
                    }
                ]
            },
        )

    selected = settings()
    registry = RawPriceRegistry(
        source="compras_gov", external_id="registry", registry_number="1", uasg="980921"
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = ComprasGovConnector(
            selected, http_client=AsyncHTTPClient(selected, client=client)
        )
        result = await connector.fetch_price_registry_units(registry)
    if scenario in {"missing_numbers", "bad_item_envelope"}:
        assert result.availability is DataAvailability.TEMPORARY_ERROR and not result.is_complete
        assert len(requests) == 1
    else:
        assert len(result.data) == (2 if scenario == "complete" else 1)
        assert result.is_complete is (scenario == "complete")
        refs = {record.record_key: record for record in result.raw_records}
        for row in result.data:
            assert refs[row.source_record_ref].request_parameters["numeroItem"] == row.item_number
        assert len({row.source_record_ref for row in result.data}) == len(result.data)
