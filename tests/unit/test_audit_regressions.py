"""Regression scenarios found by the integrity audit, without live requests."""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest

from app.connectors._common import list_payload
from app.connectors.base import ConnectorResult, DataAvailability, PaginationInfo, RawSourceRecord
from app.connectors.capabilities import EndpointDescription
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.http import AsyncHTTPClient, ConnectorContractError
from app.connectors.pncp import PNCPConnector
from app.services.ingestion.audit import _audit_operation
from app.services.ingestion.pipeline import IngestionPipeline


@pytest.mark.parametrize("payload", [["erro"], {"resultado": [None]}, {"error": "maintenance"}])
def test_invalid_collections_cannot_be_authoritative_empty(payload):
    with pytest.raises(ConnectorContractError):
        list_payload(payload, "resultado")


@pytest.mark.parametrize("kind", ["items", "results"])
async def test_detail_contract_error_does_not_claim_absence(kind):
    settings = SimpleNamespace(
        compras_gov_base_url="https://official.test",
        http_max_retries=0,
        http_min_request_interval_seconds=0,
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"error": "maintenance"})
        )
    ) as client:
        connector = ComprasGovConnector(
            settings, http_client=AsyncHTTPClient(settings, client=client)
        )
        result = await getattr(connector, f"fetch_{kind}")("98092105900012026")
    assert result.availability is DataAvailability.TEMPORARY_ERROR
    assert not result.is_complete
    assert len(result.raw_records) == 1


@pytest.mark.parametrize("late_failure", [False, True])
async def test_remaining_pages_are_followed_and_partial_rows_survive(late_failure):
    calls = []

    def response(request):
        page = int(request.url.params["pagina"])
        calls.append(page)
        payload = {
            "resultado": [
                {"idCompraItem": f"item-{page}", "numeroItem": page, "numeroItemPncp": page}
            ],
            "paginasRestantes": 2 - page,
            "totalRegistros": 2,
        }
        if late_failure and page == 2:
            payload = {"error": "maintenance"}
        return httpx.Response(200, json=payload)

    settings = SimpleNamespace(
        compras_gov_base_url="https://official.test",
        http_max_retries=0,
        http_min_request_interval_seconds=0,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        connector = ComprasGovConnector(
            settings, http_client=AsyncHTTPClient(settings, client=client)
        )
        result = await connector.fetch_items("98092105900012026")
    assert calls == [1, 2]
    assert result.availability is DataAvailability.AVAILABLE
    assert len(result.data) == (1 if late_failure else 2)
    assert result.partial is late_failure
    assert result.is_complete is not late_failure


def test_three_rows_share_the_correct_two_source_responses():
    records = [
        RawSourceRecord.build(
            source="pncp",
            endpoint="https://official.test/items",
            request_parameters={"pagina": page},
            raw_payload={"page": page},
            http_status=200,
            collected_at=datetime.now(UTC),
        )
        for page in (1, 2)
    ]
    rows = [SimpleNamespace(source_record_ref=records[page].record_key) for page in (0, 0, 1)]
    ids = [UUID(int=1), UUID(int=2)]
    mapping = IngestionPipeline._source_id_map(records, ids)
    assert [mapping[row.source_record_ref] for row in rows] == [ids[0], ids[0], ids[1]]
    with pytest.raises(ValueError, match="sem resposta"):
        IngestionPipeline._reference_id(mapping, "unknown", ids[0])
    with pytest.raises(ValueError, match="ambígua"):
        IngestionPipeline._reference_id(mapping, None, None)


@pytest.mark.parametrize(
    "pagination",
    [
        PaginationInfo(truncated=True),
        PaginationInfo(pages_remaining=1),
        PaginationInfo(total_pages=2, fetched_pages=1),
    ],
)
def test_completeness_requires_all_pages(pagination):
    assert not ConnectorResult(
        data=[], availability=DataAvailability.EMPTY, pagination=pagination
    ).is_complete


def test_openapi_requires_method_parameters_and_consumed_schema():
    endpoint = EndpointDescription(path="/items", parameters=["pagina"])
    node = {
        "get": {
            "parameters": [{"name": "pagina", "schema": {"type": "integer"}}],
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
    }
    assert _audit_operation({}, node, endpoint) == ([], True)
    node["get"]["parameters"] = []
    issues, verified = _audit_operation({}, node, endpoint)
    assert not verified and any("parâmetro" in issue for issue in issues)
    assert not _audit_operation({}, {}, endpoint)[1]


@pytest.mark.parametrize("connector_type", [PNCPConnector, ComprasGovConnector])
async def test_single_detail_preserves_invalid_raw_without_inventing_procurement(connector_type):
    settings = SimpleNamespace(
        compras_gov_base_url="https://official.test",
        pncp_base_url="https://official.test",
        pncp_integration_base_url="https://official.test",
        http_max_retries=0,
        http_min_request_interval_seconds=0,
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"error": "maintenance"})
        )
    ) as client:
        connector = connector_type(settings, http_client=AsyncHTTPClient(settings, client=client))
        result = await connector.fetch_procurement("00000000000191-1-000001/2026")
    assert result.data is None
    assert result.availability is DataAvailability.TEMPORARY_ERROR
    assert result.raw_records[0].raw_payload == {"error": "maintenance"}


async def test_discovery_uses_remaining_page_metadata():
    from app.connectors.base import ProcurementFilters

    calls = []

    def response(request):
        page = int(request.url.params["pagina"])
        calls.append(page)
        row = {
            "idCompra": f"980921059000{page}2026",
            "numeroControlePNCP": f"00000000000191-1-00000{page}/2026",
            "orgaoEntidadeCnpj": "00000000000191",
            "unidadeOrgaoUfSigla": "MA",
            "codigoModalidade": 5,
        }
        return httpx.Response(
            200, json={"resultado": [row], "paginasRestantes": 2 - page, "totalRegistros": 2}
        )

    settings = SimpleNamespace(
        compras_gov_base_url="https://official.test",
        http_max_retries=0,
        http_min_request_interval_seconds=0,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        connector = ComprasGovConnector(
            settings, http_client=AsyncHTTPClient(settings, client=client)
        )
        result = await connector.discover_procurements(
            ProcurementFilters(uf="MA", modalities=["pregao_eletronico"])
        )
    assert calls == [1, 2]
    assert len(result.data) == 2
    assert result.is_complete
