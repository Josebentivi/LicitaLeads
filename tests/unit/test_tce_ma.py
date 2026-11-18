"""Exercise SINC scope, pagination and conservative procurement identity."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from app.connectors.base import ConnectorResult, DataAvailability, RawProcurement
from app.connectors.http import AsyncHTTPClient, HTTPJSONResponse
from app.connectors.tce_ma import SincContrataResource, SincContrataRow, TCEMAConnector
from app.services.tce_ma_pilot import match_sinc_procedure

CNPJ = "06307102000130"
CONTROL = f"{CNPJ}-1-000115/2026"


def _page(number, ids, *, total=2, ente=2111300):
    return HTTPJSONResponse(
        payload={
            "content": [{"id": row_id, "enteIdEnvio": ente} for row_id in ids],
            "number": number,
            "size": 1,
            "totalPages": total,
            "totalElements": total,
            "last": number + 1 >= total,
        },
        url="https://tce-api.apps.tcema.tc.br/sinccontrata/procedimentolicitatorio",
        status_code=200,
        request_parameters={"enteId": 2111300, "page": number, "size": 1},
        headers={},
        collected_at=datetime.now(UTC),
    )


@pytest.mark.parametrize(
    ("pages", "availability", "complete", "ids"),
    [
        ([_page(0, ["A"]), _page(1, ["B"])], DataAvailability.AVAILABLE, True, ["A", "B"]),
        ([_page(0, ["A"]), _page(1, ["A"])], DataAvailability.AVAILABLE, False, ["A"]),
        ([_page(0, ["A"], ente=2111201)], DataAvailability.TEMPORARY_ERROR, False, []),
        ([_page(0, [], total=0)], DataAvailability.EMPTY, True, []),
    ],
)
async def test_sinc_only_confirms_complete_coverage_with_valid_pages(
    monkeypatch, pages, availability, complete, ids
):
    http = AsyncHTTPClient()
    request = AsyncMock(side_effect=pages)
    monkeypatch.setattr(http, "request_json", request)
    try:
        result = await TCEMAConnector(http).fetch(
            SincContrataResource.PROCEDURES, ente_id=2111300, page_size=1
        )
    finally:
        await http.aclose()
    assert result.availability is availability
    assert result.is_complete is complete
    assert [row.payload["id"] for row in result.data] == ids
    assert {row.source_record_ref for row in result.data} <= {
        raw.record_key for raw in result.raw_records
    }
    assert [call.kwargs["params"] for call in request.await_args_list] == [
        {"enteId": 2111300, "page": page, "size": 1} for page in range(len(pages))
    ]


def _procedure(**changes):
    return SincContrataRow(
        {
            "cnpjProcedimento": CNPJ,
            "idProcedimento": "PE900242026CPL",
            "idContratacaoPncp": CONTROL,
            "numeroProcedimento": 90024,
            "anoProcedimento": 2026,
            "tipoProcedimento": "PE",
            **changes,
        },
        "sinc-page",
    )


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ([_procedure()], "exato"),
        ([_procedure(idContratacaoPncp=None)], "candidato_sem_vinculo"),
        ([_procedure(cnpjProcedimento="05760293000986")], "ambiguo"),
        ([_procedure(), _procedure(idProcedimento="OUTRO")], "ambiguo"),
        ([_procedure(), _procedure(idContratacaoPncp=f"{CNPJ}-1-000116/2026")], "ambiguo"),
        ([_procedure(idContratacaoPncp=None, numeroProcedimento=90025)], "nao_localizado"),
    ],
)
def test_tce_link_requires_unambiguous_official_control(rows, expected):
    pncp = RawProcurement(
        source="pncp",
        external_id=CONTROL,
        pncp_control_number=CONTROL,
        agency_cnpj=CNPJ,
        uasg="980921",
        purchase_number="90024",
        purchase_year=2026,
    )
    result = ConnectorResult(data=rows, availability=DataAvailability.AVAILABLE)
    match = match_sinc_procedure(pncp, result)
    assert match.status == expected
    assert (match.key == (CNPJ, "PE900242026CPL")) is (expected == "exato")
