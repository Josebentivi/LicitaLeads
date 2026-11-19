from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.connectors._common import combine_discoveries
from app.connectors.base import ConnectorResult, DataAvailability, PaginationInfo, RawProcurement
from app.services.collection_periods import calendar_month_slices, validate_collection_period
from app.services.ingestion.continuation import pipeline_request_from_filters
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest
from app.services.procurement_search import OBJECT_PRESETS, ObjectScope


def test_two_year_window_and_inclusive_month_partition():
    today = date(2028, 2, 29)
    first = date(2026, 2, 28)
    kwargs = {"timezone": "America/Sao_Paulo", "today": today}
    assert validate_collection_period(first, today, **kwargs) == (first, today)
    with pytest.raises(ValueError, match="últimos dois anos"):
        validate_collection_period(date(2026, 2, 27), today, **kwargs)
    with pytest.raises(ValueError, match="até hoje"):
        validate_collection_period(first, date(2028, 3, 1), **kwargs)
    with pytest.raises(ValueError, match="duas datas"):
        validate_collection_period(first, None, **kwargs)
    assert list(calendar_month_slices(date(2027, 12, 30), date(2028, 2, 1))) == [
        (date(2028, 2, 1), date(2028, 2, 1)),
        (date(2028, 1, 1), date(2028, 1, 31)),
        (date(2027, 12, 30), date(2027, 12, 31)),
    ]


def test_collection_scope_combines_terms_and_preserves_the_frozen_filter():
    assert len(OBJECT_PRESETS) == 30
    scope = ObjectScope.build("água sanitária", ["material-limpeza", "expediente"], "bens", "M")
    assert scope is not None
    assert scope.matches("Municipal", "Compra de ÁGUA SANITÁRIA, frasco de 1 L")
    assert not scope.matches("F", "Compra de água sanitária")
    assert not scope.matches(None, "Compra de água sanitária")
    assert not scope.matches("M", "Aquisição de detergente")
    request = PipelineRequest(object_scope=scope, calendar_months=True)
    frozen = request.as_json()
    frozen["object_scope"]["preset_terms"] = ["desengordurante"]
    restored = pipeline_request_from_filters(frozen)
    assert restored.calendar_months
    assert restored.object_scope.preset_terms == ("desengordurante",)
    assert not restored.object_scope.matches("M", "Compra de água sanitária")
    assert "object_scope" not in PipelineRequest().as_json()
    assert "calendar_months" not in PipelineRequest().as_json()
    works = ObjectScope.build(object_group="obras")
    assert works is not None
    assert works.matches("M", "Execução de obras de pavimentação")
    assert not works.matches("M", "Fornecimento de mão de obra")


def test_known_unsupported_modality_preserves_other_partitions_and_replay():
    skipped = ConnectorResult(
        data=[],
        availability=DataAvailability.NOT_SUPPORTED,
        unsupported_modalities=["leilao_eletronico"],
    )
    assert combine_discoveries([skipped]).availability is DataAvailability.NOT_SUPPORTED
    response = ConnectorResult(
        data=["item"],
        availability=DataAvailability.AVAILABLE,
        pagination=PaginationInfo(total_pages=2, fetched_pages=1, truncated=True),
    )
    combined = combine_discoveries([response, skipped])
    assert combined.data == ["item"]
    assert combined.unsupported_modalities == ["leilao_eletronico"]
    assert combined.pagination is not None and combined.pagination.truncated
    assert not combined.is_complete


async def test_historical_discovery_keeps_partial_pages_and_official_identity(monkeypatch):
    pipeline = IngestionPipeline()
    for method in ("_checkpoint", "_report_activity"):
        monkeypatch.setattr(pipeline, method, AsyncMock())
    monkeypatch.setattr(pipeline, "_cancellation_requested", AsyncMock(return_value=False))
    source = RawProcurement(
        source="pncp", source_url="https://pncp.test", external_id="oficial", raw_payload={}
    )
    discover = AsyncMock(
        side_effect=[
            ConnectorResult(data=[source], availability=DataAvailability.AVAILABLE),
            ConnectorResult(data=[source], availability=DataAvailability.AVAILABLE),
            ConnectorResult(
                data=[], availability=DataAvailability.TEMPORARY_ERROR, diagnostic="timeout"
            ),
        ]
    )
    connector = SimpleNamespace(name="pncp", discover_procurements=discover)
    request = PipelineRequest(
        days=None,
        start_date=date(2026, 1, 29),
        end_date=date(2026, 3, 1),
        calendar_months=True,
    )
    result = await pipeline._discover_procurements(connector, request, request.filters(), uuid4())
    windows = [call.args[0] for call in discover.await_args_list]
    assert [(item.start_date, item.end_date) for item in windows] == list(
        calendar_month_slices(request.start_date, request.end_date)
    )
    assert result.data == [source]
    assert result.partial and result.diagnostic == "timeout"
