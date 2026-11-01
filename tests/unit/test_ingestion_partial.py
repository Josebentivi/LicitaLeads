"""Partial discovery results must be visible in the crawl summary."""

from __future__ import annotations

from uuid import uuid4

from app.connectors.base import ConnectorResult, DataAvailability, PaginationInfo
from app.services.ingestion.pipeline import IngestionPipeline, PipelineSummary


def test_truncated_discovery_marks_the_summary_as_partial() -> None:
    summary = PipelineSummary(run_id=uuid4())
    result = ConnectorResult(
        data=[],
        availability=DataAvailability.AVAILABLE,
        pagination=PaginationInfo(
            page=1,
            total_pages=3,
            fetched_pages=1,
            pages_remaining=2,
            truncated=True,
        ),
        diagnostic="partial result: one or more pages were not collected",
    )

    IngestionPipeline._note_partial_discovery(summary, "pncp", result)

    assert any("paginacao incompleta" in message for message in summary.diagnostics)
    assert any("partial result" in message for message in summary.diagnostics)
