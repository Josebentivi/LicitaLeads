from __future__ import annotations

from datetime import date

from app.models import CrawlRun, CrawlRunStatus
from app.web import (
    _crawl_view,
    _duration_label,
    _filter_date,
    _friendly_source_error,
)


def test_crawl_display_helpers_cover_human_friendly_variants() -> None:
    assert _duration_label(None) == "—"
    assert _duration_label(0.2) == "< 1 s"
    assert _duration_label(8.2) == "8 s"
    assert _duration_label(65) == "1 min 05 s"

    assert _filter_date("2026-09-17") == date(2026, 9, 17)
    assert _filter_date(date(2026, 9, 18)) == date(2026, 9, 18)
    assert _filter_date(None) is None

    assert "formato inesperado" in (
        _friendly_source_error("pncp", ["official source returned a non-JSON response"]) or ""
    )
    assert "temporariamente indisponível" in (
        _friendly_source_error("pncp", ["official source returned HTTP 500"]) or ""
    )
    assert "Não foi possível" in (_friendly_source_error("pncp", ["unexpected failure"]) or "")
    assert _friendly_source_error("pncp", []) is None


def test_crawl_view_explains_general_lease_failure() -> None:
    run = CrawlRun(
        connector="all",
        status=CrawlRunStatus.FAILED,
        records_found=0,
        errors=[{"message": "another equivalent crawl currently holds the lease"}],
        filters={"uf": "MA"},
    )

    view = _crawl_view(run)

    assert view.status_label == "Falhou"
    assert view.started_at == "—"
    assert view.duration == "—"
    assert view.general_error == "Outra coleta equivalente já estava em andamento."
    assert all(source.retryable is False for source in view.sources)


def test_crawl_view_marks_cancelled_runs_terminal() -> None:
    run = CrawlRun(
        connector="pncp",
        status=CrawlRunStatus.CANCELLED,
        cancel_requested=True,
        filters={},
    )

    view = _crawl_view(run)

    assert view.status_label == "Cancelada"
    assert view.active is False
    assert all(source.retryable is False for source in view.sources)


def test_crawl_view_shows_cancelling_state_while_active() -> None:
    run = CrawlRun(
        connector="pncp",
        status=CrawlRunStatus.RUNNING,
        cancel_requested=True,
        filters={},
    )

    view = _crawl_view(run)

    assert view.status_label == "Cancelando"
    assert view.active is True
