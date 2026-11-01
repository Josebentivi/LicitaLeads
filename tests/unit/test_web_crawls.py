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


def test_database_lock_in_job_leases_is_not_reported_as_another_crawl():
    diagnostic = "OperationalError: database is locked [SQL: UPDATE job_leases SET version=version]"
    run = CrawlRun(
        connector="all",
        status=CrawlRunStatus.FAILED,
        errors=[{"message": diagnostic}],
        filters={},
    )
    view = _crawl_view(run)
    assert "banco local" in view.general_error
    assert "equivalente" not in view.general_error
    assert "banco local" in _friendly_source_error("pncp", [diagnostic])
    assert "repetidos" in _friendly_source_error(
        "compras_gov", ["IntegrityError: UNIQUE constraint failed: procurement_items.fingerprint"]
    )


def test_per_record_failures_are_attached_to_their_source():
    duplicate = "compras_gov/16010305000542026: IntegrityError: UNIQUE constraint failed: procurement_items.fingerprint"
    run = CrawlRun(
        connector="all",
        status=CrawlRunStatus.FAILED,
        errors=[{"message": duplicate}],
        filters={},
        cursor={"sources": {"compras_gov": {"status": "running", "records_failed": 2}}},
    )
    view = _crawl_view(run)
    source = next(item for item in view.sources if item.id == "compras_gov")
    assert "registros repetidos" in source.friendly_error
    assert source.technical_detail == duplicate
    assert view.general_error is None


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
