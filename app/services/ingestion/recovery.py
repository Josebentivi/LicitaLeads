"""Recover crawl runs abandoned by an interrupted process."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.database import async_session_factory
from app.models import CrawlRun, CrawlRunStatus, JobLease

_STALE_MESSAGE = (
    "coleta interrompida antes de concluir (processo encerrado); "
    "marcada como falha para permitir nova tentativa"
)
_CANCELLED_MESSAGE = "coleta cancelada pelo usuário antes de concluir"


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _normalize_cursor(run: CrawlRun, message: str) -> None:
    """Rewrite stale per-source progress so a terminal run never shows as running."""

    cursor = dict(run.cursor or {})
    sources = cursor.get("sources")
    if not isinstance(sources, dict):
        return
    terminal = "cancelled" if run.status is CrawlRunStatus.CANCELLED else "failed"
    normalized: dict[str, Any] = {}
    for key, entry in sources.items():
        if isinstance(entry, dict) and str(entry.get("status")) in {"running", "pending"}:
            entry = dict(entry)
            entry["status"] = terminal
            diagnostics = entry.get("diagnostics")
            diagnostics_list = list(diagnostics) if isinstance(diagnostics, list) else []
            diagnostics_list.append(message)
            entry["diagnostics"] = diagnostics_list
        normalized[key] = entry
    cursor["sources"] = normalized
    run.cursor = cursor


async def _release_lease(session: AsyncSession, run: CrawlRun, now: datetime) -> None:
    """Release the crawl lease held by the interrupted run, never a newer one."""

    filters = run.filters if isinstance(run.filters, dict) else {}
    connector = str(filters.get("connector") or run.connector)
    uf = str(filters.get("uf") or "").upper()
    if not uf:
        return
    name = f"crawl:{connector.lower()}:{uf}"
    await session.execute(
        delete(JobLease).where(
            JobLease.name == name,
            JobLease.owner_id == f"pipeline:{run.id}",
        )
    )
    await session.execute(
        delete(JobLease).where(
            JobLease.name == name,
            JobLease.owner_id.like("pipeline:%"),
            JobLease.expires_at <= now,
        )
    )


async def reconcile_stale_crawls(
    *,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    now: datetime | None = None,
    stale_after_minutes: int | None = None,
) -> int:
    """Fail runs stuck in a non-terminal state beyond the configured window.

    A crash, restart, or lost background task leaves a run ``pending`` or
    ``running`` forever. This marks those as ``failed`` so operators can retry
    instead of seeing a permanent "Em andamento".
    """

    settings = get_settings()
    moment = now or datetime.now(UTC)
    window = stale_after_minutes or settings.crawl_stale_after_minutes
    threshold = moment - timedelta(minutes=window)
    factory = session_factory or async_session_factory
    reconciled = 0

    async with factory() as session, session.begin():
        candidates = list(
            (
                await session.scalars(
                    select(CrawlRun).where(
                        CrawlRun.status.in_([CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING]),
                        CrawlRun.created_at < threshold,
                    )
                )
            ).all()
        )
        for run in candidates:
            reference = _as_utc(run.started_at) or _as_utc(run.created_at)
            if reference is not None and reference >= threshold:
                continue
            if run.cancel_requested:
                run.status = CrawlRunStatus.CANCELLED
                message = _CANCELLED_MESSAGE
            else:
                run.status = CrawlRunStatus.FAILED
                message = _STALE_MESSAGE
            run.diagnostic = message
            run.errors = [{"message": message}]
            run.started_at = run.started_at or moment
            run.finished_at = moment
            _normalize_cursor(run, message)
            await _release_lease(session, run, moment)
            reconciled += 1
    return reconciled
