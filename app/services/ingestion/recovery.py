"""Recover crawl runs abandoned by an interrupted process."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.database import async_session_factory
from app.models import CollectionBatch, CrawlRun, CrawlRunStatus, JobLease
from app.services.ingestion.document_retry import DOCUMENT_LEASE

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
    if run.batch_id:
        batch = await session.get(CollectionBatch, run.batch_id)
        if batch is not None and batch.last_run_id == run.id:
            batch.status = "suspended" if run.status is CrawlRunStatus.CANCELLED else "failed"
    payment_lease = filters.get("lease_name")
    if (
        filters.get("mode") in {"payments", "payment_import"}
        and isinstance(payment_lease, str)
        and payment_lease.startswith("payments:")
    ):
        await session.execute(
            delete(JobLease).where(
                JobLease.name == payment_lease, JobLease.owner_id == payment_lease
            )
        )
        return
    connector = str(filters.get("connector") or run.connector)
    await session.execute(
        delete(JobLease).where(
            JobLease.name == DOCUMENT_LEASE,
            JobLease.owner_id == f"pipeline:{run.id}",
        )
    )
    if connector.lower().startswith("municipal:"):
        await session.execute(
            delete(JobLease).where(
                JobLease.name == f"municipal:{connector.split(':', 1)[1]}",
                JobLease.owner_id == f"municipal:{run.id}",
            )
        )
        return
    uf = str(filters.get("uf") or "").upper()
    if not uf:
        return
    connectors = ["pncp", "compras_gov"] if connector.lower() == "all" else [connector.lower()]
    if connector.lower() == "all":
        await session.execute(
            delete(JobLease).where(
                JobLease.name == f"crawl:all:{uf}",
                JobLease.owner_id == f"pipeline:{run.id}",
            )
        )
    for source_name in connectors:
        name = f"crawl:{source_name}:{uf}"
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


async def finalize_cancelled_run(
    session: AsyncSession,
    run: CrawlRun,
    *,
    now: datetime | None = None,
) -> None:
    """Mark an abandoned/pending run as cancelled and release its lease."""

    moment = now or datetime.now(UTC)
    run.cancel_requested = True
    run.status = CrawlRunStatus.CANCELLED
    from app.services.llm.queue import cancel_for_run

    await cancel_for_run(session, run.id)
    run.diagnostic = _CANCELLED_MESSAGE
    run.errors = [{"message": _CANCELLED_MESSAGE}]
    run.started_at = run.started_at or moment
    run.finished_at = moment
    _normalize_cursor(run, _CANCELLED_MESSAGE)
    await _release_lease(session, run, moment)


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
            activity_values = [
                value
                for value in (
                    _as_utc(run.started_at),
                    _as_utc(run.created_at),
                    _as_utc(run.heartbeat_at),
                )
                if value is not None
            ]
            if activity_values and max(activity_values) >= threshold:
                continue
            if run.cancel_requested:
                await finalize_cancelled_run(session, run, now=moment)
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
