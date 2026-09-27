"""Recover crawl runs abandoned by an interrupted process."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.database import async_session_factory
from app.models import CrawlRun, CrawlRunStatus

_STALE_MESSAGE = (
    "coleta interrompida antes de concluir (processo encerrado); "
    "marcada como falha para permitir nova tentativa"
)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


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
            run.status = CrawlRunStatus.FAILED
            run.started_at = run.started_at or moment
            run.finished_at = moment
            run.diagnostic = _STALE_MESSAGE
            run.errors = [{"message": _STALE_MESSAGE}]
            reconciled += 1
    return reconciled
