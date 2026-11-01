"""Crawler run tracking and scheduler lease operations."""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from sqlalchemy import delete, or_, select, text, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import CrawlRun, JobLease
from app.models.enums import CrawlRunStatus
from app.repositories.base import BaseRepository, Page

MAINTENANCE_LEASE = "maintenance:exclusive"


class MaintenanceInProgress(RuntimeError):
    """A database maintenance window currently excludes application writes."""


class CrawlRunRepository(BaseRepository[CrawlRun]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, CrawlRun)

    async def list_recent(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        connector: str | None = None,
        status: CrawlRunStatus | None = None,
    ) -> Page[CrawlRun]:
        statement = select(CrawlRun)
        if connector:
            statement = statement.where(CrawlRun.connector == connector)
        if status:
            statement = statement.where(CrawlRun.status == status)
        statement = statement.order_by(CrawlRun.created_at.desc(), CrawlRun.id)
        return await self.page(statement, page=page, page_size=page_size)

    async def mark_running(self, run: CrawlRun, *, started_at: datetime) -> CrawlRun:
        run.status = CrawlRunStatus.RUNNING
        run.started_at = started_at
        await self.session.flush()
        return run

    async def finish(
        self,
        run: CrawlRun,
        *,
        status: CrawlRunStatus,
        finished_at: datetime,
        records_found: int,
        records_created: int,
        records_updated: int,
    ) -> CrawlRun:
        if status not in {
            CrawlRunStatus.COMPLETED,
            CrawlRunStatus.PARTIAL,
            CrawlRunStatus.FAILED,
            CrawlRunStatus.CANCELLED,
        }:
            raise ValueError("finish requires a terminal crawl status")
        run.status = status
        run.finished_at = finished_at
        run.records_found = records_found
        run.records_created = records_created
        run.records_updated = records_updated
        await self.session.flush()
        return run


class JobLeaseRepository:
    """Cooperative DB lease used to keep scheduler jobs single-instance."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def lock_acquisitions(self) -> None:
        """Serialize maintenance and worker admission until transaction commit."""
        if self.session.get_bind().dialect.name == "postgresql":
            await self.session.execute(text("SELECT pg_advisory_xact_lock(83850117047118)"))
        else:
            # Even an UPDATE matching zero rows takes SQLite's writer lock. No
            # persistent guard row is needed, and acquisition stays atomic.
            await self.session.execute(
                update(JobLease)
                .where(JobLease.name == MAINTENANCE_LEASE)
                .values(version=JobLease.version)
            )

    async def assert_writes_allowed(self, *, now: datetime) -> None:
        await self.lock_acquisitions()
        if await self.session.scalar(
            select(JobLease.name).where(
                JobLease.name == MAINTENANCE_LEASE, JobLease.expires_at > now
            )
        ):
            raise MaintenanceInProgress("manutenção exclusiva em andamento; tente novamente depois")

    async def acquire(
        self,
        *,
        name: str,
        owner_id: str,
        now: datetime,
        expires_at: datetime,
    ) -> bool:
        await self.lock_acquisitions()
        if name == MAINTENANCE_LEASE:
            active = await self.session.scalar(
                select(JobLease.name).where(JobLease.expires_at > now).limit(1)
            )
            running = await self.session.scalar(
                select(CrawlRun.id)
                .where(CrawlRun.status.in_([CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING]))
                .limit(1)
            )
            if active or running:
                return False
        elif await self.session.scalar(
            select(JobLease.name).where(
                JobLease.name == MAINTENANCE_LEASE, JobLease.expires_at > now
            )
        ):
            return False
        lease = await self.session.scalar(
            select(JobLease).where(JobLease.name == name).with_for_update()
        )
        if lease is None:
            try:
                async with self.session.begin_nested():
                    self.session.add(
                        JobLease(
                            name=name,
                            owner_id=owner_id,
                            acquired_at=now,
                            heartbeat_at=now,
                            expires_at=expires_at,
                        )
                    )
                    await self.session.flush()
                return True
            except (IntegrityError, OperationalError):
                return False
        result = cast(
            CursorResult[Any],
            await self.session.execute(
                update(JobLease)
                .where(
                    JobLease.name == name,
                    JobLease.version == lease.version,
                    or_(
                        JobLease.owner_id == owner_id,
                        JobLease.expires_at <= now,
                    ),
                )
                .values(
                    owner_id=owner_id,
                    acquired_at=now,
                    heartbeat_at=now,
                    expires_at=expires_at,
                    version=JobLease.version + 1,
                )
            ),
        )
        return bool(result.rowcount)

    async def renew(
        self,
        *,
        name: str,
        owner_id: str,
        now: datetime,
        expires_at: datetime,
        generation: int | None = None,
    ) -> bool:
        await self.lock_acquisitions()
        statement = update(JobLease).where(
            JobLease.name == name, JobLease.owner_id == owner_id, JobLease.expires_at > now
        )
        if generation is not None:
            statement = statement.where(JobLease.version == generation)
        result = cast(
            CursorResult[Any],
            await self.session.execute(
                statement.values(
                    heartbeat_at=now,
                    expires_at=expires_at,
                )
            ),
        )
        return bool(result.rowcount)

    async def release(self, *, name: str, owner_id: str, generation: int | None = None) -> bool:
        await self.lock_acquisitions()
        statement = delete(JobLease).where(JobLease.name == name, JobLease.owner_id == owner_id)
        if generation is not None:
            statement = statement.where(JobLease.version == generation)
        result = cast(
            CursorResult[Any],
            await self.session.execute(statement),
        )
        return bool(result.rowcount)

    async def fence(self, *, name: str, owner_id: str, generation: int, now: datetime) -> bool:
        """Lock and validate ownership in the SAME transaction as protected writes."""
        await self.lock_acquisitions()
        result = cast(
            CursorResult[Any],
            await self.session.execute(
                update(JobLease)
                .where(
                    JobLease.name == name,
                    JobLease.owner_id == owner_id,
                    JobLease.version == generation,
                    JobLease.expires_at > now,
                )
                .values(version=JobLease.version)
            ),
        )
        return bool(result.rowcount)

    async def purge_stale(self, *, now: datetime) -> int:
        """Remove expired leases and crawl leases with no active run.

        Expired rows are inert (``acquire`` steals them), and a crawl lease is
        only meaningful while its run is pending/running. Leases of active runs
        are preserved.
        """

        await self.lock_acquisitions()
        removed = 0
        expired = cast(
            CursorResult[Any],
            await self.session.execute(delete(JobLease).where(JobLease.expires_at <= now)),
        )
        removed += int(expired.rowcount or 0)

        active_names: set[str] = set()
        active_runs = await self.session.scalars(
            select(CrawlRun).where(
                CrawlRun.status.in_([CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING])
            )
        )
        for run in active_runs.all():
            filters = run.filters if isinstance(run.filters, dict) else {}
            connector = str(filters.get("connector") or run.connector).lower()
            uf = str(filters.get("uf") or "").upper()
            if connector.startswith("municipal:"):
                active_names.add(f"municipal:{connector.removeprefix('municipal:')}")
            elif uf:
                for name in ["pncp", "compras_gov"] if connector == "all" else [connector]:
                    active_names.add(f"crawl:{name}:{uf}")

        crawl_delete = delete(JobLease).where(JobLease.name.like("crawl:%"))
        if active_names:
            crawl_delete = crawl_delete.where(JobLease.name.not_in(active_names))
        orphans = cast(CursorResult[Any], await self.session.execute(crawl_delete))
        removed += int(orphans.rowcount or 0)
        return removed
