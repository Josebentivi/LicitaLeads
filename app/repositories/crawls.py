"""Crawler run tracking and scheduler lease operations."""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from sqlalchemy import delete, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import CrawlRun, JobLease
from app.models.enums import CrawlRunStatus
from app.repositories.base import BaseRepository, Page


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

    async def acquire(
        self,
        *,
        name: str,
        owner_id: str,
        now: datetime,
        expires_at: datetime,
    ) -> bool:
        lease = await self.session.scalar(
            select(JobLease).where(JobLease.name == name).with_for_update()
        )
        if lease is None:
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
        if lease.owner_id != owner_id and lease.expires_at > now:
            return False
        lease.owner_id = owner_id
        lease.acquired_at = now
        lease.heartbeat_at = now
        lease.expires_at = expires_at
        lease.version += 1
        await self.session.flush()
        return True

    async def renew(
        self,
        *,
        name: str,
        owner_id: str,
        now: datetime,
        expires_at: datetime,
    ) -> bool:
        result = cast(
            CursorResult[Any],
            await self.session.execute(
                update(JobLease)
                .where(JobLease.name == name, JobLease.owner_id == owner_id)
                .values(
                    heartbeat_at=now,
                    expires_at=expires_at,
                    version=JobLease.version + 1,
                )
            ),
        )
        return bool(result.rowcount)

    async def release(self, *, name: str, owner_id: str) -> bool:
        result = cast(
            CursorResult[Any],
            await self.session.execute(
                delete(JobLease).where(JobLease.name == name, JobLease.owner_id == owner_id)
            ),
        )
        return bool(result.rowcount)
