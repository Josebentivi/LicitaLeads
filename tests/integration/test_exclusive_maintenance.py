"""Maintenance admission covers workers, API writes, stale backups and cleanup."""

import asyncio
import threading
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update

from app.models import JobLease, Procurement
from app.repositories.crawls import MAINTENANCE_LEASE, JobLeaseRepository, MaintenanceInProgress
from app.services.maintenance import (
    MaintenanceBlocked,
    exclusive_maintenance,
    run_blocking_maintenance,
)


async def test_exclusive_maintenance_blocks_worker_orm_and_bulk_writes(database):
    async with database.sessions() as session, session.begin():
        procurement = Procurement(source="pncp", external_id="sentinel", fingerprint="a" * 64)
        session.add(procurement)
    async with exclusive_maintenance(database.sessions):
        now = datetime.now(UTC)
        async with database.sessions() as session, session.begin():
            assert not await JobLeaseRepository(session).acquire(
                name="crawl:pncp:MA",
                owner_id="worker",
                now=now,
                expires_at=now + timedelta(minutes=5),
            )
        async with database.sessions() as session:
            row = await session.get(Procurement, procurement.id)
            row.title = "forbidden"
            with pytest.raises(MaintenanceInProgress):
                await session.commit()
            await session.rollback()
        async with database.sessions() as session:
            with pytest.raises(MaintenanceInProgress):
                await session.execute(update(Procurement).values(title="forbidden"))
            await session.rollback()
        with pytest.raises(MaintenanceBlocked):
            async with exclusive_maintenance(database.sessions):
                pytest.fail("two maintenance windows admitted")
    async with database.sessions() as session:
        assert (await session.get(Procurement, procurement.id)).title is None
        assert (await session.scalars(select(JobLease))).all() == []


async def test_maintenance_and_worker_acquisition_cannot_both_win(database):
    async def worker():
        now = datetime.now(UTC)
        async with database.sessions() as session, session.begin():
            return await JobLeaseRepository(session).acquire(
                name="crawl:pncp:MA",
                owner_id="worker",
                now=now,
                expires_at=now + timedelta(minutes=5),
            )

    entered = asyncio.Event()
    release = asyncio.Event()

    async def maintenance():
        try:
            async with exclusive_maintenance(database.sessions):
                entered.set()
                await release.wait()
                return True
        except MaintenanceBlocked:
            entered.set()
            return False

    task = asyncio.create_task(maintenance())
    result = await worker()
    await entered.wait()
    release.set()
    maintained = await task
    assert result != maintained


async def test_maintenance_releases_lease_on_cancellation(database):
    entered = asyncio.Event()

    async def operation():
        async with exclusive_maintenance(database.sessions):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(operation())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with database.sessions() as session:
        assert (await session.scalars(select(JobLease))).all() == []


async def test_expired_maintenance_owner_cannot_write_or_release_a_new_owner(database):
    async with exclusive_maintenance(database.sessions) as owner:
        async with database.sessions() as session, session.begin():
            await session.execute(
                update(JobLease)
                .where(JobLease.name == MAINTENANCE_LEASE)
                .values(
                    owner_id="replacement",
                    version=JobLease.version + 1,
                )
            )
        async with database.sessions() as session:
            session.info["maintenance_owner"] = owner
            session.add(Procurement(source="pncp", external_id="stale", fingerprint="b" * 64))
            with pytest.raises(MaintenanceInProgress, match="perdeu"):
                await session.commit()
            await session.rollback()
    async with database.sessions() as session:
        lease = await session.get(JobLease, MAINTENANCE_LEASE)
        assert lease.owner_id == "replacement"
        assert (await session.scalars(select(Procurement))).all() == []


async def test_heartbeat_loss_is_reported_and_cleanup_releases_the_lease(database, monkeypatch):
    from app.services import maintenance

    sleep = asyncio.sleep

    async def fast_heartbeat(delay):
        await sleep(0 if delay == 30 else delay)

    async def cannot_renew(_self, **_kwargs):
        return False

    monkeypatch.setattr(maintenance.asyncio, "sleep", fast_heartbeat)
    monkeypatch.setattr(JobLeaseRepository, "renew", cannot_renew)
    with pytest.raises(MaintenanceBlocked, match="perdeu"):
        async with exclusive_maintenance(database.sessions):
            heartbeat = next(
                task for task in asyncio.all_tasks() if task.get_name() == "maintenance-heartbeat"
            )
            await heartbeat
    async with database.sessions() as session:
        assert (await session.scalars(select(JobLease))).all() == []


async def test_cancelling_filesystem_work_keeps_maintenance_exclusive_until_thread_exits(database):
    entered = threading.Event()
    finish = threading.Event()

    def operation():
        entered.set()
        assert finish.wait(timeout=5)

    async def maintenance():
        async with exclusive_maintenance(database.sessions):
            await run_blocking_maintenance(operation)

    task = asyncio.create_task(maintenance())
    assert await asyncio.to_thread(entered.wait, 5)
    task.cancel()
    await asyncio.sleep(0.01)
    assert not task.done()
    with pytest.raises(MaintenanceBlocked):
        async with exclusive_maintenance(database.sessions):
            pytest.fail("filesystem thread lost its maintenance gate")
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with database.sessions() as session:
        assert (await session.scalars(select(JobLease))).all() == []
