"""SQLite writer admission preserves WAL readers, rollback and cancellation."""

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import event, select
from sqlalchemy.exc import OperationalError

from app.database import create_database_engine, create_session_factory, write_session
from app.models import JobLease, Procurement
from app.repositories.crawls import JobLeaseRepository
from app.services.ingestion.pipeline import PipelineCancelled


def short_busy_timeout(engine):
    def configure(connection, _record):
        cursor = connection.cursor()
        cursor.execute("PRAGMA busy_timeout=25")
        cursor.close()

    event.listen(engine.sync_engine, "connect", configure)


def entity(identifier):
    return Procurement(source="pncp", external_id=identifier, fingerprint=identifier)


async def test_writers_across_engines_queue_before_connecting_and_wal_readers_remain_available(
    database,
):
    second_engine = create_database_engine(str(database.engine.url))
    second_sessions = create_session_factory(second_engine)
    short_busy_timeout(database.engine)
    short_busy_timeout(second_engine)
    await database.engine.dispose()
    entered = asyncio.Event()

    async def another_writer():
        async with write_session(second_sessions) as session:
            entered.set()
            session.add(entity("second"))

    task = None
    try:
        async with write_session(database.sessions) as session:
            session.add(entity("first"))
            await session.flush()
            task = asyncio.create_task(another_writer())
            # The DB would fail after 25ms if the other writer bypassed the queue.
            await asyncio.sleep(0.08)
            assert not entered.is_set() and not task.done()
            async with second_sessions() as reader:
                assert (await reader.scalars(select(Procurement))).all() == []
        await asyncio.wait_for(task, 2)
        async with second_sessions() as reader:
            assert len((await reader.scalars(select(Procurement))).all()) == 2
    finally:
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await second_engine.dispose()


async def test_cancellation_waiting_for_writer_does_not_enter_or_leave_queue_locked(database):
    entered = False

    async def waiting():
        nonlocal entered
        async with write_session(database.sessions) as session:
            entered = True
            session.add(entity("cancelled"))

    async with write_session(database.sessions):
        task = asyncio.create_task(waiting())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not entered
    async with asyncio.timeout(2):
        async with write_session(database.sessions) as session:
            session.add(entity("after-cancellation"))
    async with database.sessions() as session:
        assert [row.external_id for row in (await session.scalars(select(Procurement))).all()] == [
            "after-cancellation"
        ]


async def test_external_writer_contention_retries_admission_without_replaying_body(database):
    short_busy_timeout(database.engine)
    await database.engine.dispose()
    holder = sqlite3.connect(database.engine.url.database)
    holder.execute("BEGIN IMMEDIATE")
    calls = 0

    async def waiting():
        nonlocal calls
        async with write_session(database.sessions) as session:
            calls += 1
            session.add(entity("external-contention"))

    task = asyncio.create_task(waiting())
    try:
        await asyncio.sleep(0.08)
        assert not task.done() and calls == 0
        holder.commit()
        await asyncio.wait_for(task, 2)
        assert calls == 1
    finally:
        holder.close()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_errors_in_transaction_body_are_rolled_back_and_never_replayed(database):
    calls = 0
    with pytest.raises(OperationalError):
        async with write_session(database.sessions) as session:
            calls += 1
            session.add(entity("rolled-back"))
            await session.flush()
            error = sqlite3.OperationalError("database is locked")
            error.sqlite_errorcode = sqlite3.SQLITE_BUSY
            raise OperationalError("test write", {}, error)
    assert calls == 1
    async with write_session(database.sessions) as session:
        assert (await session.scalars(select(Procurement))).all() == []
        session.add(entity("after-rollback"))


async def test_cancelled_writer_body_rolls_back_and_releases_queue(database):
    entered = asyncio.Event()

    async def writer():
        async with write_session(database.sessions) as session:
            session.add(entity("cancelled-body"))
            await session.flush()
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(writer())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with asyncio.timeout(2):
        async with write_session(database.sessions) as session:
            assert (await session.scalars(select(Procurement))).all() == []
            session.add(entity("after-cancelled-body"))


async def test_writer_checks_lease_generation_after_waiting_for_admission(database):
    now = datetime.now(UTC)
    name = "crawl:pncp:MA"
    async with write_session(database.sessions) as session:
        assert await JobLeaseRepository(session).acquire(
            name=name, owner_id="old", now=now, expires_at=now + timedelta(minutes=5)
        )
    entered = False

    async def old_worker():
        nonlocal entered
        async with write_session(database.sessions) as session:
            if not await JobLeaseRepository(session).fence(
                name=name, owner_id="old", generation=1, now=now
            ):
                raise PipelineCancelled("lease perdida")
            entered = True
            session.add(entity("lost-worker"))

    async with write_session(database.sessions) as session:
        lease = await session.get(JobLease, name)
        lease.owner_id = "replacement"
        lease.version = 2
        task = asyncio.create_task(old_worker())
        await asyncio.sleep(0)
        assert not task.done()
    with pytest.raises(PipelineCancelled):
        await asyncio.wait_for(task, 2)
    assert not entered
    async with database.sessions() as session:
        assert (await session.scalars(select(Procurement))).all() == []
        assert (await session.get(JobLease, name)).owner_id == "replacement"
