"""Cancellation at a committed lease boundary must leave no abandoned worker."""

import asyncio

import pytest
from sqlalchemy import func, select, update

from app.config import Settings
from app.models import CrawlRun, CrawlRunStatus, JobLease
from app.services.ingestion.municipal_payments import MunicipalPaymentIngestionService
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest


async def test_pipeline_cancellation_immediately_after_lease_commit_is_finalized(database):
    class InterruptedAdmission(IngestionPipeline):
        async def _acquire_lease(self, name, owner):
            await super()._acquire_lease(name, owner)
            raise asyncio.CancelledError

    pipeline = InterruptedAdmission(session_factory=database.sessions, connectors={})
    with pytest.raises(asyncio.CancelledError):
        await pipeline.run(PipelineRequest(connector="all", process_documents=False))
    async with database.sessions() as session:
        run = await session.scalar(select(CrawlRun))
        assert run.status is CrawlRunStatus.CANCELLED
        assert run.finished_at is not None
        assert await session.scalar(select(func.count()).select_from(JobLease)) == 0


async def test_municipal_cancellation_immediately_after_lease_commit_is_finalized(database):
    class InterruptedAdmission(MunicipalPaymentIngestionService):
        async def _acquire_lease(self, name, owner):
            await super()._acquire_lease(name, owner)
            raise asyncio.CancelledError

    service = InterruptedAdmission(
        Settings(_env_file=None, allow_insecure_http_sources=True),
        session_factory=database.sessions,
    )
    try:
        with pytest.raises(asyncio.CancelledError):
            await service.run("imperatriz", year=2026)
        async with database.sessions() as session:
            run = await session.scalar(select(CrawlRun))
            assert run.status is CrawlRunStatus.CANCELLED
            assert run.finished_at is not None
            assert await session.scalar(select(func.count()).select_from(JobLease)) == 0
    finally:
        await service.aclose()


async def test_pipeline_cancellation_after_run_creation_commit_is_finalized(database):
    class InterruptedCreation(IngestionPipeline):
        async def _create_run_record(self, request, created):
            await super()._create_run_record(request, created)
            raise asyncio.CancelledError

    pipeline = InterruptedCreation(session_factory=database.sessions, connectors={})
    with pytest.raises(asyncio.CancelledError):
        await pipeline.create_run(PipelineRequest(connector="pncp", process_documents=False))
    async with database.sessions() as session:
        run = await session.scalar(select(CrawlRun))
        assert run.status is CrawlRunStatus.CANCELLED and run.finished_at is not None


async def test_municipal_cancellation_after_run_creation_commit_is_finalized(database):
    class InterruptedCreation(MunicipalPaymentIngestionService):
        async def _create_run_record(self, source_id, year, company_cnpj, include_details, created):
            await super()._create_run_record(
                source_id, year, company_cnpj, include_details, created
            )
            raise asyncio.CancelledError

    service = InterruptedCreation(Settings(_env_file=None), session_factory=database.sessions)
    try:
        with pytest.raises(asyncio.CancelledError):
            await service.create_run("imperatriz", 2026, None, False)
        async with database.sessions() as session:
            run = await session.scalar(select(CrawlRun))
            assert run.status is CrawlRunStatus.CANCELLED and run.finished_at is not None
    finally:
        await service.aclose()


async def test_recovery_between_admission_and_start_cannot_be_overwritten(database):
    class RecoveredBeforeStart(IngestionPipeline):
        async def _acquire_lease(self, name, owner):
            acquired = await super()._acquire_lease(name, owner)
            async with database.sessions() as session, session.begin():
                await session.execute(
                    update(JobLease).values(owner_id="replacement", version=JobLease.version + 1)
                )
                await session.execute(
                    update(CrawlRun).values(
                        status=CrawlRunStatus.FAILED, diagnostic="already recovered"
                    )
                )
            return acquired

    pipeline = RecoveredBeforeStart(session_factory=database.sessions, connectors={})
    await pipeline.run(PipelineRequest(connector="pncp", process_documents=False))
    async with database.sessions() as session:
        run = await session.scalar(select(CrawlRun))
        assert run.status is CrawlRunStatus.FAILED and run.diagnostic == "already recovered"
        assert (await session.get(JobLease, "crawl:pncp:MA")).owner_id == "replacement"


async def test_late_admission_failure_cannot_overwrite_operator_cancellation(database):
    pipeline = IngestionPipeline(session_factory=database.sessions, connectors={})
    run = await pipeline.create_run(PipelineRequest(connector="pncp", process_documents=False))
    async with database.sessions() as session, session.begin():
        stored = await session.get(CrawlRun, run.id)
        stored.status = CrawlRunStatus.CANCELLED
        stored.diagnostic = "cancelled by operator"
    await pipeline._fail_run(run.id, "late admission error")
    async with database.sessions() as session:
        stored = await session.get(CrawlRun, run.id)
        assert (
            stored.status is CrawlRunStatus.CANCELLED
            and stored.diagnostic == "cancelled by operator"
        )
