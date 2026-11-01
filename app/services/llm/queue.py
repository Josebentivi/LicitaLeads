"""Transactional outbox for optional AI; no HTTP inside a domain transaction."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import AsyncIterator, Callable, Iterable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from time import monotonic
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.database import async_session_factory, write_session
from app.models import (
    CrawlRun,
    CrawlRunStatus,
    Document,
    DocumentChunk,
    ExtractionStatus,
    FieldChange,
    JobLease,
    LLMAttempt,
    LLMBatch,
    LLMDemand,
    LLMProviderState,
    LLMResult,
    LLMWorkItem,
    Procurement,
)
from app.repositories.crawls import JobLeaseRepository
from app.services.llm.analyzer import EvidenceBoundLLMEventAnalyzer
from app.services.llm.providers import PROMPT_VERSION, LLMReply, OpenAIChatLLMProvider

logger = logging.getLogger(__name__)
LLM_LEASE = "llm:processing"


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, default=str).encode()
    ).hexdigest()


def request_config(settings: Settings) -> dict[str, Any]:
    return {
        "provider": settings.llm_provider,
        "base_url": settings.llm_base_url.rstrip("/"),
        "model": settings.llm_model,
        "max_completion_tokens": settings.llm_max_output_tokens,
        "prompt_version": PROMPT_VERSION,
        "validator_version": 2,
        "format": "json_object",
    }


def config_key(settings: Settings) -> str:
    # Credential rotation releases only its own circuit; never expose this key.
    return digest([request_config(settings), digest(settings.llm_api_key)])


async def document_snapshot(session: AsyncSession, document: Document) -> dict[str, Any] | None:
    from app.services.documents.identity import noncanonical_document_ids

    if (
        await session.scalar(
            select(Document.id).where(
                Document.id == document.id, Document.id.in_(noncanonical_document_ids())
            )
        )
        is not None
    ):
        return None
    chain = []
    visited = set()
    current: Document | None = document
    while current is not None:
        if current.id in visited or not current.is_active:
            return None
        visited.add(current.id)
        revision = await session.scalar(
            select(func.count())
            .select_from(FieldChange)
            .where(
                FieldChange.entity_type == "document",
                FieldChange.entity_id == current.id,
                FieldChange.field_name == "is_active",
                FieldChange.applied.is_(True),
            )
        )
        chain.append(
            {
                "id": str(current.id),
                "hash": current.file_hash,
                "deactivated_at": current.deactivated_at.isoformat()
                if current.deactivated_at
                else None,
                "activity_revision": revision,
                "original_url": current.original_url,
            }
        )
        if current.parent_document_id is None:
            break
        current = await session.get(Document, current.parent_document_id, populate_existing=True)
        if current is None:
            return None
    if (
        document.extraction_status != ExtractionStatus.EXTRACTED
        or not document.file_hash
        or document.mime_type == "application/zip"
    ):
        return None
    return {
        "chain": chain,
        "procurement_id": str(document.procurement_id),
        "document_type": document.document_type,
        "published_at": document.published_at.isoformat() if document.published_at else None,
    }


async def get_batch(
    session: AsyncSession, *, run_id: UUID | None = None, label: str = "Detecção automática"
) -> LLMBatch:
    batch = await session.scalar(
        select(LLMBatch).where(
            LLMBatch.crawl_run_id == run_id
            if run_id
            else (LLMBatch.crawl_run_id.is_(None) & (LLMBatch.label == label))
        )
    )
    if batch is None:
        batch = LLMBatch(crawl_run_id=run_id, label=label, cancelled=False)
        session.add(batch)
        await session.flush()
    return batch


async def enqueue_document(
    session: AsyncSession,
    document: Document,
    settings: Settings,
    *,
    run_id: UUID | None = None,
    batch: LLMBatch | None = None,
    explicit: bool = False,
) -> int:
    if not settings.llm_enabled or settings.llm_provider != "openai":
        return 0
    await session.flush()
    snapshot = await document_snapshot(session, document)
    if snapshot is None:
        return 0
    batch = batch or await get_batch(session, run_id=run_id)
    if batch.cancelled:
        return 0
    if run_id:
        run = await session.get(CrawlRun, run_id)
        if run is None or run.cancel_requested or run.status == CrawlRunStatus.CANCELLED:
            return 0
    chunks = list(
        (
            await session.scalars(
                select(DocumentChunk)
                .where(DocumentChunk.document_id == document.id)
                .where(func.length(DocumentChunk.text) >= settings.llm_min_chunk_characters)
                .order_by(DocumentChunk.sequence, DocumentChunk.id)
                .limit(settings.llm_max_chunks_per_document)
            )
        ).all()
    )
    count = 0
    for chunk in chunks:
        cache_key = digest(
            [
                request_config(settings),
                chunk.text,
                chunk.page_number,
                chunk.start_offset,
                chunk.end_offset,
                chunk.locator,
            ]
        )
        key = digest([str(document.id), str(chunk.id), snapshot, cache_key, config_key(settings)])
        work = await session.scalar(select(LLMWorkItem).where(LLMWorkItem.job_key == key))
        if work is None:
            work = LLMWorkItem(
                job_key=key,
                cache_key=cache_key,
                config_key=config_key(settings),
                document_id=document.id,
                chunk_id=chunk.id,
                snapshot=snapshot,
            )
            session.add(work)
            await session.flush()
        elif work.status in {"failed", "cancelled", "obsolete"}:
            if not explicit:
                continue
            work.status, work.error, work.next_attempt_at = "pending", None, None
            work.attempts = 0
        demand = await session.scalar(
            select(LLMDemand.id).where(LLMDemand.batch_id == batch.id, LLMDemand.work_id == work.id)
        )
        if demand is None:
            session.add(
                LLMDemand(batch_id=batch.id, work_id=work.id, reused=work.status == "succeeded")
            )
            count += 1
    return count


async def cancel_for_run(session: AsyncSession, run_id: UUID) -> bool:
    batch = await session.scalar(select(LLMBatch).where(LLMBatch.crawl_run_id == run_id))
    if batch is None or batch.cancelled:
        return False
    pending = await session.scalar(
        select(LLMWorkItem.id)
        .join(LLMDemand)
        .where(LLMDemand.batch_id == batch.id, LLMWorkItem.status.in_(["pending", "running"]))
        .limit(1)
    )
    if pending is None:
        return False
    await cancel_batch(session, batch)
    return True


async def cancel_batch(session: AsyncSession, batch: LLMBatch) -> None:
    batch.cancelled = True
    await session.flush()
    requested = select(LLMDemand.work_id).where(LLMDemand.batch_id == batch.id)
    active = (
        select(LLMDemand.work_id)
        .join(LLMBatch, LLMBatch.id == LLMDemand.batch_id)
        .where(LLMBatch.cancelled.is_(False))
    )
    await session.execute(
        update(LLMWorkItem)
        .where(
            LLMWorkItem.id.in_(requested),
            LLMWorkItem.id.not_in(active),
            LLMWorkItem.status.in_(["pending", "running"]),
        )
        .values(status="cancelled")
    )


async def live_batches(session: AsyncSession, work_id: UUID) -> list[LLMBatch]:
    return list(
        (
            await session.scalars(
                select(LLMBatch)
                .join(LLMDemand, LLMDemand.batch_id == LLMBatch.id)
                .outerjoin(CrawlRun, CrawlRun.id == LLMBatch.crawl_run_id)
                .where(
                    LLMDemand.work_id == work_id,
                    LLMBatch.cancelled.is_(False),
                    or_(
                        CrawlRun.id.is_(None),
                        (
                            CrawlRun.cancel_requested.is_(False)
                            & (CrawlRun.status != CrawlRunStatus.CANCELLED)
                        ),
                    ),
                )
                .order_by(LLMBatch.created_at, LLMBatch.id)
            )
        ).all()
    )


async def next_request_at(session: AsyncSession, settings: Settings, now: datetime) -> datetime:
    last = await session.scalar(select(func.max(LLMAttempt.started_at)))
    deadline = (
        max(now, last + timedelta(seconds=3600 / settings.llm_max_requests_per_hour))
        if last
        else now
    )
    # The Nth most recent request releases the rolling window's next slot.
    # Read one timestamp instead of transferring every attempt in that window.
    boundary = await session.scalar(
        select(LLMAttempt.started_at)
        .where(LLMAttempt.started_at > now - timedelta(hours=1))
        .order_by(LLMAttempt.started_at.desc())
        .offset(settings.llm_max_requests_per_hour - 1)
        .limit(1)
    )
    if boundary is not None:
        deadline = max(deadline, boundary + timedelta(hours=1))
    return deadline


class LLMQueueBusy(RuntimeError):
    pass


@asynccontextmanager
async def queue_lease(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[Callable[[AsyncSession], Any]]:
    owner = f"llm:{uuid4()}"
    now = datetime.now(UTC)
    async with write_session(factory) as session:
        repo = JobLeaseRepository(session)
        if not await repo.acquire(
            name=LLM_LEASE, owner_id=owner, now=now, expires_at=now + timedelta(minutes=15)
        ):
            raise LLMQueueBusy("Já existe processamento de IA em andamento")
        lease = await session.get(JobLease, LLM_LEASE)
        assert lease is not None
        generation = lease.version
    worker = asyncio.current_task()

    async def fence(session: AsyncSession) -> None:
        if not await JobLeaseRepository(session).fence(
            name=LLM_LEASE, owner_id=owner, generation=generation, now=datetime.now(UTC)
        ):
            raise asyncio.CancelledError("lease da IA perdida")

    async def renew() -> None:
        try:
            while True:
                await asyncio.sleep(20)
                async with write_session(factory) as session:
                    ok = await JobLeaseRepository(session).renew(
                        name=LLM_LEASE,
                        owner_id=owner,
                        generation=generation,
                        now=datetime.now(UTC),
                        expires_at=datetime.now(UTC) + timedelta(minutes=15),
                    )
                if not ok:
                    if worker:
                        worker.cancel()
                    return
        except Exception:
            if worker:
                worker.cancel()
            raise

    heartbeat = asyncio.create_task(renew(), name="llm-heartbeat")
    try:
        yield fence
    finally:
        heartbeat.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await heartbeat

        async def release() -> None:
            async with write_session(factory) as session:
                await JobLeaseRepository(session).release(
                    name=LLM_LEASE, owner_id=owner, generation=generation
                )

        cleanup = asyncio.create_task(release())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
            raise


@dataclass(frozen=True)
class Dispatch:
    work_id: UUID
    attempt_id: UUID
    text: str
    page: int | None


class LLMQueueService:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        provider: Any = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.factory = session_factory or async_session_factory
        self.provider = provider
        self.clock = clock or (lambda: datetime.now(UTC))

    async def backfill(self) -> dict[str, Any]:
        if not self.settings.llm_enabled:
            raise ValueError("Ative LLM_ENABLED antes de enfileirar análises")
        async with self.factory() as session:
            ids = list(
                (
                    await session.scalars(
                        select(Document.id)
                        .where(
                            Document.is_active.is_(True),
                            Document.extraction_status == ExtractionStatus.EXTRACTED,
                        )
                        .order_by(Document.created_at, Document.id)
                    )
                ).all()
            )
        async with write_session(self.factory) as session:
            await JobLeaseRepository(session).assert_writes_allowed(now=self.clock())
            batch = LLMBatch(label="Documentos extraídos", cancelled=False)
            session.add(batch)
            await session.flush()
            batch_id = batch.id
        documents = jobs = 0
        for doc_id in ids:
            async with write_session(self.factory) as session:
                await JobLeaseRepository(session).assert_writes_allowed(now=self.clock())
                current_batch = await session.get(LLMBatch, batch_id)
                doc = await session.get(Document, doc_id)
                if current_batch and doc:
                    added = await enqueue_document(
                        session, doc, self.settings, batch=current_batch, explicit=True
                    )
                    documents += bool(added)
                    jobs += added
        return {"batch_id": str(batch_id), "documents": documents, "analyses": jobs}

    async def _current(
        self, session: AsyncSession, work: LLMWorkItem
    ) -> tuple[Document, DocumentChunk] | None:
        doc = await session.get(Document, work.document_id, populate_existing=True)
        chunk = await session.get(DocumentChunk, work.chunk_id) if work.chunk_id else None
        if (
            doc is None
            or chunk is None
            or chunk.document_id != doc.id
            or await document_snapshot(session, doc) != work.snapshot
        ):
            return None
        key = digest(
            [
                request_config(self.settings),
                chunk.text,
                chunk.page_number,
                chunk.start_offset,
                chunk.end_offset,
                chunk.locator,
            ]
        )
        return (doc, chunk) if key == work.cache_key else None

    async def _apply(
        self, session: AsyncSession, work: LLMWorkItem, result: LLMResult, *, reused: bool
    ) -> None:
        current = await self._current(session, work)
        if current is None:
            work.status = "obsolete"
            return
        if not await live_batches(session, work.id):
            work.status = "cancelled"
            return
        doc, chunk = current
        analyses = EvidenceBoundLLMEventAnalyzer.validate(
            result.analyses,
            chunk.text,
            pages={chunk.page_number: chunk.text} if chunk.page_number else None,
        )
        # Import lazily: the processor only imports enqueue_document at commit.
        from app.services.ingestion.processor import DocumentProcessingService

        service = DocumentProcessingService(self.settings, session_factory=self.factory)
        try:
            procurement = await session.get(Procurement, doc.procurement_id)
            if procurement:
                await service._detect_llm_events(
                    session, procurement, doc, chunk, analyses=analyses
                )
        finally:
            await service.aclose()
        work.status, work.result_id = "succeeded", result.id
        work.reused, work.completed_at, work.error = reused, self.clock(), None

    async def _claim(
        self, fence: Callable[[AsyncSession], Any]
    ) -> tuple[Dispatch | None, datetime | None]:
        async with write_session(self.factory) as session:
            await fence(session)
            state = await session.scalar(
                select(LLMProviderState).where(
                    LLMProviderState.config_key == config_key(self.settings)
                )
            )
            if state and state.blocked_at:
                return None, None
            work = await session.scalar(
                select(LLMWorkItem)
                .where(
                    LLMWorkItem.config_key == config_key(self.settings),
                    LLMWorkItem.status == "pending",
                    or_(
                        LLMWorkItem.next_attempt_at.is_(None),
                        LLMWorkItem.next_attempt_at <= self.clock(),
                    ),
                )
                .order_by(LLMWorkItem.created_at, LLMWorkItem.id)
                .limit(1)
            )
            if work is None:
                due = await session.scalar(
                    select(func.min(LLMWorkItem.next_attempt_at)).where(
                        LLMWorkItem.config_key == config_key(self.settings),
                        LLMWorkItem.status == "pending",
                    )
                )
                return None, due
            batches = await live_batches(session, work.id)
            current = await self._current(session, work)
            if not batches or current is None:
                work.status = "cancelled" if not batches else "obsolete"
                return None, self.clock()
            cached = await session.scalar(
                select(LLMResult).where(LLMResult.cache_key == work.cache_key)
            )
            if cached:
                await self._apply(session, work, cached, reused=True)
                return None, self.clock()
            due = await next_request_at(session, self.settings, self.clock())
            if state and state.cooldown_until:
                due = max(due, state.cooldown_until)
            if due > self.clock():
                return None, due
            if work.attempts > self.settings.llm_max_retries:
                work.status = "failed"
                return None, self.clock()
            doc, chunk = current
            attempt = LLMAttempt(
                work_id=work.id,
                batch_id=batches[0].id,
                started_at=self.clock(),
                model=self.settings.llm_model,
                input_price=self.settings.llm_input_price_per_million_usd,
                output_price=self.settings.llm_output_price_per_million_usd,
            )
            session.add(attempt)
            work.status, work.attempts = "running", work.attempts + 1
            await session.flush()
            return Dispatch(work.id, attempt.id, chunk.text, chunk.page_number), None

    async def _send(self, dispatch: Dispatch, fence: Callable[[AsyncSession], Any]) -> LLMReply:
        async with write_session(self.factory) as session:
            await fence(session)
            work = await session.get(LLMWorkItem, dispatch.work_id)
            if (
                work is None
                or not await live_batches(session, work.id)
                or await self._current(session, work) is None
            ):
                return LLMReply(diagnostic={"error": True, "code": "cancelled_or_obsolete"})
        pages = {dispatch.page: dispatch.text} if dispatch.page else None
        task = asyncio.create_task(self.provider.request(dispatch.text, pages))
        try:
            while not task.done():
                await asyncio.wait({task}, timeout=0.5)
                if task.done():
                    break
                async with write_session(self.factory) as session:
                    await fence(session)
                    work = await session.get(LLMWorkItem, dispatch.work_id)
                    if (
                        work is None
                        or not await live_batches(session, work.id)
                        or await self._current(session, work) is None
                    ):
                        return LLMReply(diagnostic={"error": True, "code": "cancelled_or_obsolete"})
            return await task
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _finish(
        self, dispatch: Dispatch, reply: LLMReply, fence: Callable[[AsyncSession], Any]
    ) -> None:
        analyses = None
        if not reply.diagnostic.get("error"):
            try:
                analyses = [
                    a.model_dump(mode="json")
                    for a in EvidenceBoundLLMEventAnalyzer.validate(
                        reply.analyses,
                        dispatch.text,
                        pages={dispatch.page: dispatch.text} if dispatch.page else None,
                    )
                ]
            except Exception:
                logger.warning(
                    "llm_evidence_rejected", extra={"context": {"work_id": str(dispatch.work_id)}}
                )
        async with write_session(self.factory) as session:
            await fence(session)
            attempt = await session.get(LLMAttempt, dispatch.attempt_id)
            work = await session.get(LLMWorkItem, dispatch.work_id)
            if attempt is None or work is None:
                return
            attempt.finished_at = self.clock()
            attempt.input_tokens, attempt.output_tokens = reply.input_tokens, reply.output_tokens
            if reply.input_tokens is not None and reply.output_tokens is not None:
                attempt.cost_usd = (
                    Decimal(reply.input_tokens) * attempt.input_price
                    + Decimal(reply.output_tokens) * attempt.output_price
                ) / Decimal(1_000_000)
            attempt.model = reply.model or attempt.model
            diagnostic = (
                reply.diagnostic
                if analyses is not None or reply.diagnostic.get("error")
                else {**reply.diagnostic, "error": True, "code": "evidence_rejected"}
            )
            attempt.diagnostic = diagnostic
            attempt.status = "succeeded" if analyses is not None else "failed"
            if reply.systemic or reply.retry_seconds > 0:
                state = await session.scalar(
                    select(LLMProviderState).where(LLMProviderState.config_key == work.config_key)
                )
                if state is None:
                    state = LLMProviderState(config_key=work.config_key)
                    session.add(state)
                if reply.systemic:
                    state.blocked_at, state.error = self.clock(), diagnostic
                if reply.retry_seconds > 0:
                    state.cooldown_until = max(
                        state.cooldown_until or self.clock(),
                        self.clock() + timedelta(seconds=reply.retry_seconds),
                    )
            if analyses is None:
                work.error = diagnostic
                work.status = (
                    "pending"
                    if reply.transient and work.attempts <= self.settings.llm_max_retries
                    else "failed"
                )
                work.next_attempt_at = (
                    self.clock()
                    + timedelta(
                        seconds=max(
                            reply.retry_seconds, min(300, 5 * 2 ** max(0, work.attempts - 1))
                        )
                    )
                    if work.status == "pending"
                    else None
                )
                if not await live_batches(session, work.id):
                    work.status = "cancelled"
                elif await self._current(session, work) is None:
                    work.status = "obsolete"
                return
            result = await session.scalar(
                select(LLMResult).where(LLMResult.cache_key == work.cache_key)
            )
            if result is None:
                result = LLMResult(cache_key=work.cache_key, analyses=analyses)
                session.add(result)
                await session.flush()
            work.result_id = result.id
            attempt.result_id = result.id
        # Usage/result survives even if applying derived facts fails.
        async with write_session(self.factory) as session:
            await fence(session)
            work = await session.get(LLMWorkItem, dispatch.work_id)
            if work and work.result_id:
                result = await session.get(LLMResult, work.result_id)
                if result:
                    await self._apply(session, work, result, reused=False)

    async def drain(
        self, *, max_seconds: float = 300, max_calls: int = 100, wait: bool = True
    ) -> int:
        if not self.settings.llm_enabled or self.settings.llm_provider != "openai":
            return 0
        self.provider = self.provider or OpenAIChatLLMProvider(self.settings)
        calls, end = 0, monotonic() + max_seconds
        try:
            async with queue_lease(self.factory) as fence:
                async with write_session(self.factory) as session:
                    await fence(session)
                    for work in (
                        await session.scalars(
                            select(LLMWorkItem).where(LLMWorkItem.status == "running")
                        )
                    ).all():
                        work.status = "pending"
                    for attempt in (
                        await session.scalars(
                            select(LLMAttempt).where(LLMAttempt.finished_at.is_(None))
                        )
                    ).all():
                        attempt.status, attempt.finished_at = "interrupted", self.clock()
                while monotonic() < end and calls < max_calls:
                    dispatch, due = await self._claim(fence)
                    if dispatch is None:
                        if due is None:
                            break
                        delay = max(0, (due - self.clock()).total_seconds())
                        if delay and (not wait or delay > end - monotonic()):
                            break
                        await asyncio.sleep(min(delay, 5))
                        continue
                    reply = await self._send(dispatch, fence)
                    await self._finish(dispatch, reply, fence)
                    calls += 1
        except LLMQueueBusy:
            return 0
        finally:
            await self.provider.aclose()
        return calls


async def retry_for_run(session: AsyncSession, run_id: UUID, settings: Settings) -> int:
    await JobLeaseRepository(session).assert_writes_allowed(now=datetime.now(UTC))
    run = await session.get(CrawlRun, run_id)
    if run is None:
        raise LookupError("Coleta não encontrada")
    batch = await session.scalar(select(LLMBatch).where(LLMBatch.crawl_run_id == run_id))
    if batch is None:
        raise ValueError("Esta coleta não possui análises de IA enfileiradas")
    return await retry_batch(session, batch.id, settings)


async def retry_batch(session: AsyncSession, batch_id: UUID, settings: Settings) -> int:
    await JobLeaseRepository(session).assert_writes_allowed(now=datetime.now(UTC))
    lease = await session.get(JobLease, LLM_LEASE)
    if lease and lease.expires_at > datetime.now(UTC):
        raise LLMQueueBusy("Aguarde a execução de IA em andamento")
    batch = await session.get(LLMBatch, batch_id)
    if batch is None:
        raise LookupError("Lote de IA não encontrado")
    state = await session.scalar(
        select(LLMProviderState).where(LLMProviderState.config_key == config_key(settings))
    )
    works = list(
        (
            await session.scalars(
                select(LLMWorkItem)
                .join(LLMDemand)
                .where(
                    LLMDemand.batch_id == batch.id,
                    or_(
                        LLMWorkItem.status.in_(["failed", "cancelled"]),
                        (LLMWorkItem.status == "pending")
                        & (
                            (LLMWorkItem.config_key != config_key(settings))
                            | bool(state and state.blocked_at)
                        ),
                    ),
                )
            )
        ).all()
    )
    if not works:
        raise ValueError("Nenhuma análise de IA disponível para nova tentativa")
    if not settings.llm_enabled:
        raise ValueError("A análise de IA está desativada")
    run = await session.get(CrawlRun, batch.crawl_run_id) if batch.crawl_run_id else None
    # A terminal cancelled crawl stays cancelled; its new explicit AI demand
    # is represented by a separate batch, without rewriting crawl history.
    target = batch
    if run and (run.cancel_requested or run.status == CrawlRunStatus.CANCELLED):
        target = LLMBatch(label=f"Nova tentativa IA {run.id}", cancelled=False)
        session.add(target)
        await session.flush()
    else:
        batch.cancelled = False
    count = 0
    reconfigured_documents: set[UUID] = set()
    for work in works:
        if work.config_key != config_key(settings) and work.document_id in reconfigured_documents:
            work.status = "obsolete"
            continue
        doc = await session.get(Document, work.document_id)
        if doc:
            work.status, work.attempts, work.error, work.next_attempt_at = "pending", 0, None, None
            if target.id != batch.id:
                session.add(LLMDemand(batch_id=target.id, work_id=work.id))
            if work.config_key == config_key(settings):
                count += 1
            else:
                work.status = "obsolete"
                reconfigured_documents.add(work.document_id)
                count += await enqueue_document(session, doc, settings, batch=target, explicit=True)
    if state:
        state.blocked_at, state.error = None, None
    if not count:
        raise ValueError("Nenhuma análise de IA elegível na configuração atual")
    return count


async def queue_status(
    session: AsyncSession, settings: Settings, *, run_id: UUID | None = None
) -> dict[str, Any] | None:
    summaries = await queue_statuses(
        session, settings, run_ids=(run_id,) if run_id else (), include_global=run_id is None
    )
    return summaries[run_id]


async def queue_statuses(
    session: AsyncSession,
    settings: Settings,
    *,
    run_ids: Iterable[UUID] = (),
    include_global: bool = False,
) -> dict[UUID | None, dict[str, Any] | None]:
    from app.services.llm.status import queue_statuses as summarize

    return await summarize(session, settings, run_ids=run_ids, include_global=include_global)
