"""Conservative document retry scopes and a shared document-processing lease."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.database import write_session
from app.models import CrawlRun, CrawlRunStatus, Document, ExtractionStatus, JobLease, SourceRecord
from app.repositories.crawls import JobLeaseRepository

if TYPE_CHECKING:
    from app.services.ingestion.pipeline import PipelineRequest

DOCUMENT_LEASE = "documents:processing"


class DocumentProcessingBusy(RuntimeError):
    pass


@dataclass(frozen=True)
class _CorrectionAuthorization:
    key: str
    reason: str


def batch_document_ids(run: CrawlRun) -> list[UUID] | None:
    cursor = run.cursor if isinstance(run.cursor, dict) else {}
    sources = cursor.get("sources", {})
    entry = sources.get("documents", {}) if isinstance(sources, dict) else {}
    values = entry.get("document_ids") if isinstance(entry, dict) else None
    if not isinstance(values, list):
        return None
    identifiers = []
    for value in values:
        try:
            identifiers.append(UUID(str(value)))
        except ValueError:
            continue
    return identifiers


async def documents_for_run(session: AsyncSession, run: CrawlRun) -> list[Document]:
    from app.services.documents.identity import canonical_document_ids, noncanonical_document_ids

    identifiers = batch_document_ids(run)
    query = select(Document)
    if identifiers is not None:
        query = query.where(Document.id.in_(await canonical_document_ids(session, identifiers)))
    else:
        # Legacy runs lack a processing snapshot. Never widen this to all
        # failed documents: use only evidence currently linked to this run.
        query = query.join(SourceRecord, SourceRecord.id == Document.source_record_id).where(
            SourceRecord.crawl_run_id == run.id
        )
    query = query.where(Document.id.not_in(noncanonical_document_ids()))
    return list((await session.scalars(query.order_by(Document.created_at, Document.id))).all())


def eligible(document: Document, *, now: datetime, max_attempts: int) -> bool:
    due = document.next_attempt_at
    if due is not None and due.tzinfo is None:
        due = due.replace(tzinfo=UTC)
    return bool(
        document.is_active
        and document.parent_document_id is None
        and document.extraction_status is ExtractionStatus.FAILED
        and document.extraction_attempts < max_attempts
        and (due is None or due <= now)
    )


async def document_processing_busy(session: AsyncSession, *, now: datetime) -> bool:
    if await session.scalar(
        select(JobLease.name).where(
            JobLease.name == DOCUMENT_LEASE,
            JobLease.expires_at > now,
        )
    ):
        return True
    runs = await session.scalars(
        select(CrawlRun).where(
            CrawlRun.status.in_([CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING])
        )
    )
    return any(
        run.connector in {"pncp", "compras_gov", "all"}
        and isinstance(run.filters, dict)
        and (
            run.filters.get("mode") == "documents"
            or run.filters.get("mode", "procurements") == "procurements"
            and run.filters.get("process_documents", True)
        )
        for run in runs
    )


async def create_document_retry(
    session: AsyncSession,
    run_id: UUID,
    settings: Settings,
    *,
    document_ids: tuple[UUID, ...] | None = None,
    _correction: _CorrectionAuthorization | None = None,
) -> tuple[CrawlRun, PipelineRequest]:
    from app.services.ingestion.pipeline import PipelineRequest

    now = datetime.now(UTC)
    await JobLeaseRepository(session).assert_writes_allowed(now=now)
    if _correction:
        if not document_ids or not _correction.key or not _correction.reason.strip():
            raise ValueError(
                "A correção exige documentos explícitos, identificação e justificativa"
            )
        existing = await session.scalar(
            select(CrawlRun.id)
            .where(
                CrawlRun.filters["_document_correction"]["key"].as_string() == _correction.key,
                CrawlRun.filters["retry_of_run_id"].as_string() == str(run_id),
            )
            .limit(1)
        )
        if existing:
            raise DocumentProcessingBusy("Esta tentativa corretiva já foi registrada")
    parent = await session.get(CrawlRun, run_id)
    if parent is None:
        raise LookupError("Coleta não encontrada")
    if parent.status in {
        CrawlRunStatus.PENDING,
        CrawlRunStatus.RUNNING,
    } or await document_processing_busy(session, now=now):
        raise DocumentProcessingBusy(
            "Há uma coleta/processamento de documentos em andamento. Aguarde sua conclusão."
        )
    documents = await documents_for_run(session, parent)
    selected = set(document_ids) if document_ids is not None else None
    if selected is not None and not selected.issubset({document.id for document in documents}):
        raise ValueError("O documento selecionado não pertence ao lote original")
    identifiers = tuple(
        str(document.id)
        for document in documents
        if selected is None or document.id in selected
        if eligible(document, now=now, max_attempts=settings.document_max_attempts)
        or bool(
            _correction
            and document.is_active
            and document.parent_document_id is None
            and document.extraction_status == ExtractionStatus.FAILED
            and document.extraction_attempts < settings.document_max_attempts
        )
    )
    if not identifiers:
        raise ValueError(
            "Nenhum documento com falha está elegível agora. Confira o prazo de nova tentativa, o limite de falhas e a situação dos arquivos."
        )
    filters = parent.filters if isinstance(parent.filters, dict) else {}
    request = PipelineRequest(
        connector=parent.connector if parent.connector in {"all", "pncp", "compras_gov"} else "all",
        mode="documents",
        uf=str(filters.get("uf") or settings.default_uf),
        document_ids=identifiers,
        document_batch_size=len(identifiers),
        retry_of_run_id=str(parent.id),
    )
    child = CrawlRun(
        connector=request.connector,
        status=CrawlRunStatus.PENDING,
        filters=request.as_json(),
        cursor={
            "progress": {
                "stage": "queued",
                "message": "Aguardando reprocessamento seletivo de documentos",
                "updated_at": now.isoformat(),
            }
        },
    )
    if _correction:
        child.filters = {
            **child.filters,
            "_document_correction": {
                "key": _correction.key,
                "reason": _correction.reason[:1000],
                "created_at": now.isoformat(),
                "attempts_before": {
                    str(d.id): d.extraction_attempts for d in documents if str(d.id) in identifiers
                },
                "retry_after_before": {
                    str(d.id): d.next_attempt_at.isoformat() if d.next_attempt_at else None
                    for d in documents
                    if str(d.id) in identifiers
                },
            },
        }
    session.add(child)
    await session.flush()
    return child, request


async def create_document_correction(
    session: AsyncSession,
    run_id: UUID,
    settings: Settings,
    *,
    document_ids: tuple[UUID, ...],
    key: str,
    reason: str,
) -> tuple[CrawlRun, PipelineRequest]:
    """Internal, auditable one-off correction; normal REST/CLI retries cannot request it."""
    return await create_document_retry(
        session,
        run_id,
        settings,
        document_ids=document_ids,
        _correction=_CorrectionAuthorization(key, reason),
    )


async def corrective_scope(session: AsyncSession, run_id: UUID | None) -> dict[UUID, int]:
    run = await session.get(CrawlRun, run_id) if run_id else None
    if (
        run is None
        or run.cancel_requested
        or run.status not in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}
    ):
        return {}
    filters = run.filters if isinstance(run.filters, dict) else {}
    correction = filters.get("_document_correction")
    if (
        not isinstance(correction, dict)
        or filters.get("mode") != "documents"
        or not filters.get("retry_of_run_id")
    ):
        return {}
    claims = (run.cursor or {}).get("document_correction_claims", {})
    result = {}
    for identifier, attempts in correction.get("attempts_before", {}).items():
        if (
            identifier in filters.get("document_ids", [])
            and identifier not in claims
            and isinstance(attempts, int)
            and not isinstance(attempts, bool)
            and attempts >= 0
        ):
            result[UUID(identifier)] = attempts
    return result


async def claim_corrective_attempt(
    session: AsyncSession, run_id: UUID | None, document: Document
) -> bool:
    scope = await corrective_scope(session, run_id)
    if (
        not document.is_active
        or document.extraction_status != ExtractionStatus.FAILED
        or scope.get(document.id) != document.extraction_attempts
    ):
        return False
    run = await session.get(CrawlRun, run_id)
    assert run is not None
    now = datetime.now(UTC)
    cursor = dict(run.cursor or {})
    cursor["document_correction_claims"] = {
        **cursor.get("document_correction_claims", {}),
        str(document.id): {
            "at": now.isoformat(),
            "attempts_before": document.extraction_attempts,
            "wait_bypassed": document.next_attempt_at is not None
            and document.next_attempt_at > now,
        },
    }
    run.cursor = cursor
    return True


@asynccontextmanager
async def document_processing_lease(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[Callable[[AsyncSession], Awaitable[None]]]:
    owner, generation = f"documents:{uuid4()}", None
    heartbeat = None
    worker = asyncio.current_task()

    async def fence(session: AsyncSession) -> None:
        if generation is None or not await JobLeaseRepository(session).fence(
            name=DOCUMENT_LEASE,
            owner_id=owner,
            generation=generation,
            now=datetime.now(UTC),
        ):
            raise asyncio.CancelledError("lease de documentos perdida")

    async def renew() -> None:
        try:
            while True:
                await asyncio.sleep(30)
                now = datetime.now(UTC)
                async with write_session(factory) as session:
                    valid = await JobLeaseRepository(session).renew(
                        name=DOCUMENT_LEASE,
                        owner_id=owner,
                        generation=generation,
                        now=now,
                        expires_at=now + timedelta(hours=2),
                    )
                if not valid:
                    if worker is not None:
                        worker.cancel()
                    return
        except Exception:
            if worker is not None:
                worker.cancel()
            raise

    try:
        now = datetime.now(UTC)
        async with write_session(factory) as session:
            repo = JobLeaseRepository(session)
            await repo.lock_acquisitions()
            if await document_processing_busy(session, now=now) or not await repo.acquire(
                name=DOCUMENT_LEASE,
                owner_id=owner,
                now=now,
                expires_at=now + timedelta(hours=2),
            ):
                raise DocumentProcessingBusy(
                    "Já existe processamento de documentos em andamento. Aguarde sua conclusão."
                )
            lease = await session.get(JobLease, DOCUMENT_LEASE)
            assert lease is not None
            generation = lease.version
        heartbeat = asyncio.create_task(renew(), name=f"document-heartbeat:{owner}")
        yield fence
    finally:
        if heartbeat is not None:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await heartbeat

        async def release() -> None:
            async with write_session(factory) as session:
                await JobLeaseRepository(session).release(
                    name=DOCUMENT_LEASE, owner_id=owner, generation=generation
                )

        cleanup = asyncio.create_task(release())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
            raise
