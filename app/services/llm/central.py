"""Bounded reads and guarded actions shared by the AI HTML and API surfaces."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import exists, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer
from sqlalchemy.sql.elements import ColumnElement

from app.api.serialization import model_dict
from app.config import Settings
from app.models import (
    Company,
    Document,
    DocumentChunk,
    Evidence,
    ExtractionStatus,
    FieldObservation,
    LLMAttempt,
    LLMBatch,
    LLMDemand,
    LLMProviderState,
    LLMResult,
    LLMWorkItem,
    Participant,
    Procurement,
)
from app.models.llm_review import LLMEventCitation, LLMSuggestion
from app.repositories.crawls import JobLeaseRepository
from app.repositories.current_evidence import evidence_current, inactive_document_ids
from app.services.identifiers import normalize_cnpj
from app.services.llm.queue import (
    cancel_batch,
    document_snapshot,
    enqueue_document,
    queue_status,
    retry_batch,
)
from app.services.participation_integrity import participant_current
from app.services.scheduler_control import control_state

PAGE_SIZE = 20
DOC_FIELDS = {"id", "procurement_id", "title", "document_type", "extraction_status", "is_active"}


def _filters(filters: dict[str, str], *, suggestions: bool = False) -> list[Any]:
    clauses: list[Any] = []
    if filters.get("procurement"):
        clauses.append(Document.procurement_id == UUID(filters["procurement"]))
    if filters.get("source"):
        from app.models import SourceRecord

        clauses.append(
            or_(
                exists(
                    select(SourceRecord.id).where(
                        SourceRecord.id == Document.source_record_id,
                        SourceRecord.source == filters["source"],
                    )
                ),
                (Document.source_record_id.is_(None)) & (Procurement.source == filters["source"]),
            )
        )
    if filters.get("company"):
        cnpj = normalize_cnpj(filters["company"])
        related: ColumnElement[bool] = exists(
            select(Participant.id)
            .join(Company, Company.id == Participant.company_id)
            .where(
                Participant.procurement_id == Document.procurement_id,
                Company.cnpj == cnpj,
                participant_current(),
            )
        )
        if suggestions:
            related = or_(
                LLMSuggestion.company_id.in_(select(Company.id).where(Company.cnpj == cnpj)),
                LLMSuggestion.analysis["company_cnpj"].as_string() == cnpj,
            )
        clauses.append(related)
    if filters.get("run"):
        clauses.append(
            Document.id.in_(
                select(LLMWorkItem.document_id)
                .join(LLMDemand, LLMDemand.work_id == LLMWorkItem.id)
                .join(LLMBatch, LLMBatch.id == LLMDemand.batch_id)
                .where(LLMBatch.crawl_run_id == UUID(filters["run"]))
            )
        )
    return clauses


async def overview(db: AsyncSession, settings: Settings) -> dict[str, Any]:
    from app.services.documents.identity import noncanonical_document_ids

    summary = await queue_status(db, settings)
    tokens = (
        await db.execute(
            select(
                func.sum(LLMAttempt.input_tokens),
                func.sum(LLMAttempt.output_tokens),
                func.count(LLMAttempt.id),
            )
        )
    ).one()
    extracted = await db.scalar(
        select(func.count())
        .select_from(Document)
        .where(
            Document.extraction_status == ExtractionStatus.EXTRACTED,
            Document.id.not_in(noncanonical_document_ids()),
            Document.id.not_in(select(inactive_document_ids().c.id)),
            or_(Document.mime_type.is_(None), Document.mime_type != "application/zip"),
        )
    )
    suggestions = dict(
        (
            await db.execute(
                select(LLMSuggestion.status, func.count()).group_by(LLMSuggestion.status)
            )
        ).all()
    )
    return {
        "queue": summary,
        "controls": control_state(settings),
        "extracted_documents": extracted or 0,
        "suggestions": suggestions,
        "input_tokens": tokens[0] or 0,
        "output_tokens": tokens[1] or 0,
        "connection": "tentativa registrada" if tokens[2] else "não verificada",
        "configuration": {
            "enabled": settings.llm_enabled,
            "provider": settings.llm_provider,
            "model": settings.llm_model,
            "key_configured": bool(settings.llm_api_key),
            "concurrency": 1,
            "max_completion_tokens": settings.llm_max_output_tokens,
            "max_chunks": settings.llm_max_chunks_per_document,
            "requests_per_hour": settings.llm_max_requests_per_hour,
            "input_price": str(settings.llm_input_price_per_million_usd),
            "output_price": str(settings.llm_output_price_per_million_usd),
        },
    }


async def documents(db: AsyncSession, filters: dict[str, str], page: int) -> dict[str, Any]:
    clauses = _filters(filters)
    clauses.append(
        or_(
            Document.extraction_status == ExtractionStatus.EXTRACTED,
            Document.id.in_(select(LLMWorkItem.document_id)),
        )
    )
    if filters.get("status"):
        requested = filters["status"]
        state = LLMWorkItem.status == requested
        if requested == "blocked":
            state = (LLMWorkItem.status == "pending") & exists(
                select(LLMProviderState.id).where(
                    LLMProviderState.config_key == LLMWorkItem.config_key,
                    LLMProviderState.blocked_at.is_not(None),
                )
            )
        elif requested == "reused":
            state = or_(
                LLMWorkItem.reused.is_(True),
                exists(
                    select(LLMDemand.id).where(
                        LLMDemand.work_id == LLMWorkItem.id, LLMDemand.reused.is_(True)
                    )
                ),
            )
        clauses.append(Document.id.in_(select(LLMWorkItem.document_id).where(state)))
    base = select(Document).join(Procurement).where(*clauses)
    total = await db.scalar(select(func.count()).select_from(base.subquery())) or 0
    docs = list(
        (
            await db.scalars(
                base.options(defer(Document.extracted_text))
                .order_by(Document.created_at.desc(), Document.id)
                .offset((page - 1) * PAGE_SIZE)
                .limit(PAGE_SIZE)
            )
        ).all()
    )
    ids = [d.id for d in docs]
    counts: dict[UUID, dict[str, int]] = {id_: {} for id_ in ids}
    for doc_id, status, count in await db.execute(
        select(LLMWorkItem.document_id, LLMWorkItem.status, func.count())
        .where(LLMWorkItem.document_id.in_(ids))
        .group_by(LLMWorkItem.document_id, LLMWorkItem.status)
    ):
        counts[doc_id][status] = count
    return {
        "items": [{**model_dict(d, include=DOC_FIELDS), "analyses": counts[d.id]} for d in docs],
        "total": total,
        "page": page,
        "page_size": PAGE_SIZE,
    }


async def document_detail(db: AsyncSession, identifier: UUID, page: int) -> dict[str, Any]:
    doc = await db.get(Document, identifier)
    if doc is None:
        raise LookupError("Documento não encontrado")
    works = list(
        (
            await db.scalars(
                select(LLMWorkItem)
                .where(LLMWorkItem.document_id == identifier)
                .order_by(LLMWorkItem.created_at.desc(), LLMWorkItem.id)
                .offset((page - 1) * PAGE_SIZE)
                .limit(PAGE_SIZE)
            )
        ).all()
    )
    total = (
        await db.scalar(
            select(func.count())
            .select_from(LLMWorkItem)
            .where(LLMWorkItem.document_id == identifier)
        )
        or 0
    )
    return {
        "document": model_dict(doc, include=DOC_FIELDS),
        "eligible": await document_snapshot(db, doc) is not None,
        "items": [
            model_dict(
                w,
                include={
                    "id",
                    "status",
                    "attempts",
                    "error",
                    "coverage",
                    "completed_at",
                    "reused",
                    "next_attempt_at",
                },
            )
            for w in works
        ],
        "total": total,
        "page": page,
        "page_size": PAGE_SIZE,
    }


async def job_detail(db: AsyncSession, identifier: UUID) -> dict[str, Any]:
    work = await db.get(LLMWorkItem, identifier)
    if work is None:
        raise LookupError("Análise não encontrada")
    result = await db.get(LLMResult, work.result_id) if work.result_id else None
    attempts = list(
        (
            await db.scalars(
                select(LLMAttempt)
                .where(LLMAttempt.work_id == identifier)
                .order_by(LLMAttempt.started_at.desc())
                .limit(20)
            )
        ).all()
    )
    evidence = list(
        (
            await db.scalars(
                select(Evidence)
                .where(
                    Evidence.document_id == work.document_id,
                    Evidence.document_chunk_id == work.chunk_id,
                    evidence_current(Evidence.id),
                )
                .order_by(Evidence.collected_at.desc())
                .limit(20)
            )
        ).all()
    )
    batch_ids = list(
        (
            await db.scalars(
                select(LLMDemand.batch_id).where(LLMDemand.work_id == identifier).limit(20)
            )
        ).all()
    )
    return {
        "work": model_dict(
            work,
            include={
                "id",
                "document_id",
                "chunk_id",
                "status",
                "attempts",
                "coverage",
                "error",
                "reused",
                "created_at",
                "completed_at",
                "next_attempt_at",
            },
        ),
        "analyses": result.analyses if result else None,
        "attempts": [
            {
                **model_dict(
                    a,
                    include={
                        "started_at",
                        "finished_at",
                        "model",
                        "status",
                        "diagnostic",
                        "input_tokens",
                        "output_tokens",
                    },
                ),
                "input_price": str(a.input_price),
                "output_price": str(a.output_price),
                "cost_usd": str(a.cost_usd) if a.cost_usd is not None else None,
            }
            for a in attempts
        ],
        "evidence": [
            model_dict(e, include={"id", "page_number", "locator", "text_excerpt"})
            for e in evidence
        ],
        "batch_ids": [str(b) for b in batch_ids],
    }


async def suggestions(db: AsyncSession, filters: dict[str, str], page: int) -> dict[str, Any]:
    clauses = _filters(filters, suggestions=True)
    if filters.get("status"):
        clauses.append(LLMSuggestion.status == filters["status"])
    base = (
        select(LLMSuggestion)
        .join(Document, Document.id == LLMSuggestion.document_id)
        .join(Procurement, Procurement.id == Document.procurement_id)
        .where(*clauses)
    )
    total = await db.scalar(select(func.count()).select_from(base.subquery())) or 0
    rows = list(
        (
            await db.scalars(
                base.order_by(LLMSuggestion.created_at.desc(), LLMSuggestion.id)
                .offset((page - 1) * PAGE_SIZE)
                .limit(PAGE_SIZE)
            )
        ).all()
    )
    return {
        "items": [
            model_dict(
                r,
                include={
                    "id",
                    "procurement_id",
                    "document_id",
                    "company_id",
                    "analysis",
                    "status",
                    "reason",
                    "reviewer",
                    "note",
                    "created_at",
                    "reviewed_at",
                },
            )
            for r in rows
        ],
        "total": total,
        "page": page,
        "page_size": PAGE_SIZE,
    }


async def suggestion_detail(db: AsyncSession, identifier: UUID) -> dict[str, Any]:
    row = await db.get(LLMSuggestion, identifier)
    if row is None:
        raise LookupError("Sugestão não encontrada")
    proof = list(
        (
            await db.scalars(
                select(Evidence).where(Evidence.id.in_([UUID(e) for e in row.evidence_ids]))
            )
        ).all()
    )
    parties = list(
        (
            await db.scalars(
                select(Company)
                .join(Participant)
                .where(Participant.procurement_id == row.procurement_id, participant_current())
                .distinct()
                .order_by(Company.cnpj)
                .limit(200)
            )
        ).all()
    )
    doc = await db.get(Document, row.document_id)
    current = bool(doc and row.snapshot and await document_snapshot(db, doc) == row.snapshot)
    return {
        "suggestion": model_dict(row),
        "current": current,
        "evidence": [
            model_dict(e, include={"id", "page_number", "locator", "text_excerpt"}) for e in proof
        ],
        "companies": [model_dict(c, include={"id", "cnpj", "legal_name"}) for c in parties],
    }


async def enqueue(db: AsyncSession, identifier: UUID, settings: Settings) -> int:
    await JobLeaseRepository(db).assert_writes_allowed(now=datetime.now(UTC))
    doc = await db.get(Document, identifier)
    if doc is None:
        raise LookupError("Documento não encontrado")
    batch = await db.scalar(
        select(LLMBatch)
        .where(
            LLMBatch.label == "Central de IA",
            LLMBatch.cancelled.is_(False),
            LLMBatch.crawl_run_id.is_(None),
        )
        .order_by(LLMBatch.created_at.desc())
        .limit(1)
    )
    if batch is None:
        batch = LLMBatch(label="Central de IA", cancelled=False)
        db.add(batch)
        await db.flush()
    count = await enqueue_document(
        db, doc, settings, batch=batch, explicit=True, count_reactivated=True
    )
    if not count:
        raise ValueError(
            "Nenhuma análise elegível; confira atividade, extração, configuração e trabalhos existentes"
        )
    return count


async def batch_action(db: AsyncSession, identifier: UUID, action: str, settings: Settings) -> int:
    await JobLeaseRepository(db).assert_writes_allowed(now=datetime.now(UTC))
    batch = await db.get(LLMBatch, identifier)
    if batch is None:
        raise LookupError("Lote não encontrado")
    if action == "retry":
        return await retry_batch(db, identifier, settings)
    if action != "cancel":
        raise ValueError("Ação desconhecida")
    pending = await db.scalar(
        select(LLMWorkItem.id)
        .join(LLMDemand)
        .where(LLMDemand.batch_id == identifier, LLMWorkItem.status.in_(["pending", "running"]))
        .limit(1)
    )
    if batch.cancelled or pending is None:
        raise ValueError("Não há trabalho pendente neste lote")
    await cancel_batch(db, batch)
    return 1


async def review_suggestion(
    db: AsyncSession,
    identifier: UUID,
    *,
    confirm: bool,
    company_id: UUID | None,
    reviewer: str | None,
    note: str | None,
    settings: Settings,
) -> None:
    await JobLeaseRepository(db).assert_writes_allowed(now=datetime.now(UTC))
    row = await db.scalar(
        select(LLMSuggestion).where(LLMSuggestion.id == identifier).with_for_update()
    )
    if row is None:
        raise LookupError("Sugestão não encontrada")
    if row.status != "pending":
        raise ValueError("Esta sugestão já foi revisada")
    if confirm:
        detail = await suggestion_detail(db, identifier)
        if not detail["current"]:
            raise ValueError("A versão ou atividade do documento mudou; a sugestão está obsoleta")
        if not company_id or not await db.scalar(
            select(Participant.id)
            .where(
                Participant.company_id == company_id,
                Participant.procurement_id == row.procurement_id,
                participant_current(),
            )
            .limit(1)
        ):
            raise ValueError("Escolha uma participante comprovada desta contratação")
        from app.services.event_detection.detector import EventDetector
        from app.services.event_detection.types import DetectedEvent, DocumentEvidence
        from app.services.ingestion.processor import DocumentProcessingService
        from app.services.llm.analyzer import LLMEventAnalysis

        analysis = LLMEventAnalysis.model_validate(row.analysis)
        doc = await db.get(Document, row.document_id)
        proc = await db.get(Procurement, row.procurement_id)
        if not doc or not proc:
            raise ValueError("Evidência indisponível")

        proofs = list(
            (
                await db.scalars(
                    select(Evidence).where(
                        Evidence.id.in_([UUID(v) for v in row.evidence_ids]),
                        Evidence.document_id == doc.id,
                        Evidence.procurement_id == proc.id,
                        evidence_current(Evidence.id),
                    )
                )
            ).all()
        )
        if len(proofs) != len(set(row.evidence_ids)):
            raise ValueError("Evidência indisponível ou inativa")
        detector = EventDetector()
        proof = next(
            (
                p
                for p in proofs
                if any(
                    event.event_type == analysis.event_type
                    for event in detector.detect(
                        DocumentEvidence(
                            text=p.text_excerpt,
                            document_type=doc.document_type,
                            document_title=doc.title,
                        )
                    )
                )
            ),
            None,
        )
        if proof is None:
            raise ValueError("Nenhuma citação comprova o ato sugerido")
        if proof.document_chunk_id is None:
            raise ValueError("Evidência indisponível")
        chunk = await db.get(DocumentChunk, proof.document_chunk_id)
        if chunk is None or chunk.document_id != doc.id:
            raise ValueError("Evidência indisponível")
        detected = DetectedEvent(
            event_type=analysis.event_type,
            raw_description=proof.text_excerpt or "",
            normalized_reason=analysis.reason_summary,
            reason_category=analysis.reason_category,
            evidence=DocumentEvidence(
                text=proof.text_excerpt or "",
                source_url=proof.source_url,
                page_number=proof.page_number,
                locator=proof.locator,
                document_type=doc.document_type,
                document_title=doc.title,
                start_offset=proof.start_offset,
                end_offset=proof.end_offset,
            ),
            confidence=min(analysis.confidence, 0.9),
            requires_manual_review=True,
            item_number=analysis.item_number,
            occurred_at=analysis.event_date,
        )
        service = DocumentProcessingService(settings)
        try:
            from app.models import ProcurementEvent

            event = await db.get(ProcurementEvent, row.event_id) if row.event_id else None
            if event is None:
                event, _ = await service._persist_event(
                    db, proc, doc, chunk, detected, source="llm"
                )
            elif (
                event.procurement_id != row.procurement_id
                or str(event.evidence_id) not in row.evidence_ids
            ):
                raise ValueError("O evento não corresponde às evidências da sugestão")
            if event.company_id is not None and event.company_id != company_id:
                raise ValueError("O evento já possui outra atribuição")
            await service.link_event_company(
                event_id=event.id, company_id=company_id, reviewer=reviewer, note=note, session=db
            )
            row.event_id, row.company_id = event.id, company_id
            for eid in row.evidence_ids:
                if not await db.scalar(
                    select(LLMEventCitation.id).where(
                        LLMEventCitation.event_id == event.id,
                        LLMEventCitation.evidence_id == UUID(eid),
                    )
                ):
                    db.add(LLMEventCitation(event_id=event.id, evidence_id=UUID(eid)))
            from app.services.procurement_phase import refresh_procurement_phase

            await refresh_procurement_phase(db, proc)
        finally:
            await service.aclose()
        row.status = "confirmed"
    else:
        row.status = "rejected"
    row.reviewer, row.note, row.reviewed_at = reviewer, note, datetime.now(UTC)
    db.add(
        FieldObservation(
            entity_type="llm_suggestion",
            entity_id=row.id,
            field_name="review",
            value={
                "status": row.status,
                "company_id": str(row.company_id) if row.company_id else None,
                "reviewer": reviewer,
                "note": note,
            },
            source="manual",
            fingerprint=f"llm-review:{row.id}",
        )
    )
