"""Bounded, restartable repairs based exclusively on retained official material."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.connectors.pncp import parse_pncp_external_id
from app.database import write_session
from app.models import (
    Company,
    Document,
    DocumentAlias,
    DocumentChunk,
    Evidence,
    FactVerification,
    FieldObservation,
    Participant,
    Procurement,
    ProcurementEvent,
    ProcurementItem,
    ReconciliationRun,
    SourceRecord,
)
from app.services.backups import BackupReceipt
from app.services.documents.identity import official_document_key, reconcile_aliases
from app.services.documents.pncp_urls import _resource
from app.services.documents.security import (
    ArchiveBudget,
    ArchiveLimits,
    UnsafeArchiveError,
    read_safe_zip,
)
from app.services.event_detection import DocumentEvidence, ParticipantDetector
from app.services.event_detection.participation_values import (
    VERSION,
    extract_values,
    reference_status,
)
from app.services.identifiers import normalize_company_name
from app.services.maintenance import fence_maintenance, run_blocking_maintenance
from app.services.participation_integrity import (
    audit_change,
    is_procurement_buyer,
    persist_values,
    review_event_item_scope,
    verify_participant_reference,
)


@dataclass
class IntegrityReport:
    totals: dict[str, int] = field(default_factory=dict)
    diagnostics: list[str] = field(default_factory=list)

    def add(self, key: str, value: int = 1) -> None:
        self.totals[key] = self.totals.get(key, 0) + value

    def as_dict(self) -> dict[str, Any]:
        return {**self.totals, "diagnostics": self.diagnostics}


async def retained_pages(
    db: AsyncSession, document: Document, settings: Settings, cache: dict
) -> tuple[list[DocumentEvidence], str | None]:
    if not document.extracted_text or document.mime_type == "application/zip":
        return [], "sem extração final armazenada"
    current: Document | None = document
    root_document = document
    visited = set()
    member_path: list[str] = []
    while current:
        if current.id in visited or not current.is_active:
            return [], "documento ou ancestral sem vigência"
        visited.add(current.id)
        root_document = current
        if current.parent_document_id:
            if not current.title:
                return [], "nome do membro arquivado ausente"
            member_path.insert(0, current.title)
        current = (
            await db.get(Document, current.parent_document_id)
            if current.parent_document_id
            else None
        )
    if await db.scalar(
        select(DocumentAlias.id)
        .where(DocumentAlias.document_id.in_(visited), DocumentAlias.proof == "previous_content")
        .limit(1)
    ):
        return [], "versão documental histórica"
    source = (
        await db.get(SourceRecord, root_document.source_record_id)
        if root_document.source_record_id
        else None
    )
    if source is None or source.raw_payload is None:
        return [], "registro oficial de origem ausente"
    if source.source == "pncp":
        resource = _resource(root_document.original_url)
        procurement = await db.get(Procurement, document.procurement_id)
        try:
            reference = parse_pncp_external_id(
                (procurement.pncp_control_number or procurement.external_id) if procurement else ""
            )
        except ValueError:
            return [], "identidade oficial da contratação não comprovada"
        if resource is None or resource[:3] != (
            reference.cnpj,
            str(reference.year),
            str(reference.sequence),
        ):
            return [], "arquivo e contratação têm identificadores oficiais divergentes"
        payload = source.raw_payload
        rows = (
            payload
            if isinstance(payload, list)
            else next(
                (
                    payload[k]
                    for k in ("data", "documentos", "itens")
                    if isinstance(payload, dict) and isinstance(payload.get(k), list)
                ),
                [],
            )
        )
        if not any(
            isinstance(row, dict)
            and official_document_key(str(row.get("url") or row.get("uri") or ""), source)
            == "/".join(resource)
            for row in rows
        ):
            return [], "arquivo sem correspondência no catálogo oficial armazenado"
    elif source.source == "compras_gov":

        def contains_url(value):
            if isinstance(value, str):
                return value == root_document.original_url
            if isinstance(value, list):
                return any(contains_url(v) for v in value)
            if isinstance(value, dict):
                return any(contains_url(v) for v in value.values())
            return False

        if not contains_url(source.raw_payload):
            return [], "URL documental sem correspondência na resposta armazenada"
    else:
        return [], "fonte documental sem identidade verificável neste reparo"
    if not document.file_hash or not root_document.local_path or not root_document.file_hash:
        return [], "conteúdo bruto verificável ausente"
    root = settings.document_storage_path.resolve()
    path = Path(root_document.local_path).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        return [], "arquivo bruto fora do armazenamento ou ausente"
    stat = path.stat()
    key = (str(path), stat.st_size, stat.st_mtime_ns, bool(member_path))
    if key not in cache:

        def digest():
            if stat.st_size > settings.document_max_bytes:
                raise UnsafeArchiveError("arquivo excede limite")
            if member_path:
                limits = ArchiveLimits(
                    max_members=settings.archive_max_members,
                    max_uncompressed_bytes=settings.archive_max_bytes,
                    max_member_bytes=settings.document_max_bytes,
                )
                budget = ArchiveBudget(limits)
                hashes = {}

                def visit(body, names, depth):
                    hashes[names] = hashlib.sha256(body).hexdigest()
                    if body.startswith(b"PK"):
                        from app.services.documents.security import safe_archive_name

                        office = names and str(names[-1]).lower().endswith(
                            (".docx", ".xlsx", ".pptx")
                        )
                        if not office and depth >= settings.archive_max_depth:
                            raise UnsafeArchiveError("profundidade de arquivo excedida")
                        members = read_safe_zip(body, limits, budget=budget)
                        for name, data in members:
                            safe_archive_name(name)
                            if not office:
                                visit(data, (*names, name), depth + 1)

                visit(path.read_bytes(), (), 0)
                return hashes
            with path.open("rb") as stream:
                return {(): hashlib.file_digest(stream, "sha256").hexdigest()}

        try:
            cache[key] = await run_blocking_maintenance(digest)
        except (UnsafeArchiveError, OSError):
            cache[key] = {}
    if (
        cache[key].get(()) != root_document.file_hash
        or cache[key].get(tuple(member_path)) != document.file_hash
    ):
        return [], "hash do arquivo divergente; nenhum fato reparado"
    chunks = (
        await db.scalars(
            select(DocumentChunk)
            .where(DocumentChunk.document_id == document.id)
            .order_by(DocumentChunk.sequence)
        )
    ).all()
    if not chunks:
        return [], "trechos localizáveis ausentes"
    pages = [
        DocumentEvidence(
            c.text,
            source_url=document.resolved_url or document.original_url,
            page_number=c.page_number,
            locator=c.locator,
            document_type=document.document_type,
            published_at=document.published_at,
            start_offset=c.start_offset,
            end_offset=c.end_offset,
        )
        for c in chunks
    ]
    return pages, None


async def inspect_event_item_scopes(
    db: AsyncSession,
    procurement_id: UUID,
    verified_document_ids: set[UUID],
    *,
    apply: bool,
    report: IntegrityReport,
) -> None:
    events = (
        await db.scalars(
            select(ProcurementEvent).where(
                ProcurementEvent.procurement_id == procurement_id,
                ProcurementEvent.source.in_(["document", "llm"]),
                ProcurementEvent.item_id.is_not(None),
            )
        )
    ).all()
    for event in events:
        item = await db.get(ProcurementItem, event.item_id)
        evidence = await db.get(Evidence, event.evidence_id) if event.evidence_id else None
        description = (
            normalize_company_name(item.description, strip_legal_suffix=False) if item else None
        )
        published = (
            normalize_company_name(evidence.text_excerpt, strip_legal_suffix=False)
            if evidence
            else ""
        )
        if description and len(description) >= 8 and description in (published or ""):
            continue
        report.add("unsupported_event_item_links")
        report.diagnostics.append(
            f"evento {event.id}: escopo de item sem correspondência oficial comprovada"
        )
        if apply:
            manual = await review_event_item_scope(
                db,
                event,
                material_verified=bool(evidence and evidence.document_id in verified_document_ids),
            )
            report.add("manual_event_item_links_preserved", int(manual))


async def inspect_procurement(
    db: AsyncSession,
    procurement_id: UUID,
    settings: Settings,
    *,
    apply: bool,
    cache: dict,
    report: IntegrityReport,
) -> None:
    procurement = await db.get(Procurement, procurement_id)
    documents = list(
        (
            await db.scalars(
                select(Document)
                .where(Document.procurement_id == procurement_id)
                .order_by(Document.created_at, Document.id)
            )
        ).all()
    )
    aliases = await reconcile_aliases(db, documents, apply=apply)
    for key, count in aliases.items():
        report.add(key, count)
    pages_by_id = {}
    for document in documents:
        pages, error = await retained_pages(db, document, settings, cache)
        if pages:
            pages_by_id[document.id] = pages
            report.add("verified_documents")
        elif document.extracted_text:
            report.add("documents_requiring_review")
            report.diagnostics.append(f"documento {document.id}: {error}")
        elif str(document.extraction_status) in {"pending", "failed", "ocr_required"}:
            report.add("documents_pending_extraction")
            report.diagnostics.append(
                f"documento {document.id}: extração pendente ({document.extraction_status})"
            )
    participants = list(
        (
            await db.scalars(
                select(Participant).where(
                    Participant.procurement_id == procurement_id, Participant.source == "document"
                )
            )
        ).all()
    )
    for participant in participants:
        company = await db.get(Company, participant.company_id)
        evidence = (
            await db.get(Evidence, participant.source_evidence_id)
            if participant.source_evidence_id
            else None
        )
        pages = (
            pages_by_id.get(evidence.document_id, []) if evidence and evidence.document_id else []
        )
        text = "\n".join(p.text for p in pages) if pages else ""
        status = (
            reference_status(text, company.cnpj, pages[0].document_type)
            if company and company.cnpj and pages
            else "review"
        )
        if company and is_procurement_buyer(company.cnpj, procurement):
            status = "review"
        existing = await db.scalar(
            select(FactVerification).where(
                FactVerification.entity_type == "participant",
                FactVerification.entity_id == participant.id,
            )
        )
        report.add("inspected_participants")
        if not existing or existing.status != status:
            report.add("participation_verifications_to_change")
        report.add("participants_" + status)
        if apply:
            await verify_participant_reference(
                db, participant, text, pages[0].document_type if pages else None
            )
        if participant.item_id is not None and evidence:
            item = await db.get(ProcurementItem, participant.item_id)
            description = (
                normalize_company_name(item.description, strip_legal_suffix=False) if item else None
            )
            published = (
                normalize_company_name(evidence.text_excerpt, strip_legal_suffix=False) or ""
            )
            if not description or len(description) < 8 or description not in published:
                report.add("unsupported_item_links")
                if apply:
                    manual_item = await db.scalar(
                        select(FieldObservation.id)
                        .where(
                            FieldObservation.entity_type == "participant",
                            FieldObservation.entity_id == participant.id,
                            FieldObservation.field_name == "item_id",
                            FieldObservation.source.in_(["manual", "manual_operator"]),
                        )
                        .limit(1)
                    )
                    if manual_item:
                        report.add("manual_item_links_preserved")
                        report.diagnostics.append(
                            f"participante {participant.id}: vínculo manual de item requer revisão"
                        )
                        continue
                    await audit_change(
                        db,
                        entity_type="participant",
                        entity_id=participant.id,
                        procurement_id=procurement_id,
                        field="item_id",
                        previous=participant.item_id,
                        received=None,
                        evidence_id=evidence.id,
                    )
                    participant.item_id = None
    await inspect_event_item_scopes(
        db, procurement_id, set(pages_by_id), apply=apply, report=report
    )
    # Restore genuine documentary bidders missed by the old token-window rule,
    # without generating events, deadlines, leads, contacts or LLM demands.
    detector = ParticipantDetector()
    service = None
    seen_references: set[tuple] = set()
    try:
        if apply:
            from app.services.ingestion.processor import DocumentProcessingService

            service = DocumentProcessingService(settings)
        for document in documents:
            pages = pages_by_id.get(document.id, [])
            if not pages:
                continue
            for page in pages:
                detected = await asyncio.to_thread(detector.detect, page)
                for candidate in detected:
                    if candidate.requires_manual_review or is_procurement_buyer(
                        candidate.company_cnpj, procurement
                    ):
                        continue
                    report.add("confirmed_references")
                    if apply and service:
                        item = await service._item(
                            db,
                            procurement_id,
                            candidate.item_number,
                            context=candidate.evidence.text,
                        )
                        reference_key = (
                            candidate.company_cnpj,
                            candidate.role.value,
                            candidate.status,
                            item.id if item else None,
                        )
                        if reference_key in seen_references:
                            continue
                        chunk = await db.scalar(
                            select(DocumentChunk)
                            .where(
                                DocumentChunk.document_id == document.id,
                                DocumentChunk.locator == page.locator,
                            )
                            .order_by(DocumentChunk.sequence)
                            .limit(1)
                        )
                        procurement = await db.get(Procurement, procurement_id)
                        if chunk and procurement:
                            seen_references.add(reference_key)
                            created = await service._persist_participant(
                                db, procurement, document, chunk, candidate
                            )
                            report.add("participants_created", int(created))
            extracted = await asyncio.to_thread(extract_values, pages)
            report.add("eligible_value_observations", len(extracted.values))
            for diagnostic in extracted.diagnostics[:5]:
                report.diagnostics.append(f"documento {document.id}: {diagnostic}")
            if apply:
                report.add(
                    "monetary_values_created",
                    await persist_values(db, document, pages, extraction=extracted),
                )
    finally:
        if service:
            await service.aclose()
    if apply:
        from app.services.procurement_phase import refresh_procurement_phase

        procurement = await db.get(Procurement, procurement_id)
        if procurement:
            await refresh_procurement_phase(db, procurement)


async def reconcile_participation(
    sessions,
    settings: Settings,
    *,
    apply: bool = False,
    backup: BackupReceipt | None = None,
    maintenance_owner: str | None = None,
    resume_id: UUID | None = None,
) -> dict[str, Any]:
    if apply:
        if backup is None or maintenance_owner is None:
            raise ValueError("reparo exige backup verificado e manutenção exclusiva")
        await run_blocking_maintenance(
            lambda: backup.verify(settings.database_url, maintenance_owner)
        )
    owner = maintenance_owner or ""

    async def fence(db: AsyncSession) -> None:
        db.info["maintenance_owner"] = owner
        await fence_maintenance(db, owner)

    report = IntegrityReport()
    cache: dict = {}
    run_id = None
    checkpoint = None
    if apply:
        assert backup is not None
        async with write_session(sessions) as db:
            await fence(db)
            run = await db.get(ReconciliationRun, resume_id) if resume_id else None
            if resume_id and (run is None or run.version != VERSION):
                raise ValueError("execução de reparo ausente ou de outra versão")
            if run is None:
                run = ReconciliationRun(version=VERSION, backup={"receipts": [backup.as_dict()]})
                db.add(run)
                await db.flush()
            else:
                if run.status == "completed":
                    return {
                        "run_id": str(run.id),
                        "applied": True,
                        "already_completed": True,
                        "result": run.totals,
                    }
                run.backup = {"receipts": [*(run.backup.get("receipts") or []), backup.as_dict()]}
                checkpoint = run.checkpoint
                report.totals = {k: v for k, v in run.totals.items() if isinstance(v, int)}
                report.diagnostics = list(run.totals.get("diagnostics") or [])
            run_id = run.id
    try:
        while True:
            async with sessions() as db:
                statement = (
                    select(Procurement.id)
                    .where(
                        exists(select(Document.id).where(Document.procurement_id == Procurement.id))
                    )
                    .order_by(Procurement.id)
                    .limit(20)
                )
                if checkpoint:
                    statement = statement.where(Procurement.id > UUID(checkpoint))
                identifiers = list((await db.scalars(statement)).all())
            if not identifiers:
                break
            for identifier in identifiers:
                if apply:
                    # Verify retained bytes outside the write transaction/SQLite lock.
                    async with sessions() as reader:
                        documents = (
                            await reader.scalars(
                                select(Document).where(Document.procurement_id == identifier)
                            )
                        ).all()
                        for document in documents:
                            await retained_pages(reader, document, settings, cache)
                context = write_session(sessions) if apply else sessions.begin()
                async with context as db:
                    if apply:
                        await fence(db)
                    await inspect_procurement(
                        db, identifier, settings, apply=apply, cache=cache, report=report
                    )
                    report.add("inspected_procurements")
                    checkpoint = str(identifier)
                    if apply:
                        await fence(db)
                        run = await db.get(ReconciliationRun, run_id)
                        assert run is not None
                        run.checkpoint = checkpoint
                        run.totals = report.as_dict()
            await asyncio.sleep(0)
        if apply:
            async with write_session(sessions) as db:
                await fence(db)
                run = await db.get(ReconciliationRun, run_id)
                assert run is not None
                run.status = "completed"
                run.finished_at = datetime.now(UTC)
                run.totals = report.as_dict()
    except BaseException:
        # Per-procurement commits and the persisted checkpoint make reruns safe;
        # cancellation/lease failure is propagated, never converted to success.
        raise
    return {"run_id": str(run_id) if run_id else None, "applied": apply, "result": report.as_dict()}
