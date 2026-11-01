"""Transactional verification and monetary observations shared by collection/repair."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import and_, exists, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.models.collection_history import FieldChange
from app.models.entities import (
    Company,
    Deadline,
    Document,
    DocumentChunk,
    Evidence,
    FieldObservation,
    Lead,
    LeadReview,
    Participant,
    Procurement,
    ProcurementEvent,
    ProcurementItem,
)
from app.models.enums import LeadStatus, ProcurementEventType
from app.models.participation import FactVerification, ParticipantValue
from app.services.event_detection.participation_values import (
    VERSION,
    MonetaryFinding,
    ValueExtraction,
    extract_values,
    reference_status,
)
from app.services.event_detection.types import DocumentEvidence


def is_procurement_buyer(cnpj: str | None, procurement: Procurement | None) -> bool:
    from app.services.identifiers import normalize_cnpj

    normalized = normalize_cnpj(cnpj)
    return bool(
        normalized and procurement and normalized == normalize_cnpj(procurement.agency_cnpj)
    )


def fact_current(entity_type: str, identifier):
    """Legacy rows keep their existing semantics until explicit verification."""
    return ~exists(
        select(FactVerification.id)
        .where(
            FactVerification.entity_type == entity_type,
            FactVerification.entity_id == identifier,
            FactVerification.status != "confirmed",
        )
        .correlate_except(FactVerification)
    )


def participant_current():
    from app.repositories.current_evidence import evidence_current

    return and_(
        fact_current("participant", Participant.id),
        evidence_current(Participant.source_evidence_id),
    )


def event_verified():
    from app.repositories.current_evidence import evidence_current

    return and_(
        fact_current("procurement_event", ProcurementEvent.id),
        evidence_current(ProcurementEvent.evidence_id),
    )


def lead_current():
    return and_(
        fact_current("lead", Lead.id),
        exists(
            select(ProcurementEvent.id)
            .where(ProcurementEvent.id == Lead.triggering_event_id, event_verified())
            .correlate(Lead)
        ),
    )


def deadline_current():
    return and_(
        fact_current("deadline", Deadline.id),
        or_(
            Deadline.event_id.is_(None),
            exists(
                select(ProcurementEvent.id)
                .where(
                    ProcurementEvent.id == Deadline.event_id,
                    event_verified(),
                )
                .correlate(Deadline)
            ),
        ),
    )


def monetary_current():
    """Only the accepted content and a supported company attribution can price a lead."""
    from app.repositories.current_evidence import evidence_current

    version = aliased(Document)
    parent = aliased(Participant)
    cancelled = aliased(Participant)
    cancelled_result = exists(
        select(cancelled.id)
        .where(
            cancelled.company_id == ParticipantValue.company_id,
            cancelled.procurement_id == ParticipantValue.procurement_id,
            cancelled.status_code == "cancelled",
            fact_current("participant", cancelled.id),
            evidence_current(cancelled.source_evidence_id),
            or_(
                ParticipantValue.official_item_id.is_(None),
                cancelled.item_id == ParticipantValue.official_item_id,
            ),
        )
        .correlate_except(cancelled)
    )
    return and_(
        ParticipantValue.is_current.is_(True),
        evidence_current(ParticipantValue.evidence_id),
        evidence_current(ParticipantValue.subject_evidence_id),
        exists(
            select(version.id)
            .where(
                version.id == ParticipantValue.document_id,
                version.file_hash == ParticipantValue.document_hash,
            )
            .correlate_except(version)
        ),
        exists(
            select(parent.id)
            .where(
                parent.id == ParticipantValue.participant_id,
                parent.company_id == ParticipantValue.company_id,
                parent.procurement_id == ParticipantValue.procurement_id,
                fact_current("participant", parent.id),
                evidence_current(parent.source_evidence_id),
            )
            .correlate_except(parent)
        ),
        or_(ParticipantValue.kind != "final_result", ~cancelled_result),
    )


async def audit_change(
    db: AsyncSession,
    *,
    entity_type: str,
    entity_id: UUID,
    procurement_id: UUID,
    field: str,
    previous: Any,
    received: Any,
    evidence_id: UUID | None = None,
    source_record_id: UUID | None = None,
    decision: str = "integrity_repair",
) -> None:
    from app.services.ingestion.field_updates import json_value

    if json_value(previous) == json_value(received):
        return
    db.add(
        FieldChange(
            entity_type=entity_type,
            entity_id=entity_id,
            parent_type="procurement",
            parent_id=procurement_id,
            field_name=field,
            previous_value=json_value(previous),
            received_value=json_value(received),
            decision=decision,
            applied=True,
            source=VERSION,
            evidence_id=evidence_id,
            source_record_id=source_record_id,
            observed_at=datetime.now(UTC),
            timestamp_origin="local_verification",
        )
    )


async def verify_fact(
    db: AsyncSession,
    entity_type: str,
    entity_id: UUID,
    procurement_id: UUID,
    status: str,
    reason: str,
    *,
    evidence_id: UUID | None = None,
    source_record_id: UUID | None = None,
) -> bool:
    row = await db.scalar(
        select(FactVerification).where(
            FactVerification.entity_type == entity_type,
            FactVerification.entity_id == entity_id,
        )
    )
    if row and row.status == status and row.reason == reason and row.rule_version == VERSION:
        return False
    previous = row.status if row else None
    if row is None:
        row = FactVerification(entity_type=entity_type, entity_id=entity_id)
        db.add(row)
    row.status, row.reason, row.rule_version = status, reason, VERSION
    row.evidence_id, row.source_record_id = evidence_id, source_record_id
    await audit_change(
        db,
        entity_type=entity_type,
        entity_id=entity_id,
        procurement_id=procurement_id,
        field="verification_status",
        previous=previous,
        received=status,
        evidence_id=evidence_id,
        source_record_id=source_record_id,
    )
    await db.flush()
    return True


async def invalidate_participation_derivatives(
    db: AsyncSession, participant: Participant, *, status: str = "invalidated"
) -> None:
    """Withdraw only unsupported derived facts; preserve decisions and historical scores."""
    other = await db.scalar(
        select(Participant.id)
        .where(
            Participant.procurement_id == participant.procurement_id,
            Participant.company_id == participant.company_id,
            Participant.id != participant.id,
            participant_current(),
        )
        .limit(1)
    )
    company = await db.get(Company, participant.company_id)
    procurement = await db.get(Procurement, participant.procurement_id)
    buyer = bool(company and is_procurement_buyer(company.cnpj, procurement))
    if buyer:
        other = None
    events = list(
        (
            await db.scalars(
                select(ProcurementEvent).where(
                    ProcurementEvent.procurement_id == participant.procurement_id,
                    ProcurementEvent.company_id == participant.company_id,
                    ProcurementEvent.source.in_(["document", "llm"]),
                )
            )
        ).all()
    )
    for event in events:
        if event.event_type == ProcurementEventType.PARTICIPATION_DETECTED and other is not None:
            # Another official observation proves participation, but does not
            # validate an unrelated adverse act extracted from a cost quotation.
            continue
        if event.event_type != ProcurementEventType.PARTICIPATION_DETECTED and not buyer:
            source_evidence = (
                await db.get(Evidence, event.evidence_id) if event.evidence_id else None
            )
            company = await db.get(Company, participant.company_id)
            if (
                not source_evidence
                or not company
                or not company.cnpj
                or reference_status(source_evidence.text_excerpt, company.cnpj) != "invalidated"
            ):
                continue
        manual = await db.scalar(
            select(FieldObservation.id)
            .where(
                FieldObservation.entity_type == "procurement_event",
                FieldObservation.entity_id == event.id,
                FieldObservation.field_name == "company_id",
                FieldObservation.source.in_(["manual", "manual_operator"]),
            )
            .limit(1)
        )
        await verify_fact(
            db,
            "procurement_event",
            event.id,
            event.procurement_id,
            "review" if manual else status,
            "Participação sem comprovação documental atual; revisar a atribuição.",
            evidence_id=event.evidence_id,
            source_record_id=event.source_record_id,
        )
        event.requires_manual_review = True
        await _verify_event_dependents(db, event, status, "participação sem comprovação")


async def _verify_event_dependents(
    db: AsyncSession,
    event: ProcurementEvent,
    status: str,
    reason: str,
    *,
    preserve_noncurrent: bool = False,
) -> None:
    deadlines = list(
        (await db.scalars(select(Deadline).where(Deadline.event_id == event.id))).all()
    )
    leads = list((await db.scalars(select(Lead).where(Lead.triggering_event_id == event.id))).all())
    rows: list[Deadline | Lead] = [*deadlines, *leads]
    for row in rows:
        entity_type = "deadline" if isinstance(row, Deadline) else "lead"
        existing = (
            await db.scalar(
                select(FactVerification).where(
                    FactVerification.entity_type == entity_type,
                    FactVerification.entity_id == row.id,
                )
            )
            if preserve_noncurrent
            else None
        )
        if not existing or existing.status == "confirmed":
            await verify_fact(
                db,
                entity_type,
                row.id,
                event.procurement_id,
                status,
                f"Prazo derivado de {reason}."
                if entity_type == "deadline"
                else f"Lead preservado como histórico; {reason}.",
                evidence_id=event.evidence_id,
            )
        if isinstance(row, Deadline):
            row.requires_manual_review = True
        else:
            reviewed = await db.scalar(
                select(LeadReview.id).where(LeadReview.lead_id == row.id).limit(1)
            )
            if reviewed is None and row.lead_status not in {
                LeadStatus.APPROVED,
                LeadStatus.NEEDS_CHANGES,
                LeadStatus.REJECTED,
                LeadStatus.ARCHIVED,
            }:
                row.lead_status = LeadStatus.PENDING_REVIEW


async def review_event_item_scope(
    db: AsyncSession, event: ProcurementEvent, *, material_verified: bool
) -> bool:
    """Retain manual links; unsupported automatic scope cannot price or operate a lead."""
    manual = await db.scalar(
        select(FieldObservation.id)
        .where(
            FieldObservation.entity_type == "procurement_event",
            FieldObservation.entity_id == event.id,
            FieldObservation.field_name == "item_id",
            FieldObservation.source.in_(["manual", "manual_operator"]),
        )
        .limit(1)
    )
    if material_verified and manual is None:
        await audit_change(
            db,
            entity_type="procurement_event",
            entity_id=event.id,
            procurement_id=event.procurement_id,
            field="item_id",
            previous=event.item_id,
            received=None,
            evidence_id=event.evidence_id,
            source_record_id=event.source_record_id,
            decision="unsupported_item_scope",
        )
        event.item_id = None
    else:
        payload = {
            "item_id": str(event.item_id),
            "manual_link": manual is not None,
            "material_verified": material_verified,
        }
        previous = await db.scalar(
            select(FieldChange)
            .where(
                FieldChange.entity_type == "procurement_event",
                FieldChange.entity_id == event.id,
                FieldChange.field_name == "item_scope_review",
            )
            .order_by(FieldChange.recorded_at.desc())
            .limit(1)
        )
        await audit_change(
            db,
            entity_type="procurement_event",
            entity_id=event.id,
            procurement_id=event.procurement_id,
            field="item_scope_review",
            previous=previous.received_value if previous else None,
            received=payload,
            evidence_id=event.evidence_id,
            source_record_id=event.source_record_id,
            decision="review_required",
        )
    state = await db.scalar(
        select(FactVerification).where(
            FactVerification.entity_type == "procurement_event",
            FactVerification.entity_id == event.id,
        )
    )
    if state is None or state.status == "confirmed":
        await verify_fact(
            db,
            "procurement_event",
            event.id,
            event.procurement_id,
            "review",
            "Escopo de item sem correspondência oficial comprovada; revisar o vínculo.",
            evidence_id=event.evidence_id,
            source_record_id=event.source_record_id,
        )
    event.requires_manual_review = True
    await _verify_event_dependents(
        db,
        event,
        "review",
        "escopo de item sem correspondência oficial comprovada",
        preserve_noncurrent=True,
    )
    return manual is not None


async def verify_participant_reference(
    db: AsyncSession, participant: Participant, text: str, document_type: str | None = None
) -> str:
    company = await db.get(Company, participant.company_id)
    procurement = await db.get(Procurement, participant.procurement_id)
    buyer = bool(company and is_procurement_buyer(company.cnpj, procurement))
    status = (
        reference_status(text, company.cnpj, document_type)
        if company and company.cnpj
        else "review"
    )
    if buyer:
        status = "review"
    reason = {
        "confirmed": "Licitante identificada explicitamente no documento.",
        "review": "Referência documental ambígua; confirmação humana necessária.",
        "invalidated": "Referência a fornecedor/custo não comprova participação no certame.",
    }[status]
    if buyer:
        reason = (
            "O identificador pertence ao órgão comprador; não comprova participação como licitante."
        )
    await verify_fact(
        db,
        "participant",
        participant.id,
        participant.procurement_id,
        status,
        reason,
        evidence_id=participant.source_evidence_id,
        source_record_id=participant.source_record_id,
    )
    if status != "confirmed":
        await invalidate_participation_derivatives(db, participant, status=status)
    return status


async def _value_evidence(db: AsyncSession, document: Document, page: DocumentEvidence) -> Evidence:
    import hashlib

    key = hashlib.sha256(
        repr(
            (
                str(document.id),
                document.file_hash,
                page.page_number,
                page.locator,
                page.start_offset,
                page.text,
            )
        ).encode()
    ).hexdigest()
    row = await db.scalar(select(Evidence).where(Evidence.fingerprint == key))
    if row:
        return row
    chunk = await db.scalar(
        select(DocumentChunk)
        .where(
            DocumentChunk.document_id == document.id,
            DocumentChunk.page_number == page.page_number,
            *([DocumentChunk.locator == page.locator] if page.locator else []),
        )
        .order_by(DocumentChunk.sequence)
        .limit(1)
    )
    row = Evidence(
        procurement_id=document.procurement_id,
        document_id=document.id,
        document_chunk_id=chunk.id if chunk else None,
        source_record_id=document.source_record_id,
        page_number=page.page_number,
        locator=page.locator,
        text_excerpt=page.text,
        start_offset=page.start_offset,
        end_offset=page.end_offset,
        source_url=document.resolved_url or document.original_url,
        fingerprint=key,
    )
    db.add(row)
    await db.flush()
    return row


async def _official_value_item(
    db: AsyncSession, document: Document, finding: MonetaryFinding
) -> UUID | None:
    """A published PNCP reference plus its full description proves correspondence."""
    import re

    from app.services.identifiers import normalize_company_name

    text = finding.evidence.text
    reference = re.search(
        r"\bitem\s*(?:n[ºo°.]?\s*)?[:#-]?\s*(\d+)\s*(?:oficial\s*)?(?:do|no)\s+PNCP\b", text, re.I
    )
    if reference is None or finding.local_item_number != str(int(reference.group(1))):
        return None
    candidates = (
        await db.scalars(
            select(ProcurementItem).where(
                ProcurementItem.procurement_id == document.procurement_id,
                ProcurementItem.item_number.in_({reference.group(1), finding.local_item_number}),
            )
        )
    ).all()
    normalized = normalize_company_name(text, strip_legal_suffix=False) or ""
    matches = [
        item
        for item in candidates
        if (description := normalize_company_name(item.description, strip_legal_suffix=False))
        and len(description) >= 8
        and description in normalized
    ]
    return matches[0].id if len(matches) == 1 else None


async def persist_values(
    db: AsyncSession,
    document: Document,
    pages: list[DocumentEvidence],
    *,
    extraction: ValueExtraction | None = None,
) -> int:
    """Store immutable observations and audit acceptance changes in the same transaction."""
    import hashlib

    result = (
        extraction if extraction is not None else await asyncio.to_thread(extract_values, pages)
    )
    previous_review = await db.scalar(
        select(FieldChange)
        .where(
            FieldChange.entity_type == "document",
            FieldChange.entity_id == document.id,
            FieldChange.field_name == "monetary_review",
        )
        .order_by(FieldChange.recorded_at.desc(), FieldChange.id.desc())
        .limit(1)
    )
    review_payload = {"document_hash": document.file_hash, "diagnostics": list(result.diagnostics)}
    if (result.diagnostics or previous_review) and (
        previous_review is None or previous_review.received_value != review_payload
    ):
        db.add(
            FieldChange(
                entity_type="document",
                entity_id=document.id,
                parent_type="procurement",
                parent_id=document.procurement_id,
                field_name="monetary_review",
                previous_value=previous_review.received_value if previous_review else None,
                received_value=review_payload,
                decision="review_required" if result.diagnostics else "review_resolved",
                applied=False,
                source=VERSION,
                source_record_id=document.source_record_id,
                timestamp_origin="local_verification",
                observed_at=document.last_checked_at or datetime.now(UTC),
            )
        )
    created = 0
    supported_fingerprints: set[str] = set()
    old = list(
        (
            await db.scalars(
                select(ParticipantValue).where(
                    ParticipantValue.document_id == document.id,
                    ParticipantValue.is_current.is_(True),
                    ParticipantValue.document_hash != document.file_hash,
                )
            )
        ).all()
    )
    for previous_value in old:
        await audit_change(
            db,
            entity_type="participant_value",
            entity_id=previous_value.id,
            procurement_id=previous_value.procurement_id,
            field="is_current",
            previous=True,
            received=False,
            evidence_id=previous_value.evidence_id,
            source_record_id=document.source_record_id,
        )
        previous_value.is_current = False
    predecessors = list(
        (
            await db.scalars(
                select(ParticipantValue)
                .join(Document, Document.id == ParticipantValue.document_id)
                .where(
                    Document.procurement_id == document.procurement_id,
                    Document.original_url == document.original_url,
                    ParticipantValue.document_id != document.id,
                    ParticipantValue.is_current.is_(False),
                )
                .order_by(ParticipantValue.observed_at.desc(), ParticipantValue.created_at.desc())
            )
        ).all()
    )
    for finding in result.values:
        company = await db.scalar(select(Company).where(Company.cnpj == finding.company_cnpj))
        procurement = await db.get(Procurement, document.procurement_id)
        if company is None or is_procurement_buyer(company.cnpj, procurement):
            continue
        participant = await db.scalar(
            select(Participant)
            .where(
                Participant.procurement_id == document.procurement_id,
                Participant.company_id == company.id,
                participant_current(),
            )
            .order_by(
                Participant.source != "document",
                Participant.item_id.is_not(None),
                Participant.created_at,
            )
            .limit(1)
        )
        if participant is None:
            continue
        evidence = await _value_evidence(db, document, finding.evidence)
        subject = await _value_evidence(db, document, finding.subject_evidence)
        official_item_id = await _official_value_item(db, document, finding)
        fingerprint = hashlib.sha256(
            repr(
                (
                    str(document.procurement_id),
                    str(company.id),
                    str(document.id),
                    document.file_hash or str(document.id),
                    finding.kind,
                    finding.scope,
                    finding.lot_number,
                    finding.local_item_number,
                    str(finding.amount),
                    finding.evidence.page_number,
                    finding.evidence.start_offset,
                )
            ).encode()
        ).hexdigest()
        supported_fingerprints.add(fingerprint)
        value = await db.scalar(
            select(ParticipantValue).where(ParticipantValue.fingerprint == fingerprint)
        )
        previous_amount = next(
            (
                v.amount
                for v in [*old, *predecessors]
                if v.company_id == company.id
                and v.kind == finding.kind
                and v.scope == finding.scope
                and v.lot_number == finding.lot_number
                and v.local_item_number == finding.local_item_number
            ),
            None,
        )
        if value is None:
            value = ParticipantValue(
                procurement_id=document.procurement_id,
                company_id=company.id,
                participant_id=participant.id,
                official_item_id=official_item_id,
                document_id=document.id,
                evidence_id=evidence.id,
                subject_evidence_id=subject.id,
                source_record_id=document.source_record_id,
                kind=finding.kind,
                scope=finding.scope,
                lot_number=finding.lot_number,
                local_item_number=finding.local_item_number,
                amount=finding.amount,
                document_hash=document.file_hash,
                rule_version=VERSION,
                fingerprint=fingerprint,
                observed_at=document.last_checked_at
                or document.downloaded_at
                or document.created_at
                or datetime.now(UTC),
            )
            db.add(value)
            await db.flush()
            created += 1
            await audit_change(
                db,
                entity_type="participant_value",
                entity_id=value.id,
                procurement_id=value.procurement_id,
                field="amount",
                previous=previous_amount,
                received=value.amount,
                evidence_id=value.evidence_id,
                source_record_id=value.source_record_id,
            )
        elif not value.is_current:
            await audit_change(
                db,
                entity_type="participant_value",
                entity_id=value.id,
                procurement_id=value.procurement_id,
                field="amount",
                previous=previous_amount,
                received=value.amount,
                evidence_id=value.evidence_id,
                source_record_id=value.source_record_id,
            )
            await audit_change(
                db,
                entity_type="participant_value",
                entity_id=value.id,
                procurement_id=value.procurement_id,
                field="is_current",
                previous=False,
                received=True,
                evidence_id=value.evidence_id,
                source_record_id=value.source_record_id,
            )
            value.is_current = True
        if value.official_item_id != official_item_id:
            await audit_change(
                db,
                entity_type="participant_value",
                entity_id=value.id,
                procurement_id=value.procurement_id,
                field="official_item_id",
                previous=value.official_item_id,
                received=official_item_id,
                evidence_id=value.evidence_id,
                source_record_id=value.source_record_id,
            )
            value.official_item_id = official_item_id
    await db.flush()
    unsupported = (
        await db.scalars(
            select(ParticipantValue).where(
                ParticipantValue.document_id == document.id,
                ParticipantValue.is_current.is_(True),
                ParticipantValue.fingerprint.not_in(supported_fingerprints),
            )
        )
    ).all()
    for value in unsupported:
        await audit_change(
            db,
            entity_type="participant_value",
            entity_id=value.id,
            procurement_id=value.procurement_id,
            field="is_current",
            previous=True,
            received=False,
            evidence_id=value.evidence_id,
            source_record_id=document.source_record_id,
            decision="unsupported_interpretation",
        )
        value.is_current, value.verification_status = False, "review"
    await db.flush()
    # Different published amounts in the same kind/scope are unresolved, not summed.
    values = list(
        (
            await db.scalars(
                select(ParticipantValue).where(
                    ParticipantValue.procurement_id == document.procurement_id,
                    monetary_current(),
                )
            )
        ).all()
    )
    groups: dict[tuple, list[ParticipantValue]] = {}
    for value in values:
        groups.setdefault(
            (value.company_id, value.kind, value.scope, value.lot_number, value.local_item_number),
            [],
        ).append(value)
    for group in groups.values():
        status = "confirmed" if len({v.amount for v in group}) == 1 else "review"
        for value in group:
            if value.verification_status != status:
                await audit_change(
                    db,
                    entity_type="participant_value",
                    entity_id=value.id,
                    procurement_id=value.procurement_id,
                    field="verification_status",
                    previous=value.verification_status,
                    received=status,
                    evidence_id=value.evidence_id,
                    source_record_id=value.source_record_id,
                )
                value.verification_status = status
    # Compatibility columns are projections of a single, evidenced scope.
    for participant in (
        await db.scalars(
            select(Participant).where(
                Participant.procurement_id == document.procurement_id,
                Participant.source == "document",
                participant_current(),
            )
        )
    ).all():
        eligible = [
            v
            for v in values
            if v.company_id == participant.company_id
            and v.verification_status == "confirmed"
            and (
                (participant.item_id is None and v.scope == "procurement")
                or (participant.item_id is not None and v.official_item_id == participant.item_id)
            )
        ]
        for field, kinds in (
            ("proposal_value", ("revised_proposal", "proposal", "initial_proposal")),
            ("final_value", ("final_result",)),
        ):
            selected = next(
                (
                    {v.amount for v in eligible if v.kind == kind}
                    for kind in kinds
                    if any(v.kind == kind for v in eligible)
                ),
                set(),
            )
            if len(selected) == 1:
                amount = next(iter(selected))
                previous = getattr(participant, field)
                if previous != amount:
                    setattr(participant, field, amount)
                    await audit_change(
                        db,
                        entity_type="participant",
                        entity_id=participant.id,
                        procurement_id=participant.procurement_id,
                        field=field,
                        previous=previous,
                        received=amount,
                        evidence_id=eligible[0].evidence_id,
                        source_record_id=eligible[0].source_record_id,
                    )
            elif getattr(participant, field) is not None:
                # Only retract projections written by this service, preserving unrelated
                # legacy/manual observations. The monetary journal retains their history.
                proof = await db.scalar(
                    select(FieldChange.id)
                    .where(
                        FieldChange.entity_type == "participant",
                        FieldChange.entity_id == participant.id,
                        FieldChange.field_name == field,
                        FieldChange.source == VERSION,
                    )
                    .limit(1)
                )
                if proof:
                    previous = getattr(participant, field)
                    setattr(participant, field, None)
                    await audit_change(
                        db,
                        entity_type="participant",
                        entity_id=participant.id,
                        procurement_id=participant.procurement_id,
                        field=field,
                        previous=previous,
                        received=None,
                        source_record_id=document.source_record_id,
                        decision="unsupported_current_scope",
                    )
    return created


KIND_LABELS = {
    "proposal": "Proposta publicada",
    "initial_proposal": "Proposta inicial",
    "revised_proposal": "Proposta readequada",
    "final_result": "Resultado final",
}


async def participant_payloads(
    db: AsyncSession, participants: list[Participant], *, historical: bool = False
) -> list[dict[str, Any]]:
    from app.api.serialization import model_dict

    if not participants:
        return []
    ids = [p.id for p in participants]
    verifications = {
        v.entity_id: v
        for v in (
            await db.scalars(
                select(FactVerification).where(
                    FactVerification.entity_type == "participant",
                    FactVerification.entity_id.in_(ids),
                )
            )
        ).all()
    }
    statement = select(ParticipantValue).where(
        ParticipantValue.procurement_id.in_({p.procurement_id for p in participants}),
        ParticipantValue.company_id.in_({p.company_id for p in participants}),
    )
    if not historical:
        statement = statement.where(monetary_current())
    values = list(
        (
            await db.scalars(
                statement.order_by(ParticipantValue.lot_number, ParticipantValue.created_at)
            )
        ).all()
    )
    current_ids = (
        set(
            (
                await db.scalars(
                    select(ParticipantValue.id).where(
                        ParticipantValue.id.in_([value.id for value in values]), monetary_current()
                    )
                )
            ).all()
        )
        if historical and values
        else {value.id for value in values}
    )
    evidence_pages = (
        dict(
            (
                await db.execute(
                    select(Evidence.id, Evidence.page_number).where(
                        Evidence.id.in_({v.evidence_id for v in values})
                    )
                )
            ).all()
        )
        if values
        else {}
    )
    result = []
    for participant in participants:
        verification = verifications.get(participant.id)
        payload = model_dict(participant)
        payload["verification_status"] = (
            verification.status
            if verification
            else "legacy"
            if participant.source == "document"
            else "confirmed"
        )
        payload["verification_reason"] = verification.reason if verification else None
        relevant = [
            v
            for v in values
            if v.company_id == participant.company_id
            and v.procurement_id == participant.procurement_id
            and (participant.item_id is None or v.official_item_id == participant.item_id)
        ]
        unique: dict[tuple, dict[str, Any]] = {}
        for value in relevant:
            key = (
                value.kind,
                value.scope,
                value.lot_number,
                value.local_item_number,
                value.amount,
                value.verification_status,
                value.is_current,
            )
            if key not in unique:
                row = model_dict(value)
                row["is_current"] = value.id in current_ids
                row["kind_label"] = KIND_LABELS.get(value.kind, value.kind)
                row["evidence_url"] = f"/api/evidence/{value.evidence_id}"
                row["evidence_page"] = evidence_pages.get(value.evidence_id)
                row["amount_display"] = "R$ " + format(value.amount, ",.2f").replace(
                    ",", "_"
                ).replace(".", ",").replace("_", ".")
                row["scope_label"] = (
                    f"Lote {value.lot_number}" if value.lot_number else "Contratação"
                ) + (
                    f" · Item {'PNCP' if value.official_item_id else 'local'} {value.local_item_number}"
                    if value.local_item_number
                    else ""
                )
                unique[key] = row
        payload["monetary_values"] = list(unique.values())
        payload["scope"] = [
            {
                "scope": v["scope"],
                "lot_number": v["lot_number"],
                "local_item_number": v["local_item_number"],
                "official_item_id": v["official_item_id"],
            }
            for v in unique.values()
        ]
        result.append(payload)
    return result
