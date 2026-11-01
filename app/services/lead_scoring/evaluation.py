"""Transactional, evidence-bound evaluation used by normal and explicit paths."""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Company,
    CompanyContact,
    Contract,
    Deadline,
    Evidence,
    FieldState,
    Lead,
    LeadReview,
    LeadScoreRevision,
    LeadStatus,
    Participant,
    PriceRegistry,
    PriceRegistryItem,
    Procurement,
    ProcurementEvent,
    SourceRecord,
)
from app.models.participation import ParticipantValue
from app.services.company_overview import (
    contract_procurement_link,
    registry_procurement_link,
    supplier_identity,
)
from app.services.ingestion.field_updates import json_value
from app.services.lead_scoring.v2 import LeadScoreInput, LeadScorer
from app.services.participation_integrity import monetary_current, participant_current


async def economic_reference(
    db: AsyncSession, company: Company, procurement: Procurement, event: ProcurementEvent
) -> tuple[Decimal | None, list[dict[str, Any]]]:
    """Never treat a procurement-wide budget or payment as this supplier's value."""
    from app.services.event_detection.participation_values import published_scope

    if event.company_id != company.id or event.procurement_id != procurement.id:
        return None, []
    origin = await db.get(Evidence, event.evidence_id) if event.evidence_id else None
    lots, local_item = published_scope(origin.text_excerpt if origin else event.raw_description)
    if local_item and event.item_id is None:
        return None, []
    candidates: list[tuple[Decimal, dict[str, Any]]] = []
    participants = select(Participant).where(
        Participant.company_id == company.id,
        Participant.procurement_id == procurement.id,
        participant_current(),
    )
    if event.item_id is not None:
        participants = participants.where(Participant.item_id == event.item_id)
    rows = list((await db.scalars(participants)).all())
    if lots and event.item_id is None:
        rows = []
    for row in rows:
        if row.source == "document":
            continue
        if row.status_code is not None and str(row.status_code) == "cancelled":
            continue
        if not row.source_record_id and not row.source_evidence_id:
            continue
        for field in ("final_value", "proposal_value"):
            amount = getattr(row, field)
            if amount is not None:
                candidates.append(
                    (
                        amount,
                        {
                            "entity_type": "participant",
                            "entity_id": str(row.id),
                            "field": field,
                            "source_record_id": str(row.source_record_id)
                            if row.source_record_id
                            else None,
                            "evidence_id": str(row.source_evidence_id)
                            if row.source_evidence_id
                            else None,
                        },
                    )
                )
                break
    document_values = select(ParticipantValue).where(
        ParticipantValue.company_id == company.id,
        ParticipantValue.procurement_id == procurement.id,
        monetary_current(),
        ParticipantValue.verification_status == "confirmed",
    )
    if event.item_id is not None:
        document_values = document_values.where(ParticipantValue.official_item_id == event.item_id)
    elif lots:
        document_values = document_values.where(ParticipantValue.lot_number.in_(lots))
    for value in (await db.scalars(document_values)).all():
        candidates.append(
            (
                value.amount,
                {
                    "entity_type": "participant_value",
                    "entity_id": str(value.id),
                    "field": "amount",
                    "source_record_id": str(value.source_record_id)
                    if value.source_record_id
                    else None,
                    "evidence_id": str(value.evidence_id),
                },
            )
        )
    if event.item_id is None and not lots:
        contracts = await db.scalars(
            select(Contract)
            .join(Procurement, contract_procurement_link())
            .where(
                supplier_identity(Contract, company),
                Procurement.id == procurement.id,
            )
        )
        for contract in contracts:
            if not contract.source_record_id:
                continue
            if any(
                token in (contract.status or "").lower()
                for token in ("cancel", "anulad", "revogad")
            ):
                continue
            # The header belongs to this company. Values are alternatives, never added.
            for field in ("global_value", "accumulated_value", "initial_value"):
                amount = getattr(contract, field)
                if amount is not None:
                    candidates.append(
                        (
                            amount,
                            {
                                "entity_type": "contract",
                                "entity_id": str(contract.id),
                                "field": field,
                                "source_record_id": str(contract.source_record_id),
                            },
                        )
                    )
                    break
        registries = await db.scalars(
            select(PriceRegistryItem)
            .join(PriceRegistry)
            .join(Procurement, registry_procurement_link())
            .where(
                supplier_identity(PriceRegistryItem, company),
                Procurement.id == procurement.id,
            )
        )
        for item in registries:
            if item.total_value is not None and item.source_record_id:
                candidates.append(
                    (
                        item.total_value,
                        {
                            "entity_type": "price_registry_item",
                            "entity_id": str(item.id),
                            "field": "total_value",
                            "source_record_id": str(item.source_record_id),
                        },
                    )
                )
    # State uncertainty marks unresolved divergent observations, even if a value was accepted.
    ids = [UUID(ref["entity_id"]) for _, ref in candidates]
    conflicts = (
        set(
            (
                await db.execute(
                    select(FieldState.entity_id, FieldState.field_name).where(
                        FieldState.entity_id.in_(ids), FieldState.uncertain.is_(True)
                    )
                )
            ).all()
        )
        if ids
        else set()
    )
    candidates = [
        (value, ref)
        for value, ref in candidates
        if value >= 0 and (UUID(ref["entity_id"]), ref["field"]) not in conflicts
    ]
    if not candidates:
        return None, []
    amount, reference = max(candidates, key=lambda item: (item[0], item[1]["entity_id"]))
    return amount, [reference]


async def evaluate_lead(
    db: AsyncSession,
    lead: Lead,
    *,
    explicit: bool = False,
    request_id: UUID | None = None,
    origin: str = "processing",
    now: datetime | None = None,
    minimum_score: int | None = None,
    initial: bool = False,
) -> dict[str, Any]:
    if request_id:
        previous = await db.scalar(
            select(LeadScoreRevision).where(
                LeadScoreRevision.lead_id == lead.id, LeadScoreRevision.request_id == request_id
            )
        )
        if previous:
            return {
                "lead_id": str(lead.id),
                "previous_score": previous.previous_score,
                "score": previous.score,
                "version": previous.scoring_version,
                "reused": True,
            }
    if not explicit and lead.scoring_version != "2.0":
        return {
            "lead_id": str(lead.id),
            "skipped": "Versão anterior preservada; recálculo explícito necessário",
        }
    # Import locally: processor calls this service after persisting the event/deadline.
    from app.services.ingestion.processor import (
        event_award_inference_is_current,
        event_document_is_active,
    )

    event = await db.get(ProcurementEvent, lead.triggering_event_id)
    if (
        event is None
        or event.company_id != lead.company_id
        or event.procurement_id != lead.procurement_id
        or not await event_document_is_active(db, event)
        or not await event_award_inference_is_current(db, event)
    ):
        return {"lead_id": str(lead.id), "skipped": "Evento ou evidência sem vigência atual"}
    evidence = await db.get(Evidence, event.evidence_id) if event.evidence_id else None
    if evidence is not None and evidence.procurement_id != lead.procurement_id:
        return {
            "lead_id": str(lead.id),
            "skipped": "Evidência de outra contratação; revisar vínculo",
        }
    source = await db.get(SourceRecord, event.source_record_id) if event.source_record_id else None
    if evidence is None and source is None:
        return {"lead_id": str(lead.id), "skipped": "Sem origem rastreável"}
    company = await db.get(Company, lead.company_id)
    procurement = await db.get(Procurement, lead.procurement_id)
    if company is None or procurement is None:
        return {"lead_id": str(lead.id), "skipped": "Identidade indisponível"}
    deadline = await db.get(Deadline, lead.deadline_id) if lead.deadline_id else None
    moment = now or datetime.now(UTC)
    due = (deadline.explicit_deadline_at or deadline.estimated_deadline_at) if deadline else None
    hours = (due - moment).total_seconds() / 3600 if due else None
    expired = hours is not None and hours < 0
    contacts = list(
        (
            await db.scalars(
                select(CompanyContact).where(
                    CompanyContact.company_id == company.id, CompanyContact.is_corporate.is_(True)
                )
            )
        ).all()
    )
    amount, references = await economic_reference(db, company, procurement, event)
    inputs = LeadScoreInput(
        event_type=event.event_type.value,
        deadline_status="EXPIRED" if expired else "OPEN" if due else "UNKNOWN",
        hours_remaining=hours,
        evidence_confidence=float(event.confidence),
        has_direct_evidence=evidence is not None,
        estimated_value=amount,
        verified_contact_types=tuple(
            str(c.contact_type) for c in contacts if str(c.status) == "verified"
        ),
        pending_contact_types=tuple(
            str(c.contact_type)
            for c in contacts
            if str(c.status) in {"discovered", "requires_review"}
        ),
        company_confirmed=bool(company.cnpj),
        cnpj_present=bool(company.cnpj),
        reason_known=str(event.reason_category) != "UNKNOWN",
        deadline_estimated=bool(deadline and not deadline.explicit_deadline_at and due),
        deadline_expired=expired,
        process_closed=any(
            token in (procurement.status or "").lower()
            for token in ("encerr", "cancel", "anulad", "revogad")
        ),
    )
    score = LeadScorer().score(inputs)
    previous_score = None if initial else lead.score
    previous_version = None if initial else lead.scoring_version
    references.extend(
        [
            {
                "entity_type": "procurement_event",
                "entity_id": str(event.id),
                "source_record_id": str(source.id) if source else None,
                "evidence_id": str(evidence.id) if evidence else None,
            }
        ]
    )
    references.extend(
        {"entity_type": "company_contact", "entity_id": str(c.id), "source_url": c.source_url}
        for c in contacts
        if str(c.status) != "invalid"
    )
    components = dict(score.breakdown)
    # Compare with the last transition; identical values after a different revision remain distinct.
    latest = await db.scalar(
        select(LeadScoreRevision)
        .where(LeadScoreRevision.lead_id == lead.id)
        .order_by(LeadScoreRevision.calculated_at.desc(), LeadScoreRevision.id.desc())
        .limit(1)
    )
    from app.config import get_settings

    settings = get_settings()
    threshold = settings.min_lead_score if minimum_score is None else minimum_score
    manual = await db.scalar(select(LeadReview.id).where(LeadReview.lead_id == lead.id).limit(1))
    if not manual and lead.lead_status not in {
        LeadStatus.APPROVED,
        LeadStatus.REJECTED,
        LeadStatus.NEEDS_CHANGES,
        LeadStatus.ARCHIVED,
    }:
        lead.lead_status = (
            LeadStatus.BELOW_THRESHOLD
            if score.score < threshold
            else LeadStatus.PENDING_REVIEW
            if event.requires_manual_review
            or (deadline and deadline.requires_manual_review)
            or float(event.confidence) < settings.manual_review_confidence_threshold
            else LeadStatus.NEW
        )
    for field in (
        "score",
        "urgency_score",
        "evidence_score",
        "legal_relevance_score",
        "contact_score",
        "economic_value_score",
        "scoring_version",
    ):
        setattr(lead, field, getattr(score, field))
    lead.fit_score = score.score
    lead.score_breakdown = components
    if (
        request_id
        or latest is None
        or latest.score != score.score
        or latest.components != components
        or latest.references != references
        or latest.inputs.get("minimum_score") != threshold
    ):
        db.add(
            LeadScoreRevision(
                lead_id=lead.id,
                request_id=request_id,
                scoring_version=score.scoring_version,
                previous_score=previous_score,
                previous_version=previous_version,
                score=score.score,
                components=components,
                inputs=json_value(
                    dict(
                        asdict(inputs),
                        minimum_score=threshold,
                        deadline_at=due,
                        deadline_id=deadline.id if deadline else None,
                        manual_review_confidence_threshold=settings.manual_review_confidence_threshold,
                    )
                ),
                references=references,
                origin=origin,
                calculated_at=moment,
            )
        )
    await db.flush()
    return {
        "lead_id": str(lead.id),
        "previous_score": previous_score,
        "score": score.score,
        "version": score.scoring_version,
        "reused": False,
    }
