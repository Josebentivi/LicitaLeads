"""Lead review, controlled mutation, and draft-only outreach endpoints."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.serialization import model_dict
from app.config import get_settings
from app.dependencies import get_db
from app.models import (
    Company,
    CompanyContact,
    Deadline,
    Lead,
    LeadReview,
    LeadStatus,
    OutreachChannel,
    OutreachDraft,
    ProcurementEvent,
    ReviewDecision,
)
from app.schemas import LeadPatch, LeadReviewRequest, OutreachRequest
from app.services.outreach import OutreachContext, OutreachDraftService

router = APIRouter(prefix="/leads", tags=["leads"])


def _detail_options():
    return (
        selectinload(Lead.procurement),
        selectinload(Lead.company).selectinload(Company.contacts),
        selectinload(Lead.triggering_event).selectinload(ProcurementEvent.evidence),
        selectinload(Lead.deadline),
        selectinload(Lead.reviews),
        selectinload(Lead.outreach_drafts),
    )


async def _lead(lead_id: UUID, db: AsyncSession) -> Lead:
    lead = await db.scalar(select(Lead).where(Lead.id == lead_id).options(*_detail_options()))
    if lead is None:
        raise HTTPException(status_code=404, detail="Lead not found")
    return lead


def _lead_dict(lead: Lead, *, nested: bool = False) -> dict[str, object]:
    payload = model_dict(lead)
    if nested:
        payload.update(
            {
                "procurement": model_dict(lead.procurement),
                "company": model_dict(lead.company),
                "triggering_event": model_dict(lead.triggering_event),
                "deadline": model_dict(lead.deadline) if lead.deadline else None,
                "reviews": [model_dict(item) for item in lead.reviews],
                "outreach_drafts": [model_dict(item) for item in lead.outreach_drafts],
            }
        )
    return payload


@router.get("")
async def list_leads(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    lead_status: str | None = Query(None, alias="status"),
    minimum_score: int | None = Query(None, ge=0, le=100),
    event_type: str | None = None,
    deadline_status: str | None = None,
    has_contact: bool | None = None,
    pending_review: bool | None = None,
    created_from: datetime | None = None,
    created_to: datetime | None = None,
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """List candidates, including below-threshold and expired opportunities."""

    statement = select(Lead).options(
        selectinload(Lead.company),
        selectinload(Lead.triggering_event),
        selectinload(Lead.deadline),
    )
    if lead_status:
        statement = statement.where(Lead.lead_status == lead_status)
    if minimum_score is not None:
        statement = statement.where(Lead.score >= minimum_score)
    if event_type:
        statement = statement.join(Lead.triggering_event).where(
            ProcurementEvent.event_type == event_type
        )
    if deadline_status:
        statement = statement.join(Lead.deadline).where(Deadline.status == deadline_status)
    if pending_review is not None:
        condition = Lead.lead_status == LeadStatus.PENDING_REVIEW
        statement = statement.where(condition if pending_review else ~condition)
    if created_from:
        statement = statement.where(Lead.created_at >= created_from)
    if created_to:
        statement = statement.where(Lead.created_at <= created_to)
    if has_contact is not None:
        corporate_contact = exists(
            select(CompanyContact.id).where(
                CompanyContact.company_id == Lead.company_id,
                CompanyContact.is_corporate.is_(True),
            )
        )
        statement = statement.where(corporate_contact if has_contact else ~corporate_contact)
    count_statement = select(func.count()).select_from(statement.order_by(None).subquery())
    total = int(await db.scalar(count_statement) or 0)
    rows = list(
        (
            await db.scalars(
                statement.order_by(Lead.score.desc(), Lead.created_at.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
        )
        .unique()
        .all()
    )
    return {
        "items": [_lead_dict(row) for row in rows],
        "page": page,
        "page_size": page_size,
        "total": total,
    }


@router.get("/{lead_id}")
async def get_lead(lead_id: UUID, db: AsyncSession = Depends(get_db)) -> dict[str, object]:
    return _lead_dict(await _lead(lead_id, db), nested=True)


@router.post("/{lead_id}/review")
async def review_lead(
    lead_id: UUID,
    payload: LeadReviewRequest,
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    lead = await _lead(lead_id, db)
    previous = lead.lead_status.value
    decision = ReviewDecision(payload.decision)
    lead.lead_status = LeadStatus(decision.value)
    review = LeadReview(
        lead_id=lead.id,
        decision=decision,
        notes=payload.notes,
        reviewer=payload.reviewer,
        previous_status=previous,
        reviewed_at=datetime.now(UTC),
    )
    db.add(review)
    await db.commit()
    await db.refresh(review)
    return {"lead": model_dict(lead), "review": model_dict(review)}


@router.patch("/{lead_id}")
async def patch_lead(
    lead_id: UUID,
    payload: LeadPatch,
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    lead = await _lead(lead_id, db)
    changes = payload.model_dump(exclude_unset=True)
    if "lead_status" in changes and changes["lead_status"] is not None:
        lead.lead_status = LeadStatus(changes.pop("lead_status"))
    for key, value in changes.items():
        setattr(lead, key, value)
    await db.commit()
    return model_dict(lead)


@router.post("/{lead_id}/generate-outreach")
async def generate_outreach(
    lead_id: UUID,
    payload: OutreachRequest,
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """Generate, but never transmit, a deterministic Portuguese draft."""

    settings = get_settings()
    lead = await _lead(lead_id, db)
    event = lead.triggering_event
    procurement = lead.procurement
    deadline = lead.deadline
    source_url = (
        event.source_url
        or (event.evidence.source_url if event.evidence else None)
        or procurement.source_url
    )
    if not source_url:
        raise HTTPException(status_code=409, detail="Lead has no traceable public source URL")
    due_at = None
    estimated = False
    if deadline:
        due_at = deadline.explicit_deadline_at or deadline.estimated_deadline_at
        estimated = deadline.explicit_deadline_at is None and due_at is not None
    deadline_label = (
        due_at.astimezone(UTC).strftime("prazo em %d/%m/%Y às %H:%M UTC")
        if due_at
        else "prazo não publicado, que precisa ser confirmado na fonte oficial"
    )
    context = OutreachContext(
        company_name=lead.company.legal_name or lead.company.normalized_name,
        procurement=(
            procurement.title
            or procurement.pncp_control_number
            or f"processo {procurement.purchase_number or procurement.external_id}"
        ),
        agency=procurement.agency_name or "órgão contratante",
        event=event.event_type.value,
        reason=event.normalized_reason or event.raw_description,
        deadline=deadline_label,
        deadline_estimated=estimated,
        source_label=event.source.upper(),
        source_url=source_url,
        sender_name=settings.outreach_sender_name,
        law_firm=settings.outreach_law_firm,
        sender_contact=settings.outreach_sender_contact,
    )
    service = OutreachDraftService(mode=settings.outreach_mode)
    service_channel = "phone_script" if payload.channel == "phone" else payload.channel
    rendered = service.generate(context, channel=service_channel)
    facts = json.dumps(asdict(context), ensure_ascii=False, sort_keys=True, default=str)
    facts_hash = hashlib.sha256(facts.encode()).hexdigest()
    template_hash = hashlib.sha256(service.template_source.encode()).hexdigest()
    existing = await db.scalar(
        select(OutreachDraft)
        .where(
            OutreachDraft.lead_id == lead.id,
            OutreachDraft.channel == OutreachChannel(payload.channel),
            OutreachDraft.facts_hash == facts_hash,
            OutreachDraft.template_hash == template_hash,
        )
        .order_by(OutreachDraft.created_at.desc())
        .limit(1)
    )
    if existing is not None and not payload.force_regenerate:
        return {"created": False, "draft": model_dict(existing)}
    fingerprint_source = rendered.idempotency_key
    if payload.force_regenerate:
        fingerprint_source += f":{datetime.now(UTC).isoformat()}"
    draft = OutreachDraft(
        lead_id=lead.id,
        channel=OutreachChannel(payload.channel),
        subject=rendered.subject,
        message=rendered.message,
        generation_method=rendered.generation_method,
        facts_hash=facts_hash,
        template_hash=template_hash,
        approved=False,
        sent=False,
        fingerprint=hashlib.sha256(fingerprint_source.encode()).hexdigest(),
    )
    db.add(draft)
    await db.commit()
    await db.refresh(draft)
    return {"created": True, "draft": model_dict(draft)}
