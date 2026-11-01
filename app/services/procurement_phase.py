"""Derive the operational phase of a procurement from auditable source facts.

The phase is an operator aid, not a legal conclusion.  It is derived only from
published status text, proposal/session dates and document-backed events.  When
none of those are sufficient the result is ``UNKNOWN``.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Document,
    Evidence,
    FieldObservation,
    FieldValueStatus,
    Procurement,
    ProcurementEvent,
    ProcurementEventType,
    ProcurementPhase,
)
from app.services.procurement_status import (
    CANCELLED_TOKENS,
    CLOSED_TOKENS,
    OPEN_TOKENS,
    SUSPENDED_TOKENS,
    procurement_is_active,
)

_EVENT_PHASES: tuple[tuple[ProcurementEventType, ProcurementPhase], ...] = (
    (ProcurementEventType.HOMOLOGATED, ProcurementPhase.HOMOLOGATION),
    (ProcurementEventType.ADJUDICATED, ProcurementPhase.ADJUDICATION),
    (ProcurementEventType.APPEAL_DECIDED, ProcurementPhase.JUDGMENT),
    (ProcurementEventType.COUNTERARGUMENT_SUBMITTED, ProcurementPhase.COUNTERARGUMENT),
    (ProcurementEventType.COUNTERARGUMENT_OPENED, ProcurementPhase.COUNTERARGUMENT),
    (ProcurementEventType.APPEAL_SUBMITTED, ProcurementPhase.APPEAL),
    (ProcurementEventType.INTENT_TO_APPEAL, ProcurementPhase.APPEAL),
    (ProcurementEventType.DILIGENCE_REQUESTED, ProcurementPhase.DILIGENCE),
    (ProcurementEventType.DOCUMENTATION_REQUESTED, ProcurementPhase.DILIGENCE),
    (ProcurementEventType.NEGOTIATION_REQUESTED, ProcurementPhase.DISPUTE),
    (ProcurementEventType.BEST_OFFER_DECLARED, ProcurementPhase.DISPUTE),
    (ProcurementEventType.SESSION_REOPENED, ProcurementPhase.DISPUTE),
    (ProcurementEventType.SESSION_OPENED, ProcurementPhase.DISPUTE),
    (ProcurementEventType.SESSION_SUSPENDED, ProcurementPhase.SUSPENDED),
    (ProcurementEventType.DISQUALIFIED, ProcurementPhase.JUDGMENT),
    (ProcurementEventType.INELIGIBLE, ProcurementPhase.QUALIFICATION),
    (ProcurementEventType.PROPOSAL_REJECTED, ProcurementPhase.JUDGMENT),
    (ProcurementEventType.PROPOSAL_ACCEPTED, ProcurementPhase.QUALIFICATION),
    (ProcurementEventType.QUALIFIED, ProcurementPhase.QUALIFICATION),
    (ProcurementEventType.WINNER_DECLARED, ProcurementPhase.ADJUDICATION),
)
_EVENT_PHASE_BY_TYPE = dict(_EVENT_PHASES)

_ACTIVE_PHASES = frozenset(
    {
        ProcurementPhase.DISPUTE,
        ProcurementPhase.DILIGENCE,
        ProcurementPhase.JUDGMENT,
        ProcurementPhase.APPEAL,
        ProcurementPhase.COUNTERARGUMENT,
        ProcurementPhase.QUALIFICATION,
        ProcurementPhase.ADJUDICATION,
    }
)


def _contains(status: str | None, tokens: tuple[str, ...]) -> bool:
    text = (status or "").lower()
    return any(token in text for token in tokens)


def derive_procurement_phase(
    procurement: Procurement,
    *,
    last_event: ProcurementEvent | None = None,
    now: datetime | None = None,
) -> ProcurementPhase:
    """Return the most defensible phase for one procurement.

    Status tokens win over dates because sources publish cancellation and
    suspension explicitly.  A dated session event wins over the proposal window
    because it is the later fact.  A future proposal window always means the
    process is still in the proposal phase.
    """

    moment = now or datetime.now(UTC)
    status = procurement.status

    if _contains(status, CANCELLED_TOKENS):
        return ProcurementPhase.CANCELLED
    if _contains(status, SUSPENDED_TOKENS):
        return ProcurementPhase.SUSPENDED
    if _contains(status, ("desert", "fracass")):
        return ProcurementPhase.FAILED
    if _contains(status, ("homolog",)):
        return ProcurementPhase.HOMOLOGATION
    if _contains(status, ("adjudic",)):
        return ProcurementPhase.ADJUDICATION

    if last_event is not None:
        mapped = _EVENT_PHASE_BY_TYPE.get(last_event.event_type)
        if mapped is not None:
            return mapped
    if procurement.session_start_at is not None and procurement.session_start_at <= moment:
        if not _contains(status, CLOSED_TOKENS):
            return ProcurementPhase.DISPUTE
    if procurement.proposal_end_at is not None and procurement.proposal_end_at >= moment:
        return ProcurementPhase.PROPOSALS
    if _contains(status, OPEN_TOKENS) and procurement.proposal_end_at is None:
        return ProcurementPhase.PROPOSALS
    if _contains(status, OPEN_TOKENS) and procurement.proposal_end_at is not None:
        return ProcurementPhase.DISPUTE
    if _contains(status, CLOSED_TOKENS):
        return ProcurementPhase.CLOSED
    return ProcurementPhase.UNKNOWN


def procurement_is_in_session(
    procurement: Procurement,
    *,
    last_event: ProcurementEvent | None = None,
    now: datetime | None = None,
) -> bool:
    """Return True while the dispute/judgment/appeal window is plausible."""

    return derive_procurement_phase(procurement, last_event=last_event, now=now) in _ACTIVE_PHASES


def procurement_accepts_participation_leads(
    procurement: Procurement,
    *,
    last_event: ProcurementEvent | None = None,
    now: datetime | None = None,
) -> bool:
    """Return True while documented participation may create a generic lead.

    The proposal window and the live dispute/diligence phases are covered.
    Judgment/appeal/habilitation phases already produce specific event leads,
    so a generic participation event would duplicate them.
    """

    phase = derive_procurement_phase(procurement, last_event=last_event, now=now)
    if phase in {
        ProcurementPhase.PROPOSALS,
        ProcurementPhase.DISPUTE,
        ProcurementPhase.DILIGENCE,
    }:
        return True
    return procurement_is_active(procurement.status, procurement.proposal_end_at, now=now)


async def latest_procurement_event(
    session: AsyncSession,
    procurement_id: UUID,
) -> ProcurementEvent | None:
    """Return the most recent event by its dated source occurrence."""

    from app.services.participation_integrity import event_verified

    return await session.scalar(
        select(ProcurementEvent)
        .outerjoin(Evidence, Evidence.id == ProcurementEvent.evidence_id)
        .outerjoin(Document, Document.id == Evidence.document_id)
        .where(ProcurementEvent.procurement_id == procurement_id, event_verified())
        .where(or_(Evidence.document_id.is_(None), Document.is_active.is_(True)))
        .order_by(
            func.coalesce(
                ProcurementEvent.occurred_at,
                ProcurementEvent.published_at,
                ProcurementEvent.created_at,
            ).desc(),
            ProcurementEvent.created_at.desc(),
        )
        .limit(1)
    )


async def refresh_procurement_phase(
    session: AsyncSession,
    procurement: Procurement,
    *,
    now: datetime | None = None,
) -> ProcurementPhase:
    """Refresh one procurement from its latest dated event."""

    latest = await latest_procurement_event(session, procurement.id)
    return await update_procurement_phase(session, procurement, last_event=latest, now=now)


async def update_procurement_phase(
    session: AsyncSession,
    procurement: Procurement,
    *,
    last_event: ProcurementEvent | None = None,
    now: datetime | None = None,
) -> ProcurementPhase:
    """Persist a phase transition and append its audit observation."""

    moment = now or datetime.now(UTC)
    phase = derive_procurement_phase(procurement, last_event=last_event, now=moment)
    if phase == procurement.current_phase:
        procurement.phase_observed_at = moment
        return phase

    phase_without_event = derive_procurement_phase(procurement, last_event=None, now=moment)
    event_phase = (
        _EVENT_PHASE_BY_TYPE.get(last_event.event_type) if last_event is not None else None
    )
    event_used = (
        last_event is not None
        and event_phase is not None
        and phase == event_phase
        and phase_without_event != event_phase
    )

    procurement.current_phase = phase
    procurement.phase_observed_at = moment
    observation_key = f"phase:{procurement.id}:{phase.value}:{moment.isoformat()}"
    session.add(
        FieldObservation(
            entity_type="procurement",
            entity_id=procurement.id,
            field_name="current_phase",
            value=phase.value,
            value_status=(
                FieldValueStatus.OBSERVED if event_used else FieldValueStatus.REQUIRES_MANUAL_REVIEW
            ),
            source=last_event.source if event_used and last_event else procurement.source,
            source_record_id=(last_event.source_record_id if event_used and last_event else None),
            evidence_id=last_event.evidence_id if event_used and last_event else None,
            confidence=last_event.confidence if event_used and last_event else None,
            collected_at=moment,
            fingerprint=hashlib.sha256(observation_key.encode("utf-8")).hexdigest(),
        )
    )
    await session.flush()
    return phase
