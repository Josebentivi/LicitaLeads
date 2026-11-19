"""Shared, evidence-bound queries for the analyst's agenda and review queue."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from urllib.parse import parse_qsl, urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import and_, exists, false, func, literal, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased, selectinload
from sqlalchemy.sql.elements import ColumnElement

from app.models import (
    Company,
    CrawlRun,
    Deadline,
    DeadlineCalculationMethod,
    DeadlineStatus,
    Document,
    Evidence,
    FieldChange,
    FieldObservation,
    Lead,
    LeadStatus,
    Procurement,
    ProcurementEvent,
    ProcurementEventType,
    ProcurementSource,
    SourceRecord,
)
from app.repositories.current_evidence import evidence_current
from app.services.deadlines.response import (
    OFFICIAL_RESPONSE_EVENT_TYPES,
    RESPONSE_ACT_OBSERVATION,
    RESPONSE_CLOSING_EVENT_TYPES,
    RESPONSE_PROOF_VERSION,
    RESPONSE_SCOPE_OBSERVATION,
)
from app.services.identifiers import normalize_cnpj
from app.services.participation_integrity import deadline_current, fact_current, lead_current
from app.services.procurement_status import CANCELLED_TOKENS, SUSPENDED_TOKENS

ACTIONABLE_STATUSES = (LeadStatus.NEW, LeadStatus.PENDING_REVIEW, LeadStatus.NEEDS_CHANGES)


def _response_was_interrupted(now: datetime):
    later = aliased(ProcurementEvent)
    later_scope = aliased(FieldObservation)
    opening_scope = aliased(FieldObservation)
    later_evidence = aliased(Evidence)
    opening_evidence = aliased(Evidence)
    occurrence = func.coalesce(later.occurred_at, later.published_at)
    same_party = later.company_id == Deadline.company_id
    later_kind = later_scope.value["scope"].as_string()
    opening_kind = opening_scope.value["scope"].as_string()
    opening_item = opening_scope.value["item"].as_string()
    opening_lot = opening_scope.value["lote"].as_string()
    later_lot = later_scope.value["lote"].as_string()
    same_scope = and_(
        later_kind != "unknown",
        later_kind == opening_kind,
        later_scope.value["item"].as_string() == opening_item,
        later_lot == opening_lot,
    )

    def packed(field):
        return func.coalesce(later_scope.value[field].as_string(), "")

    def includes(field, identifier):
        return and_(
            identifier != "",
            packed(field).contains(literal("|").concat(identifier).concat("|")),
        )

    listed = or_(
        and_(later_kind == "lot_list", includes("lots", opening_lot)),
        and_(
            later_kind == "item_list",
            later_lot == opening_lot,
            includes("items", opening_item),
        ),
    )
    limited = or_(
        same_scope,
        listed,
        and_(later_kind == "lot", opening_kind == "item", later_lot == opening_lot),
    )
    covered = or_(
        later_kind == "procurement",
        limited,
        and_(
            later_kind == "procurement_except",
            opening_kind.in_(("item", "lot")),
            or_(
                packed("excluded_items") == "",
                and_(opening_item != "", ~includes("excluded_items", opening_item)),
            ),
            or_(
                packed("excluded_lots") == "",
                and_(opening_lot != "", ~includes("excluded_lots", opening_lot)),
            ),
        ),
    )
    return exists(
        select(later.id)
        .join(later_scope, later_scope.entity_id == later.id)
        .join(opening_scope, opening_scope.entity_id == Deadline.id)
        .join(later_evidence, later_evidence.id == later.evidence_id)
        .join(opening_evidence, opening_evidence.id == ProcurementEvent.evidence_id)
        .where(
            later.procurement_id == Deadline.procurement_id,
            later.id != ProcurementEvent.id,
            later_scope.entity_type == "procurement_event",
            later_scope.field_name == RESPONSE_ACT_OBSERVATION,
            later_scope.evidence_id == later.evidence_id,
            later_scope.value["version"].as_integer() == RESPONSE_PROOF_VERSION,
            opening_scope.entity_type == "deadline",
            opening_scope.field_name == RESPONSE_SCOPE_OBSERVATION,
            opening_scope.evidence_id == ProcurementEvent.evidence_id,
            opening_scope.value["version"].as_integer() == RESPONSE_PROOF_VERSION,
            or_(
                ~later.requires_manual_review,
                and_(
                    later.event_type.in_(
                        (ProcurementEventType.SESSION_SUSPENDED, ProcurementEventType.HOMOLOGATED)
                    ),
                    later.company_id.is_(None),
                    later.confidence >= 0.75,
                ),
            ),
            fact_current("procurement_event", later.id),
            evidence_current(later.evidence_id),
            occurrence <= now,
            occurrence <= Deadline.explicit_deadline_at,
            or_(
                occurrence > Deadline.trigger_at,
                and_(
                    occurrence == Deadline.trigger_at,
                    later_evidence.document_id == opening_evidence.document_id,
                    later_scope.value["follows_previous"].as_boolean().is_(True),
                    later_scope.value["position"].as_integer()
                    > opening_scope.value["position"].as_integer(),
                ),
            ),
            or_(
                and_(
                    later.event_type.in_(
                        (ProcurementEventType.SESSION_SUSPENDED, ProcurementEventType.HOMOLOGATED)
                    ),
                    covered,
                ),
                and_(
                    later.event_type == ProcurementEventType.APPEAL_DECIDED,
                    same_party,
                    or_(same_scope, listed),
                ),
                *(
                    and_(
                        Deadline.deadline_type == kind,
                        later.event_type.in_(event_types),
                        same_party,
                        or_(same_scope, listed),
                    )
                    for kind, event_types in RESPONSE_CLOSING_EVENT_TYPES.items()
                ),
            ),
        )
        .correlate(Deadline, ProcurementEvent)
    )


def _response_was_superseded(now: datetime):
    replacement = aliased(Deadline)
    replacement_act = aliased(ProcurementEvent)
    original_scope = aliased(FieldObservation)
    replacement_scope = aliased(FieldObservation)
    return exists(
        select(replacement.id)
        .join(replacement_act, replacement_act.id == replacement.event_id)
        .join(replacement_scope, replacement_scope.entity_id == replacement.id)
        .join(original_scope, original_scope.entity_id == Deadline.id)
        .where(
            replacement.deadline_type == Deadline.deadline_type,
            replacement.company_id == Deadline.company_id,
            replacement.procurement_id == Deadline.procurement_id,
            replacement_act.procurement_id == replacement.procurement_id,
            replacement_act.company_id == replacement.company_id,
            replacement.trigger_at > Deadline.trigger_at,
            replacement.trigger_at <= now,
            replacement.trigger_at <= Deadline.explicit_deadline_at,
            replacement.explicit_deadline_at.is_not(None),
            ~replacement.requires_manual_review,
            ~replacement_act.requires_manual_review,
            fact_current("deadline", replacement.id),
            fact_current("procurement_event", replacement_act.id),
            evidence_current(replacement_act.evidence_id),
            original_scope.entity_type == "deadline",
            original_scope.field_name == RESPONSE_SCOPE_OBSERVATION,
            original_scope.evidence_id == ProcurementEvent.evidence_id,
            original_scope.value["version"].as_integer() == RESPONSE_PROOF_VERSION,
            replacement_scope.entity_type == "deadline",
            replacement_scope.field_name == RESPONSE_SCOPE_OBSERVATION,
            replacement_scope.evidence_id == replacement_act.evidence_id,
            replacement_scope.value["version"].as_integer() == RESPONSE_PROOF_VERSION,
            replacement_scope.value["extension"].as_boolean().is_(True),
            replacement_scope.value["scope"].as_string() != "unknown",
            replacement_scope.value["scope"].as_string()
            == original_scope.value["scope"].as_string(),
            replacement_scope.value["item"].as_string() == original_scope.value["item"].as_string(),
            replacement_scope.value["lote"].as_string() == original_scope.value["lote"].as_string(),
        )
        .correlate(Deadline, ProcurementEvent)
    )


def official_response_proof_current() -> ColumnElement[bool]:
    scope = aliased(FieldObservation)
    event = aliased(ProcurementEvent)
    return exists(
        select(scope.id)
        .join(event, event.evidence_id == scope.evidence_id)
        .where(
            event.id == Deadline.event_id,
            scope.entity_type == "deadline",
            scope.entity_id == Deadline.id,
            scope.field_name == RESPONSE_SCOPE_OBSERVATION,
            scope.value["version"].as_integer() == RESPONSE_PROOF_VERSION,
        )
        .correlate(Deadline)
    )


async def legacy_official_response_deadline_ids(session: AsyncSession, deadline_ids) -> set[UUID]:
    if not deadline_ids:
        return set()
    return set(
        await session.scalars(
            select(Deadline.id).where(
                Deadline.id.in_(deadline_ids),
                Deadline.deadline_type.in_(OFFICIAL_RESPONSE_EVENT_TYPES),
                ~official_response_proof_current(),
            )
        )
    )


def _official_response_deadline_valid(now: datetime) -> ColumnElement[bool]:
    status = func.lower(func.coalesce(Procurement.status, ""))
    closed = or_(
        *(status.contains(token) for token in CANCELLED_TOKENS + SUSPENDED_TOKENS),
        status.in_(("encerrada", "encerrado", "homologada", "homologado", "fracassada", "deserta")),
    )
    return and_(
        official_response_proof_current(),
        Deadline.calculation_method.in_(
            (DeadlineCalculationMethod.EXPLICIT, DeadlineCalculationMethod.DOCUMENT_EXTRACTED)
        ),
        ~Deadline.requires_manual_review,
        Deadline.status != DeadlineStatus.REQUIRES_REVIEW,
        Deadline.explicit_deadline_at.is_not(None),
        Deadline.company.has(Company.cnpj.is_not(None)),
        Deadline.procurement.has(~closed),
        Deadline.event.has(
            and_(
                ProcurementEvent.company_id == Deadline.company_id,
                ProcurementEvent.procurement_id == Deadline.procurement_id,
                ProcurementEvent.evidence_id.is_not(None),
                exists(
                    select(Evidence.id)
                    .where(
                        Evidence.id == ProcurementEvent.evidence_id,
                        Evidence.procurement_id == ProcurementEvent.procurement_id,
                        Evidence.document_id.is_not(None),
                    )
                    .correlate(ProcurementEvent)
                ),
                or_(
                    *(
                        and_(
                            Deadline.deadline_type == kind,
                            ProcurementEvent.event_type.in_(event_types),
                        )
                        for kind, event_types in OFFICIAL_RESPONSE_EVENT_TYPES.items()
                    )
                ),
                ~ProcurementEvent.requires_manual_review,
                ~_response_was_interrupted(now),
                ~_response_was_superseded(now),
            )
        ),
    )


def deadline_usable(now: datetime) -> ColumnElement[bool]:
    """Only active evidence and unsuperseded official windows can drive time-based decisions."""
    return and_(
        deadline_current(),
        or_(
            Deadline.deadline_type.not_in(OFFICIAL_RESPONSE_EVENT_TYPES),
            _official_response_deadline_valid(now),
        ),
    )


def _open_official_response_deadline(now: datetime):
    return and_(
        deadline_current(),
        Deadline.deadline_type.in_(OFFICIAL_RESPONSE_EVENT_TYPES),
        Deadline.trigger_at <= now,
        Deadline.explicit_deadline_at >= now,
        _official_response_deadline_valid(now),
    )


def procurement_has_open_response_deadline(now: datetime):
    return exists(
        select(Deadline.id)
        .where(Deadline.procurement_id == Procurement.id, _open_official_response_deadline(now))
        .correlate(Procurement)
    )


def _lead_deadline_at():
    return (
        select(Deadline.explicit_deadline_at)
        .where(Deadline.id == Lead.deadline_id)
        .correlate(Lead)
        .scalar_subquery()
    )


def safe_list_url(value: str | None, *, path: str = "/leads") -> str:
    """Accept only a local list URL, never a client-supplied redirect destination."""
    if not value or len(value) > 4000 or any(ord(c) < 32 for c in value) or "\\" in value:
        return path
    try:
        parts = urlsplit(value)
    except ValueError:
        return path
    if parts.scheme or parts.netloc or parts.path != path or parts.fragment:
        return path
    return value


def run_procurement_ids(run_id: UUID, document_ids: tuple[UUID, ...] = ()):
    """Use recorded provenance, never date proximity, to scope collection results."""
    records = select(SourceRecord.id).where(SourceRecord.crawl_run_id == run_id)
    sources = select(ProcurementSource.procurement_id).where(
        ProcurementSource.source_record_id.in_(records)
    )
    documents = select(Document.procurement_id).where(
        or_(Document.source_record_id.in_(records), Document.id.in_(document_ids))
    )
    changes = select(FieldChange.parent_id).where(
        FieldChange.parent_type == "procurement", FieldChange.crawl_run_id == run_id
    )
    return sources.union(documents, changes)


@dataclass(frozen=True, slots=True)
class OpportunityFilters:
    query: str = ""
    status: str = ""
    event_type: str = ""
    min_score: int | None = None
    requires_review: bool = False
    include_history: bool = False
    created_from: date | None = None
    created_to: date | None = None
    due: str = ""
    run_id: UUID | None = None
    document_ids: tuple[UUID, ...] = ()
    timezone: str = "America/Sao_Paulo"

    @classmethod
    def from_url(cls, value: str, *, timezone: str) -> OpportunityFilters:
        params = dict(parse_qsl(urlsplit(safe_list_url(value)).query, keep_blank_values=True))
        if params.keys() <= {"page"}:
            params["due"] = "open"
        return cls.from_params(params, timezone=timezone)

    @classmethod
    def from_params(cls, params: Mapping[str, str], *, timezone: str) -> OpportunityFilters:
        def day(key: str) -> date | None:
            try:
                return date.fromisoformat(params.get(key, ""))
            except ValueError:
                return None

        try:
            score = int(params["min_score"])
        except (ValueError, KeyError):
            score = None
        try:
            run_id = UUID(params.get("run", ""))
        except ValueError:
            run_id = None
        return cls(
            query=params.get("q", "").strip()[:200],
            status=params.get("lead_status", ""),
            event_type=params.get("event_type", ""),
            min_score=score if score is not None and 0 <= score <= 100 else None,
            requires_review=params.get("requires_review") == "true",
            include_history=params.get("include_history") == "true",
            created_from=day("created_from"),
            created_to=day("created_to"),
            due=params.get("due", ""),
            run_id=run_id,
            timezone=timezone,
        )


def opportunity_query(filters: OpportunityFilters, *, now: datetime | None = None):
    """Keep cards, lists and next-item navigation on the very same predicates."""
    statement = select(Lead)
    if not filters.include_history or filters.due == "open":
        statement = statement.where(lead_current())
    if filters.status == "actionable":
        statement = statement.where(Lead.lead_status.in_(ACTIONABLE_STATUSES))
    elif filters.status:
        statement = statement.where(
            Lead.lead_status == filters.status
            if filters.status in {status.value for status in LeadStatus}
            else false()
        )
    if filters.min_score is not None:
        statement = statement.where(Lead.score >= filters.min_score)
    if filters.event_type:
        statement = statement.where(
            Lead.triggering_event.has(ProcurementEvent.event_type == filters.event_type)
            if filters.event_type in {event.value for event in ProcurementEventType}
            else false()
        )
    if filters.requires_review:
        statement = statement.where(
            Lead.triggering_event.has(ProcurementEvent.requires_manual_review.is_(True))
        )
    if filters.query:
        query = filters.query
        company = or_(
            Company.legal_name.icontains(query, autoescape=True),
            Company.normalized_name.icontains(query, autoescape=True),
        )
        cnpj = normalize_cnpj(query)
        if cnpj:
            company = or_(company, Company.cnpj.contains(cnpj, autoescape=True))
        statement = statement.where(
            or_(
                Lead.company.has(company),
                Lead.procurement.has(
                    or_(
                        Procurement.purchase_number.icontains(query, autoescape=True),
                        Procurement.pncp_control_number.icontains(query, autoescape=True),
                        Procurement.title.icontains(query, autoescape=True),
                    )
                ),
            )
        )
    tz = ZoneInfo(filters.timezone)
    if filters.created_from:
        statement = statement.where(
            Lead.created_at >= datetime.combine(filters.created_from, time.min, tz).astimezone(UTC)
        )
    if filters.created_to:
        statement = statement.where(
            Lead.created_at <= datetime.combine(filters.created_to, time.max, tz).astimezone(UTC)
        )
    if filters.due == "open":
        moment = now or datetime.now(UTC)
        statement = statement.where(
            Lead.lead_status.not_in((LeadStatus.REJECTED, LeadStatus.ARCHIVED)),
            Lead.deadline.has(
                and_(
                    Deadline.company_id == Lead.company_id,
                    Deadline.event_id == Lead.triggering_event_id,
                    Deadline.procurement_id == Lead.procurement_id,
                    _open_official_response_deadline(moment),
                )
            ),
        ).order_by(_lead_deadline_at())
    elif filters.due:
        moment = now or datetime.now(UTC)
        end = moment + timedelta(hours=72)
        if filters.due == "today":
            end = datetime.combine(moment.astimezone(tz).date(), time.max, tz).astimezone(UTC)
        elif filters.due == "24h":
            end = moment + timedelta(hours=24)
        effective = func.coalesce(Deadline.explicit_deadline_at, Deadline.estimated_deadline_at)
        statement = statement.where(
            Lead.deadline.has(
                and_(
                    deadline_usable(moment),
                    effective >= moment,
                    effective <= end,
                )
            )
            if filters.due in {"today", "24h", "72h"}
            else false()
        )
    if filters.run_id:
        statement = statement.where(
            Lead.procurement_id.in_(run_procurement_ids(filters.run_id, filters.document_ids))
        )
    return statement.order_by(Lead.score.desc(), Lead.created_at.desc(), Lead.id)


def loaded_opportunities(statement):
    return statement.options(
        selectinload(Lead.company),
        selectinload(Lead.triggering_event),
        selectinload(Lead.deadline),
        selectinload(Lead.procurement),
    )


async def opportunity_count(
    session: AsyncSession, filters: OpportunityFilters, *, now: datetime | None = None
) -> int:
    return int(
        await session.scalar(
            select(func.count()).select_from(
                opportunity_query(filters, now=now).order_by(None).subquery()
            )
        )
        or 0
    )


async def next_opportunity(
    session: AsyncSession, lead: Lead, filters: OpportunityFilters, *, for_review: bool = False
):
    filters = await resolve_queue_filters(session, filters)
    statement = opportunity_query(filters)
    if for_review or (filters.due != "open" and not filters.include_history and not filters.status):
        statement = statement.where(Lead.lead_status.in_(ACTIONABLE_STATUSES), lead_current())
    after = or_(
        Lead.score < lead.score,
        and_(Lead.score == lead.score, Lead.created_at < lead.created_at),
        and_(Lead.score == lead.score, Lead.created_at == lead.created_at, Lead.id > lead.id),
    )
    if filters.due == "open":
        previous_due = await session.scalar(
            select(Deadline.explicit_deadline_at).where(Deadline.id == lead.deadline_id)
        )
        if previous_due is None:
            return None
        due = _lead_deadline_at()
        after = or_(due > previous_due, and_(due == previous_due, after))
    return await session.scalar(statement.where(after).limit(1))


def deadline_indication(
    instant: datetime | None, *, explicit: bool, current: bool, now: datetime, timezone: str
) -> str:
    if instant is None:
        return "Data-limite não identificada"
    if not current:
        return "Data histórica"
    due = instant if instant.tzinfo else instant.replace(tzinfo=UTC)
    if due < now:
        return "Data-limite ultrapassada" if explicit else "Data estimada ultrapassada"
    tz = ZoneInfo(timezone)
    if due.astimezone(tz).date() == now.astimezone(tz).date():
        return "Data-limite hoje"
    if due - now <= timedelta(hours=24):
        return "Nas próximas 24 horas"
    return "Data futura"


async def resolve_queue_filters(
    session: AsyncSession, filters: OpportunityFilters
) -> OpportunityFilters:
    if not filters.run_id:
        return filters
    from app.services.ingestion.document_retry import batch_document_ids

    run = await session.get(CrawlRun, filters.run_id)
    return replace(filters, document_ids=tuple(batch_document_ids(run) or ())) if run else filters
