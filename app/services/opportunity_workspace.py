"""Shared, evidence-bound queries for the analyst's agenda and review queue."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from urllib.parse import parse_qsl, urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import and_, false, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import (
    Company,
    CrawlRun,
    Deadline,
    Document,
    FieldChange,
    Lead,
    LeadStatus,
    Procurement,
    ProcurementEvent,
    ProcurementEventType,
    ProcurementSource,
    SourceRecord,
)
from app.services.identifiers import normalize_cnpj
from app.services.participation_integrity import deadline_current, lead_current

ACTIONABLE_STATUSES = (LeadStatus.NEW, LeadStatus.PENDING_REVIEW, LeadStatus.NEEDS_CHANGES)


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
        params = dict(parse_qsl(urlsplit(safe_list_url(value)).query))
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
    if not filters.include_history:
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
    if filters.due:
        moment = now or datetime.now(UTC)
        end = moment + timedelta(hours=72)
        if filters.due == "today":
            end = datetime.combine(moment.astimezone(tz).date(), time.max, tz).astimezone(UTC)
        elif filters.due == "24h":
            end = moment + timedelta(hours=24)
        effective = func.coalesce(Deadline.explicit_deadline_at, Deadline.estimated_deadline_at)
        statement = statement.where(
            Lead.deadline.has(and_(deadline_current(), effective >= moment, effective <= end))
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


async def opportunity_count(session: AsyncSession, filters: OpportunityFilters) -> int:
    return int(
        await session.scalar(
            select(func.count()).select_from(opportunity_query(filters).order_by(None).subquery())
        )
        or 0
    )


async def next_opportunity(
    session: AsyncSession, lead: Lead, filters: OpportunityFilters, *, for_review: bool = False
):
    filters = await resolve_queue_filters(session, filters)
    statement = opportunity_query(filters)
    if for_review or (not filters.include_history and not filters.status):
        statement = statement.where(Lead.lead_status.in_(ACTIONABLE_STATUSES), lead_current())
    return await session.scalar(
        statement.where(
            or_(
                Lead.score < lead.score,
                and_(Lead.score == lead.score, Lead.created_at < lead.created_at),
                and_(
                    Lead.score == lead.score, Lead.created_at == lead.created_at, Lead.id > lead.id
                ),
            )
        ).limit(1)
    )


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
