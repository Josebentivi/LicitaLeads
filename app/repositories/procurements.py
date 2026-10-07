"""Repositories for canonical procurements and their traceable source records."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import Select, case, false, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import (
    Company,
    Evidence,
    Participant,
    Procurement,
    ProcurementEvent,
    ProcurementSource,
    SourceRecord,
)
from app.models.enums import ParticipantStatus, ProcurementEventType
from app.repositories.base import BaseRepository, Page
from app.services.identifiers import canonical_modality

_STATUS_CANCELLED_TOKENS = ("cancel", "revog", "anulad", "exclu")
_STATUS_SUSPENDED_TOKENS = ("suspens",)
_STATUS_CLOSED_TOKENS = ("encerr", "homolog", "adjudic", "desert", "fracass")


def _status_tokens_condition(tokens: tuple[str, ...]):
    # Coalesce keeps NULL statuses from poisoning the negated conditions below.
    status_text = func.coalesce(func.lower(Procurement.status), "")
    return or_(*[status_text.like(f"%{token}%") for token in tokens])


def _status_category_condition(category: str, *, now: datetime):
    """Translate an operational status category into honest SQL conditions."""

    cancelled = _status_tokens_condition(_STATUS_CANCELLED_TOKENS)
    suspended = _status_tokens_condition(_STATUS_SUSPENDED_TOKENS)
    closed_tokens = _status_tokens_condition(_STATUS_CLOSED_TOKENS)
    if category == "cancelada":
        return (cancelled,)
    if category == "suspensa":
        return (suspended,)
    if category == "aberta":
        return (
            Procurement.proposal_end_at.is_not(None),
            Procurement.proposal_end_at >= now,
            ~cancelled,
            ~suspended,
        )
    if category == "encerrada":
        return (
            ~cancelled,
            ~suspended,
            or_(
                Procurement.proposal_end_at < now,
                closed_tokens,
            ),
        )
    if category == "desconhecida":
        return (
            ~cancelled,
            ~suspended,
            Procurement.proposal_end_at.is_(None),
            ~closed_tokens,
        )
    raise ValueError(f"unsupported status category: {category}")


class ProcurementRepository(BaseRepository[Procurement]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Procurement)

    async def get_detail(self, procurement_id: UUID | str) -> Procurement | None:
        statement = (
            select(Procurement)
            .where(Procurement.id == procurement_id)
            .options(
                selectinload(Procurement.sources),
                selectinload(Procurement.items),
                selectinload(Procurement.documents),
                selectinload(Procurement.participants),
                selectinload(Procurement.events),
                selectinload(Procurement.deadlines),
            )
        )
        return await self.session.scalar(statement)

    async def find_by_pncp_control_number(self, control_number: str) -> Procurement | None:
        return await self.session.scalar(
            select(Procurement).where(Procurement.pncp_control_number == control_number)
        )

    async def find_by_source(self, source: str, external_id: str) -> Procurement | None:
        direct = await self.session.scalar(
            select(Procurement).where(
                Procurement.source == source,
                Procurement.external_id == external_id,
            )
        )
        if direct is not None:
            return direct
        return await self.session.scalar(
            select(Procurement)
            .join(ProcurementSource)
            .where(
                ProcurementSource.source == source,
                ProcurementSource.external_id == external_id,
            )
        )

    async def find_by_strong_identity(
        self,
        *,
        agency_cnpj: str,
        purchase_number: str,
        purchase_year: int,
        modality: str,
        uasg: str | None,
    ) -> Procurement | None:
        """Match only the explicit cross-source key; textual fields are excluded."""

        modality_key = canonical_modality(modality)
        if modality_key is None:
            return None
        conditions = [
            Procurement.agency_cnpj == agency_cnpj,
            Procurement.purchase_number == purchase_number,
            Procurement.purchase_year == purchase_year,
            Procurement.modality_key == modality_key,
        ]
        if uasg is not None:
            conditions.append(Procurement.uasg == uasg)
        return await self.session.scalar(select(Procurement).where(*conditions).limit(1))

    async def resolve_canonical(
        self,
        *,
        source: str,
        external_id: str,
        pncp_control_number: str | None = None,
        agency_cnpj: str | None = None,
        uasg: str | None = None,
        purchase_number: str | None = None,
        purchase_year: int | None = None,
        modality: str | None = None,
    ) -> Procurement | None:
        """Apply the documented PNCP-first and exact strong-key resolution order."""

        if pncp_control_number:
            matched = await self.find_by_pncp_control_number(pncp_control_number)
            if matched is not None:
                return matched
        matched = await self.find_by_source(source, external_id)
        if matched is not None:
            return matched
        if all((agency_cnpj, purchase_number, purchase_year, modality)):
            return await self.find_by_strong_identity(
                agency_cnpj=agency_cnpj or "",
                uasg=uasg,
                purchase_number=purchase_number or "",
                purchase_year=purchase_year or 0,
                modality=modality or "",
            )
        return None

    async def list_filtered(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        uf: str | None = None,
        municipality: str | None = None,
        agency: str | None = None,
        modality: str | None = None,
        modalities: list[str] | None = None,
        published_from: datetime | None = None,
        published_to: datetime | None = None,
        status: str | None = None,
        status_category: str | None = None,
        procurement_type: str | None = None,
        is_srp: bool | None = None,
        value_min: Decimal | None = None,
        value_max: Decimal | None = None,
        now: datetime | None = None,
    ) -> Page[Procurement]:
        statement: Select[tuple[Procurement]] = select(Procurement)
        if uf:
            statement = statement.where(Procurement.uf == uf.upper())
        if municipality:
            statement = statement.where(Procurement.municipality.ilike(f"%{municipality}%"))
        if agency:
            statement = statement.where(Procurement.agency_name.ilike(f"%{agency}%"))
        selected_modalities = [
            key
            for key in (canonical_modality(value) for value in (modalities or []))
            if key is not None
        ]
        if selected_modalities:
            statement = statement.where(Procurement.modality_key.in_(selected_modalities))
        elif modality:
            modality_key = canonical_modality(modality)
            statement = statement.where(
                Procurement.modality_key == modality_key if modality_key is not None else false()
            )
        if published_from:
            statement = statement.where(Procurement.publication_at >= published_from)
        if published_to:
            statement = statement.where(Procurement.publication_at <= published_to)
        if status:
            statement = statement.where(Procurement.status == status)
        if status_category:
            reference = now or datetime.now(UTC)
            statement = statement.where(*_status_category_condition(status_category, now=reference))
        if procurement_type:
            statement = statement.where(Procurement.procurement_type == procurement_type)
        if is_srp is not None:
            statement = statement.where(Procurement.is_srp.is_(is_srp))
        if value_min is not None:
            statement = statement.where(Procurement.estimated_value >= value_min)
        if value_max is not None:
            statement = statement.where(Procurement.estimated_value <= value_max)
        statement = statement.order_by(Procurement.publication_at.desc(), Procurement.id)
        return await self.page(statement, page=page, page_size=page_size)


class ProcurementSourceRepository(BaseRepository[ProcurementSource]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, ProcurementSource)

    async def find(self, source: str, external_id: str) -> ProcurementSource | None:
        return await self.session.scalar(
            select(ProcurementSource).where(
                ProcurementSource.source == source,
                ProcurementSource.external_id == external_id,
            )
        )


class SourceRecordRepository(BaseRepository[SourceRecord]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, SourceRecord)

    async def find_response(
        self,
        *,
        source: str,
        endpoint: str,
        response_hash: str,
    ) -> SourceRecord | None:
        return await self.session.scalar(
            select(SourceRecord)
            .where(
                SourceRecord.source == source,
                SourceRecord.endpoint == endpoint,
                SourceRecord.response_hash == response_hash,
            )
            .order_by(SourceRecord.collected_at.desc())
            .limit(1)
        )


class CompanyRepository(BaseRepository[Company]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Company)

    async def find_by_cnpj(self, cnpj: str) -> Company | None:
        return await self.session.scalar(select(Company).where(Company.cnpj == cnpj))

    async def find_exact_name_candidates(
        self,
        normalized_name: str,
        *,
        uf: str | None = None,
    ) -> list[Company]:
        statement = select(Company).where(Company.normalized_name == normalized_name)
        if uf:
            statement = statement.where(or_(Company.uf == uf, Company.uf.is_(None)))
        return list((await self.session.scalars(statement)).all())

    @staticmethod
    def _stats_subquery():
        """Aggregate auditable participation outcomes per company."""

        return (
            select(
                Participant.company_id.label("company_id"),
                func.count(Participant.id).label("participations"),
                func.sum(
                    case(
                        (
                            Participant.status_code.in_(
                                (ParticipantStatus.WINNER, ParticipantStatus.AWARDED)
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label("awarded"),
                func.sum(
                    case((Participant.status_code == ParticipantStatus.DISQUALIFIED, 1), else_=0)
                ).label("disqualified"),
                func.sum(
                    case((Participant.status_code == ParticipantStatus.INELIGIBLE, 1), else_=0)
                ).label("ineligible"),
                func.max(Procurement.publication_at).label("last_participation_at"),
            )
            .join(Procurement, Participant.procurement_id == Procurement.id)
            .group_by(Participant.company_id)
            .subquery()
        )

    async def list_with_stats(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        search: str | None = None,
        uf: str | None = None,
    ) -> Page[tuple[Company, dict[str, object]]]:
        stats = self._stats_subquery()
        statement = (
            select(Company, stats)
            .outerjoin(stats, stats.c.company_id == Company.id)
            .order_by(func.coalesce(stats.c.participations, 0).desc(), Company.legal_name)
        )
        if search:
            term = f"%{search}%"
            statement = statement.where(
                or_(
                    Company.cnpj.like(f"%{''.join(char for char in search if char.isalnum())}%"),
                    Company.legal_name.ilike(term),
                    Company.trade_name.ilike(term),
                    Company.normalized_name.ilike(term),
                )
            )
        if uf:
            statement = statement.where(Company.uf == uf.upper())
        total = int(
            (
                await self.session.scalar(
                    select(func.count()).select_from(statement.order_by(None).subquery())
                )
            )
            or 0
        )
        rows = (
            await self.session.execute(statement.offset((page - 1) * page_size).limit(page_size))
        ).all()
        items = [(row[0], _stats_dict(row)) for row in rows]
        return Page(items=items, page=page, page_size=page_size, total=total)

    async def stats_for_company(self, company_id: UUID | str) -> dict[str, object]:
        stats = self._stats_subquery()
        row = (
            await self.session.execute(select(stats).where(stats.c.company_id == company_id))
        ).one_or_none()
        return _stats_dict(row)


def _stats_dict(row) -> dict[str, object]:
    if row is None:
        return {
            "participations": 0,
            "awarded": 0,
            "disqualified": 0,
            "ineligible": 0,
            "last_participation_at": None,
        }
    return {
        "participations": int(row.participations or 0),
        "awarded": int(row.awarded or 0),
        "disqualified": int(row.disqualified or 0),
        "ineligible": int(row.ineligible or 0),
        "last_participation_at": row.last_participation_at,
    }


class ParticipantRepository(BaseRepository[Participant]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Participant)

    async def list_for_company(
        self,
        company_id: UUID | str,
        *,
        page: int = 1,
        page_size: int = 50,
        statuses: list[str] | None = None,
        uf: str | None = None,
        agency: str | None = None,
        modality: str | None = None,
        procurement_type: str | None = None,
        is_srp: bool | None = None,
        value_min: Decimal | None = None,
        value_max: Decimal | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        active_only: bool = False,
        now: datetime | None = None,
    ) -> Page[Participant]:
        statement: Select[tuple[Participant]] = (
            select(Participant)
            .join(Procurement, Participant.procurement_id == Procurement.id)
            .where(Participant.company_id == company_id)
            .options(selectinload(Participant.procurement), selectinload(Participant.item))
        )
        if statuses:
            statement = statement.where(Participant.status_code.in_(statuses))
        if uf:
            statement = statement.where(Procurement.uf == uf.upper())
        if agency:
            statement = statement.where(Procurement.agency_name.ilike(f"%{agency}%"))
        if modality:
            modality_key = canonical_modality(modality)
            statement = statement.where(
                Procurement.modality_key == modality_key if modality_key is not None else false()
            )
        if procurement_type:
            statement = statement.where(Procurement.procurement_type == procurement_type)
        if is_srp is not None:
            statement = statement.where(Procurement.is_srp.is_(is_srp))
        if value_min is not None:
            statement = statement.where(Procurement.estimated_value >= value_min)
        if value_max is not None:
            statement = statement.where(Procurement.estimated_value <= value_max)
        if date_from:
            statement = statement.where(Procurement.publication_at >= date_from)
        if date_to:
            statement = statement.where(Procurement.publication_at <= date_to)
        if active_only:
            reference = now or datetime.now(UTC)
            statement = statement.where(*_status_category_condition("aberta", now=reference))
        statement = statement.order_by(
            Procurement.publication_at.desc().nullslast(), Participant.created_at.desc()
        )
        return await self.page(statement, page=page, page_size=page_size)


class EvidenceRepository(BaseRepository[Evidence]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Evidence)

    async def get_trace(self, evidence_id: UUID | str) -> Evidence | None:
        return await self.session.scalar(
            select(Evidence)
            .where(Evidence.id == evidence_id)
            .options(
                selectinload(Evidence.document),
                selectinload(Evidence.document_chunk),
                selectinload(Evidence.source_record),
            )
        )


class ProcurementEventRepository(BaseRepository[ProcurementEvent]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, ProcurementEvent)

    async def list_for_procurement(
        self,
        procurement_id: UUID | str,
        *,
        event_type: ProcurementEventType | None = None,
    ) -> list[ProcurementEvent]:
        statement = select(ProcurementEvent).where(
            ProcurementEvent.procurement_id == procurement_id
        )
        if event_type:
            statement = statement.where(ProcurementEvent.event_type == event_type)
        statement = statement.order_by(
            ProcurementEvent.occurred_at.desc(), ProcurementEvent.created_at.desc()
        )
        return list((await self.session.scalars(statement)).all())

    async def list_for_company(
        self,
        company_id: UUID | str,
        *,
        page: int = 1,
        page_size: int = 50,
        event_type: ProcurementEventType | None = None,
        reason_category: str | None = None,
        requires_manual_review: bool | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> Page[ProcurementEvent]:
        statement: Select[tuple[ProcurementEvent]] = (
            select(ProcurementEvent)
            .where(ProcurementEvent.company_id == company_id)
            .options(
                selectinload(ProcurementEvent.procurement),
                selectinload(ProcurementEvent.evidence),
            )
        )
        if event_type:
            statement = statement.where(ProcurementEvent.event_type == event_type)
        if reason_category:
            statement = statement.where(ProcurementEvent.reason_category == reason_category)
        if requires_manual_review is not None:
            statement = statement.where(
                ProcurementEvent.requires_manual_review.is_(requires_manual_review)
            )
        if date_from:
            statement = statement.where(ProcurementEvent.occurred_at >= date_from)
        if date_to:
            statement = statement.where(ProcurementEvent.occurred_at <= date_to)
        statement = statement.order_by(
            ProcurementEvent.occurred_at.desc().nullslast(),
            ProcurementEvent.created_at.desc(),
        )
        return await self.page(statement, page=page, page_size=page_size)
