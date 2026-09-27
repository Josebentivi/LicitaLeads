"""Repositories for canonical procurements and their traceable source records."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import Select, false, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import (
    Company,
    Evidence,
    Procurement,
    ProcurementEvent,
    ProcurementSource,
    SourceRecord,
)
from app.models.enums import ProcurementEventType
from app.repositories.base import BaseRepository, Page
from app.services.identifiers import canonical_modality


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
        published_from: datetime | None = None,
        published_to: datetime | None = None,
        status: str | None = None,
    ) -> Page[Procurement]:
        statement: Select[tuple[Procurement]] = select(Procurement)
        if uf:
            statement = statement.where(Procurement.uf == uf.upper())
        if municipality:
            statement = statement.where(Procurement.municipality.ilike(f"%{municipality}%"))
        if agency:
            statement = statement.where(Procurement.agency_name.ilike(f"%{agency}%"))
        if modality:
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
