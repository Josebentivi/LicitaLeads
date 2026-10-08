"""Lead, review, contact, and outreach persistence operations."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import Select, exists, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import (
    Company,
    CompanyContact,
    Deadline,
    Lead,
    LeadReview,
    OutreachDraft,
    ProcurementEvent,
)
from app.models.enums import DeadlineStatus, LeadStatus, ProcurementEventType
from app.repositories.base import BaseRepository, Page


class LeadRepository(BaseRepository[Lead]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Lead)

    async def get_detail(self, lead_id: UUID | str) -> Lead | None:
        return await self.session.scalar(
            select(Lead)
            .where(Lead.id == lead_id)
            .options(
                selectinload(Lead.procurement),
                selectinload(Lead.company).selectinload(Company.contacts),
                selectinload(Lead.triggering_event).selectinload(ProcurementEvent.evidence),
                selectinload(Lead.deadline),
                selectinload(Lead.reviews),
                selectinload(Lead.outreach_drafts),
            )
        )

    async def list_filtered(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        status: LeadStatus | None = None,
        minimum_score: int | None = None,
        event_type: ProcurementEventType | None = None,
        deadline_status: DeadlineStatus | None = None,
        has_contact: bool | None = None,
        pending_review: bool | None = None,
        created_from: datetime | None = None,
        created_to: datetime | None = None,
    ) -> Page[Lead]:
        statement: Select[tuple[Lead]] = select(Lead)
        if event_type is not None:
            statement = statement.join(Lead.triggering_event).where(
                ProcurementEvent.event_type == event_type
            )
        if deadline_status is not None:
            statement = statement.join(Lead.deadline).where(Deadline.status == deadline_status)
        if status is not None:
            statement = statement.where(Lead.lead_status == status)
        if minimum_score is not None:
            statement = statement.where(Lead.score >= minimum_score)
        if pending_review is True:
            statement = statement.where(Lead.lead_status == LeadStatus.PENDING_REVIEW)
        elif pending_review is False:
            statement = statement.where(Lead.lead_status != LeadStatus.PENDING_REVIEW)
        if created_from:
            statement = statement.where(Lead.created_at >= created_from)
        if created_to:
            statement = statement.where(Lead.created_at <= created_to)
        if has_contact is not None:
            contact_exists = exists(
                select(CompanyContact.id).where(
                    CompanyContact.company_id == Lead.company_id,
                    CompanyContact.is_corporate.is_(True),
                )
            )
            statement = statement.where(contact_exists if has_contact else ~contact_exists)
        statement = statement.order_by(Lead.score.desc(), Lead.created_at.desc(), Lead.id)
        return await self.page(statement, page=page, page_size=page_size)


class LeadReviewRepository(BaseRepository[LeadReview]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, LeadReview)


class OutreachDraftRepository(BaseRepository[OutreachDraft]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, OutreachDraft)

    async def find_idempotent(
        self,
        *,
        lead_id: UUID | str,
        channel: str,
        facts_hash: str,
        template_hash: str,
    ) -> OutreachDraft | None:
        return await self.session.scalar(
            select(OutreachDraft)
            .where(
                OutreachDraft.lead_id == lead_id,
                OutreachDraft.channel == channel,
                OutreachDraft.facts_hash == facts_hash,
                OutreachDraft.template_hash == template_hash,
            )
            .order_by(OutreachDraft.created_at.desc())
            .limit(1)
        )


class CompanyContactRepository(BaseRepository[CompanyContact]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, CompanyContact)

    async def find_identity(
        self,
        *,
        company_id: UUID | str,
        contact_type: str,
        contact_value: str,
    ) -> CompanyContact | None:
        return await self.session.scalar(
            select(CompanyContact).where(
                CompanyContact.company_id == company_id,
                CompanyContact.contact_type == contact_type,
                CompanyContact.contact_value == contact_value,
            )
        )

    async def list_for_company(
        self,
        company_id: UUID | str,
        *,
        corporate_only: bool = False,
    ) -> list[CompanyContact]:
        """List contacts with the most trustworthy rows first."""

        statement = select(CompanyContact).where(CompanyContact.company_id == company_id)
        if corporate_only:
            statement = statement.where(CompanyContact.is_corporate.is_(True))
        statement = statement.order_by(
            CompanyContact.confidence.desc().nullslast(),
            CompanyContact.contact_type,
            CompanyContact.contact_value,
        )
        return list((await self.session.scalars(statement)).all())
