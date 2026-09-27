"""Database adapter for conservative public corporate-contact enrichment."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.database import async_session_factory
from app.models import Company, CompanyContact, ContactStatus, ContactType
from app.services.contacts import (
    CompanyContactTarget,
    ContactEnrichmentProvider,
    PublicWebsiteContactProvider,
)
from app.services.ingestion.pipeline import stable_fingerprint


@dataclass(frozen=True, slots=True)
class ContactEnrichmentSummary:
    companies_checked: int = 0
    contacts_created: int = 0
    contacts_updated: int = 0
    errors: int = 0


class ContactEnrichmentService:
    """Enrich only companies with an already evidenced corporate domain."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        provider: ContactEnrichmentProvider | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.session_factory = session_factory or async_session_factory
        self.provider = provider or PublicWebsiteContactProvider(
            user_agent=self.settings.http_user_agent,
            max_pages=5,
        )

    async def run(self, *, force: bool = False) -> ContactEnrichmentSummary:
        if not self.settings.contact_search_enabled and not force:
            return ContactEnrichmentSummary()
        async with self.session_factory() as session:
            companies = list(
                (
                    await session.scalars(
                        select(Company)
                        .where(Company.domain.is_not(None))
                        .order_by(Company.created_at)
                    )
                ).all()
            )
        checked = created = updated = errors = 0
        for company in companies:
            checked += 1
            try:
                candidates = await self.provider.find_contacts(
                    CompanyContactTarget(
                        company_id=str(company.id),
                        legal_name=company.legal_name or company.normalized_name,
                        cnpj=company.cnpj,
                        website=company.website,
                        domain=company.domain,
                    )
                )
                async with self.session_factory() as session, session.begin():
                    for candidate in candidates:
                        if not candidate.is_corporate or candidate.is_personal:
                            continue
                        existing = await session.scalar(
                            select(CompanyContact).where(
                                CompanyContact.company_id == company.id,
                                CompanyContact.contact_type
                                == ContactType(candidate.contact_type.value),
                                CompanyContact.contact_value == candidate.contact_value,
                            )
                        )
                        if existing is None:
                            session.add(
                                CompanyContact(
                                    company_id=company.id,
                                    contact_type=ContactType(candidate.contact_type.value),
                                    contact_value=candidate.contact_value,
                                    source_url=candidate.source_url,
                                    is_corporate=True,
                                    is_personal=False,
                                    confidence=Decimal(str(candidate.confidence)),
                                    status=(
                                        ContactStatus.VERIFIED
                                        if candidate.confidence >= 0.9
                                        else ContactStatus.DISCOVERED
                                    ),
                                    fingerprint=stable_fingerprint(
                                        "contact",
                                        company.id,
                                        candidate.contact_type.value,
                                        candidate.contact_value.lower(),
                                    ),
                                )
                            )
                            created += 1
                        else:
                            existing.source_url = candidate.source_url
                            existing.confidence = max(
                                existing.confidence,
                                Decimal(str(candidate.confidence)),
                            )
                            existing.is_corporate = True
                            existing.is_personal = False
                            updated += 1
            except Exception:
                errors += 1
        return ContactEnrichmentSummary(checked, created, updated, errors)
