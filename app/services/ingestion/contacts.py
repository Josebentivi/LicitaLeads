"""Database adapter for conservative public corporate-contact enrichment."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.database import async_session_factory, write_session
from app.models import Company, CompanyContact, ContactStatus, ContactType
from app.services.contacts import (
    CompanyContactTarget,
    ContactEnrichmentProvider,
    PublicWebsiteContactProvider,
)
from app.services.contacts.identity import contact_identity
from app.services.ingestion.pipeline import stable_fingerprint
from app.services.write_admission import admit_before_row_lock


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
                company_created = company_updated = 0
                async with write_session(self.session_factory) as session:
                    await admit_before_row_lock(session)
                    current = (
                        await session.execute(
                            select(Company.domain, Company.website)
                            .where(Company.id == company.id)
                            .with_for_update()
                        )
                    ).one_or_none()
                    if current is None or (current.domain, current.website) != (
                        company.domain,
                        company.website,
                    ):
                        continue
                    stored = await session.scalars(
                        select(CompanyContact)
                        .where(CompanyContact.company_id == company.id)
                        .order_by(
                            CompanyContact.source_record_id.is_not(None).desc(), CompanyContact.id
                        )
                    )
                    contacts: dict[tuple[ContactType, str], CompanyContact] = {}
                    for row in stored:
                        contacts.setdefault(
                            (
                                row.contact_type,
                                contact_identity(row.contact_type, row.contact_value),
                            ),
                            row,
                        )
                    for candidate in candidates:
                        if not candidate.is_corporate or candidate.is_personal:
                            continue
                        kind = ContactType(candidate.contact_type.value)
                        identity = contact_identity(kind, candidate.contact_value)
                        key = kind, identity
                        existing = contacts.get(key)
                        if existing is None:
                            existing = CompanyContact(
                                company_id=company.id,
                                contact_type=kind,
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
                                    "contact_identity", company.id, kind.value, identity
                                ),
                            )
                            session.add(existing)
                            contacts[key] = existing
                            company_created += 1
                        elif existing.source_record_id is None:
                            existing.source_url = candidate.source_url
                            existing.confidence = max(
                                existing.confidence,
                                Decimal(str(candidate.confidence)),
                            )
                            existing.is_corporate = True
                            existing.is_personal = False
                            company_updated += 1
                created += company_created
                updated += company_updated
            except Exception:
                errors += 1
        return ContactEnrichmentSummary(checked, created, updated, errors)
