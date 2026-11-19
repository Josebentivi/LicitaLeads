"""Persist declared corporate details with an auditable manual source."""

from datetime import UTC, datetime
from decimal import Decimal
from ipaddress import ip_address
from urllib.parse import urlsplit

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Company,
    CompanyContact,
    ContactStatus,
    ContactType,
    FieldObservation,
    SourceRecord,
)
from app.services.contacts.domains import is_valid_email, valid_dns_name
from app.services.contacts.identity import contact_identity, phone_identity
from app.services.identifiers import normalize_company_name
from app.services.ingestion.pipeline import stable_fingerprint
from app.services.public_source_links import public_link
from app.services.write_admission import admit_before_row_lock


def _website_domain(url: str) -> str:
    error = "Informe um endereço público válido para o site corporativo."
    if not public_link(url):
        raise ValueError(error)
    host = (urlsplit(url).hostname or "").rstrip(".")
    if len(host) > 253 or "%" in host:
        raise ValueError(error)
    if valid_dns_name(host):
        return host.encode("idna").decode("ascii").removeprefix("www.")
    try:
        return str(ip_address(host))
    except ValueError as exc:
        raise ValueError(error) from exc


async def begin_corporate_write(db: AsyncSession) -> None:
    if db.get_bind().dialect.name == "sqlite":
        await db.execute(text("BEGIN IMMEDIATE"))
    else:
        await admit_before_row_lock(db)


async def save_corporate_name(
    db: AsyncSession, company: Company, name: str, source_url: str
) -> None:
    name = name.strip()
    if not name or len(name) > 500:
        raise ValueError("Informe a razão social com até 500 caracteres.")
    existing_name = (company.legal_name or "").strip()
    if existing_name == name:
        return
    if existing_name:
        raise ValueError("A razão social já está cadastrada.")
    source = await _manual_source(db, source_url, {"cnpj": company.cnpj, "legal_name": name})
    company.legal_name = name
    company.normalized_name = normalize_company_name(name) or name
    db.add(
        FieldObservation(
            entity_type="company",
            entity_id=company.id,
            field_name="legal_name",
            value=name,
            source="manual",
            source_record_id=source.id,
            confidence=Decimal("1"),
        )
    )


async def save_corporate_contact(
    db: AsyncSession,
    company: Company,
    contact_type: ContactType,
    value: str,
    source_url: str,
) -> bool:
    """Return whether the verified contact was created."""
    value = value.strip()
    if not value or len(value) > 1000:
        raise ValueError("Informe um contato com até 1.000 caracteres.")
    if contact_type == ContactType.EMAIL and not is_valid_email(value):
        raise ValueError("Informe um endereço de e-mail válido.")
    if contact_type in {ContactType.PHONE, ContactType.WHATSAPP} and phone_identity(value) is None:
        raise ValueError("Informe um número de telefone válido.")
    website_url = None
    website_domain = None
    if contact_type == ContactType.WEBSITE:
        website_url = value if "://" in value else f"https://{value}"
        website_domain = _website_domain(website_url)
    source = await _manual_source(
        db,
        source_url,
        {
            "cnpj": company.cnpj,
            "contact_type": contact_type.value,
            "contact_value": value,
            "is_corporate": True,
        },
    )
    query = select(CompanyContact).where(
        CompanyContact.company_id == company.id, CompanyContact.contact_type == contact_type
    )
    if contact_type in {ContactType.EMAIL, ContactType.PHONE, ContactType.WHATSAPP}:
        matches = await db.scalars(
            query.order_by(
                (CompanyContact.contact_value == value).desc(),
                CompanyContact.source_record_id.is_not(None).desc(),
                CompanyContact.id,
            )
        )
        identity = contact_identity(contact_type, value)
        contact = next(
            (
                row
                for row in matches
                if contact_identity(contact_type, row.contact_value) == identity
            ),
            None,
        )
    else:
        contact = await db.scalar(query.where(CompanyContact.contact_value == value))
    created = contact is None
    if contact is None:
        contact = CompanyContact(
            company_id=company.id, contact_type=contact_type, contact_value=value
        )
        db.add(contact)
    contact.contact_value = value
    if contact.verified_at is None or contact.source_record_id != source.id:
        contact.verified_at = datetime.now(UTC)
    contact.source_record_id = source.id
    contact.source_url = source.endpoint
    contact.is_corporate = True
    contact.is_personal = False
    contact.confidence = Decimal("1")
    contact.status = ContactStatus.VERIFIED
    if website_url and website_domain:
        company.website = website_url
        company.domain = website_domain
    await db.flush()
    return created


async def _manual_source(
    db: AsyncSession, source_url: str, attributes: dict[str, object]
) -> SourceRecord:
    source_url = source_url.strip()
    if not public_link(source_url):
        raise ValueError("Informe uma URL pública HTTP(S) como origem.")
    identity = {**attributes, "source_url": source_url}
    fingerprint = stable_fingerprint("manual_corporate_detail", identity)
    source = await db.scalar(select(SourceRecord).where(SourceRecord.fingerprint == fingerprint))
    if source is None:
        source = SourceRecord(
            source="manual",
            endpoint=source_url,
            raw_payload=identity,
            response_hash=fingerprint,
            fingerprint=fingerprint,
            diagnostic="Dado corporativo informado pelo operador",
        )
        db.add(source)
        await db.flush()
    return source
