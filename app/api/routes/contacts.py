"""Manual, auditable corporate-contact import."""

from __future__ import annotations

import csv
import io
from decimal import Decimal
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db
from app.models import Company, CompanyContact, ContactStatus, ContactType
from app.services.identifiers import is_valid_cnpj, normalize_cnpj

router = APIRouter(prefix="/contacts", tags=["contacts"])

REQUIRED_COLUMNS = {"cnpj", "contact_type", "contact_value", "source_url", "is_corporate"}


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "sim"}:
        return True
    if normalized in {"0", "false", "no", "nao", "não", ""}:
        return False
    raise ValueError("is_corporate must be true or false")


@router.post("/import")
async def import_contacts(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """Import corporate contacts from a bounded UTF-8 CSV file."""
    raw = await file.read(2 * 1024 * 1024 + 1)
    if len(raw) > 2 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="CSV exceeds the 2 MB limit")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail="CSV must be UTF-8 encoded") from exc
    reader = csv.DictReader(io.StringIO(text))
    columns = set(reader.fieldnames or [])
    missing = REQUIRED_COLUMNS - columns
    if missing:
        raise HTTPException(
            status_code=400, detail=f"Missing columns: {', '.join(sorted(missing))}"
        )

    created = updated = skipped = 0
    errors: list[dict[str, object]] = []
    for row_number, row in enumerate(reader, start=2):
        try:
            cnpj = normalize_cnpj(row.get("cnpj"))
            if not cnpj or not is_valid_cnpj(cnpj):
                raise ValueError("invalid CNPJ")
            try:
                contact_type = ContactType((row.get("contact_type") or "").strip().lower())
            except ValueError as exc:
                raise ValueError("unsupported contact_type") from exc
            value = (row.get("contact_value") or "").strip()
            if not value:
                raise ValueError("contact_value is required")
            source_url = (row.get("source_url") or "").strip()
            if urlparse(source_url).scheme not in {"http", "https"}:
                raise ValueError("source_url must be an HTTP(S) public evidence URL")
            is_corporate = _parse_bool(row.get("is_corporate") or "")
            if not is_corporate:
                raise ValueError("only explicitly corporate contacts may be imported")
            website_url: str | None = None
            website_domain: str | None = None
            if contact_type == ContactType.WEBSITE:
                website_url = value if "://" in value else f"https://{value}"
                parsed = urlparse(website_url)
                if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                    raise ValueError("website contact must contain a valid HTTP(S) domain")
                website_domain = parsed.hostname.lower().removeprefix("www.")
            company = await db.scalar(select(Company).where(Company.cnpj == cnpj))
            if company is None:
                skipped += 1
                errors.append({"row": row_number, "message": "company not found for CNPJ"})
                continue
            existing = await db.scalar(
                select(CompanyContact).where(
                    CompanyContact.company_id == company.id,
                    CompanyContact.contact_type == contact_type,
                    CompanyContact.contact_value == value,
                )
            )
            if existing:
                existing.source_url = source_url
                existing.is_corporate = True
                existing.is_personal = False
                existing.confidence = Decimal("1")
                existing.status = ContactStatus.VERIFIED
                updated += 1
            else:
                db.add(
                    CompanyContact(
                        company_id=company.id,
                        contact_type=contact_type,
                        contact_value=value,
                        source_url=source_url,
                        is_corporate=True,
                        is_personal=False,
                        confidence=Decimal("1"),
                        status=ContactStatus.VERIFIED,
                    )
                )
                created += 1
            if website_url and website_domain:
                company.website = website_url
                company.domain = website_domain
        except ValueError as exc:
            skipped += 1
            errors.append({"row": row_number, "message": str(exc)})
    await db.commit()
    return {"created": created, "updated": updated, "skipped": skipped, "errors": errors}
