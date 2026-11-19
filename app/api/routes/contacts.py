"""Manual, auditable corporate-contact import."""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from itertools import batched

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db
from app.models import Company, ContactType
from app.services.contacts.manual import begin_corporate_write, save_corporate_contact
from app.services.identifiers import is_valid_cnpj, normalize_cnpj

router = APIRouter(prefix="/contacts", tags=["contacts"])

REQUIRED_COLUMNS = {"cnpj", "contact_type", "contact_value", "source_url", "is_corporate"}


@dataclass(frozen=True, slots=True)
class _ContactRow:
    cnpj: str
    contact_type: ContactType
    value: str
    source_url: str


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "sim"}:
        return True
    if normalized in {"0", "false", "no", "nao", "não", ""}:
        return False
    raise ValueError("is_corporate must be true or false")


def _parse_row(row: dict[str, str | None]) -> _ContactRow:
    cnpj = normalize_cnpj(row.get("cnpj"))
    if not cnpj or not is_valid_cnpj(cnpj):
        raise ValueError("invalid CNPJ")
    try:
        contact_type = ContactType((row.get("contact_type") or "").strip().lower())
    except ValueError as exc:
        raise ValueError("unsupported contact_type") from exc
    if not _parse_bool(row.get("is_corporate") or ""):
        raise ValueError("only explicitly corporate contacts may be imported")
    return _ContactRow(
        cnpj,
        contact_type,
        (row.get("contact_value") or "").strip(),
        (row.get("source_url") or "").strip(),
    )


async def _lock_companies(db: AsyncSession, cnpjs: set[str]) -> dict[str, Company]:
    # Keep the database's lock order identical to the batch order on PostgreSQL.
    order = (
        Company.cnpj.collate("C") if db.get_bind().dialect.name == "postgresql" else Company.cnpj
    )
    companies: dict[str, Company] = {}
    for batch in batched(sorted(cnpjs), 500):
        matches = await db.scalars(
            select(Company).where(Company.cnpj.in_(batch)).order_by(order).with_for_update()
        )
        companies.update({company.cnpj: company for company in matches if company.cnpj is not None})
    return companies


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

    parsed: list[tuple[int, _ContactRow]] = []
    errors: dict[int, str] = {}
    for row_number, raw_row in enumerate(reader, start=2):
        try:
            parsed.append((row_number, _parse_row(raw_row)))
        except ValueError as exc:
            errors[row_number] = str(exc)
    companies = {}
    if parsed:
        await begin_corporate_write(db)
        companies = await _lock_companies(db, {row.cnpj for _, row in parsed})
    created = updated = 0
    for row_number, row in parsed:
        try:
            company = companies.get(row.cnpj)
            if company is None:
                raise ValueError("company not found for CNPJ")
            if await save_corporate_contact(
                db, company, row.contact_type, row.value, row.source_url
            ):
                created += 1
            else:
                updated += 1
        except ValueError as exc:
            errors[row_number] = str(exc)
    await db.commit()
    return {
        "created": created,
        "updated": updated,
        "skipped": len(errors),
        "errors": [{"row": line, "message": message} for line, message in sorted(errors.items())],
    }
