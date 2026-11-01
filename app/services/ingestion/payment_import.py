"""Operator-controlled CSV/JSON payment import for non-federal portals."""

from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import io
import json
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.connectors.base import RawPayment
from app.database import async_session_factory
from app.models import DataAvailability, SourceRecord
from app.services.identifiers import is_valid_cnpj, normalize_cnpj
from app.services.ingestion.payments import PaymentIngestionService
from app.services.ingestion.pipeline import stable_fingerprint


@dataclass(slots=True)
class PaymentImportError:
    row: int
    message: str


@dataclass(slots=True)
class PaymentImportSummary:
    created: int = 0
    updated: int = 0
    linked: int = 0
    unlinked: int = 0
    errors: list[PaymentImportError] = field(default_factory=list)
    source_record_id: str | None = None
    changed: int = 0
    unchanged: int = 0
    stale_ignored: int = 0
    conflicted: int = 0
    diagnostic: str | None = None
    crawl_run_id: str | None = None


class PaymentImportErrorList(ValueError):
    """Raised when the file itself cannot be read."""


_ALIASES = {
    "external_id": ("external_id", "id_pagamento", "identificador_oficial"),
    "source_updated_at": ("source_updated_at", "data_atualizacao", "ultima_atualizacao"),
    "supplier_cnpj": ("supplier_cnpj", "cnpj", "cnpj_fornecedor", "ni_fornecedor"),
    "supplier_name": ("supplier_name", "fornecedor", "nome_fornecedor", "razao_social"),
    "agency_cnpj": ("agency_cnpj", "cnpj_orgao", "orgao_cnpj"),
    "agency_name": ("agency_name", "orgao", "nome_orgao"),
    "contract_number": ("contract_number", "contrato", "numero_contrato"),
    "process_number": ("process_number", "processo", "numero_processo"),
    "payment_date": ("payment_date", "data", "data_pagamento", "data_movimento"),
    "amount": ("amount", "valor", "valor_pago", "valor_documento"),
    "phase": ("phase", "fase"),
    "document_number": ("document_number", "documento", "numero_documento"),
    "commitment_number": ("commitment_number", "empenho", "numero_empenho"),
    "status": ("status", "situacao"),
    "source_url": ("source_url", "url", "link"),
}
_BRL = re.compile(r"^-?(?:\d{1,3}(?:\.\d{3})+|\d+)(?:,\d+)?$")


def _key(value: str) -> str:
    folded = unicodedata.normalize("NFKD", value or "")
    folded = "".join(char for char in folded if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", "_", folded.lower()).strip("_")


def _pick(row: dict[str, Any], field_name: str) -> str | None:
    for key in _ALIASES[field_name]:
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _amount(value: str | None) -> Decimal | None:
    if not value:
        return None
    text = value.replace("R$", "").strip()
    if _BRL.match(text):
        text = text.replace(".", "").replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _date(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    for pattern in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, pattern).replace(tzinfo=UTC)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.replace(tzinfo=parsed.tzinfo or UTC).astimezone(UTC)


class PaymentImportService:
    """Import a portal export while preserving the original row as provenance."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        ingestion: PaymentIngestionService | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.session_factory = session_factory or async_session_factory
        self.ingestion = ingestion or PaymentIngestionService(
            self.settings, session_factory=self.session_factory
        )

    async def import_bytes(
        self,
        content: bytes,
        *,
        filename: str,
        source_name: str,
    ) -> PaymentImportSummary:
        async with self.ingestion.ingestion_lease():
            return await self._import_bytes(content, filename=filename, source_name=source_name)

    async def _import_bytes(
        self, content: bytes, *, filename: str, source_name: str
    ) -> PaymentImportSummary:
        if len(content) > 10 * 1024 * 1024:
            raise PaymentImportErrorList("arquivo maior que 10 MB")
        rows = await asyncio.to_thread(self._read_rows, content, filename)
        payments, raw_rows, errors = await asyncio.to_thread(
            self._parse_rows, rows, filename, source_name
        )
        summary = PaymentImportSummary(errors=errors)
        if raw_rows or not errors:
            source = await self._source_record(content, filename, source_name, raw_rows)
            summary.source_record_id = str(source)
            from app.connectors.base import ConnectorResult

            async def fetch():
                rows = [row.model_copy(update={"source_record_id": source}) for row in payments]
                return ConnectorResult(
                    data=rows,
                    availability=DataAvailability.AVAILABLE
                    if rows or errors
                    else DataAvailability.EMPTY,
                    partial=bool(errors),
                )

            persisted = await self.ingestion.collect_result(
                {
                    "connector": "payments:manual",
                    "mode": "payment_import",
                    "source": "manual",
                    "source_name": source_name,
                    "filename": filename,
                    "file_hash": hashlib.sha256(content).hexdigest(),
                },
                fetch,
            )
            summary.crawl_run_id = str(persisted.crawl_run_id) if persisted.crawl_run_id else None
            summary.created = persisted.payments_created
            summary.updated = persisted.payments_updated
            summary.changed = persisted.payments_changed
            summary.unchanged = persisted.payments_unchanged
            summary.stale_ignored = persisted.payments_stale_ignored
            summary.conflicted = persisted.payments_conflicted
            if any(not _pick(row, "external_id") for row in raw_rows):
                summary.diagnostic = "Linhas sem identificador oficial usam o conteúdo completo para deduplicação; alterações podem gerar outro registro e exigem revisão."
            summary.linked = persisted.payments_linked
            summary.unlinked = persisted.payments_unlinked
        return summary

    def _parse_rows(
        self,
        rows: list[dict[str, Any]],
        filename: str,
        source_name: str,
    ) -> tuple[list[RawPayment], list[dict[str, Any]], list[PaymentImportError]]:
        payments: list[RawPayment] = []
        raw_rows: list[dict[str, Any]] = []
        errors: list[PaymentImportError] = []
        start = 2 if filename.lower().endswith(".csv") else 1
        for index, row in enumerate(rows, start=start):
            normalized = {_key(key): value for key, value in row.items() if key is not None}
            raw_rows.append(normalized)
            payment, error = self._row_payment(normalized, index=index, source_name=source_name)
            if error:
                errors.append(error)
                continue
            assert payment is not None
            payments.append(payment)
        return payments, raw_rows, errors

    def _read_rows(self, content: bytes, filename: str) -> list[dict[str, Any]]:
        suffix = Path(filename).suffix.lower()
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            if suffix == ".json":
                raise PaymentImportErrorList("JSON deve estar em UTF-8") from exc
            text = content.decode("latin-1")
        if suffix == ".json":
            try:
                payload = json.loads(text)
            except ValueError as exc:
                raise PaymentImportErrorList("JSON invalido") from exc
            if isinstance(payload, dict):
                payload = payload.get("payments", payload.get("items", []))
            if not isinstance(payload, list):
                raise PaymentImportErrorList("JSON deve conter uma lista de pagamentos")
            if any(not isinstance(row, dict) for row in payload):
                raise PaymentImportErrorList("Cada linha do JSON deve ser um objeto")
            return payload
        sample = text[:4096]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(io.StringIO(text), dialect=dialect)
        if not reader.fieldnames:
            raise PaymentImportErrorList("CSV sem cabecalho")
        return [dict(row) for row in reader]

    def _row_payment(
        self,
        row: dict[str, Any],
        *,
        index: int,
        source_name: str,
    ) -> tuple[RawPayment | None, PaymentImportError | None]:
        raw_cnpj = normalize_cnpj(_pick(row, "supplier_cnpj"))
        supplier_name = _pick(row, "supplier_name")
        cnpj = raw_cnpj if raw_cnpj and is_valid_cnpj(raw_cnpj) else None
        if cnpj is None and not supplier_name:
            return None, PaymentImportError(index, "informe CNPJ valido ou nome do fornecedor")
        amount = _amount(_pick(row, "amount"))
        if amount is None:
            return None, PaymentImportError(index, "valor invalido ou ausente")
        agency_cnpj = normalize_cnpj(_pick(row, "agency_cnpj"))
        if agency_cnpj and not is_valid_cnpj(agency_cnpj):
            return None, PaymentImportError(index, "CNPJ do orgao invalido")
        date = _date(_pick(row, "payment_date"))
        external_seed = json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)
        external_id = hashlib.sha256(f"{source_name}:{external_seed}".encode()).hexdigest()[:32]
        official_id = _pick(row, "external_id")
        if official_id:
            external_id = hashlib.sha256(
                f"official:{source_name}:{official_id}".encode()
            ).hexdigest()[:32]
        return (
            RawPayment(
                source="manual",
                observed_at=datetime.now(UTC),
                source_updated_at=_date(_pick(row, "source_updated_at")),
                source_update_field=next(
                    (name for name in _ALIASES["source_updated_at"] if row.get(name)), None
                ),
                source_url=_pick(row, "source_url"),
                raw_payload=row,
                external_id=external_id,
                contract_number=_pick(row, "contract_number"),
                agency_name=_pick(row, "agency_name"),
                agency_cnpj=agency_cnpj,
                process_number=_pick(row, "process_number"),
                payment_date=date,
                amount=amount,
                phase=_pick(row, "phase") or "Pagamento",
                document_number=_pick(row, "document_number"),
                commitment_number=_pick(row, "commitment_number"),
                status=_pick(row, "status"),
                supplier_cnpj=cnpj,
                supplier_name=_pick(row, "supplier_name"),
            ),
            None,
        )

    async def _source_record(
        self,
        content: bytes,
        filename: str,
        source_name: str,
        rows: list[dict[str, Any]],
    ) -> UUID:
        digest = hashlib.sha256(content).hexdigest()
        async with self.session_factory() as session, session.begin():
            await self.ingestion.assert_ingestion_fence(session)
            fingerprint = stable_fingerprint("manual_payment_source_v2", source_name, digest)
            existing = await session.scalar(
                select(SourceRecord).where(SourceRecord.fingerprint == fingerprint)
            )
            if existing is not None:
                return existing.id
            entity = SourceRecord(
                crawl_run_id=None,
                source="manual",
                endpoint=f"manual://payments/{source_name}/{filename}",
                request_parameters={"filename": filename, "source": source_name},
                response_hash=digest,
                raw_payload={
                    "rows": rows,
                    "content_base64": base64.b64encode(content).decode("ascii"),
                },
                http_status=None,
                availability=DataAvailability.AVAILABLE,
                collected_at=datetime.now(UTC),
                diagnostic="arquivo importado por operador",
                fingerprint=fingerprint,
            )
            session.add(entity)
            await session.flush()
            return entity.id

    async def aclose(self) -> None:
        await self.ingestion.aclose()
