"""Deterministic, backup-gated repairs for legacy canonical data."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.pncp import parse_pncp_external_id
from app.connectors.transparencia import payment_external_id
from app.models import Document, ExtractionStatus, Payment, Procurement, SourceRecord
from app.services.backups import BackupReceipt
from app.services.identifiers import normalize_cnpj
from app.services.ingestion.pipeline import stable_fingerprint


@dataclass(slots=True)
class ReconciliationReport:
    inspected_documents: int = 0
    deactivated_documents: int = 0
    reactivated_documents: int = 0
    inspected_federal_payments: int = 0
    repaired_payment_ids: int = 0
    identity_collisions: int = 0
    procurements_without_uasg: int = 0
    diagnostics: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "inspected_documents": self.inspected_documents,
            "deactivated_documents": self.deactivated_documents,
            "reactivated_documents": self.reactivated_documents,
            "inspected_federal_payments": self.inspected_federal_payments,
            "repaired_payment_ids": self.repaired_payment_ids,
            "identity_collisions": self.identity_collisions,
            "procurements_without_uasg": self.procurements_without_uasg,
            "diagnostics": self.diagnostics,
        }


def _rows_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "documentos", "resultado", "arquivos"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    return []


def _document_operation_identity(record: SourceRecord) -> tuple[str, int, int] | None:
    """Only the PNCP procurement-file operation can change document activity."""
    if record.source != "pncp":
        return None
    match = re.search(
        r"/orgaos/([A-Z0-9]{14})/compras/(\d{4})/(\d+)/arquivos/?$",
        urlsplit(record.endpoint).path,
        re.IGNORECASE,
    )
    if match is None:
        return None
    return match.group(1).upper(), int(match.group(2)), int(match.group(3))


def _procurement_document_identity(procurement: Procurement) -> tuple[str, int, int] | None:
    for value in (procurement.pncp_control_number, procurement.external_id):
        if value:
            try:
                reference = parse_pncp_external_id(value)
                return reference.cnpj, reference.year, reference.sequence
            except ValueError:
                continue
    return None


async def reconcile_data(
    session: AsyncSession,
    *,
    apply: bool = False,
    backup: BackupReceipt | None = None,
    maintenance_owner: str | None = None,
) -> ReconciliationReport:
    """Repair facts that can be derived unambiguously from stored raw responses.

    Ambiguous UASG merges are reported but never split automatically.
    """

    if apply:
        if backup is None:
            raise ValueError("backup verificado obrigatório antes de aplicar reparos")
        if maintenance_owner is None:
            raise ValueError("manutenção exclusiva obrigatória antes de aplicar reparos")
        from app.services.maintenance import fence_maintenance

        session.info["maintenance_owner"] = maintenance_owner
        await fence_maintenance(session, maintenance_owner)
        await asyncio.to_thread(backup.verify, session.get_bind().engine.url, maintenance_owner)
    report = ReconciliationReport()

    records = list(
        (
            await session.scalars(
                select(SourceRecord).order_by(SourceRecord.collected_at, SourceRecord.id)
            )
        ).all()
    )
    document_states: dict[tuple[str, tuple[str, int, int], str], tuple[bool, datetime | None]] = {}
    for record in records:
        if record.collection_complete is not True:
            continue
        identity = _document_operation_identity(record)
        if identity is None:
            continue
        for row in _rows_from_payload(record.raw_payload):
            url = row.get("url") or row.get("uri")
            if not url or not isinstance(row.get("statusAtivo"), bool):
                continue
            active = row.get("statusAtivo") is not False
            document_states[(record.source, identity, str(url))] = (
                active,
                None if active else record.collected_at or datetime.now(UTC),
            )

    documents = list((await session.scalars(select(Document))).all())
    report.inspected_documents = len(documents)
    for document in documents:
        procurement = await session.get(Procurement, document.procurement_id)
        identity = _procurement_document_identity(procurement) if procurement else None
        if identity is None:
            continue
        if document.source_record_id:
            linked_source = await session.get(SourceRecord, document.source_record_id)
            if linked_source is not None and linked_source.source != "pncp":
                continue
        state = document_states.get(("pncp", identity, document.original_url))
        if state is None:
            continue
        active, deactivated_at = state
        if active and not document.is_active:
            report.reactivated_documents += 1
            if apply:
                document.is_active = True
                document.deactivated_at = None
                if document.extraction_status is ExtractionStatus.UNSUPPORTED:
                    document.extraction_status = ExtractionStatus.PENDING
                    document.extraction_error = None
                    document.next_attempt_at = None
                if await session.scalar(
                    select(Document.id).where(Document.parent_document_id == document.id).limit(1)
                ):
                    document.extraction_status = ExtractionStatus.PENDING
        elif not active and document.is_active:
            report.deactivated_documents += 1
            if apply:
                document.is_active = False
                document.deactivated_at = deactivated_at or datetime.now(UTC)
                if document.extraction_status is ExtractionStatus.PENDING:
                    document.extraction_status = ExtractionStatus.UNSUPPORTED
                from app.services.ingestion.processor import suspend_document_derivatives

                await suspend_document_derivatives(session, document.id)

    federal_payments = list(
        (await session.scalars(select(Payment).where(Payment.source == "transparencia"))).all()
    )
    report.inspected_federal_payments = len(federal_payments)
    for payment in federal_payments:
        if payment.source_record_id is None:
            continue
        source = await session.get(SourceRecord, payment.source_record_id)
        if source is None:
            continue
        rows = _rows_from_payload(source.raw_payload)
        candidates = []
        from app.connectors.transparencia import TransparenciaPaymentsConnector

        connector = TransparenciaPaymentsConnector.__new__(TransparenciaPaymentsConnector)
        connector.timezone = "America/Sao_Paulo"
        for row in rows:
            if str(row.get("documento") or row.get("documentoResumido") or "") != (
                payment.document_number or ""
            ):
                continue
            if (
                not payment.supplier_cnpj
                or normalize_cnpj(str(row.get("codigoFavorecido") or "")) != payment.supplier_cnpj
            ):
                continue
            mapped = connector._map_payment(row, source.endpoint)
            if payment.payment_date and (
                not mapped.payment_date or payment.payment_date.date() != mapped.payment_date.date()
            ):
                continue
            discriminators = ("phase", "agency_cnpj", "uasg")
            if any(
                getattr(payment, key, None) is not None
                and str(getattr(payment, key)).casefold()
                != str(getattr(mapped, key, "")).casefold()
                for key in discriminators
            ):
                continue
            candidates.append(row)
        identities = {payment_external_id(row) for row in candidates}
        if len(identities) != 1:
            if candidates:
                report.diagnostics.append(
                    f"pagamento {payment.id}: evidência ambígua; revisão manual"
                )
            continue
        match = candidates[0]
        new_external_id = payment_external_id(match)
        if new_external_id == payment.external_id:
            continue
        collision = await session.scalar(
            select(Payment.id).where(
                Payment.source == payment.source,
                Payment.external_id == new_external_id,
                Payment.id != payment.id,
            )
        )
        if collision is not None:
            report.identity_collisions += 1
            report.diagnostics.append(
                f"payment {payment.external_id} colide com {new_external_id}; revisão manual"
            )
            continue
        report.repaired_payment_ids += 1
        if apply:
            payment.external_id = new_external_id
            payment.fingerprint = stable_fingerprint("payment", payment.source, new_external_id)

    report.procurements_without_uasg = int(
        await session.scalar(
            select(func.count())
            .select_from(Procurement)
            .where(
                Procurement.uasg.is_(None),
                Procurement.purchase_number.is_not(None),
                Procurement.purchase_year.is_not(None),
                Procurement.agency_cnpj.is_not(None),
            )
        )
        or 0
    )
    if report.procurements_without_uasg:
        report.diagnostics.append(
            "processos com chave forte incompleta não são fundidos nem separados automaticamente"
        )
    return report
