"""Documentary confirmation and audit history for divergent SINC-Contrata identities."""

from __future__ import annotations

import re
from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.tce_ma import SincContrataRow
from app.models import (
    Document,
    Evidence,
    FieldObservation,
    Procurement,
    SourceRecord,
    TceMaVerifiedLink,
)
from app.repositories.current_evidence import evidence_current, inactive_document_ids
from app.services.documents.identity import noncanonical_document_ids
from app.services.ingestion.pipeline import stable_fingerprint
from app.services.procurement_search import normalized_search_text
from app.services.public_source_links import public_link
from app.services.tce_ma_pilot import CNPJ, ENTE, UASG, sinc_has_control, sinc_procedure_key

IDENTITY_REVIEW = "tce_ma.identidade_para_revisao"
IDENTITY_CONFLICT = "tce_ma.identidade_conflitante"
LINK_DECISION = "tce_ma.decisao_vinculo"
_DOCUMENT_SOURCES = ("pncp", "compras_gov")
_NUMBER_YEAR = re.compile(r"(?<!\d)(\d+(?:[. ]\d{3})*)\s*[/–-]\s*(\d{4})(?!\d)")
_AGENCY_ROLE = re.compile(
    r"\b(?:orgao|contratante|prefeitura|secretaria|municipio|fundo|gerenciador|participante)\b"
)


@dataclass(frozen=True, slots=True)
class Excerpt:
    document_id: UUID
    quote: str


def number_token(value: object) -> str:
    text = str(value).strip().replace(".", "").replace(" ", "")
    return text.lstrip("0") if text.isascii() and text.isdecimal() else ""


def _normalize_quote(value: str) -> str:
    return " ".join(value.split())


def official_tce_documents(procurement_id: UUID):
    inactive = inactive_document_ids()
    return (
        select(Document)
        .join(SourceRecord, SourceRecord.id == Document.source_record_id)
        .where(
            Document.procurement_id == procurement_id,
            Document.extracted_text.is_not(None),
            ~Document.id.in_(select(inactive.c.id)),
            Document.id.not_in(noncanonical_document_ids()),
            SourceRecord.source.in_(_DOCUMENT_SOURCES),
        )
    )


async def _document_evidence(db: AsyncSession, procurement_id: UUID, excerpt: Excerpt) -> Evidence:
    document = await db.scalar(
        official_tce_documents(procurement_id).where(Document.id == excerpt.document_id)
    )
    quote = _normalize_quote(excerpt.quote)
    if (
        not document
        or len(quote) < 12
        or quote not in _normalize_quote(document.extracted_text or "")
    ):
        raise ValueError("O trecho deve constar de um documento oficial ativo deste pregão.")
    fingerprint = stable_fingerprint("tce_ma_documentary_excerpt", document.id, quote)
    evidence = await db.scalar(select(Evidence).where(Evidence.fingerprint == fingerprint))
    if evidence is None:
        evidence = Evidence(
            id=uuid4(),
            procurement_id=procurement_id,
            document_id=document.id,
            source_record_id=document.source_record_id,
            source_url=public_link(document.original_url),
            text_excerpt=quote,
            locator=document.title or document.document_type,
            fingerprint=fingerprint,
        )
        db.add(evidence)
        await db.flush()
    return evidence


async def _agency_proof(
    db: AsyncSession, procurement_id: UUID, cnpj: str, excerpt: Excerpt | None
) -> Evidence:
    formatted = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
    quote = _normalize_quote(excerpt.quote) if excerpt else ""
    identifies_cnpj = any(
        re.search(rf"(?<!\d){re.escape(value)}(?!\d)", quote) for value in (cnpj, formatted)
    )
    if not excerpt or not identifies_cnpj or not _AGENCY_ROLE.search(normalized_search_text(quote)):
        raise ValueError(f"Informe o trecho oficial que identifica o papel do órgão {formatted}.")
    return await _document_evidence(db, procurement_id, excerpt)


def add_tce_decision(
    db: AsyncSession,
    link: TceMaVerifiedLink,
    action: str,
    *,
    reviewer: str,
    reason: str,
    source_record_id: UUID | None,
    proofs: tuple[Evidence, ...] = (),
) -> FieldObservation:
    identifier = link.confirmation_id if action == "confirmed" else uuid4()
    decision = FieldObservation(
        id=identifier,
        entity_type="procurement",
        entity_id=link.procurement_id,
        field_name=LINK_DECISION,
        source="tce_ma_review",
        source_record_id=source_record_id,
        evidence_id=proofs[0].id if proofs else None,
        value={
            "action": action,
            "reviewer": reviewer,
            "reason": reason,
            "confirmation_id": str(link.confirmation_id),
            "control": link.pncp_control,
            "tce_cnpj": link.tce_cnpj,
            "tce_procedure_id": link.tce_procedure_id,
            "pncp_number": link.pncp_number,
            "tce_number": link.tce_number,
            "year": link.year,
            "proofs": [
                {"id": str(proof.id), "quote": proof.text_excerpt, "url": proof.source_url}
                for proof in proofs
            ],
        },
        fingerprint=stable_fingerprint("tce_ma_link_decision", identifier),
    )
    db.add(decision)
    return decision


async def confirm_tce_link(
    db: AsyncSession,
    procurement: Procurement,
    candidate_id: UUID,
    excerpt: Excerpt,
    *,
    pncp_agency: Excerpt | None,
    tce_agency: Excerpt | None,
    reviewer: str,
    reason: str,
) -> None:
    candidate = await db.get(FieldObservation, candidate_id)
    control = procurement.pncp_control_number
    if (
        not candidate
        or candidate.entity_type != "procurement"
        or candidate.entity_id != procurement.id
        or candidate.source != "tce_ma"
        or candidate.field_name != IDENTITY_REVIEW
        or not candidate.source_record_id
        or not isinstance(candidate.value, dict)
        or not control
        or procurement.agency_cnpj != CNPJ
        or procurement.uasg != UASG
        or procurement.modality_key != "pregao_eletronico"
    ):
        raise ValueError("A observação não identifica um candidato deste pregão.")
    source = await db.get(SourceRecord, candidate.source_record_id)
    if (
        source is None
        or source.source != "tce_ma"
        or not isinstance(source.raw_payload, dict)
        or candidate.value not in (source.raw_payload.get("content") or [])
    ):
        raise ValueError("O registro oficial do candidato não está disponível.")
    row = SincContrataRow(candidate.value, "")
    key = sinc_procedure_key(row)
    pncp_number = number_token(procurement.purchase_number)
    tce_number = number_token(row.payload.get("numeroProcedimento"))
    year = procurement.purchase_year
    if (
        not key
        or not sinc_has_control(row, control)
        or str(row.payload.get("enteIdEnvio")) != str(ENTE)
        or row.payload.get("tipoProcedimento") != "PE"
        or not pncp_number
        or not tce_number
        or not year
        or str(row.payload.get("anoProcedimento")) != str(year)
    ):
        raise ValueError(
            "Os identificadores oficiais e o ano do pregão não puderam ser confirmados."
        )
    if await db.get(TceMaVerifiedLink, procurement.id):
        raise ValueError("Já existe um vínculo ativo para este pregão.")
    subsequent = await db.execute(
        select(FieldObservation, SourceRecord.crawl_run_id)
        .join(SourceRecord, SourceRecord.id == FieldObservation.source_record_id)
        .where(
            FieldObservation.entity_type == "procurement",
            FieldObservation.entity_id == procurement.id,
            FieldObservation.source == "tce_ma",
            FieldObservation.field_name.in_([IDENTITY_REVIEW, IDENTITY_CONFLICT]),
            FieldObservation.collected_at >= candidate.collected_at,
            FieldObservation.id != candidate.id,
        )
    )
    for observation, run_id in subsequent:
        if observation.field_name == IDENTITY_CONFLICT or (
            observation.field_name == IDENTITY_REVIEW
            and (
                run_id != source.crawl_run_id
                or not isinstance(observation.value, dict)
                or sinc_procedure_key(SincContrataRow(observation.value, "")) != key
            )
        ):
            raise ValueError(
                "Existe observação oficial mais recente ou conflitante deste pregão. Confira a coleta."
            )
    numbers = {
        (number_token(number), int(year)) for number, year in _NUMBER_YEAR.findall(excerpt.quote)
    }
    if not {(pncp_number, year), (tce_number, year)} <= numbers:
        raise ValueError(
            "O mesmo trecho oficial deve identificar os dois números do pregão com ano."
        )
    proof = await _document_evidence(db, procurement.id, excerpt)
    agencies: tuple[Evidence, ...] = ()
    if key[0] != procurement.agency_cnpj:
        agencies = (
            await _agency_proof(db, procurement.id, procurement.agency_cnpj, pncp_agency),
            await _agency_proof(db, procurement.id, key[0], tce_agency),
        )
    link = TceMaVerifiedLink(
        procurement_id=procurement.id,
        confirmation_id=uuid4(),
        tce_cnpj=key[0],
        tce_procedure_id=key[1],
        pncp_control=control,
        pncp_number=pncp_number,
        tce_number=tce_number,
        year=year,
        evidence_id=proof.id,
        pncp_agency_evidence_id=agencies[0].id if agencies else None,
        tce_agency_evidence_id=agencies[1].id if agencies else None,
    )
    db.add(link)
    add_tce_decision(
        db,
        link,
        "confirmed",
        reviewer=reviewer,
        reason=reason,
        source_record_id=candidate.source_record_id,
        proofs=(proof, *agencies),
    )


def active_tce_links():
    return (
        select(TceMaVerifiedLink)
        .join(Procurement, Procurement.id == TceMaVerifiedLink.procurement_id)
        .where(
            Procurement.pncp_control_number == TceMaVerifiedLink.pncp_control,
            Procurement.purchase_year == TceMaVerifiedLink.year,
            func.ltrim(
                func.replace(func.replace(Procurement.purchase_number, ".", ""), " ", ""), "0"
            )
            == TceMaVerifiedLink.pncp_number,
            evidence_current(TceMaVerifiedLink.evidence_id),
            evidence_current(TceMaVerifiedLink.pncp_agency_evidence_id),
            evidence_current(TceMaVerifiedLink.tce_agency_evidence_id),
        )
    )


async def tce_decisions_for(db: AsyncSession, procurement_id: UUID) -> list[FieldObservation]:
    return list(
        (
            await db.scalars(
                select(FieldObservation)
                .where(
                    FieldObservation.entity_type == "procurement",
                    FieldObservation.entity_id == procurement_id,
                    FieldObservation.field_name == LINK_DECISION,
                    FieldObservation.source == "tce_ma_review",
                )
                .order_by(FieldObservation.collected_at.desc(), FieldObservation.id)
                .limit(30)
            )
        ).all()
    )
