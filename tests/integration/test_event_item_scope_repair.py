"""Editorial clause numbers cannot remain canonical product identities."""

import hashlib
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func, select

from app.models import (
    Document,
    DocumentChunk,
    Evidence,
    FactVerification,
    FieldChange,
    FieldObservation,
    Lead,
    LeadReview,
    ProcurementEvent,
    ProcurementItem,
)
from app.models.enums import LeadStatus, ReviewDecision
from app.services.participation_integrity import lead_current
from tests.integration.test_participation_integrity import SUBJECT, apply_repair, seed


@pytest.mark.parametrize(
    "manual,verified,matching",
    [(False, True, False), (True, True, False), (False, False, False), (False, True, True)],
)
async def test_event_item_scope_audits_and_preserves_decisions(
    database, tmp_path, manual, verified, matching
):
    settings, ids = await seed(database, tmp_path)
    async with database.sessions() as db, db.begin():
        doc = await db.get(Document, ids["document"])
        item = await db.scalar(select(ProcurementItem))
        text = SUBJECT + (
            f"Item 8 do PNCP: {item.description} Proposta total R$ 100,00."
            if matching
            else "Abre-se prazo de recurso, na forma do item 8 do edital."
        )
        doc.extracted_text += "\n" + text
        Path(doc.local_path).write_text(doc.extracted_text, encoding="utf-8")
        doc.file_hash = hashlib.sha256(Path(doc.local_path).read_bytes()).hexdigest()
        chunk = DocumentChunk(
            document_id=doc.id, sequence=2, text=text, page_number=3, locator="page:3"
        )
        db.add(chunk)
        await db.flush()
        evidence = Evidence(
            procurement_id=doc.procurement_id,
            document_id=doc.id,
            document_chunk_id=chunk.id,
            source_record_id=doc.source_record_id,
            text_excerpt=text,
            source_url=doc.original_url,
            page_number=3,
            locator=chunk.locator,
        )
        db.add(evidence)
        await db.flush()
        event = ProcurementEvent(
            procurement_id=doc.procurement_id,
            company_id=ids["company"],
            item_id=item.id,
            event_type="PARTICIPATION_DETECTED",
            source="document",
            evidence_id=evidence.id,
            raw_description=text,
            confidence=Decimal(".94"),
        )
        db.add(event)
        await db.flush()
        lead = Lead(
            procurement_id=doc.procurement_id,
            company_id=ids["company"],
            triggering_event_id=event.id,
            score=35,
            fit_score=35,
            urgency_score=0,
            evidence_score=25,
            legal_relevance_score=8,
            contact_score=2,
            economic_value_score=0,
            scoring_version="1.0",
            reason_summary="Participação documentada",
            recommended_action="Revisar o item",
            assigned_to="Responsável preservado",
            lead_status=LeadStatus.APPROVED if manual else LeadStatus.NEW,
        )
        db.add(lead)
        await db.flush()
        if manual:
            db.add(
                FieldObservation(
                    entity_type="procurement_event",
                    entity_id=event.id,
                    field_name="item_id",
                    value=str(item.id),
                    source="manual_operator",
                )
            )
            db.add(
                LeadReview(
                    lead_id=lead.id,
                    decision=ReviewDecision.APPROVED,
                    reviewer="Revisor",
                    notes="Observação preservada",
                )
            )
        eid, lid, iid = event.id, lead.id, item.id
        if not verified:
            Path(doc.local_path).write_bytes(b"conteudo sem hash correspondente")
    await apply_repair(database, settings)
    async with database.sessions() as db:
        event = await db.get(ProcurementEvent, eid)
        lead = await db.get(Lead, lid)
        assert event.item_id == (iid if manual or not verified or matching else None)
        state = await db.scalar(
            select(FactVerification).where(
                FactVerification.entity_type == "procurement_event",
                FactVerification.entity_id == eid,
            )
        )
        assert (state.status if state else None) == (None if matching else "review")
        assert (
            lead.score == 35
            and lead.scoring_version == "1.0"
            and lead.assigned_to == "Responsável preservado"
        )
        assert lead.lead_status == (
            LeadStatus.APPROVED
            if manual
            else LeadStatus.NEW
            if matching
            else LeadStatus.PENDING_REVIEW
        )
        assert (
            bool(await db.scalar(select(Lead.id).where(Lead.id == lid, lead_current()))) == matching
        )
        changes = await db.scalar(select(func.count()).select_from(FieldChange))
    await apply_repair(database, settings)
    async with database.sessions() as db:
        assert await db.scalar(select(func.count()).select_from(FieldChange)) == changes
