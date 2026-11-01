"""A procurement issuer cannot become its own bidder through document headers."""

import hashlib
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select

from app.models import (
    Company,
    Document,
    DocumentChunk,
    Evidence,
    FactVerification,
    Participant,
    ParticipantValue,
    Procurement,
    ProcurementEvent,
)
from app.services.event_detection import DocumentEvidence, ParticipantDetector
from app.services.ingestion.processor import DocumentProcessingService
from tests.integration.test_participation_integrity import apply_repair, seed


@pytest.mark.parametrize("event_type", ["PARTICIPATION_DETECTED", "DISQUALIFIED"])
async def test_buyer_reference_is_reviewed_and_never_generates_participation_or_amounts(
    database, tmp_path, event_type
):
    settings, ids = await seed(database, tmp_path)
    async with database.sessions() as db, db.begin():
        proc = await db.get(Procurement, ids["procurement"])
        proc.agency_cnpj = "01612325000198"
        buyer = Company(
            cnpj=proc.agency_cnpj, legal_name="Prefeitura", normalized_name="PREFEITURA"
        )
        db.add(buyer)
        await db.flush()
        doc = await db.get(Document, ids["document"])
        text = "Licitante: Prefeitura CNPJ 01.612.325/0001-98. Proposta total R$ 999.999,00."
        doc.extracted_text += "\n" + text
        Path(doc.local_path).write_text(doc.extracted_text, encoding="utf-8")
        doc.file_hash = hashlib.sha256(Path(doc.local_path).read_bytes()).hexdigest()
        chunk = DocumentChunk(
            document_id=doc.id, sequence=2, text=text, page_number=3, locator="page:3"
        )
        db.add(chunk)
        await db.flush()
        evidence = Evidence(
            procurement_id=proc.id,
            document_id=doc.id,
            document_chunk_id=chunk.id,
            source_record_id=doc.source_record_id,
            text_excerpt=text,
            source_url=doc.original_url,
        )
        db.add(evidence)
        await db.flush()
        participant = Participant(
            procurement_id=proc.id,
            company_id=buyer.id,
            source="document",
            source_evidence_id=evidence.id,
        )
        event = ProcurementEvent(
            procurement_id=proc.id,
            company_id=buyer.id,
            event_type=event_type,
            source="document",
            evidence_id=evidence.id,
            raw_description=text,
            confidence=Decimal(".76"),
        )
        db.add_all([participant, event])
        await db.flush()
        bid, pid, eid = buyer.id, participant.id, event.id
        detected = ParticipantDetector().detect(DocumentEvidence(text))[0]
        service = DocumentProcessingService(settings)
        try:
            assert await service._persist_participant(db, proc, doc, chunk, detected) is False
            assert await service._persist_participation_event(db, proc, doc, chunk, detected) == (
                None,
                False,
            )
        finally:
            await service.aclose()
    await apply_repair(database, settings)
    async with database.sessions() as db:
        for kind, identifier in [("participant", pid), ("procurement_event", eid)]:
            state = await db.scalar(
                select(FactVerification).where(
                    FactVerification.entity_type == kind, FactVerification.entity_id == identifier
                )
            )
            assert state and state.status == "review"
        assert not list(
            (
                await db.scalars(select(ParticipantValue).where(ParticipantValue.company_id == bid))
            ).all()
        )
