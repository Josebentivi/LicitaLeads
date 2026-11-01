"""Monetary observations retain transitions, exact scope and unresolved conflicts."""

import hashlib
from decimal import Decimal

from sqlalchemy import func, select

from app.models import (
    Company,
    Document,
    Evidence,
    FieldChange,
    Participant,
    ParticipantValue,
    Procurement,
    ProcurementEvent,
)
from app.services.event_detection.types import DocumentEvidence
from app.services.lead_scoring.evaluation import economic_reference
from app.services.participation_integrity import monetary_current, persist_values
from tests.integration.test_participation_integrity import PROPOSALS, SUBJECT, apply_repair, seed


async def test_accepted_a_b_a_versions_have_chronological_before_after(database, tmp_path):
    settings, ids = await seed(database, tmp_path)
    await apply_repair(database, settings)
    text_a = SUBJECT + PROPOSALS
    text_b = text_a.replace("480.823,20", "481.000,00")
    async with database.sessions() as db, db.begin():
        doc = await db.get(Document, ids["document"])
        hash_a = doc.file_hash
        doc.file_hash = hashlib.sha256(text_b.encode()).hexdigest()
        await persist_values(db, doc, [DocumentEvidence(text_b, page_number=1, locator="page:1")])
    async with database.sessions() as db, db.begin():
        doc = await db.get(Document, ids["document"])
        doc.file_hash = hash_a
        await persist_values(db, doc, [DocumentEvidence(text_a, page_number=1, locator="page:1")])
    async with database.sessions() as db:
        changes = list(
            (
                await db.scalars(
                    select(FieldChange)
                    .where(
                        FieldChange.entity_type == "participant_value",
                        FieldChange.field_name == "amount",
                    )
                    .order_by(FieldChange.recorded_at)
                )
            ).all()
        )
        transitions = [
            (Decimal(c.previous_value), Decimal(c.received_value))
            for c in changes
            if c.previous_value is not None
        ]
        assert (Decimal("480823.20"), Decimal("481000.00")) in transitions
        assert (Decimal("481000.00"), Decimal("480823.20")) in transitions
        assert (
            await db.scalar(
                select(func.count())
                .select_from(ParticipantValue)
                .where(ParticipantValue.is_current.is_(True))
            )
            == 2
        )


async def test_conflicting_active_documents_are_not_resolved_by_largest_amount(database, tmp_path):
    settings, ids = await seed(database, tmp_path)
    await apply_repair(database, settings)
    async with database.sessions() as db, db.begin():
        original = await db.get(Document, ids["document"])
        text = SUBJECT + "Lote 04, proposta R$ 900.000,00."
        other = Document(
            procurement_id=ids["procurement"],
            source_record_id=original.source_record_id,
            original_url=original.original_url + "#other",
            file_hash=hashlib.sha256(text.encode()).hexdigest(),
            extracted_text=text,
        )
        db.add(other)
        await db.flush()
        await persist_values(db, other, [DocumentEvidence(text, page_number=1)])
    async with database.sessions() as db:
        values = list(
            (
                await db.scalars(select(ParticipantValue).where(ParticipantValue.lot_number == "4"))
            ).all()
        )
        assert len(values) == 2 and {v.verification_status for v in values} == {"review"}
        company = await db.get(Company, ids["company"])
        procurement = await db.get(Procurement, ids["procurement"])
        proof = Evidence(
            procurement_id=procurement.id,
            document_id=ids["document"],
            text_excerpt=SUBJECT + "Lote 04: decisão do pregoeiro.",
            source_url="https://pncp.gov.br",
        )
        db.add(proof)
        await db.flush()
        event = ProcurementEvent(
            procurement_id=procurement.id,
            company_id=company.id,
            evidence_id=proof.id,
            raw_description=proof.text_excerpt,
        )
        amount, refs = await economic_reference(db, company, procurement, event)
        assert amount is None and refs == []


async def test_local_item_without_official_correspondence_has_no_process_wide_value(
    database, tmp_path
):
    settings, ids = await seed(database, tmp_path)
    await apply_repair(database, settings)
    async with database.sessions() as db:
        company = await db.get(Company, ids["company"])
        procurement = await db.get(Procurement, ids["procurement"])
        event = ProcurementEvent(
            procurement_id=procurement.id,
            company_id=company.id,
            raw_description="Item 8 (enalapril), Lote 05: julgamento da proposta.",
        )
        assert await economic_reference(db, company, procurement, event) == (None, [])


async def test_cancelled_result_excludes_final_but_not_published_proposal(database, tmp_path):
    settings, ids = await seed(database, tmp_path)
    await apply_repair(database, settings)
    async with database.sessions() as db, db.begin():
        doc = await db.get(Document, ids["document"])
        text = (
            SUBJECT
            + "Proposta inicial total R$ 100,00. Resultado final homologado no valor total de R$ 90,00."
        )
        await persist_values(db, doc, [DocumentEvidence(text, page_number=1)])
        db.add(
            Participant(
                procurement_id=ids["procurement"],
                company_id=ids["company"],
                source="pncp",
                status_code="cancelled",
            )
        )
        await db.flush()
        values = list((await db.scalars(select(ParticipantValue).where(monetary_current()))).all())
        assert [(v.kind, v.amount) for v in values] == [("initial_proposal", Decimal("100.00"))]


async def test_changed_interpretation_retires_prior_values_with_history(database, tmp_path):
    from app.services.event_detection.participation_values import ValueExtraction

    settings, ids = await seed(database, tmp_path)
    await apply_repair(database, settings)
    async with database.sessions() as db, db.begin():
        doc = await db.get(Document, ids["document"])
        await persist_values(db, doc, [], extraction=ValueExtraction((), ("Tabela ambígua",)))
        assert not list(
            (await db.scalars(select(ParticipantValue).where(monetary_current()))).all()
        )
        assert (
            await db.scalar(
                select(func.count())
                .select_from(FieldChange)
                .where(FieldChange.decision == "unsupported_interpretation")
            )
            == 2
        )
