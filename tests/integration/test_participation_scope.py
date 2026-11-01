"""Positive official correspondence never follows a coincident local number."""

from decimal import Decimal

import pytest
from sqlalchemy import select

from app.models import (
    Company,
    Document,
    FactVerification,
    Participant,
    ParticipantValue,
    Procurement,
    ProcurementEvent,
    ProcurementItem,
)
from app.services.event_detection.types import DocumentEvidence
from app.services.lead_scoring.evaluation import economic_reference
from app.services.participation_integrity import (
    participant_payloads,
    persist_values,
    verify_participant_reference,
)
from tests.integration.test_participation_integrity import COST, SUBJECT, seed


@pytest.mark.parametrize(
    "reference,description,matched",
    [
        ("Item 8 do PNCP", "Cloridrato de ciprofloxacino, 500mg", True),
        ("Item 8", "Cloridrato de ciprofloxacino, 500mg", False),
        ("Item 8 do PNCP", "Enalapril 20 mg", False),
    ],
)
async def test_official_value_item_needs_explicit_reference_and_full_description(
    database, tmp_path, reference, description, matched
):
    _, ids = await seed(database, tmp_path)
    async with database.sessions() as db, db.begin():
        doc = await db.get(Document, ids["document"])
        text = SUBJECT + f"{reference}: {description} Proposta total R$ 20.000,00."
        await persist_values(db, doc, [DocumentEvidence(text, page_number=1)])
        value = await db.scalar(select(ParticipantValue))
        item = await db.scalar(select(ProcurementItem))
        assert value and value.amount == Decimal("20000.00")
        assert value.official_item_id == (item.id if matched else None)
        participant = await db.get(Participant, ids["good"])
        payload = (await participant_payloads(db, [participant]))[0]
        assert ("Item PNCP" in payload["monetary_values"][0]["scope_label"]) == matched
        event = ProcurementEvent(
            procurement_id=doc.procurement_id,
            company_id=ids["company"],
            item_id=item.id,
            raw_description="Item 8: julgamento da proposta",
        )
        amount, _ = await economic_reference(
            db,
            await db.get(Company, ids["company"]),
            await db.get(Procurement, doc.procurement_id),
            event,
        )
        assert amount == (Decimal("20000.00") if matched else None)


async def test_other_participation_does_not_validate_adverse_cost_reference(database, tmp_path):
    _, ids = await seed(database, tmp_path)
    async with database.sessions() as db, db.begin():
        bad = await db.get(Participant, ids["bad"])
        db.add(
            Participant(procurement_id=ids["procurement"], company_id=bad.company_id, source="pncp")
        )
        event = ProcurementEvent(
            procurement_id=ids["procurement"],
            company_id=bad.company_id,
            event_type="DOCUMENTATION_REQUESTED",
            source="document",
            evidence_id=bad.source_evidence_id,
            raw_description=COST,
            confidence=Decimal("0.80"),
        )
        db.add(event)
        await db.flush()
        await verify_participant_reference(db, bad, COST)
        verification = await db.scalar(
            select(FactVerification).where(
                FactVerification.entity_type == "procurement_event",
                FactVerification.entity_id == event.id,
            )
        )
        assert verification and verification.status == "invalidated"
        # The generic participation fact is still supported by the separate API observation.
        assert (
            await db.scalar(
                select(FactVerification).where(
                    FactVerification.entity_type == "procurement_event",
                    FactVerification.entity_id == ids["event"],
                )
            )
            is None
        )
