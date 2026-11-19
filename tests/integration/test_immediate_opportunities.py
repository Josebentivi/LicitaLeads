"""Official response windows, provenance and the shared opportunity queries."""

import csv
from datetime import UTC, datetime, timedelta
from io import StringIO

from sqlalchemy import select

from app.models import (
    Company,
    Deadline,
    DeadlineStatus,
    Document,
    Evidence,
    FieldObservation,
    Lead,
    LeadStatus,
    Procurement,
    ProcurementEvent,
)
from app.models.enums import ProcurementEventType
from app.services.deadlines.response import (
    RESPONSE_ACT_OBSERVATION,
    RESPONSE_PROOF_VERSION,
    RESPONSE_SCOPE_OBSERVATION,
    response_scope,
)
from app.services.event_detection import DocumentEvidence
from app.services.flow_reconciliation import inspect_event
from app.services.ingestion.pipeline import stable_fingerprint
from app.services.ingestion.processor import DocumentProcessingService
from app.services.lead_scoring.evaluation import evaluate_lead
from app.services.opportunity_export import opportunity_csv
from app.services.opportunity_workspace import (
    OpportunityFilters,
    legacy_official_response_deadline_ids,
    opportunity_count,
    opportunity_query,
    procurement_has_open_response_deadline,
)
from app.services.participation_reconciliation import IntegrityReport

from .test_api_leads import _full_graph
from .test_document_processing import FakeDownloader, _pending_document, _settings


def _scope_observation(deadline, event, *, lot="", extension=False):
    return FieldObservation(
        entity_type="deadline",
        entity_id=deadline.id,
        evidence_id=event.evidence_id,
        field_name=RESPONSE_SCOPE_OBSERVATION,
        value={
            "version": RESPONSE_PROOF_VERSION,
            "scope": "lot" if lot else "unknown",
            "lote": lot,
            "item": "",
            "extension": extension,
        },
        source="document",
        fingerprint=f"deadline-scope-{deadline.id}",
    )


async def test_open_queue_requires_official_window_and_current_provenance(database):
    ids = await _full_graph(database)
    cutoff = datetime(2026, 9, 17, 16, tzinfo=UTC)
    filters = OpportunityFilters(due="open")
    async with database.sessions() as session, session.begin():
        lead = await session.get_one(Lead, ids.lead)
        lead.lead_status = LeadStatus.APPROVED
        deadline = await session.get_one(Deadline, lead.deadline_id)
        deadline.explicit_deadline_at = cutoff
        await session.flush()
        assert await opportunity_count(session, filters, now=cutoff) == 0

        event = await session.get_one(ProcurementEvent, lead.triggering_event_id)
        event.event_type = ProcurementEventType.APPEAL_WINDOW_OPENED
        deadline.deadline_type = "appeal_submission"
        await session.flush()
        assert await legacy_official_response_deadline_ids(session, [deadline.id]) == {deadline.id}
        assert await opportunity_count(session, filters, now=cutoff) == 0
        assert await opportunity_count(session, OpportunityFilters(due="24h"), now=cutoff) == 0
        session.add(_scope_observation(deadline, event))
        await session.flush()
        assert await session.scalar(opportunity_query(filters, now=cutoff)) is lead
        assert (
            await session.scalar(
                select(Procurement.id).where(procurement_has_open_response_deadline(cutoff))
            )
            == ids.procurement
        )
        export = b"".join([line async for line in opportunity_csv(session, filters, cutoff=cutoff)])
        assert [row["lead_id"] for row in csv.DictReader(StringIO(export.decode("utf-8-sig")))] == [
            str(lead.id)
        ]
        assert await opportunity_count(session, filters, now=cutoff + timedelta(seconds=1)) == 0

        lead.lead_status = LeadStatus.REJECTED
        await session.flush()
        assert await opportunity_count(session, filters, now=cutoff) == 0
        assert (
            await session.scalar(
                select(Procurement.id).where(procurement_has_open_response_deadline(cutoff))
            )
            == ids.procurement
        )

        lead.lead_status = LeadStatus.APPROVED
        document = await session.get_one(Document, ids.document)
        document.is_active = False
        await session.flush()
        assert await opportunity_count(session, filters, now=cutoff) == 0
        assert (
            await session.scalar(
                select(Procurement.id).where(procurement_has_open_response_deadline(cutoff))
            )
            is None
        )


async def test_only_a_proven_applicable_scope_interrupts_the_response_window(database):
    ids = await _full_graph(database)
    cutoff = datetime(2026, 9, 17, 16, tzinfo=UTC)
    async with database.sessions() as session, session.begin():
        lead = await session.get_one(Lead, ids.lead)
        event = await session.get_one(ProcurementEvent, lead.triggering_event_id)
        event.event_type = ProcurementEventType.APPEAL_WINDOW_OPENED
        deadline = await session.get_one(Deadline, lead.deadline_id)
        deadline.deadline_type = "appeal_submission"
        deadline.explicit_deadline_at = cutoff + timedelta(hours=5)
        event.item_id = None
        session.add(_scope_observation(deadline, event, lot="2"))
        await session.flush()
        filters = OpportunityFilters(due="open")
        assert await opportunity_count(session, filters, now=cutoff) == 1
        closure_proof = Evidence(
            procurement_id=ids.procurement,
            document_id=ids.document,
            text_excerpt="Lote 3. A sessão foi suspensa.",
            fingerprint="interruption-proof",
        )
        session.add(closure_proof)
        await session.flush()
        closure = ProcurementEvent(
            procurement_id=ids.procurement,
            event_type=ProcurementEventType.SESSION_SUSPENDED,
            occurred_at=cutoff,
            raw_description=closure_proof.text_excerpt,
            source="document",
            evidence_id=closure_proof.id,
            confidence=event.confidence,
            requires_manual_review=True,
            fingerprint="interrupted-open-window",
        )
        session.add(closure)
        await session.flush()
        scope = FieldObservation(
            entity_type="procurement_event",
            entity_id=closure.id,
            field_name=RESPONSE_ACT_OBSERVATION,
            evidence_id=closure_proof.id,
            value={"version": RESPONSE_PROOF_VERSION, "scope": "lot", "lote": "3", "item": ""},
            source="document",
            fingerprint="interruption-scope",
        )
        session.add(scope)
        await session.flush()
        assert await opportunity_count(session, filters, now=cutoff) == 1

        closure.event_type = ProcurementEventType.APPEAL_SUBMITTED
        closure.company_id = lead.company_id
        closure.requires_manual_review = False
        scope.value = {**scope.value, "lote": "2"}
        await session.flush()
        assert await opportunity_count(session, filters, now=cutoff) == 0
        assert await opportunity_count(session, OpportunityFilters(due="24h"), now=cutoff) == 0

        closure.event_type = ProcurementEventType.SESSION_SUSPENDED
        closure.company_id = None
        closure.requires_manual_review = True
        scope.value = {**scope.value, "scope": "unknown", "lote": ""}
        await session.flush()
        assert await opportunity_count(session, filters, now=cutoff) == 1
        scope.value = {**scope.value, "scope": "procurement"}
        await session.flush()
        assert await opportunity_count(session, filters, now=cutoff) == 0
        for text, expected in (
            ("Foi suspensa a sessão do certame apenas para os lotes 3 e 4.", 1),
            ("Foi suspensa a sessão do certame apenas para os lotes 2 e 3.", 0),
            ("Foi suspensa toda a sessão do certame, exceto o lote 2.", 1),
            ("Foi suspensa toda a sessão do certame, exceto o lote 3.", 0),
        ):
            scope.value = {**response_scope(text), "version": RESPONSE_PROOF_VERSION}
            await session.flush()
            assert await opportunity_count(session, filters, now=cutoff) == expected
            assert (
                await opportunity_count(session, OpportunityFilters(due="24h"), now=cutoff)
                == expected
            )


async def test_runtime_revalidation_controls_score_and_draft_for_closed_windows(
    database, api_client
):
    ids = await _full_graph(database)
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        lead = await session.get_one(Lead, ids.lead)
        event = await session.get_one(ProcurementEvent, ids.event)
        event.event_type = ProcurementEventType.APPEAL_WINDOW_OPENED
        deadline = await session.get_one(Deadline, lead.deadline_id)
        deadline.deadline_type = "appeal_submission"
        deadline.trigger_at = now - timedelta(hours=2)
        deadline.explicit_deadline_at = now + timedelta(hours=5)
        await session.flush()
        await evaluate_lead(session, lead, explicit=True, now=now, minimum_score=0)
        assert lead.urgency_score == 0
        assert lead.lead_status is LeadStatus.PENDING_REVIEW

        opening = _scope_observation(deadline, event, lot="2")
        proof = Evidence(
            procurement_id=ids.procurement,
            document_id=ids.document,
            text_excerpt="Lote 2. A licitante protocolou recurso.",
            fingerprint="proof-filing",
        )
        session.add_all([opening, proof])
        await session.flush()
        filing = ProcurementEvent(
            procurement_id=ids.procurement,
            company_id=lead.company_id,
            event_type=ProcurementEventType.APPEAL_SUBMITTED,
            occurred_at=now - timedelta(minutes=10),
            raw_description=proof.text_excerpt,
            source="document",
            evidence_id=proof.id,
            confidence=event.confidence,
            requires_manual_review=False,
            fingerprint="filing-act",
        )
        session.add(filing)
        await session.flush()
        session.add(
            FieldObservation(
                entity_type="procurement_event",
                entity_id=filing.id,
                field_name=RESPONSE_ACT_OBSERVATION,
                evidence_id=proof.id,
                value={**response_scope(proof.text_excerpt), "version": RESPONSE_PROOF_VERSION},
                source="document",
                fingerprint="proof-filing-scope",
            )
        )
        await session.flush()
        await evaluate_lead(session, lead, now=now, minimum_score=0)
        assert lead.urgency_score == 0

    result = await api_client.post(
        f"/api/leads/{ids.lead}/generate-outreach", json={"channel": "email"}
    )
    assert result.status_code == 200
    message = result.json()["draft"]["message"]
    assert "prazo registrado cuja vigência precisa ser conferida" in message
    assert "prazo em " not in message


async def test_collection_promotes_explicit_diligence_but_recalculation_keeps_legacy_out(
    database, tmp_path
):
    await _pending_document(database)
    text = (
        "Licitante EMPRESA EXEMPLO LTDA., CNPJ 00.000.000/0001-91, "
        "foi convocada para diligência até 20/09/2026 às 18:00."
    )
    service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(text.encode()),
    )
    cutoff = datetime(2026, 9, 17, 16, tzinfo=UTC)
    try:
        summary = await service.process_pending()
        assert summary.documents_processed == 1
        async with database.sessions() as session, session.begin():
            filters = OpportunityFilters(due="open")
            lead = await session.scalar(opportunity_query(filters, now=cutoff))
            assert lead is not None
            deadline = await session.get_one(Deadline, lead.deadline_id)
            assert deadline.deadline_type == "diligence_response"
            assert deadline.explicit_deadline_at == datetime(2026, 9, 20, 21, tzinfo=UTC)
            scope = await session.scalar(
                select(FieldObservation).where(
                    FieldObservation.entity_id == deadline.id,
                    FieldObservation.field_name == RESPONSE_SCOPE_OBSERVATION,
                )
            )
            assert scope is not None
            scope.value = {**scope.value, "version": 2}
            scope.fingerprint = f"old-deadline-scope-{deadline.id}"
            lead.lead_status = LeadStatus.APPROVED
            lead_id = lead.id
        await service.recalculate_deadlines_and_leads()
        async with database.sessions() as session, session.begin():
            assert await opportunity_count(session, filters, now=cutoff) == 0
            deadline = await session.get_one(Deadline, deadline.id)
            assert deadline.deadline_type == "diligence_response"
            assert deadline.explicit_deadline_at == datetime(2026, 9, 20, 21, tzinfo=UTC)
            event = await session.get_one(ProcurementEvent, deadline.event_id)
            material = [DocumentEvidence(text=text)]
            preview = IntegrityReport()
            await inspect_event(
                session,
                event,
                settings=_settings(tmp_path),
                apply=False,
                report=preview,
                material=material,
            )
            assert preview.totals["legacy_response_deadlines_requiring_validation"] == 1
            assert await opportunity_count(session, filters, now=cutoff) == 0
            await inspect_event(
                session,
                event,
                settings=_settings(tmp_path),
                apply=True,
                report=IntegrityReport(),
                material=material,
            )
            await session.flush()
            restored = await session.scalar(opportunity_query(filters, now=cutoff))
            assert restored is not None
            assert restored.id == lead_id and restored.lead_status == LeadStatus.APPROVED
            assert restored.deadline_id == deadline.id
    finally:
        await service.aclose()


async def test_explicit_extension_only_supersedes_the_same_lot(database):
    ids = await _full_graph(database)
    cutoff = datetime(2026, 9, 17, 16, tzinfo=UTC)
    async with database.sessions() as session, session.begin():
        lead = await session.get_one(Lead, ids.lead)
        event = await session.get_one(ProcurementEvent, lead.triggering_event_id)
        event.event_type = ProcurementEventType.APPEAL_WINDOW_OPENED
        event.item_id = None
        deadline = await session.get_one(Deadline, lead.deadline_id)
        deadline.deadline_type = "appeal_submission"
        extension_act = ProcurementEvent(
            procurement_id=lead.procurement_id,
            company_id=lead.company_id,
            occurred_at=cutoff,
            evidence_id=event.evidence_id,
            event_type=ProcurementEventType.APPEAL_WINDOW_OPENED,
            source="document",
            raw_description="Foi prorrogado o prazo para apresentação de recurso.",
            confidence=event.confidence,
            fingerprint="extension-act",
        )
        session.add(extension_act)
        await session.flush()
        extension = Deadline(
            procurement_id=lead.procurement_id,
            company_id=lead.company_id,
            event_id=extension_act.id,
            deadline_type=deadline.deadline_type,
            trigger_at=cutoff,
            explicit_deadline_at=cutoff + timedelta(hours=12),
            calculation_method=deadline.calculation_method,
            calculation_explanation="Prorrogação publicada.",
            confidence=deadline.confidence,
            fingerprint="extension-deadline",
        )
        session.add(extension)
        await session.flush()
        original_scope = _scope_observation(deadline, event, lot="2")
        extension_scope = _scope_observation(extension, extension_act, lot="3", extension=True)
        session.add_all([original_scope, extension_scope])
        await session.flush()

        filters = OpportunityFilters(due="open")
        assert await opportunity_count(session, filters, now=cutoff) == 1
        extension_scope.value = {**extension_scope.value, "lote": "2"}
        await session.flush()
        assert await opportunity_count(session, filters, now=cutoff) == 0


async def test_legacy_misattributed_deadline_preserves_record_and_requires_review(
    database, tmp_path
):
    ids = await _full_graph(database)
    text = (
        "Foi aberto o prazo para contrarrazões ao recurso interposto pela empresa "
        "ABC Equipamentos Ltda., CNPJ 04.252.011/0001-10, até 20/09/2026 às 18:00."
    )
    async with database.sessions() as session, session.begin():
        lead = await session.get_one(Lead, ids.lead)
        company = await session.get_one(Company, lead.company_id)
        company.cnpj = "04252011000110"
        company.legal_name = "ABC Equipamentos Ltda."
        event = await session.get_one(ProcurementEvent, lead.triggering_event_id)
        event.event_type = ProcurementEventType.COUNTERARGUMENT_OPENED
        proof = await session.get_one(Evidence, event.evidence_id)
        proof.text_excerpt = text
        deadline = await session.get_one(Deadline, lead.deadline_id)
        deadline.deadline_type = "counterarguments_submission"
        deadline.fingerprint = stable_fingerprint("deadline", event.id, "event_response")
        previous_end = deadline.explicit_deadline_at
        await session.flush()
        report = IntegrityReport()
        await inspect_event(
            session,
            event,
            settings=_settings(tmp_path),
            apply=True,
            report=report,
            material=[DocumentEvidence(text=text)],
        )
        assert report.totals["legacy_response_deadlines_requiring_review"] == 1
        assert deadline.explicit_deadline_at == previous_end
        assert deadline.status is DeadlineStatus.REQUIRES_REVIEW and deadline.requires_manual_review
