"""Document processing follows the accepted observation, including unchanged refreshes."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.connectors.base import RawDocument, RawProcurement
from app.models import (
    Company,
    Deadline,
    DeadlineCalculationMethod,
    DeadlineStatus,
    Document,
    Evidence,
    ExtractionStatus,
    FieldChange,
    FieldState,
    Lead,
    LeadStatus,
    Procurement,
    ProcurementEvent,
    ProcurementEventType,
    SourceRecord,
)
from app.services.ingestion.pipeline import IngestionPipeline


async def _observe(database, pipeline, *, active, stamp, observed):
    async with database.sessions() as session, session.begin():
        source = SourceRecord(
            source="pncp",
            endpoint="https://official.test/documents",
            response_hash=uuid4().hex * 2,
            raw_payload={"active": active},
            http_status=200,
            collected_at=observed,
            collection_complete=True,
            fingerprint=uuid4().hex,
        )
        session.add(source)
        await session.flush()
        source_id = source.id
    await pipeline._persist_procurement_bundle(
        uuid4(),
        RawProcurement(source="pncp", external_id="ordered-document"),
        [],
        [
            RawDocument(
                source="pncp",
                external_id="doc",
                procurement_external_id="ordered-document",
                download_url="https://official.test/ata.zip",
                active=active,
                source_updated_at=stamp,
                source_update_field="dataAtualizacao" if stamp else None,
                observed_at=observed,
            )
        ],
        [],
        source_record_id=source_id,
        fallback_document_source_record_id=source_id,
    )
    return source_id


async def _seed_derivatives(database):
    async with database.sessions() as session, session.begin():
        document = await session.scalar(select(Document))
        company = Company(cnpj="00000000000191", normalized_name="EMPRESA", fingerprint="company")
        session.add(company)
        await session.flush()
        session.add(
            Document(
                procurement_id=document.procurement_id,
                parent_document_id=document.id,
                title="Membro do ZIP",
                original_url="https://official.test/ata.zip#member",
                extraction_status=ExtractionStatus.EXTRACTED,
                is_active=True,
                fingerprint="member",
            )
        )
        evidence = Evidence(
            procurement_id=document.procurement_id,
            document_id=document.id,
            text_excerpt="Homologado",
            fingerprint="evidence",
        )
        session.add(evidence)
        await session.flush()
        event = ProcurementEvent(
            procurement_id=document.procurement_id,
            company_id=company.id,
            evidence_id=evidence.id,
            event_type=ProcurementEventType.HOMOLOGATED,
            raw_description="Homologado",
            source="document",
            confidence=Decimal("1"),
            requires_manual_review=False,
            fingerprint="event",
        )
        session.add(event)
        await session.flush()
        deadline = Deadline(
            procurement_id=document.procurement_id,
            event_id=event.id,
            deadline_type="manual",
            calculation_method=DeadlineCalculationMethod.MANUAL,
            calculation_explanation="Prazo revisado",
            status=DeadlineStatus.OPEN,
            confidence=Decimal("1"),
            remaining_seconds=3600,
            requires_manual_review=False,
            fingerprint="deadline",
        )
        session.add(deadline)
        await session.flush()
        session.add(
            Lead(
                procurement_id=document.procurement_id,
                company_id=company.id,
                triggering_event_id=event.id,
                deadline_id=deadline.id,
                lead_status=LeadStatus.NEW,
                score=50,
                fit_score=50,
                urgency_score=20,
                evidence_score=20,
                contact_score=10,
                reason_summary="Conclusão revisada",
                recommended_action="Analisar",
                fingerprint="lead",
            )
        )


async def _snapshot(database):
    async with database.sessions() as session:
        documents = list((await session.scalars(select(Document).order_by(Document.title))).all())
        event = await session.scalar(select(ProcurementEvent))
        deadline = await session.scalar(select(Deadline))
        lead = await session.scalar(select(Lead))
        procurement = await session.scalar(select(Procurement))
        return {
            "documents": [
                (
                    doc.is_active,
                    doc.deactivated_at,
                    doc.extraction_status,
                    doc.extraction_error,
                    doc.next_attempt_at,
                )
                for doc in documents
            ],
            "review": event.requires_manual_review,
            "deadline": (
                deadline.status,
                deadline.requires_manual_review,
                deadline.remaining_seconds,
            ),
            "lead": (lead.lead_status, lead.score, lead.recommended_action),
            "phase": procurement.current_phase,
        }


@pytest.mark.parametrize("accepted_active", [True, False])
@pytest.mark.parametrize("clock", ["source", "observation", "equal_source"])
async def test_rejected_document_transition_preserves_processing_and_derivatives(
    database, accepted_active, clock
):
    pipeline = IngestionPipeline(Settings(_env_file=None), session_factory=database.sessions)
    start = datetime(2026, 10, 20, tzinfo=UTC)
    initial_source = await _observe(
        database,
        pipeline,
        active=True,
        stamp=start if clock != "observation" else None,
        observed=start,
    )
    await _seed_derivatives(database)
    accepted_stamp = (
        start + timedelta(days=2)
        if clock == "source"
        else start
        if clock == "equal_source"
        else None
    )
    accepted_source = await _observe(
        database,
        pipeline,
        active=accepted_active,
        stamp=accepted_stamp,
        observed=start + timedelta(days=2),
    )
    before = await _snapshot(database)
    rejected_source = await _observe(
        database,
        pipeline,
        active=not accepted_active,
        stamp=start + timedelta(days=1)
        if clock == "source"
        else start
        if clock == "equal_source"
        else None,
        observed=start + timedelta(days=3 if clock == "source" else 1),
    )
    assert await _snapshot(database) == before
    async with database.sessions() as session:
        document = await session.scalar(
            select(Document).where(Document.parent_document_id.is_(None))
        )
        state = await session.scalar(
            select(FieldState).where(
                FieldState.entity_id == document.id, FieldState.field_name == "is_active"
            )
        )
        assert document.is_active is accepted_active
        assert state.value is accepted_active
        assert state.source_record_id == (
            initial_source if clock == "observation" and accepted_active else accepted_source
        )
        assert state.source_updated_at == accepted_stamp
        assert state.observed_at == start + timedelta(days=2)
        changes = list(
            (
                await session.scalars(
                    select(FieldChange).where(
                        FieldChange.entity_id == document.id,
                        FieldChange.field_name == "is_active",
                        FieldChange.source_record_id == rejected_source,
                    )
                )
            ).all()
        )
        assert len(changes) == 1 and not changes[0].applied
        assert changes[0].decision == (
            "stale_ignored" if clock == "source" else "observation_ignored"
        )


async def test_accepted_archive_reactivation_requeues_parent_without_restoring_members(database):
    pipeline = IngestionPipeline(Settings(_env_file=None), session_factory=database.sessions)
    start = datetime(2026, 10, 20, tzinfo=UTC)
    await _observe(database, pipeline, active=True, stamp=start, observed=start)
    await _seed_derivatives(database)
    for offset, active in [(1, False), (2, True)]:
        now = start + timedelta(days=offset)
        await _observe(database, pipeline, active=active, stamp=now, observed=now)
    async with database.sessions() as session:
        parent = await session.scalar(select(Document).where(Document.parent_document_id.is_(None)))
        member = await session.scalar(
            select(Document).where(Document.parent_document_id == parent.id)
        )
        assert parent.is_active and parent.deactivated_at is None
        assert parent.extraction_status is ExtractionStatus.PENDING
        assert not member.is_active
        assert (await session.scalar(select(ProcurementEvent))).requires_manual_review
        assert (await session.scalar(select(Deadline))).status is DeadlineStatus.REQUIRES_REVIEW
        assert (await session.scalar(select(Lead))).lead_status is LeadStatus.PENDING_REVIEW


async def test_document_transition_and_derivatives_roll_back_with_history(database, monkeypatch):
    from app.services.ingestion import processor

    pipeline = IngestionPipeline(Settings(_env_file=None), session_factory=database.sessions)
    start = datetime(2026, 10, 20, tzinfo=UTC)
    await _observe(database, pipeline, active=True, stamp=start, observed=start)
    await _seed_derivatives(database)
    # Refresh the phase before taking the snapshot used for the rollback assertion.
    await _observe(database, pipeline, active=True, stamp=start, observed=start)
    before = await _snapshot(database)
    async with database.sessions() as session:
        changes_before = await session.scalar(select(func.count()).select_from(FieldChange))
    original = processor.suspend_document_derivatives

    async def fail_after_suspension(session, document_id):
        await original(session, document_id)
        raise RuntimeError("derived work failed")

    monkeypatch.setattr(processor, "suspend_document_derivatives", fail_after_suspension)
    now = start + timedelta(days=1)
    with pytest.raises(RuntimeError, match="derived work failed"):
        await _observe(database, pipeline, active=False, stamp=now, observed=now)
    assert await _snapshot(database) == before
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(FieldChange)) == changes_before
