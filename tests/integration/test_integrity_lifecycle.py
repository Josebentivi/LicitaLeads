"""Operational facts must follow complete, current and correctly scoped evidence."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.connectors.base import (
    RawDocument,
    RawPayment,
    RawProcurement,
    RawProcurementItem,
    RawResult,
)
from app.models import (
    Company,
    Contract,
    Deadline,
    DeadlineStatus,
    Document,
    Evidence,
    ExtractionStatus,
    FieldObservation,
    FieldValueStatus,
    Lead,
    LeadStatus,
    Participant,
    ParticipantStatus,
    Payment,
    Procurement,
    ProcurementEvent,
    ProcurementEventType,
    ProcurementPhase,
    SourceRecord,
)
from app.services.data_reconciliation import reconcile_data
from app.services.ingestion.municipal_payments import MunicipalPaymentIngestionService
from app.services.ingestion.payments import PaymentIngestionService
from app.services.ingestion.pipeline import IngestionPipeline
from app.services.ingestion.processor import DocumentProcessingService, suspend_document_derivatives

from .test_document_processing import FakeDownloader, _pending_document, _settings


async def test_refresh_by_identity_bypasses_publication_and_preserves_per_id_failures(database):
    from app.connectors.base import ConnectorResult, DataAvailability
    from app.models import CrawlRunStatus
    from app.services.ingestion.pipeline import PipelineRequest

    from .test_pipeline import CONTROL_NUMBER, StaticConnector, _procurement

    class IdentityConnector(StaticConnector):
        calls: list[str]

        async def discover_procurements(self, filters):
            raise AssertionError("refresh by identity must not rediscover by publication")

        async def fetch_procurement(self, external_id):
            self.calls.append(external_id)
            if external_id == "unavailable":
                return ConnectorResult(
                    data=None,
                    availability=DataAvailability.TEMPORARY_ERROR,
                    diagnostic="official endpoint unavailable",
                )
            return await super().fetch_procurement(external_id)

    connector = IdentityConnector("pncp", _procurement("pncp", CONTROL_NUMBER))
    connector.calls = []
    pipeline = IngestionPipeline(session_factory=database.sessions, connectors={"pncp": connector})
    request = PipelineRequest(
        connector="pncp",
        external_ids=(CONTROL_NUMBER, "unavailable", CONTROL_NUMBER),
        process_documents=False,
    )
    summary = await pipeline.run(request)
    assert connector.calls == [CONTROL_NUMBER, "unavailable"]
    assert summary.status is CrawlRunStatus.PARTIAL
    assert summary.records_created == 1 and summary.records_failed == 1
    assert any("unavailable" in message for message in summary.diagnostics)
    assert request.as_json()["external_ids"] == list(request.external_ids)


async def test_contract_matching_checks_all_valid_candidates_before_limiting(database):
    async with database.sessions() as session, session.begin():
        for number in range(6):
            year = 2026 if number in {0, 5} else 2024
            session.add(
                Contract(
                    id=UUID(int=number + 1),
                    source="pncp",
                    external_id=f"contract-{number}",
                    contract_number="7",
                    supplier_cnpj="00000000000191",
                    agency_cnpj="11222333000181",
                    valid_from=datetime(year, 1, 1, tzinfo=UTC),
                    valid_until=datetime(year, 12, 31, tzinfo=UTC),
                    fingerprint=f"contract-{number}",
                )
            )
    service = PaymentIngestionService(session_factory=database.sessions)
    raw = RawPayment(
        source="manual",
        external_id="payment",
        amount=Decimal("10"),
        contract_number="7",
        agency_cnpj="11222333000181",
        supplier_cnpj="00000000000191",
        payment_date=datetime(2026, 6, 1, tzinfo=UTC),
    )
    try:
        async with database.sessions() as session:
            matched, confidence, _, ambiguous = await service._match_contract(
                session, raw, raw.supplier_cnpj
            )
        assert matched is None and confidence is None and ambiguous
    finally:
        await service.aclose()


async def test_cancelled_award_resolves_stored_item_when_item_endpoint_is_unavailable(database):
    pipeline = IngestionPipeline(session_factory=database.sessions, connectors={})
    raw = RawProcurement(source="pncp", external_id="procurement")
    item = RawProcurementItem(
        source="pncp",
        external_id="item-1",
        procurement_external_id=raw.external_id,
        item_number="1",
    )
    result = RawResult(
        source="pncp",
        external_id="result-1",
        procurement_external_id=raw.external_id,
        item_external_id=item.external_id,
        item_number="1",
        supplier_cnpj="00000000000191",
        supplier_name="Empresa",
        result_status="homologado",
    )
    await pipeline._persist_procurement_bundle(
        uuid4(), raw, [item], [], [result], source_record_id=None
    )
    await pipeline._persist_procurement_bundle(
        uuid4(),
        raw,
        [],
        [],
        [result.model_copy(update={"is_cancelled": True})],
        source_record_id=None,
    )
    async with database.sessions() as session:
        participants = list((await session.scalars(select(Participant))).all())
    assert len(participants) == 1
    assert participants[0].item_id is not None
    assert participants[0].status_code is ParticipantStatus.CANCELLED


async def test_cancelled_award_immediately_suspends_inferred_company_lead(database, tmp_path):
    pipeline = IngestionPipeline(session_factory=database.sessions, connectors={})
    raw = RawProcurement(source="pncp", external_id="inferred-award")
    result = RawResult(
        source="pncp",
        external_id="result-inferred",
        procurement_external_id=raw.external_id,
        supplier_cnpj="00000000000191",
        supplier_name="Empresa",
        result_status="homologado",
    )
    await pipeline._persist_procurement_bundle(
        uuid4(), raw, [], [], [result], source_record_id=None
    )
    async with database.sessions() as session, session.begin():
        participant = await session.scalar(select(Participant))
        # New evaluations require stored proof, rather than only an event URL.
        evidence = Evidence(
            procurement_id=participant.procurement_id,
            source_url="https://official.test/result",
            text_excerpt="Homologação oficial",
        )
        session.add(evidence)
        await session.flush()
        event = ProcurementEvent(
            procurement_id=participant.procurement_id,
            company_id=participant.company_id,
            event_type=ProcurementEventType.HOMOLOGATED,
            raw_description="Homologação oficial",
            source="document",
            source_url="https://official.test/result",
            evidence_id=evidence.id,
            confidence=Decimal("0.6"),
            occurred_at=datetime.now(UTC),
            requires_manual_review=True,
            fingerprint="inferred-event",
        )
        session.add(event)
        await session.flush()
        session.add(
            FieldObservation(
                entity_type="procurement_event",
                entity_id=event.id,
                field_name="company_id",
                value={"company_id": str(participant.company_id)},
                value_status=FieldValueStatus.REQUIRES_MANUAL_REVIEW,
                source="inferred_single_winner",
                fingerprint="inferred-link",
            )
        )
    service = DocumentProcessingService(_settings(tmp_path), session_factory=database.sessions)
    try:
        assert (await service.recalculate_deadlines_and_leads()).leads_created == 1
        from app.models import LeadScoreRevision

        async with database.sessions() as session, session.begin():
            initial_score = await session.scalar(select(LeadScoreRevision))
            assert initial_score.previous_score is None
            assert initial_score.previous_version is None
            historical = await session.scalar(select(Lead))
            historical.score = historical.fit_score = 61
            historical.scoring_version = "1.0"
            historical.score_breakdown = {"legacy": 61}
            action = historical.recommended_action
        await pipeline._persist_procurement_bundle(
            uuid4(),
            raw,
            [],
            [],
            [result.model_copy(update={"is_cancelled": True})],
            source_record_id=None,
        )
        async with database.sessions() as session:
            lead = await session.scalar(select(Lead))
            assert lead.lead_status is LeadStatus.PENDING_REVIEW and lead.score == 61
            assert lead.score_breakdown == {"legacy": 61}
            assert lead.recommended_action == action
            from app.services.participation_integrity import lead_current

            assert await session.scalar(select(Lead.id).where(lead_current())) is None
        await service.recalculate_deadlines_and_leads()
        async with database.sessions() as session:
            assert (await session.scalar(select(Lead))).score == 61
    finally:
        await service.aclose()


async def test_withdrawn_document_suspends_lead_deadline_and_phase_without_deleting_history(
    database, tmp_path
):
    async with database.sessions() as session, session.begin():
        procurement = Procurement(source="pncp", external_id="p", fingerprint="p")
        company = Company(cnpj="00000000000191", normalized_name="EMPRESA", fingerprint="company")
        session.add_all([procurement, company])
        await session.flush()
        document = Document(
            procurement_id=procurement.id,
            title="Ata",
            original_url="https://official.test/ata",
            fingerprint="doc",
        )
        session.add(document)
        await session.flush()
        evidence = Evidence(
            procurement_id=procurement.id,
            document_id=document.id,
            text_excerpt="Homologado",
            fingerprint="evidence",
        )
        session.add(evidence)
        await session.flush()
        event = ProcurementEvent(
            procurement_id=procurement.id,
            company_id=company.id,
            event_type=ProcurementEventType.HOMOLOGATED,
            occurred_at=datetime.now(UTC),
            raw_description="Homologado",
            source="document",
            evidence_id=evidence.id,
            confidence=Decimal("1"),
            fingerprint="event",
        )
        session.add(event)
        await session.flush()
        document_id = document.id
    service = DocumentProcessingService(_settings(tmp_path), session_factory=database.sessions)
    try:
        assert (await service.recalculate_deadlines_and_leads()).leads_created == 1
        async with database.sessions() as session, session.begin():
            historical = await session.scalar(select(Lead))
            historical.score = historical.fit_score = 61
            historical.scoring_version = "1.0"
            historical.score_breakdown = {"legacy": 61}
            await suspend_document_derivatives(session, document_id)
        assert (await service.recalculate_deadlines_and_leads()).leads_created == 0
        async with database.sessions() as session:
            lead = await session.scalar(select(Lead))
            deadline = await session.scalar(select(Deadline))
            procurement = await session.scalar(select(Procurement))
            assert lead.lead_status is LeadStatus.PENDING_REVIEW and lead.score == 61
            assert lead.score_breakdown == {"legacy": 61}
            assert deadline.status is DeadlineStatus.REQUIRES_REVIEW
            assert procurement.current_phase is ProcurementPhase.UNKNOWN
            assert await session.scalar(select(func.count()).select_from(Evidence)) == 1
            assert await session.scalar(select(func.count()).select_from(ProcurementEvent)) == 1
    finally:
        await service.aclose()


async def test_reactivation_restores_pending_document_queue(database):
    pipeline = IngestionPipeline(session_factory=database.sessions, connectors={})
    raw = RawProcurement(source="pncp", external_id="reactivated")
    document = RawDocument(
        source="pncp",
        external_id="doc",
        procurement_external_id=raw.external_id,
        download_url="https://official.test/reactivated.pdf",
    )
    for active in (True, False, True):
        await pipeline._persist_procurement_bundle(
            uuid4(),
            raw,
            [],
            [document.model_copy(update={"active": active})],
            [],
            source_record_id=None,
        )
    async with database.sessions() as session:
        stored = await session.scalar(select(Document))
    assert stored.is_active and stored.extraction_status is ExtractionStatus.PENDING


async def test_document_withdrawn_during_download_cannot_create_facts(database, tmp_path):
    document_id = await _pending_document(database)

    class WithdrawingDownloader(FakeDownloader):
        async def download(self, url):
            async with database.sessions() as session, session.begin():
                (await session.get(Document, document_id)).is_active = False
            return await super().download(url)

    downloader = WithdrawingDownloader(
        b"EMPRESA, CNPJ 00.000.000/0001-91, foi inabilitada por falta de certidao."
    )
    service = DocumentProcessingService(
        _settings(tmp_path), session_factory=database.sessions, downloader=downloader
    )
    await service.process_pending()
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ProcurementEvent)) == 0
        assert (await session.get(Document, document_id)).extracted_text is None
    assert (await service.recheck_document(document_id)).documents_processed == 0
    assert downloader.calls == 1


def _municipal_payment(group, status, amount, *, complete=False):
    return RawPayment(
        source="municipal:imperatriz",
        external_id=f"imperatriz:2026:{group}:{status}",
        commitment_external_id=f"imperatriz:2026:{group}",
        commitment_number="10",
        commitment_details_complete=complete,
        status=status,
        amount=Decimal(amount),
        phase="pago" if status == "detalhe_empenho" else "pago_agregado",
        supplier_name="Empresa",
    )


@pytest.mark.parametrize("complete", [False, True])
async def test_municipal_replacement_requires_complete_exact_commitment(database, complete):
    settings = Settings(_env_file=None)
    payment_service = PaymentIngestionService(settings, session_factory=database.sessions)
    service = MunicipalPaymentIngestionService(
        settings, session_factory=database.sessions, payment_ingestion=payment_service
    )
    initial = [_municipal_payment(group, "agregado_por_empenho", "100") for group in ("A", "B")]
    try:
        await payment_service.persist_raw_payments(initial)
        detail = _municipal_payment(
            "A", "detalhe_empenho", "100" if complete else "30", complete=complete
        )
        async with database.sessions() as session, session.begin():
            staged = await service._supersede_aggregates(session, [detail], year=2026)
            await payment_service.persist_raw_payments(staged, session=session)
        async with database.sessions() as session:
            current = list(
                (await session.scalars(select(Payment).where(Payment.is_current.is_(True)))).all()
            )
            history = list((await session.scalars(select(Payment))).all())
        assert len(history) == 3
        assert sum(row.amount for row in current) == Decimal("200")
        assert (
            next(row for row in current if row.commitment_external_id.endswith(":B")).status
            == "agregado_por_empenho"
        )
        assert next(row for row in current if row.commitment_external_id.endswith(":A")).status == (
            "detalhe_empenho" if complete else "agregado_por_empenho"
        )
    finally:
        await service.aclose()


async def test_new_aggregate_replaces_stale_details_but_preserves_their_history(database):
    service = MunicipalPaymentIngestionService(session_factory=database.sessions)
    payment_service = service.payment_ingestion
    try:
        await payment_service.persist_raw_payments(
            [_municipal_payment("A", "detalhe_empenho", "30", complete=True)]
        )
        aggregate = _municipal_payment("A", "agregado_por_empenho", "100")
        async with database.sessions() as session, session.begin():
            staged = await service._supersede_aggregates(session, [aggregate], year=2026)
            await payment_service.persist_raw_payments(staged, session=session)
        async with database.sessions() as session:
            rows = list((await session.scalars(select(Payment))).all())
        assert len(rows) == 2
        assert [(row.status, row.amount) for row in rows if row.is_current] == [
            ("agregado_por_empenho", Decimal("100"))
        ]
    finally:
        await service.aclose()


async def test_reconciliation_scopes_shared_document_url_to_its_official_operation(database):
    now = datetime.now(UTC)
    shared = "https://official.test/shared.pdf"
    async with database.sessions() as session, session.begin():
        for sequence in (1, 2):
            procurement = Procurement(
                source="pncp",
                external_id=f"00000000000191:2026:{sequence}",
                fingerprint=f"p{sequence}",
            )
            session.add(procurement)
            await session.flush()
            session.add(
                Document(
                    procurement_id=procurement.id,
                    original_url=shared,
                    title="Ata",
                    fingerprint=f"doc{sequence}",
                )
            )
            session.add(
                SourceRecord(
                    source="pncp",
                    endpoint=f"https://official.test/orgaos/00000000000191/compras/2026/{sequence}/arquivos",
                    raw_payload=[{"url": shared, "statusAtivo": sequence == 2}],
                    collection_complete=True,
                    response_hash=f"hash{sequence}",
                    fingerprint=f"source{sequence}",
                    collected_at=now + timedelta(seconds=sequence),
                )
            )
        session.add(
            SourceRecord(
                source="municipal:fake",
                endpoint="https://official.test/unrelated",
                raw_payload=[{"url": shared, "statusAtivo": False}],
                collection_complete=True,
                response_hash="unrelated",
                fingerprint="unrelated",
                collected_at=now + timedelta(hours=1),
            )
        )
    async with database.sessions() as session:
        report = await reconcile_data(session)
    assert report.inspected_documents == 2 and report.deactivated_documents == 1
