import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.connectors.base import RawContract, RawPayment, RawResult
from app.database import write_session
from app.models import (
    Company,
    Contract,
    Document,
    DocumentChunk,
    Evidence,
    ExtractionStatus,
    FieldChange,
    LLMWorkItem,
    Participant,
    Payment,
    Procurement,
    ProcurementEvent,
    ProcurementItem,
)
from app.models.llm_review import LLMEventCitation, LLMSuggestion
from app.services.ingestion.payments import PaymentIngestionService
from app.services.ingestion.pipeline import IngestionPipeline
from app.services.llm import central
from app.services.llm.providers import LLMReply
from app.services.llm.queue import LLMQueueService, document_snapshot, enqueue_document
from app.services.scheduler_control import set_component_paused
from tests.integration.test_document_processing import _pending_document
from tests.integration.test_llm_queue import Provider

CN_A, CN_B = "04252011000110", "10258066000130"


@pytest.fixture
def ai_settings(tmp_path):
    return Settings(
        _env_file=None,
        app_env="test",
        llm_enabled=True,
        llm_provider="openai",
        llm_model="gpt-6-luna",
        llm_api_key="sk-private-test",
        llm_min_chunk_characters=1,
        raw_data_storage_path=tmp_path / "raw",
        document_storage_path=tmp_path / "docs",
    )


async def extracted(database, settings, texts):
    identifier = await _pending_document(database)
    async with write_session(database.sessions) as db:
        doc = await db.get(Document, identifier)
        doc.file_hash = hashlib.sha256("".join(texts).encode()).hexdigest()
        doc.extraction_status = ExtractionStatus.EXTRACTED
        doc.extracted_text = "\n".join(texts)
        for index, text in enumerate(texts):
            db.add(
                DocumentChunk(
                    document_id=doc.id,
                    sequence=index,
                    text=text,
                    page_number=index + 1,
                    start_offset=index * 1000,
                    end_offset=index * 1000 + len(text),
                )
            )
        await db.flush()
        await enqueue_document(db, doc, settings)
    return identifier


async def test_relevant_late_chunk_and_context_are_selected(database, ai_settings):
    settings = ai_settings.model_copy(update={"llm_max_chunks_per_document": 2})
    identifier = await extracted(
        database,
        settings,
        [f"Informações gerais número {n}" for n in range(49)]
        + ["A empresa foi inabilitada por ausência de documento."],
    )
    async with database.sessions() as db:
        works = list((await db.scalars(select(LLMWorkItem))).all())
        assert len(works) == 2
        assert any("inabilitada" in w.context["text"] for w in works)
        assert all(w.coverage["omitted"] == 48 for w in works)
        assert all(len(w.context["chunks"]) <= 3 for w in works)
        detail = await central.document_detail(db, identifier, 1)
        assert detail["total"] == 2


async def test_ai_pause_stops_claims_without_pausing_collections(database, ai_settings):
    await extracted(
        database, ai_settings, ["A proposta foi desclassificada por ausência de documento."]
    )
    set_component_paused("llm", True, settings=ai_settings)
    provider = Provider()
    assert (
        await LLMQueueService(
            ai_settings, session_factory=database.sessions, provider=provider
        ).drain(wait=False)
        == 0
    )
    assert provider.calls == 0


async def _documented_company(db, procurement_id):
    company = Company(
        cnpj=CN_A, legal_name="ALFA", normalized_name="alfa", fingerprint="company-alfa"
    )
    db.add(company)
    await db.flush()
    db.add(
        Participant(
            company_id=company.id,
            procurement_id=procurement_id,
            source="pncp",
            confidence=1,
            fingerprint="participant-alfa",
        )
    )
    return company.id


@pytest.mark.parametrize("item_in_citation", [True, False])
async def test_ambiguous_party_is_only_a_suggestion_until_human_confirmation(
    database, ai_settings, item_in_citation
):
    header = "A licitante ALFA tem CNPJ 04.252.011/0001-10."
    description = "Monitor profissional de 24 polegadas"
    item_reference = f"item 7 ({description})" if item_in_citation else "item 7"
    act = f"A proposta do {item_reference} foi desclassificada por ausência de documento."
    identifier = await extracted(database, ai_settings, [header + "\n" + act])
    async with write_session(database.sessions) as db:
        doc = await db.get(Document, identifier)
        company_id = await _documented_company(db, doc.procurement_id)
        item = ProcurementItem(
            procurement_id=doc.procurement_id,
            item_number="7",
            description=description,
            fingerprint="documented-item",
        )
        db.add(item)
        await db.flush()
        item_id = item.id
    reply = LLMReply(
        analyses=[
            {
                "event_type": "DISQUALIFIED",
                "company_cnpj": CN_A,
                "company_name": "ALFA",
                "item_number": "7",
                "evidence_quotes": [{"text": header}, {"text": act}],
                "confidence": 0.9,
                "requires_manual_review": True,
            }
        ]
    )
    await LLMQueueService(
        ai_settings, session_factory=database.sessions, provider=Provider(reply)
    ).drain(wait=False)
    async with database.sessions() as db:
        row = await db.scalar(select(LLMSuggestion))
        assert row and row.status == "pending" and row.event_id is None
        assert len(row.evidence_ids) == 2
        assert await db.scalar(select(func.count()).select_from(ProcurementEvent)) == 0
        sid = row.id
    async with write_session(database.sessions) as db:
        await central.review_suggestion(
            db,
            sid,
            confirm=True,
            company_id=company_id,
            reviewer="Revisora",
            note="Conferido",
            settings=ai_settings,
        )
    async with database.sessions() as db:
        row = await db.get(LLMSuggestion, sid)
        event = await db.get(ProcurementEvent, row.event_id)
        assert row.status == "confirmed" and event.company_id == company_id
        assert event.raw_description == act
        assert event.item_id == (item_id if item_in_citation else None)
        assert await db.scalar(select(func.count()).select_from(LLMEventCitation)) == 2
    async with write_session(database.sessions) as db:
        with pytest.raises(ValueError, match="já foi revisada"):
            await central.review_suggestion(
                db,
                sid,
                confirm=False,
                company_id=None,
                reviewer=None,
                note=None,
                settings=ai_settings,
            )


@pytest.mark.parametrize(
    ("citation", "document_title"),
    [
        ("O recurso foi protocolado na sessão pública.", "Ata da sessão"),
        ("O item não foi homologado na sessão pública.", "Ata da sessão"),
        ("O item foi homologado na sessão pública.", "Minuta de Ata"),
    ],
)
async def test_confirmation_requires_evidence_of_the_act(
    api_client, database, ai_settings, citation, document_title
):
    header = "A licitante ALFA tem CNPJ 04.252.011/0001-10."
    identifier = await extracted(database, ai_settings, [header + "\n" + citation])
    async with write_session(database.sessions) as db:
        doc = await db.get_one(Document, identifier)
        company_id = await _documented_company(db, doc.procurement_id)
    reply = LLMReply(
        analyses=[
            {
                "event_type": "HOMOLOGATED",
                "company_cnpj": CN_A,
                "company_name": "ALFA",
                "evidence_quotes": [{"text": header}, {"text": citation}],
                "confidence": 0.9,
                "requires_manual_review": True,
            }
        ]
    )
    await LLMQueueService(
        ai_settings, session_factory=database.sessions, provider=Provider(reply)
    ).drain(wait=False)
    async with write_session(database.sessions) as db:
        doc = await db.get_one(Document, identifier)
        # Model a legacy suggestion saved before the draft-title guard recognized this title.
        doc.title = document_title
        row = (await db.scalars(select(LLMSuggestion))).one()
        assert row.status == "pending"
        row.snapshot = await document_snapshot(db, doc)
        suggestion_id = row.id

    response = await api_client.post(
        f"/api/llm/suggestions/{suggestion_id}/confirm", json={"company_id": str(company_id)}
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"] == "Nenhuma citação comprova o ato sugerido"
    async with database.sessions() as db:
        row = await db.get_one(LLMSuggestion, suggestion_id)
        assert row.status == "pending" and row.event_id is None
        assert await db.scalar(select(func.count()).select_from(ProcurementEvent)) == 0


async def test_html_api_pagination_controls_and_safe_config(
    api_client, database, ai_settings, monkeypatch
):
    import app.ai_web
    from app.api.routes import llm

    monkeypatch.setattr(llm, "get_settings", lambda: ai_settings)
    monkeypatch.setattr(app.ai_web, "get_settings", lambda: ai_settings)
    identifier = await extracted(
        database, ai_settings, ["A proposta foi desclassificada por ausência de documento."]
    )
    for tab in ("summary", "documents", "suggestions", "cost", "configuration"):
        response = await api_client.get("/ia", params={"tab": tab})
        assert response.status_code == 200, response.text
        assert "Central de IA" in response.text and "sk-private-test" not in response.text
    assert (await api_client.get(f"/ia/documents/{identifier}")).status_code == 200
    async with database.sessions() as db:
        work = await db.scalar(select(LLMWorkItem))
        wid = work.id
    assert (await api_client.get(f"/ia/jobs/{wid}")).status_code == 200
    overview = (await api_client.get("/api/llm/overview")).json()
    assert overview["connection"] == "não verificada"
    assert "sk-private-test" not in str(overview)
    assert (await api_client.get("/api/llm/documents", params={"page": 0})).status_code == 422
    assert (await api_client.get(f"/api/llm/jobs/{uuid4()}")).status_code == 404
    assert (await api_client.get("/api/llm/documents", params={"run": "broken"})).status_code == 422
    response = await api_client.post("/api/llm/controls/llm", json={"paused": True})
    assert response.status_code == 200 and response.json() == {"collections": False, "llm": True}
    assert (await api_client.post(f"/api/llm/documents/{uuid4()}/enqueue")).status_code == 404
    assert (await api_client.post(f"/api/llm/batches/{uuid4()}/cancel")).status_code == 404


async def test_stale_contract_cannot_change_accepted_supplier_or_procurement(database, ai_settings):
    controls = ["01612325000198-1-000001/2026", "01612325000198-1-000002/2026"]
    async with write_session(database.sessions) as db:
        for i, control in enumerate(controls):
            db.add(
                Procurement(
                    source="pncp",
                    external_id=control,
                    pncp_control_number=control,
                    title="Teste",
                    fingerprint=f"proc-{i}",
                )
            )
    pipeline = IngestionPipeline(ai_settings, session_factory=database.sessions)
    try:
        for month, cnpj, control in [
            (1, CN_A, controls[0]),
            (3, CN_A, controls[0]),
            (2, CN_B, controls[1]),
        ]:
            raw = RawContract(
                source="pncp",
                external_id="contract-test",
                supplier_cnpj=cnpj,
                procurement_control_number=control,
                source_updated_at=datetime(2026, month, 1, tzinfo=UTC),
                source_update_field="dataAtualizacao",
            )
            await pipeline._persist_contract_bundle(raw, source_record_id=None)
        async with database.sessions() as db:
            contract = await db.scalar(select(Contract))
            assert contract.supplier_cnpj == CN_A
            assert (await db.get(Company, contract.company_id)).cnpj == CN_A
            assert (
                await db.get(Procurement, contract.procurement_id)
            ).pncp_control_number == controls[0]
            assert await db.scalar(
                select(func.count())
                .select_from(FieldChange)
                .where(FieldChange.decision == "stale_ignored")
            )
    finally:
        for connector in (pipeline._connectors or {}).values():
            await connector.aclose()


async def test_exact_contract_identifier_and_date_safety(database, ai_settings):
    control = "01612325000198-2-000001/2026"
    async with write_session(database.sessions) as db:
        contract = Contract(
            source="pncp",
            external_id="c",
            pncp_control_number=control,
            procurement_control_number="01612325000198-1-000001/2026",
            supplier_cnpj=CN_A,
            agency_cnpj="01612325000198",
            fingerprint="contract-exact",
        )
        db.add(contract)
        await db.flush()
        cid = contract.id
    service = PaymentIngestionService(ai_settings, session_factory=database.sessions)
    try:
        rows = [
            RawPayment(
                source="manual",
                external_id="wrong-procurement",
                contract_control_number=control,
                procurement_control_number="01612325000198-1-000002/2026",
                supplier_cnpj=CN_A,
                payment_date=datetime(2026, 11, 2, tzinfo=UTC),
            ),
            RawPayment(
                source="manual",
                external_id="dated",
                contract_control_number=control,
                supplier_cnpj=CN_A,
                agency_cnpj="01612325000198",
                payment_date=datetime(2026, 11, 2, tzinfo=UTC),
            ),
            RawPayment(
                source="manual",
                external_id="undated",
                contract_control_number=control,
                supplier_cnpj=CN_A,
            ),
            RawPayment(
                source="manual",
                external_id="wrong",
                contract_control_number=control,
                supplier_cnpj=CN_B,
                payment_date=datetime(2026, 11, 2, tzinfo=UTC),
            ),
        ]
        await service.persist_raw_payments(rows)
        async with database.sessions() as db:
            payments = {p.external_id: p for p in (await db.scalars(select(Payment))).all()}
            assert payments["dated"].contract_id == cid
            assert payments["wrong-procurement"].contract_id is None
            assert payments["undated"].contract_id is None and payments["wrong"].contract_id is None
            assert payments["dated"].contract_control_number == control
    finally:
        await service.aclose()


async def test_listing_query_count_is_independent_of_documents(database, ai_settings):
    from sqlalchemy import event

    identifier = await extracted(
        database, ai_settings, ["A proposta foi desclassificada por ausência de documento."]
    )
    calls = []

    def count(_connection, _cursor, statement, _params, _context, _many):
        calls.append(statement)

    event.listen(database.engine.sync_engine, "before_cursor_execute", count)
    try:
        async with database.sessions() as db:
            await central.documents(db, {}, 1)
        baseline = len(calls)
        event.remove(database.engine.sync_engine, "before_cursor_execute", count)
        async with write_session(database.sessions) as db:
            original = await db.get(Document, identifier)
            for index in range(24):
                db.add(
                    Document(
                        procurement_id=original.procurement_id,
                        title=f"Documento {index}",
                        original_url=f"https://official.test/{index}",
                        extraction_status=ExtractionStatus.EXTRACTED,
                        fingerprint=f"volume-{index}",
                    )
                )
        calls.clear()
        event.listen(database.engine.sync_engine, "before_cursor_execute", count)
        async with database.sessions() as db:
            page = await central.documents(db, {}, 1)
            assert len(page["items"]) == 20 and page["total"] == 25
        assert len(calls) == baseline
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", count)


async def test_all_citations_keep_their_own_page(database, ai_settings):
    settings = ai_settings.model_copy(update={"llm_max_chunks_per_document": 1})
    act = "A empresa ALFA, CNPJ 04.252.011/0001-10, foi inabilitada."
    reason = "Motivo: ausência da certidão de regularidade fiscal exigida."
    await extracted(database, settings, [act, reason])
    reply = LLMReply(
        analyses=[
            {
                "event_type": "INELIGIBLE",
                "company_cnpj": CN_A,
                "company_name": "ALFA",
                "evidence_quotes": [{"text": act, "page": 1}, {"text": reason, "page": 2}],
                "confidence": 0.9,
                "requires_manual_review": True,
            }
        ]
    )
    await LLMQueueService(
        settings, session_factory=database.sessions, provider=Provider(reply)
    ).drain(wait=False)
    async with database.sessions() as db:
        citations = (
            await db.scalars(
                select(Evidence).join(LLMEventCitation, LLMEventCitation.evidence_id == Evidence.id)
            )
        ).all()
        assert len(citations) == 2 and {e.page_number for e in citations} == {1, 2}
        assert await db.scalar(select(func.count()).select_from(ProcurementEvent)) == 1


async def test_inactive_old_act_does_not_suppress_new_occurrence(database, ai_settings):
    first = "A proposta foi desclassificada por ausência de documento em 01/11/2026."
    second = "A proposta foi desclassificada por ausência de documento em 02/11/2026."
    identifier = await extracted(database, ai_settings, [first])

    def reply(text):
        return LLMReply(
            analyses=[
                {
                    "event_type": "DISQUALIFIED",
                    "evidence_quotes": [{"text": text}],
                    "confidence": 0.9,
                    "requires_manual_review": True,
                }
            ]
        )

    await LLMQueueService(
        ai_settings, session_factory=database.sessions, provider=Provider(reply(first))
    ).drain(wait=False)
    async with write_session(database.sessions) as db:
        original = await db.get(Document, identifier)
        original.is_active = False
        current = Document(
            procurement_id=original.procurement_id,
            title="Nova ata",
            original_url="https://official.test/new.txt",
            file_hash=hashlib.sha256(second.encode()).hexdigest(),
            extraction_status=ExtractionStatus.EXTRACTED,
            extracted_text=second,
            fingerprint="new-act-document",
        )
        db.add(current)
        await db.flush()
        db.add(DocumentChunk(document_id=current.id, sequence=0, text=second))
        await db.flush()
        await enqueue_document(db, current, ai_settings)
    await LLMQueueService(
        ai_settings,
        session_factory=database.sessions,
        provider=Provider(reply(second)),
        clock=lambda: datetime.now(UTC) + timedelta(seconds=10),
    ).drain(wait=False)
    async with database.sessions() as db:
        assert await db.scalar(select(func.count()).select_from(ProcurementEvent)) == 2


async def test_deactivation_obsoletes_suggestions_and_unsubmitted_work(database, ai_settings):
    from app.services.ingestion.processor import suspend_document_derivatives

    identifier = await extracted(
        database, ai_settings, ["A proposta foi desclassificada por ausência de documento."]
    )
    async with write_session(database.sessions) as db:
        doc = await db.get(Document, identifier)
        row = LLMSuggestion(
            fingerprint="to-be-obsolete",
            procurement_id=doc.procurement_id,
            document_id=identifier,
            analysis={},
            snapshot=await document_snapshot(db, doc),
            evidence_ids=[],
            reason="test",
            status="pending",
        )
        db.add(row)
        await db.flush()
        await suspend_document_derivatives(db, identifier)
    async with database.sessions() as db:
        assert (await db.scalar(select(LLMSuggestion))).status == "obsolete"
        assert (await db.scalar(select(LLMWorkItem))).status == "obsolete"


async def test_name_only_does_not_link_to_a_company_from_another_process(database, ai_settings):
    quote = "A empresa ALFA foi inabilitada por ausência de certidão."
    async with write_session(database.sessions) as db:
        db.add(
            Company(
                cnpj=CN_A, legal_name="ALFA", normalized_name="ALFA", fingerprint="unrelated-alfa"
            )
        )
    await extracted(database, ai_settings, [quote])
    reply = LLMReply(
        analyses=[
            {
                "event_type": "INELIGIBLE",
                "company_name": "ALFA",
                "evidence_quotes": [{"text": quote}],
                "confidence": 0.9,
                "requires_manual_review": True,
            }
        ]
    )
    await LLMQueueService(
        ai_settings, session_factory=database.sessions, provider=Provider(reply)
    ).drain(wait=False)
    async with database.sessions() as db:
        assert await db.scalar(select(func.count()).select_from(ProcurementEvent)) == 0
        assert (await db.scalar(select(LLMSuggestion))).status == "pending"


async def test_derived_payment_links_refresh_the_accepted_identity_clock(database, ai_settings):
    from app.models import FieldState

    service = PaymentIngestionService(ai_settings, session_factory=database.sessions)
    try:
        for month, supplier, name in [(1, CN_A, "ALFA"), (3, CN_A, None), (2, CN_B, "BETA")]:
            await service.persist_raw_payments(
                [
                    RawPayment(
                        source="manual",
                        external_id="versioned-payment",
                        supplier_cnpj=supplier,
                        supplier_name=name,
                        source_updated_at=datetime(2026, month, 1, tzinfo=UTC),
                        source_update_field="dataAtualizacao",
                        amount=10,
                    )
                ]
            )
        async with database.sessions() as db:
            payment = await db.scalar(select(Payment))
            assert payment.supplier_cnpj == CN_A and payment.supplier_name == "ALFA"
            assert (await db.get(Company, payment.company_id)).cnpj == CN_A
            clock = await db.scalar(
                select(FieldState).where(
                    FieldState.entity_type == "payment",
                    FieldState.entity_id == payment.id,
                    FieldState.field_name == "company_id",
                )
            )
            assert clock.source_updated_at == datetime(2026, 3, 1, tzinfo=UTC)
    finally:
        await service.aclose()


async def test_saved_old_result_dto_cannot_create_a_nonaward_supplier(database, ai_settings):
    identifier = await _pending_document(database)
    pipeline = IngestionPipeline(ai_settings, session_factory=database.sessions)
    async with write_session(database.sessions) as db:
        doc = await db.get(Document, identifier)
        procurement = await db.get(Procurement, doc.procurement_id)
        placeholder = RawResult(
            source="pncp",
            external_id="old-result",
            procurement_external_id=procurement.external_id,
            supplier_cnpj=CN_A,
            supplier_name="NÃO HOMOLOGADO",
            role="awarded",
            total_value=0,
        )
        await pipeline._upsert_results(db, procurement, [placeholder], {}, {})
        assert await db.scalar(select(func.count()).select_from(Company)) == 0
        assert await db.scalar(select(func.count()).select_from(Participant)) == 0
        real = placeholder.model_copy(
            update={"external_id": "real-result", "supplier_name": "ALFA"}
        )
        await pipeline._upsert_results(db, procurement, [real], {}, {})
        await db.flush()
        participant = await db.scalar(select(Participant))
        assert participant.final_value == 0
