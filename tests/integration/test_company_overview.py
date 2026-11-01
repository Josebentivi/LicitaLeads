from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.models import (
    Company,
    CompanyContact,
    ContactStatus,
    Contract,
    DataAvailability,
    Document,
    Evidence,
    FieldState,
    JobLease,
    Lead,
    LeadReview,
    LeadScoreRevision,
    LeadStatus,
    Participant,
    Payment,
    PriceRegistry,
    PriceRegistryItem,
    Procurement,
    ProcurementEvent,
    ProcurementEventType,
    ProcurementItem,
    ReasonCategory,
    SourceRecord,
)
from app.services.lead_scoring.evaluation import evaluate_lead

CNPJ = "04252011000110"
OTHER = "11222333000181"


async def seed(database):
    async with database.sessions() as db, db.begin():
        a = Company(cnpj=CNPJ, normalized_name="EMPRESA ALVO", legal_name="Empresa Alvo")
        b = Company(cnpj=OTHER, normalized_name="OUTRA EMPRESA")
        procurement = Procurement(
            source="pncp",
            external_id=str(uuid4()),
            pncp_control_number="official-process",
            estimated_value=Decimal("99999999"),
        )
        source = SourceRecord(
            source="pncp",
            endpoint="https://pncp.gov.br/oficial",
            request_parameters={},
            response_hash="a" * 64,
            raw_payload={},
            availability=DataAvailability.AVAILABLE,
        )
        db.add_all([a, b, procurement, source])
        await db.flush()
        archive = Document(
            procurement_id=procurement.id,
            original_url="https://pncp.gov.br/archive.zip",
            title="Arquivo",
            is_active=True,
        )
        db.add(archive)
        await db.flush()
        document = Document(
            procurement_id=procurement.id,
            parent_document_id=archive.id,
            original_url="https://pncp.gov.br/file.pdf",
            title="Documento oficial",
            is_active=True,
            extracted_text="<script>alert('unsafe')</script> Prova documental",
        )
        db.add(document)
        await db.flush()
        evidence = Evidence(
            procurement_id=procurement.id,
            document_id=document.id,
            source_record_id=source.id,
            source_url="https://pncp.gov.br/oficial",
            text_excerpt="Empresa inabilitada",
        )
        db.add(evidence)
        await db.flush()
        event = ProcurementEvent(
            procurement_id=procurement.id,
            company_id=a.id,
            event_type=ProcurementEventType.INELIGIBLE,
            raw_description="Empresa inabilitada",
            source="document",
            source_url="https://pncp.gov.br/oficial",
            source_record_id=source.id,
            evidence_id=evidence.id,
            reason_category=ReasonCategory.OTHER,
            confidence=Decimal("1"),
        )
        db.add(event)
        await db.flush()
        lead = Lead(
            procurement_id=procurement.id,
            company_id=a.id,
            triggering_event_id=event.id,
            score=61,
            fit_score=61,
            urgency_score=12,
            evidence_score=25,
            legal_relevance_score=20,
            contact_score=4,
            economic_value_score=0,
            scoring_version="1.0",
            reason_summary="Indício com evidência",
            assigned_to="Analista",
        )
        registry = PriceRegistry(
            source="pncp",
            external_id=str(uuid4()),
            linked_pncp_control_number="official-process",
            registry_number="ARP-ALVO",
            source_record_id=source.id,
        )
        db.add_all([lead, registry])
        await db.flush()
        db.add_all(
            [
                PriceRegistryItem(
                    price_registry_id=registry.id,
                    item_number="1",
                    supplier_cnpj=CNPJ,
                    total_value=Decimal("50000"),
                    source_record_id=source.id,
                ),
                PriceRegistryItem(
                    price_registry_id=registry.id,
                    item_number="2",
                    supplier_cnpj=OTHER,
                    total_value=Decimal("9000000"),
                    source_record_id=source.id,
                ),
                Contract(
                    source="pncp",
                    external_id=str(uuid4()),
                    supplier_cnpj=CNPJ,
                    procurement_control_number="official-process",
                    global_value=Decimal("200000"),
                    source_record_id=source.id,
                ),
                Payment(
                    source="manual:test",
                    external_id=str(uuid4()),
                    supplier_cnpj=CNPJ,
                    amount=Decimal("8000000"),
                    phase="empenho",
                    is_current=True,
                    match_status="unlinked",
                    source_record_id=source.id,
                ),
                CompanyContact(
                    company_id=a.id,
                    contact_type="email",
                    contact_value="empresa@example.com",
                    is_corporate=True,
                    confidence=Decimal(1),
                    status=ContactStatus.VERIFIED,
                ),
            ]
        )
        return {
            "company": a.id,
            "other": b.id,
            "procurement": procurement.id,
            "lead": lead.id,
            "event": event.id,
            "registry": registry.id,
            "archive": archive.id,
            "document": document.id,
            "source": source.id,
        }


@pytest.mark.asyncio
async def test_dossier_exact_supplier_membership_pagination_and_escaping(api_client, database):
    ids = await seed(database)
    overview = await api_client.get(f"/api/companies/{CNPJ}/overview")
    assert overview.status_code == 200, overview.text
    assert overview.json()["counts"]["price_registries"] == 1
    assert overview.json()["counts"]["contracts"] == 1
    assert overview.json()["counts"]["payments"] == 1
    response = await api_client.get(
        f"/api/companies/{CNPJ}/sections/price_registries/{ids['registry']}"
    )
    items = response.json()["groups"][0]["items"]
    assert [i["supplier_cnpj"] for i in items] == [CNPJ]
    forbidden = await api_client.get(f"/api/companies/{OTHER}/sections/leads/{ids['lead']}")
    assert forbidden.status_code == 404
    html = await api_client.get(f"/empresas/{CNPJ}")
    assert html.status_code == 200, html.text
    assert "Visão completa da empresa" in html.text
    assert "data-company-section" in html.text
    fragment = await api_client.get(
        f"/empresas/{CNPJ}/sections/documents/{ids['document']}", headers={"HX-Request": "true"}
    )
    assert fragment.status_code == 200, fragment.text
    assert "&lt;script&gt;" in fragment.text
    assert "<script>alert" not in fragment.text
    assert "local_path" not in fragment.text


@pytest.mark.asyncio
async def test_rescore_explicit_atomic_idempotent_preserves_review(api_client, database):
    ids = await seed(database)
    async with database.sessions() as db, db.begin():
        lead = await db.get(Lead, ids["lead"])
        await evaluate_lead(db, lead)
        assert lead.scoring_version == "1.0"
        lead.lead_status = LeadStatus.APPROVED
        db.add(
            LeadReview(
                lead_id=lead.id, decision="approved", reviewer="Pessoa", notes="Manter decisão"
            )
        )
    payload = {"request_id": str(uuid4()), "lead_ids": [str(ids["lead"])]}
    first = await api_client.post(f"/api/companies/{CNPJ}/rescore", json=payload)
    assert first.status_code == 200, first.text
    repeat = await api_client.post(f"/api/companies/{CNPJ}/rescore", json=payload)
    assert repeat.status_code == 200, repeat.text
    assert repeat.json()["items"][0]["reused"] is True
    async with database.sessions() as db:
        lead = await db.get(Lead, ids["lead"])
        assert lead.scoring_version == "2.0"
        assert lead.lead_status == LeadStatus.APPROVED
        assert lead.assigned_to == "Analista"
        assert lead.economic_value_score == 8  # company contract, never other supplier or payments
        assert await db.scalar(select(func.count()).select_from(LeadScoreRevision)) == 1


@pytest.mark.asyncio
async def test_withdrawn_archive_blocks_evaluation_and_current_documents(api_client, database):
    ids = await seed(database)
    async with database.sessions() as db, db.begin():
        archive = await db.get(Document, ids["archive"])
        archive.is_active = False
    response = await api_client.get(f"/api/companies/{CNPJ}/sections/documents")
    assert response.status_code == 200, response.text
    assert response.json()["total"] == 0
    response = await api_client.post(
        f"/api/companies/{CNPJ}/rescore",
        json={"request_id": str(uuid4()), "lead_ids": [str(ids["lead"])]},
    )
    assert response.status_code == 200, response.text
    assert "skipped" in response.json()["items"][0]
    async with database.sessions() as db:
        assert (await db.get(Lead, ids["lead"])).scoring_version == "1.0"


@pytest.mark.asyncio
async def test_rescore_lease_and_selection_validation(api_client, database):
    ids = await seed(database)
    async with database.sessions() as db, db.begin():
        db.add(
            JobLease(
                name=f"rescore:company:{ids['company']}",
                owner_id="other",
                acquired_at=datetime.now(UTC),
                heartbeat_at=datetime.now(UTC),
                expires_at=datetime.now(UTC) + timedelta(minutes=2),
            )
        )
    payload = {"request_id": str(uuid4()), "lead_ids": [str(ids["lead"])]}
    assert (
        await api_client.post(f"/api/companies/{CNPJ}/rescore", json=payload)
    ).status_code == 409
    assert (
        await api_client.post(f"/api/companies/{OTHER}/rescore", json=payload)
    ).status_code == 404
    assert (
        await api_client.post(
            f"/api/companies/{CNPJ}/rescore", json={"request_id": str(uuid4()), "lead_ids": []}
        )
    ).status_code == 422
    assert (await api_client.get(f"/api/companies/{CNPJ}/sections/unknown")).status_code == 422


@pytest.mark.asyncio
async def test_score_transitions_and_withdrawn_item_scope(api_client, database):
    ids = await seed(database)

    async def rescore_once():
        response = await api_client.post(
            f"/api/companies/{CNPJ}/rescore",
            json={"request_id": str(uuid4()), "lead_ids": [str(ids["lead"])]},
        )
        assert response.status_code == 200, response.text
        return response.json()["items"][0]["score"]

    a = await rescore_once()
    async with database.sessions() as db, db.begin():
        contact = await db.scalar(
            select(CompanyContact).where(CompanyContact.company_id == ids["company"])
        )
        contact.status = ContactStatus.INVALID
    b = await rescore_once()
    async with database.sessions() as db, db.begin():
        contact = await db.scalar(
            select(CompanyContact).where(CompanyContact.company_id == ids["company"])
        )
        contact.status = ContactStatus.VERIFIED
    assert a != b
    assert await rescore_once() == a
    async with database.sessions() as db:
        revisions = list(
            (
                await db.scalars(
                    select(LeadScoreRevision).order_by(LeadScoreRevision.calculated_at)
                )
            ).all()
        )
        assert [r.score for r in revisions] == [a, b, a]


@pytest.mark.asyncio
async def test_volume_query_count_and_supplier_conflicts(api_client, database):
    from sqlalchemy import event

    ids = await seed(database)
    queries = []

    def count_query(*args):
        queries.append(args[2])

    event.listen(database.engine.sync_engine, "before_cursor_execute", count_query)
    try:
        await api_client.get(f"/api/companies/{CNPJ}/overview")
        initial_count = len(queries)
        async with database.sessions() as db, db.begin():
            db.add_all(
                Payment(
                    source="manual:volume",
                    external_id=str(i),
                    supplier_cnpj=CNPJ,
                    phase="pagamento",
                    amount=Decimal("0.01"),
                )
                for i in range(2000)
            )
            db.add(
                Payment(
                    source="manual:contradiction",
                    external_id="conflicting",
                    company_id=ids["other"],
                    supplier_cnpj=CNPJ,
                )
            )
        queries.clear()
        response = await api_client.get(f"/api/companies/{CNPJ}/overview")
        assert response.status_code == 200, response.text
        assert len(queries) == initial_count
        assert response.json()["counts"]["payments"] == 2001
        page = await api_client.get(f"/api/companies/{CNPJ}/sections/payments?page=2")
        assert len(page.json()["items"]) == 20
        assert "conflicting" not in {x["external_id"] for x in page.json()["items"]}
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", count_query)


@pytest.mark.asyncio
async def test_item_scope_and_unresolved_value_conflict(database):
    from app.services.lead_scoring.evaluation import economic_reference

    ids = await seed(database)
    async with database.sessions() as db, db.begin():
        company = await db.get(Company, ids["company"])
        procurement = await db.get(Procurement, ids["procurement"])
        event = await db.get(ProcurementEvent, ids["event"])
        item = ProcurementItem(
            procurement_id=procurement.id, item_number="scope-1", description="Item da ocorrência"
        )
        db.add(item)
        await db.flush()
        event.item_id = item.id
        amount, refs = await economic_reference(db, company, procurement, event)
        assert (
            amount is None and refs == []
        )  # process-wide contract/ARP cannot stand in for the item
        participant = Participant(
            procurement_id=procurement.id,
            company_id=company.id,
            item_id=item.id,
            source="pncp",
            source_record_id=ids["source"],
            final_value=Decimal("50000"),
        )
        db.add(participant)
        await db.flush()
        amount, refs = await economic_reference(db, company, procurement, event)
        assert amount == Decimal("50000")
        db.add(
            FieldState(
                entity_type="participant",
                entity_id=participant.id,
                field_name="final_value",
                source="pncp",
                value="50000",
                uncertain=True,
                observed_at=datetime.now(UTC),
            )
        )
        await db.flush()
        assert (await economic_reference(db, company, procurement, event))[0] is None


@pytest.mark.asyncio
async def test_rescore_rolls_back_every_lead_and_audit_on_failure(
    api_client, database, monkeypatch
):
    from app.api.routes import company_overview

    ids = await seed(database)
    async with database.sessions() as db, db.begin():
        db.add(
            Lead(
                procurement_id=ids["procurement"],
                company_id=ids["company"],
                triggering_event_id=ids["event"],
                score=61,
                urgency_score=12,
                evidence_score=25,
                legal_relevance_score=20,
                contact_score=4,
                economic_value_score=0,
                fit_score=61,
                scoring_version="1.0",
                reason_summary="Outro lead",
            )
        )
    async with database.sessions() as db:
        identifiers = list((await db.scalars(select(Lead.id))).all())
    original = company_overview.evaluate_lead
    calls = 0

    async def failing(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("falha controlada após primeiro cálculo")
        return await original(*args, **kwargs)

    monkeypatch.setattr(company_overview, "evaluate_lead", failing)
    with pytest.raises(RuntimeError, match="falha controlada"):
        await api_client.post(
            f"/api/companies/{CNPJ}/rescore",
            json={"request_id": str(uuid4()), "lead_ids": [str(x) for x in identifiers]},
        )
    async with database.sessions() as db:
        assert await db.scalar(select(func.count()).select_from(LeadScoreRevision)) == 0
        assert (await db.scalars(select(Lead.scoring_version))).all() == ["1.0", "1.0"]
        assert await db.get(JobLease, f"rescore:company:{ids['company']}") is None


@pytest.mark.asyncio
async def test_blank_official_identifiers_do_not_link_unrelated_process(api_client, database):
    async with database.sessions() as db, db.begin():
        company = Company(cnpj=CNPJ, normalized_name="EMPRESA SOMENTE NA ATA")
        unrelated = Procurement(source="pncp", external_id="unrelated", pncp_control_number="")
        registry = PriceRegistry(
            source="pncp", external_id="blank-control", linked_pncp_control_number=""
        )
        db.add_all([company, unrelated, registry])
        await db.flush()
        db.add(
            PriceRegistryItem(price_registry_id=registry.id, item_number="1", supplier_cnpj=CNPJ)
        )
    response = await api_client.get(f"/api/companies/{CNPJ}/overview")
    assert response.status_code == 200, response.text
    assert response.json()["counts"]["price_registries"] == 1
    assert response.json()["counts"]["procurements"] == 0
