"""Analyst journeys preserve proof, deadline semantics, filters and review context."""

from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlsplit

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import func, select

from app.models import (
    Contract,
    CrawlRun,
    Deadline,
    DeadlineCalculationMethod,
    Document,
    Evidence,
    Lead,
    LeadReview,
    LeadStatus,
    PriceRegistry,
    ProcurementSource,
    SourceRecord,
)
from app.models.participation import FactVerification

from .test_api_leads import _full_graph


@pytest.mark.parametrize("kind", ["unknown", "estimated", "explicit", "historical"])
async def test_deadline_display_does_not_promote_trigger_or_history(api_client, database, kind):
    ids = await _full_graph(database)
    async with database.sessions() as session, session.begin():
        lead = await session.get_one(Lead, ids.lead)
        deadline = await session.get_one(Deadline, lead.deadline_id)
        deadline.explicit_deadline_at = None
        deadline.estimated_deadline_at = None
        if kind == "estimated":
            deadline.estimated_deadline_at = datetime.now(UTC) + timedelta(hours=12)
            deadline.calculation_method = DeadlineCalculationMethod.LEGAL_ESTIMATE
        elif kind in {"explicit", "historical"}:
            deadline.explicit_deadline_at = datetime.now(UTC) + timedelta(hours=12)
        if kind == "historical":
            session.add(
                FactVerification(
                    entity_type="deadline",
                    entity_id=deadline.id,
                    status="invalidated",
                    reason="Fonte desativada",
                    rule_version="test",
                )
            )
    page = await api_client.get(f"/leads/{ids.lead}")
    section = BeautifulSoup(page.text, "html.parser").select_one("[aria-labelledby=deadline-title]")
    assert page.status_code == 200
    if kind == "unknown":
        assert "Prazo não identificado" in section.text
        assert "Data-limite não identificada" in section.text
        assert section.select_one("strong").text == "Data-limite não identificada"
    elif kind == "historical":
        assert "Prazo histórico" in section.text
    elif kind == "estimated":
        assert "Prazo estimado — confirmar" in section.text
    else:
        assert "Prazo expresso na fonte" in section.text
    due = await api_client.get("/leads?due=24h")
    assert ("Empresa Exemplo Ltda." in due.text) is (kind in {"estimated", "explicit"})


async def test_evidence_is_readable_escaped_and_links_to_document(api_client, database):
    ids = await _full_graph(database)
    async with database.sessions() as session, session.begin():
        evidence = await session.get_one(Evidence, ids.evidence)
        evidence.text_excerpt = '<script>alert("documento")</script> Trecho oficial'
        evidence.source_url = "javascript:alert(1)"
    page = await api_client.get(f"/evidencias/{ids.evidence}")
    assert page.status_code == 200
    assert "Trecho que fundamenta o registro" in page.text
    assert "&lt;script&gt;" in page.text and '<script>alert("documento")' not in page.text
    assert f"/api/documents/{ids.document}/view#page=7" in page.text
    assert 'href="javascript:' not in page.text
    async with database.sessions() as session, session.begin():
        (await session.get_one(Document, ids.document)).is_active = False
    historical = await api_client.get(f"/evidencias/{ids.evidence}")
    assert "histórica ou foi desativada" in historical.text


async def test_relative_deadline_display_does_not_reuse_a_stale_open_status(api_client, database):
    ids = await _full_graph(database)
    async with database.sessions() as session, session.begin():
        lead = await session.get_one(Lead, ids.lead)
        deadline = await session.get_one(Deadline, lead.deadline_id)
        deadline.explicit_deadline_at = datetime.now(UTC) - timedelta(hours=1)
    page = await api_client.get(f"/leads/{ids.lead}")
    assert "Data-limite ultrapassada" in page.text
    assert "Situação registrada no último cálculo: Aberto" in page.text
    async with database.sessions() as session:
        lead = await session.get_one(Lead, ids.lead)
        assert (await session.get_one(Deadline, lead.deadline_id)).status.value == "OPEN"


async def test_dashboard_cards_link_to_identically_filtered_queues(api_client, database):
    ids = await _full_graph(database)
    async with database.sessions() as session, session.begin():
        lead = await session.get_one(Lead, ids.lead)
        lead.lead_status = LeadStatus.PENDING_REVIEW
        deadline = await session.get_one(Deadline, lead.deadline_id)
        deadline.explicit_deadline_at = datetime.now(UTC) + timedelta(hours=12)
    page = await api_client.get("/dashboard")
    soup = BeautifulSoup(page.text, "html.parser")
    cards = soup.select(".agenda-metrics a")
    assert len(cards) == 4
    for card in cards:
        listing = await api_client.get(card["href"])
        assert listing.status_code == 200
        count = int(card.select_one("strong").text)
        assert f"{count} oportunidade(s) encontrada(s)" in listing.text
    assert "Última consulta bem-sucedida" in page.text
    assert "atualizar-dados" in page.text


async def test_search_and_review_continue_keep_the_same_queue(api_client, database):
    ids = await _full_graph(database)
    async with database.sessions() as session, session.begin():
        original = await session.get_one(Lead, ids.lead)
        original.lead_status = LeadStatus.PENDING_REVIEW
        other = Lead(
            procurement_id=original.procurement_id,
            company_id=original.company_id,
            triggering_event_id=original.triggering_event_id,
            lead_status=LeadStatus.PENDING_REVIEW,
            score=40,
            urgency_score=10,
            evidence_score=10,
            legal_relevance_score=10,
            contact_score=0,
            reason_summary="Outra análise documentada",
            fingerprint="next-workspace",
        )
        session.add(other)
        await session.flush()
        next_id = other.id
    context = "/leads?q=Empresa&lead_status=pending_review&page=1"
    listing = await api_client.get(context)
    first = BeautifulSoup(listing.text, "html.parser").select_one("tbody a")
    detail = await api_client.get(first["href"])
    soup = BeautifulSoup(detail.text, "html.parser")
    assert soup.select_one("nav[aria-label='Navegação da análise'] a")["href"] == context
    form = soup.select_one("form.review-form")
    response = await api_client.post(
        form["action"],
        data={
            "decision": "approved",
            "reviewer": "Analista",
            "notes": "Conferido no documento",
            "return_to": context,
            "continue_review": "true",
        },
    )
    assert response.status_code == 303
    target = urlsplit(response.headers["location"])
    assert target.path == f"/leads/{next_id}"
    assert parse_qs(target.query)["return_to"] == [context]
    assert parse_qs(target.query)["review"] == ["previous"]
    async with database.sessions() as session:
        assert (await session.get_one(Lead, ids.lead)).lead_status == LeadStatus.APPROVED
        assert await session.scalar(select(func.count(LeadReview.id))) == 1
    for query in ["Empresa", "00.000.000/0001-91", "14"]:
        assert "Empresa Exemplo Ltda." in (await api_client.get("/leads", params={"q": query})).text
    assert "Nenhum lead encontrado" in (await api_client.get("/leads?q=%25")).text


async def test_invalid_review_preserves_values_and_blocks_historical_actions(api_client, database):
    ids = await _full_graph(database)
    response = await api_client.post(
        f"/leads/{ids.lead}/review",
        data={
            "decision": "approved",
            "reviewer": " ",
            "notes": "Conferir o documento original",
        },
    )
    assert response.status_code == 422
    assert "Conferir o documento original" in response.text
    assert "Informe o nome do revisor" in response.text
    async with database.sessions() as session, session.begin():
        (await session.get_one(Document, ids.document)).is_active = False
    blocked = await api_client.post(
        f"/leads/{ids.lead}/review",
        data={
            "decision": "approved",
            "reviewer": "Analista",
        },
    )
    assert blocked.status_code == 409
    async with database.sessions() as session:
        assert await session.scalar(select(func.count(LeadReview.id))) == 0


@pytest.mark.parametrize(
    "unsafe",
    ["https://evil.test/leads", "//evil.test/leads", "http://[", "/settings", "/leads\\evil"],
)
async def test_review_navigation_never_redirects_outside_the_queue(api_client, database, unsafe):
    ids = await _full_graph(database)
    page = await api_client.get(f"/leads/{ids.lead}", params={"return_to": unsafe})
    back = BeautifulSoup(page.text, "html.parser").select_one(
        "nav[aria-label='Navegação da análise'] a"
    )
    assert back["href"] == "/leads"


async def test_collection_result_scope_uses_provenance_not_timestamps(api_client, database):
    ids = await _full_graph(database)
    before = await api_client.get("/leads", params={"run": str(ids.run)})
    assert "0 oportunidade(s) encontrada(s)" in before.text
    async with database.sessions() as session, session.begin():
        record = SourceRecord(
            crawl_run_id=ids.run,
            source="pncp",
            endpoint="https://pncp.gov.br/test",
            response_hash="workspace-response",
            fingerprint="workspace-record",
            http_status=200,
        )
        session.add(record)
        await session.flush()
        source = await session.scalar(
            select(ProcurementSource).where(ProcurementSource.procurement_id == ids.procurement)
        )
        source.source_record_id = record.id
    after = await api_client.get("/leads", params={"run": str(ids.run)})
    assert "1 oportunidade(s) encontrada(s)" in after.text
    assert "Podem incluir oportunidades anteriores" in after.text
    procurements = await api_client.get("/procurements", params={"run": str(ids.run)})
    assert str(ids.procurement) in procurements.text


async def test_document_retry_results_and_historical_labels(api_client, database):
    ids = await _full_graph(database)
    async with database.sessions() as session, session.begin():
        run = await session.get_one(CrawlRun, ids.run)
        run.cursor = {"sources": {"documents": {"document_ids": [str(ids.document)]}}}
    scoped = await api_client.get("/leads", params={"run": str(ids.run)})
    assert "1 oportunidade(s) encontrada(s)" in scoped.text
    async with database.sessions() as session, session.begin():
        session.add(
            FactVerification(
                entity_type="lead",
                entity_id=ids.lead,
                status="invalidated",
                reason="Atribuição antiga invalidada",
                rule_version="test",
            )
        )
    history = await api_client.get("/leads?include_history=true")
    assert "Histórica ou invalidada" in history.text
    assert "Consultar histórico" in history.text
    assert "0 oportunidade(s) encontrada(s)" in (await api_client.get("/leads")).text


async def test_collection_validation_keeps_fields_on_the_correction_form(api_client):
    response = await api_client.post(
        "/crawls/run",
        data={
            "uf": "PI",
            "connector": "pncp",
            "start_date": "2026-02-01",
            "end_date": "2026-01-01",
            "batch_size": "45",
            "process_documents": "true",
        },
    )
    assert response.status_code == 422
    soup = BeautifulSoup(response.text, "html.parser")
    assert soup.select_one("input[name=start_date]")["value"] == "2026-02-01"
    assert soup.select_one("input[name=batch_size]")["value"] == "45"
    assert soup.select_one("select[name=uf] option[selected]")["value"] == "PI"
    assert soup.select_one("input[name=process_documents]").has_attr("checked")
    malformed = await api_client.post(
        "/contratos/collect",
        data={
            "connector": "pncp",
            "agency_cnpj": "00000000000191",
            "start_date": "bad-date",
        },
    )
    assert malformed.status_code == 422
    assert 'value="00000000000191"' in malformed.text
    assert "text/html" in malformed.headers["content-type"]


@pytest.mark.parametrize("path", ["/atas", "/contratos", "/pagamentos"])
async def test_supplier_lists_keep_invalid_filter_instead_of_broadening(api_client, path):
    page = await api_client.get(path, params={"supplier_cnpj": "123"})
    assert page.status_code == 422
    assert 'value="123"' in page.text
    assert "CNPJ do fornecedor inválido" in page.text
    assert "filtro(s) ativo(s)" in page.text


async def test_payment_feedback_is_not_counted_as_a_search_filter(api_client):
    page = await api_client.get("/pagamentos?municipal_started=true&phase=pago")
    assert page.status_code == 200
    assert "1 filtro(s) ativo(s)" in page.text
    assert 'name="source"' in page.text and '<select name="phase">' in page.text


async def test_ata_and_contract_details_return_to_the_filtered_list(api_client, database):
    async with database.sessions() as session, session.begin():
        contract = Contract(
            source="pncp", external_id="context-contract", contract_number="CTX-1", year=2026
        )
        registry = PriceRegistry(
            source="pncp",
            external_id="context-registry",
            registry_number="CTX-1",
            year=2026,
            object_description="CTX registro de teste",
            fingerprint="context-registry",
        )
        session.add_all([contract, registry])
        await session.flush()
        cases = [("/contratos", contract.id), ("/atas", registry.id)]
    for path, identifier in cases:
        context = path + "?search=CTX"
        listing = await api_client.get(context)
        assert listing.status_code == 200
        link = BeautifulSoup(listing.text, "html.parser").find(
            "a",
            href=lambda value, prefix=f"{path}/{identifier}": value and value.startswith(prefix),
        )
        detail = await api_client.get(link["href"])
        assert detail.status_code == 200
        assert BeautifulSoup(detail.text, "html.parser").find("a", href=context)
