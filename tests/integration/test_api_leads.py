"""Operational API and server-rendered workflow coverage."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import httpx
import pytest

from app import web as web_routes
from app.api.routes import crawls as crawl_routes
from app.api.routes import evidence as evidence_routes
from app.config import Settings
from app.models import (
    Company,
    CrawlRun,
    CrawlRunStatus,
    Deadline,
    DeadlineCalculationMethod,
    DeadlineStatus,
    Document,
    Evidence,
    ExtractionStatus,
    Lead,
    LeadStatus,
    Participant,
    ParticipantRole,
    Procurement,
    ProcurementEvent,
    ProcurementEventType,
    ProcurementItem,
    ProcurementSource,
    ReasonCategory,
)

from .conftest import DatabaseContext


async def _full_graph(database: DatabaseContext):
    observed_at = datetime(2026, 9, 17, 15, 30, tzinfo=UTC)
    async with database.sessions() as session, session.begin():
        procurement = Procurement(
            source="pncp",
            external_id="api-graph-1",
            pncp_control_number="00000000000191-1-000099/2026",
            purchase_number="14",
            purchase_year=2026,
            modality="pregao_eletronico",
            title="Pregão Eletrônico 14/2026",
            object_description="Equipamentos médicos",
            agency_name="Prefeitura de Exemplo",
            agency_cnpj="00000000000191",
            uf="MA",
            municipality="São Luís",
            estimated_value=Decimal("180000"),
            proposal_end_at=observed_at + timedelta(days=2),
            publication_at=observed_at,
            status="em julgamento",
            source_url="https://pncp.gov.br/processo/99",
            fingerprint="1" * 64,
        )
        company = Company(
            cnpj="00000000000191",
            legal_name="Empresa Exemplo Ltda.",
            normalized_name="EMPRESA EXEMPLO",
            fingerprint="2" * 64,
        )
        session.add_all([procurement, company])
        await session.flush()
        source = ProcurementSource(
            procurement_id=procurement.id,
            source="pncp",
            external_id=procurement.external_id,
            source_url=procurement.source_url,
            is_primary=True,
            fingerprint="3" * 64,
        )
        item = ProcurementItem(
            procurement_id=procurement.id,
            item_number="3",
            description="Monitor",
            quantity=Decimal("12"),
            unit="Unidade",
            estimated_unit_value=Decimal("1500"),
            estimated_total_value=Decimal("18000"),
            result_status="em andamento",
            fingerprint="4" * 64,
        )
        second_item = ProcurementItem(
            procurement_id=procurement.id,
            item_number="19",
            description="Conjunto de equipamentos para monitoramento hospitalar com acessórios e "
            "instalação técnica especializada para as unidades de atendimento do município, "
            "incluindo garantia, treinamento da equipe e manutenção assistida inicial.",
            quantity=Decimal("1"),
            unit="Conjunto",
            estimated_unit_value=Decimal("162000"),
            estimated_total_value=Decimal("162000"),
            result_status="em julgamento",
            fingerprint="b" * 64,
        )
        document = Document(
            procurement_id=procurement.id,
            document_type="ata",
            title="Ata",
            original_url="https://pncp.gov.br/ata/99",
            extracted_text="A licitante foi inabilitada por não apresentar certidão.",
            extraction_status=ExtractionStatus.EXTRACTED,
            published_at=observed_at,
            fingerprint="5" * 64,
        )
        session.add_all([source, item, second_item, document])
        await session.flush()
        evidence = Evidence(
            procurement_id=procurement.id,
            document_id=document.id,
            page_number=7,
            locator="page:7",
            text_excerpt="A licitante foi inabilitada por não apresentar certidão.",
            source_url=document.original_url,
            fingerprint="6" * 64,
        )
        session.add(evidence)
        await session.flush()
        participant = Participant(
            procurement_id=procurement.id,
            company_id=company.id,
            item_id=item.id,
            participation_role=ParticipantRole.PARTICIPANT,
            source="document",
            source_evidence_id=evidence.id,
            confidence=Decimal("0.95"),
            fingerprint="7" * 64,
        )
        event = ProcurementEvent(
            procurement_id=procurement.id,
            company_id=company.id,
            item_id=item.id,
            event_type=ProcurementEventType.INELIGIBLE,
            occurred_at=observed_at,
            raw_description=evidence.text_excerpt,
            normalized_reason="Certidão exigida não apresentada.",
            reason_category=ReasonCategory.MISSING_DOCUMENT,
            source="document",
            source_url=document.original_url,
            evidence_id=evidence.id,
            confidence=Decimal("0.95"),
            requires_manual_review=False,
            fingerprint="8" * 64,
        )
        session.add_all([participant, event])
        await session.flush()
        deadline = Deadline(
            procurement_id=procurement.id,
            company_id=company.id,
            event_id=event.id,
            deadline_type="appeal",
            trigger_at=observed_at,
            explicit_deadline_at=observed_at + timedelta(days=2),
            calculation_method=DeadlineCalculationMethod.EXPLICIT,
            remaining_seconds=172800,
            status=DeadlineStatus.OPEN,
            confidence=Decimal("0.98"),
            requires_manual_review=False,
            calculation_explanation="Prazo explicitamente publicado.",
            fingerprint="9" * 64,
        )
        session.add(deadline)
        await session.flush()
        lead = Lead(
            procurement_id=procurement.id,
            company_id=company.id,
            triggering_event_id=event.id,
            deadline_id=deadline.id,
            lead_status=LeadStatus.NEW,
            score=90,
            urgency_score=24,
            evidence_score=25,
            legal_relevance_score=20,
            contact_score=10,
            economic_value_score=8,
            fit_score=90,
            reason_summary=event.normalized_reason or "",
            recommended_action="Revisar recurso.",
            score_breakdown={"evidence": "25/25"},
            fingerprint="a" * 64,
            created_at=observed_at,
            updated_at=observed_at,
        )
        run = CrawlRun(
            connector="pncp",
            status=CrawlRunStatus.COMPLETED,
            started_at=observed_at,
            finished_at=observed_at,
            filters={"uf": "MA"},
        )
        session.add_all([lead, run])
        await session.flush()
        return SimpleNamespace(
            observed_at=observed_at,
            procurement=procurement.id,
            item=item.id,
            document=document.id,
            evidence=evidence.id,
            event=event.id,
            lead=lead.id,
            run=run.id,
        )


@pytest.mark.asyncio
async def test_procurement_evidence_and_web_pages(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    ids = await _full_graph(database)

    responses = [
        await api_client.get(f"/api/procurements/{ids.procurement}"),
        await api_client.get(f"/api/procurements/{ids.procurement}/items"),
        await api_client.get(f"/api/procurements/{ids.procurement}/documents"),
        await api_client.get(f"/api/procurements/{ids.procurement}/participants"),
        await api_client.get(f"/api/procurements/{ids.procurement}/events"),
        await api_client.get(f"/api/evidence/{ids.evidence}"),
        await api_client.get(f"/api/documents/{ids.document}/view"),
        await api_client.get(f"/api/crawls/{ids.run}"),
        await api_client.get("/api/source-capabilities"),
        await api_client.get("/dashboard"),
        await api_client.get("/procurements"),
        await api_client.get(f"/procurements/{ids.procurement}"),
        await api_client.get("/leads"),
        await api_client.get(f"/leads/{ids.lead}"),
        await api_client.get("/crawls"),
        await api_client.get("/settings"),
        await api_client.get("/source-capabilities"),
    ]

    assert all(response.status_code == 200 for response in responses)
    assert responses[0].json()["sources"][0]["source"] == "pncp"
    assert responses[1].json()[0]["item_number"] == "3"
    assert responses[5].json()["document_view_url"].endswith("/view")
    assert "inabilitada" in responses[6].text
    assert len(responses[8].json()) == 2

    procurement_page = responses[11].text
    assert "Pregão eletrônico" in procurement_page
    assert "Em julgamento" in procurement_page
    assert "R$ 180.000,00" in procurement_page
    assert "17/09/2026 12:30 BRT" in procurement_page
    assert "00.000.000/0001-91" in procurement_page
    assert "Texto extraído" in procurement_page
    assert "Não informado" in procurement_page
    assert "Abrir evidência" in procurement_page
    assert "Ver origem técnica" in procurement_page
    assert '<details class="technical-details">' in procurement_page
    assert "fingerprint" not in procurement_page
    assert "procurement_id" not in procurement_page
    assert "source_record_id" not in procurement_page
    assert "external_item_id" not in procurement_page
    assert "{'" not in procurement_page

    item_text_search = await api_client.get(
        f"/procurements/{ids.procurement}", params={"item_query": "monitor"}
    )
    item_number_search = await api_client.get(
        f"/procurements/{ids.procurement}", params={"item_query": "19"}
    )
    assert item_text_search.status_code == 200
    assert "Monitor" in item_text_search.text
    assert "Conjunto de equipamentos" in item_number_search.text
    assert '<td class="item-number">3</td>' not in item_number_search.text
    assert '<details class="description-details">' in item_number_search.text

    expected_created = ids.observed_at.astimezone(ZoneInfo("America/Sao_Paulo")).strftime(
        "%d/%m/%Y %H:%M:%S %Z"
    )
    leads_page = responses[12].text
    assert "Adicionado em" in leads_page
    assert expected_created in leads_page

    lead_detail_page = responses[13].text
    assert "Cronologia" in lead_detail_page
    assert expected_created in lead_detail_page
    assert "Nenhuma revisão registrada" in lead_detail_page

    dashboard_page = responses[9].text
    assert "Adicionado em" in dashboard_page
    assert expected_created in dashboard_page
    assert '<select name="uf">' in dashboard_page
    assert '<option value="MA" selected>MA · Maranhão</option>' in dashboard_page
    assert '<input name="uf"' not in dashboard_page

    procurements_page = responses[10].text
    assert '<select name="uf">' in procurements_page
    assert '<option value="">Todas</option>' in procurements_page
    assert '<option value="MA">MA · Maranhão</option>' in procurements_page
    assert '<input name="uf"' not in procurements_page

    filtered_procurements = await api_client.get("/procurements", params={"uf": "ma"})
    assert filtered_procurements.status_code == 200
    assert '<option value="MA" selected>MA · Maranhão</option>' in filtered_procurements.text
    assert f"/procurements/{ids.procurement}" in filtered_procurements.text


@pytest.mark.asyncio
async def test_lead_filters_review_patch_and_idempotent_outreach(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    ids = await _full_graph(database)
    listing = await api_client.get(
        "/api/leads",
        params={
            "minimum_score": 80,
            "event_type": "INELIGIBLE",
            "deadline_status": "OPEN",
            "pending_review": False,
            "has_contact": False,
        },
    )
    detail = await api_client.get(f"/api/leads/{ids.lead}")
    review = await api_client.post(
        f"/api/leads/{ids.lead}/review",
        json={"decision": "needs_changes", "notes": "Conferir prazo", "reviewer": "Analista"},
    )
    patch = await api_client.patch(
        f"/api/leads/{ids.lead}",
        json={"lead_status": "approved", "assigned_to": "Equipe A"},
    )
    first = await api_client.post(
        f"/api/leads/{ids.lead}/generate-outreach",
        json={"channel": "email", "force_regenerate": False},
    )
    repeated = await api_client.post(
        f"/api/leads/{ids.lead}/generate-outreach",
        json={"channel": "email", "force_regenerate": False},
    )
    forced = await api_client.post(
        f"/api/leads/{ids.lead}/generate-outreach",
        json={"channel": "email", "force_regenerate": True},
    )

    assert listing.status_code == 200 and listing.json()["total"] == 1
    assert detail.status_code == 200 and detail.json()["company"]["cnpj"] == "00000000000191"
    assert review.status_code == 200 and review.json()["lead"]["lead_status"] == "needs_changes"
    assert patch.status_code == 200 and patch.json()["assigned_to"] == "Equipe A"
    assert first.status_code == 200 and first.json()["created"] is True
    assert repeated.json()["created"] is False
    assert forced.json()["created"] is True
    assert "Nenhuma mensagem" not in first.json()["draft"]["message"]
    assert first.json()["draft"]["sent"] is False

    local_day = ids.observed_at.astimezone(ZoneInfo("America/Sao_Paulo")).date()
    filtered = await api_client.get(
        "/leads",
        params={"created_from": local_day.isoformat(), "created_to": local_day.isoformat()},
    )
    excluded = await api_client.get(
        "/leads", params={"created_from": (local_day + timedelta(days=1)).isoformat()}
    )
    assert filtered.status_code == 200 and "Empresa Exemplo Ltda." in filtered.text
    assert excluded.status_code == 200 and "Nenhum lead encontrado." in excluded.text

    lead_page = await api_client.get(f"/leads/{ids.lead}")
    assert "Revisão: Ajustes solicitados" in lead_page.text
    assert "Analista" in lead_page.text
    assert "Rascunho gerado em" in lead_page.text


@pytest.mark.asyncio
async def test_local_file_view_and_rejected_paths(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ids = await _full_graph(database)
    root = tmp_path / "documents"
    root.mkdir()
    stored = root / "proof.txt"
    stored.write_text("evidência local", encoding="utf-8")
    monkeypatch.setattr(
        evidence_routes,
        "get_settings",
        lambda: Settings(app_env="test", document_storage_path=root),
    )
    async with database.sessions() as session, session.begin():
        document = await session.get(Document, ids.document)
        assert document is not None
        document.local_path = str(stored)

    good = await api_client.get(f"/api/documents/{ids.document}/view")
    async with database.sessions() as session, session.begin():
        document = await session.get(Document, ids.document)
        assert document is not None
        document.local_path = str(tmp_path / "outside.txt")
    forbidden = await api_client.get(f"/api/documents/{ids.document}/view")
    missing = await api_client.get("/api/documents/00000000-0000-0000-0000-000000000000/view")

    assert good.status_code == 200 and good.content.decode() == "evidência local"
    assert forbidden.status_code == 403
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_crawl_post_returns_202_without_waiting_for_source(
    api_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = SimpleNamespace(
        id="00000000-0000-0000-0000-000000000123",
        status=CrawlRunStatus.PENDING,
        connector="pncp",
        created_at=datetime.now(UTC),
    )

    async def create_run(_self, _request):
        return fake

    launched: list[object] = []
    monkeypatch.setattr(crawl_routes.IngestionPipeline, "create_run", create_run)
    monkeypatch.setattr(crawl_routes, "launch_crawl", lambda *args: launched.append(args))

    response = await api_client.post(
        "/api/crawls/run",
        json={"connector": "pncp", "uf": "MA", "days": 7, "modalities": ["pregao_eletronico"]},
    )

    assert response.status_code == 202
    assert response.json()["id"] == fake.id
    assert len(launched) == 1


@pytest.mark.asyncio
async def test_crawl_page_localizes_partial_result_and_retries_only_failed_source(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = datetime(2026, 9, 17, 6, 32, 15, tzinfo=UTC)
    async with database.sessions() as session, session.begin():
        run = CrawlRun(
            connector="all",
            status=CrawlRunStatus.PARTIAL,
            started_at=started,
            finished_at=started + timedelta(minutes=3, seconds=1),
            records_found=9,
            errors=[{"message": "pncp: temporary network failure: ReadTimeout"}],
            filters={
                "connector": "all",
                "uf": "MA",
                "days": 7,
                "modalities": ["pregao_eletronico"],
                "process_documents": True,
            },
        )
        session.add(run)
        await session.flush()
        run_id = run.id

    page = await api_client.get("/crawls")
    assert page.status_code == 200
    assert "Parcial" in page.text
    assert "17/09/2026 03:32:15" in page.text
    assert "PNCP não respondeu dentro do tempo limite" in page.text
    assert "Compras.gov.br" in page.text
    assert "9 registro(s)" in page.text
    assert "Tentar PNCP novamente" in page.text
    assert "Detalhes técnicos" in page.text

    fake_retry = SimpleNamespace(id="00000000-0000-0000-0000-000000000321")
    captured: list[object] = []

    async def create_retry(_self, request):
        captured.append(request)
        return fake_retry

    monkeypatch.setattr(web_routes.IngestionPipeline, "create_run", create_retry)
    monkeypatch.setattr(web_routes, "launch_crawl", lambda *args: captured.append(args))

    retry = await api_client.post(
        f"/crawls/{run_id}/retry",
        data={"connector": "pncp"},
        follow_redirects=False,
    )
    assert retry.status_code == 303
    assert retry.headers["location"] == "/crawls"
    assert captured[0].connector == "pncp"
    assert captured[0].uf == "MA"
    assert captured[0].days == 7
    assert len(captured) == 2

    rejected = await api_client.post(
        f"/crawls/{run_id}/retry",
        data={"connector": "compras_gov"},
        follow_redirects=False,
    )
    assert rejected.status_code == 409


@pytest.mark.asyncio
async def test_crawl_page_retry_preserves_price_registry_mode(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retrying a failed ARP crawl keeps collecting atas, not procurements."""

    started = datetime(2026, 9, 17, 6, 32, 15, tzinfo=UTC)
    async with database.sessions() as session, session.begin():
        run = CrawlRun(
            connector="compras_gov",
            status=CrawlRunStatus.PARTIAL,
            started_at=started,
            finished_at=started + timedelta(minutes=1),
            errors=[{"message": "compras_gov: temporary network failure: ReadTimeout"}],
            filters={
                "connector": "compras_gov",
                "mode": "price_registries",
                "uf": "MA",
                "days": 365,
                "modalities": ["pregao_eletronico"],
                "process_documents": False,
            },
        )
        session.add(run)
        await session.flush()
        run_id = run.id

    fake_retry = SimpleNamespace(id="00000000-0000-0000-0000-000000000322")
    captured: list[object] = []

    async def create_retry(_self, request):
        captured.append(request)
        return fake_retry

    monkeypatch.setattr(web_routes.IngestionPipeline, "create_run", create_retry)
    monkeypatch.setattr(web_routes, "launch_crawl", lambda *args: captured.append(args))

    retry = await api_client.post(
        f"/crawls/{run_id}/retry",
        data={"connector": "compras_gov"},
        follow_redirects=False,
    )

    assert retry.status_code == 303
    assert captured[0].mode == "price_registries"
    assert captured[0].days == 365
    assert len(captured) == 2
