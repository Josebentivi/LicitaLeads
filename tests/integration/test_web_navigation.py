"""Sidebar, group hubs and real operational indicators."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from sqlalchemy import select

from app.models import (
    Company,
    Contract,
    CrawlRun,
    CrawlRunStatus,
    Deadline,
    DeadlineCalculationMethod,
    DeadlineStatus,
    Lead,
    LeadReview,
    LeadStatus,
    Payment,
    PriceRegistry,
    Procurement,
    ProcurementEvent,
    ProcurementEventType,
    ProcurementPhase,
)

from .conftest import DatabaseContext


async def _seed_hub_data(database: DatabaseContext) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        company = Company(
            cnpj="00000000000191",
            legal_name="Empresa Hub Ltda.",
            normalized_name="EMPRESA HUB",
            fingerprint="a" * 64,
        )
        session.add(company)
        procurement = Procurement(
            source="pncp",
            external_id="hub-procurement",
            title="Pregão de equipamentos",
            agency_name="Prefeitura de Exemplo",
            municipality="São Luís",
            uf="MA",
            status="Em andamento",
            proposal_end_at=now + timedelta(days=2),
            current_phase=ProcurementPhase.PROPOSALS,
            fingerprint="b" * 64,
        )
        session.add(procurement)
        session.add(
            PriceRegistry(
                source="pncp",
                external_id="hub-arp",
                registry_number="1/2026",
                agency_name="Prefeitura de Exemplo",
                valid_from=now - timedelta(days=1),
                valid_until=now + timedelta(days=30),
                fingerprint="c" * 64,
            )
        )
        session.add(
            Contract(
                source="pncp",
                external_id="hub-contract",
                contract_number="1/2026",
                year=2026,
                agency_name="Prefeitura de Exemplo",
                global_value=Decimal("1000.00"),
                valid_until=now + timedelta(days=30),
                fingerprint="d" * 64,
            )
        )
        session.add(
            Payment(
                source="municipal:timon",
                external_id="hub-payment",
                supplier_name="Fornecedor Hub",
                amount=Decimal("100.00"),
                payment_date=now,
                phase="pago",
                match_status="unlinked",
                requires_manual_review=True,
                fingerprint="e" * 64,
            )
        )
        session.add(
            CrawlRun(
                connector="municipal:timon",
                status=CrawlRunStatus.FAILED,
                started_at=now,
                finished_at=now,
                records_found=0,
                filters={"connector": "municipal:timon"},
            )
        )
        await session.flush()
        event = ProcurementEvent(
            procurement_id=procurement.id,
            company_id=company.id,
            event_type=ProcurementEventType.DISQUALIFIED,
            raw_description="Empresa desclassificada.",
            source="document",
            confidence=Decimal("1"),
            requires_manual_review=True,
            fingerprint="f" * 64,
        )
        deadline = Deadline(
            procurement_id=procurement.id,
            company_id=company.id,
            deadline_type="recurso",
            calculation_method=DeadlineCalculationMethod.EXPLICIT,
            explicit_deadline_at=now + timedelta(hours=12),
            status=DeadlineStatus.DUE_WITHIN_24H,
            confidence=Decimal("1"),
            calculation_explanation="Prazo explícito no documento.",
            fingerprint="g" * 64,
        )
        session.add_all([event, deadline])
        await session.flush()
        session.add(
            Lead(
                procurement_id=procurement.id,
                company_id=company.id,
                triggering_event_id=event.id,
                deadline_id=deadline.id,
                lead_status=LeadStatus.PENDING_REVIEW,
                score=80,
                urgency_score=10,
                evidence_score=10,
                legal_relevance_score=5,
                contact_score=0,
                reason_summary="Oportunidade para revisão.",
                fingerprint="h" * 64,
            )
        )


@pytest.mark.asyncio
async def test_group_hubs_show_real_indicators(
    database: DatabaseContext,
    api_client: httpx.AsyncClient,
) -> None:
    await _seed_hub_data(database)

    compras = await api_client.get("/grupos/compras")
    financeiro = await api_client.get("/grupos/financeiro")
    oportunidades = await api_client.get("/grupos/oportunidades")
    operacao = await api_client.get("/grupos/operacao")
    sistema = await api_client.get("/grupos/sistema")

    assert all(
        response.status_code == 200
        for response in (compras, financeiro, oportunidades, operacao, sistema)
    )
    assert compras.text.count('<article class="metric">') == 4
    assert "Contratações abertas" in compras.text
    assert "Contratos vigentes" in compras.text
    assert "Conciliação pendente" in financeiro.text
    assert "Portais municipais indisponíveis" in financeiro.text
    assert "Leads aguardando revisão" in oportunidades.text
    assert "Prazos em 24 horas" in oportunidades.text
    assert "Falhas recentes" in operacao.text
    assert "municipal:timon" in operacao.text
    assert "Dados armazenados" in sistema.text
    assert "Abrir testes" in sistema.text


@pytest.mark.asyncio
async def test_sidebar_marks_active_group_and_supports_mobile(
    api_client: httpx.AsyncClient,
) -> None:
    page = await api_client.get("/grupos/compras")
    listing = await api_client.get("/contratos")

    assert page.status_code == 200
    assert 'id="app-sidebar"' in page.text
    assert "data-sidebar-collapse" in page.text
    assert "data-sidebar-open" in page.text
    assert 'class="sidebar-group active"' in page.text
    assert 'aria-current="page"' in page.text
    assert listing.status_code == 200
    assert 'class="sidebar-item active"' in listing.text
    assert 'aria-label="Trilha de navegação"' in listing.text


@pytest.mark.asyncio
async def test_payments_import_card_lists_exact_portal_links(
    api_client: httpx.AsyncClient,
) -> None:
    page = await api_client.get("/pagamentos")

    assert page.status_code == 200
    assert 'id="importar"' in page.text
    assert "Colunas aceitas" in page.text
    assert 'id="payment-portal-options"' in page.text
    for url in (
        "https://www.saoluis.ma.gov.br/dados-abertos",
        "https://transparencia.imperatriz.ma.gov.br/portal-transparencia",
        "http://scpi3.adtrcloud.com.br:8079/transparencia/"
        "?AcessoIndividual=lnkDespesasPor_NotaEmpenho",
        "https://transparencia.timon.ma.gov.br/",
        "http://177.10.118.5:8079/Transparencia_Despesa/"
        "?AcessoIndividual=lnkDespesasPor_NotaEmpenho",
        "https://www.transparencia.caxias.ma.gov.br/transparencia?filtro=despesas",
        "http://177.136.250.170:5656/SCPIWEB_PMDECAXIAS/",
        "https://www.bacabal.ma.gov.br/despesas",
        "https://transparenciapublica.cloud/bacabal/"
        "Default.aspx?AcessoIndividual=lnkDespesasPor_NotaEmpenho",
    ):
        assert url in page.text
    assert "PREFEITURA MUNICIPAL DE TIMON (18)" in page.text


@pytest.mark.asyncio
async def test_unknown_group_hub_is_not_found(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get("/grupos/inexistente")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_lead_review_can_be_recorded_from_the_web_page(
    database: DatabaseContext,
    api_client: httpx.AsyncClient,
) -> None:
    await _seed_hub_data(database)
    async with database.sessions() as session:
        lead = await session.scalar(select(Lead))
    assert lead is not None

    page = await api_client.get(f"/leads/{lead.id}")
    assert page.status_code == 200
    assert "Revisão humana" in page.text

    response = await api_client.post(
        f"/leads/{lead.id}/review",
        data={"decision": "approved", "reviewer": "Operador Teste", "notes": "ok"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    async with database.sessions() as session:
        stored_lead = await session.get(Lead, lead.id)
        review = await session.scalar(select(LeadReview).where(LeadReview.lead_id == lead.id))
    assert stored_lead is not None and stored_lead.lead_status.value == "approved"
    assert review is not None and review.reviewer == "Operador Teste"
