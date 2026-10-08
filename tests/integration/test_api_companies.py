"""Company history API and pages built on auditable participation facts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from app.models import (
    Company,
    CompanyContact,
    Lead,
    Participant,
    ParticipantStatus,
    Procurement,
    ProcurementEvent,
)
from app.models.enums import LeadStatus, ProcurementEventType, ReasonCategory

from .conftest import DatabaseContext

CNPJ = "00000000000191"


async def _seed_company_history(database: DatabaseContext) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        company = Company(
            cnpj=CNPJ,
            legal_name="Empresa Histórica Ltda.",
            normalized_name="EMPRESA HISTORICA",
            uf="MA",
            municipality="São Luís",
            fingerprint="h" * 64,
        )
        open_procurement = Procurement(
            source="pncp",
            external_id="open-1",
            title="Pregão em aberto",
            agency_name="Prefeitura de Exemplo",
            uf="MA",
            modality="Pregão - Eletrônico",
            procurement_type="licitacao",
            estimated_value=Decimal("100000.00"),
            publication_at=now,
            proposal_end_at=now + timedelta(days=3),
            status="Divulgada no PNCP",
            fingerprint="i" * 64,
        )
        closed_procurement = Procurement(
            source="pncp",
            external_id="closed-1",
            title="Concorrência encerrada",
            agency_name="Secretaria Estadual",
            uf="PI",
            modality="Concorrência - Eletrônica",
            procurement_type="licitacao",
            estimated_value=Decimal("900000.00"),
            publication_at=now - timedelta(days=30),
            proposal_end_at=now - timedelta(days=10),
            status="Homologada",
            fingerprint="j" * 64,
        )
        session.add_all([company, open_procurement, closed_procurement])
        await session.flush()
        session.add_all(
            [
                Participant(
                    procurement_id=open_procurement.id,
                    company_id=company.id,
                    participation_role="participant",
                    status_code=ParticipantStatus.DISQUALIFIED,
                    status="desclassificada",
                    source="document",
                    confidence=Decimal("0.90"),
                    fingerprint="k" * 64,
                ),
                Participant(
                    procurement_id=closed_procurement.id,
                    company_id=company.id,
                    participation_role="awarded",
                    status_code=ParticipantStatus.AWARDED,
                    status="homologado",
                    final_value=Decimal("850000.00"),
                    source="pncp",
                    confidence=Decimal("1"),
                    fingerprint="l" * 64,
                ),
                ProcurementEvent(
                    procurement_id=open_procurement.id,
                    company_id=company.id,
                    event_type=ProcurementEventType.DISQUALIFIED,
                    raw_description="Empresa desclassificada por não responder à diligência.",
                    reason_category=ReasonCategory.FAILURE_TO_RESPOND,
                    source="document",
                    confidence=Decimal("0.90"),
                    requires_manual_review=True,
                    fingerprint="m" * 64,
                ),
                ProcurementEvent(
                    procurement_id=closed_procurement.id,
                    company_id=company.id,
                    event_type=ProcurementEventType.APPEAL_SUBMITTED,
                    raw_description="Recurso apresentado pela empresa.",
                    reason_category=ReasonCategory.OTHER,
                    source="document",
                    confidence=Decimal("0.80"),
                    requires_manual_review=False,
                    fingerprint="n" * 64,
                ),
            ]
        )


@pytest.mark.asyncio
async def test_company_endpoints_report_auditable_counters(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    """Company list/detail/participations/events filter on normalized facts."""

    await _seed_company_history(database)

    listing = await api_client.get("/api/companies", params={"search": "Histórica"})
    detail = await api_client.get(f"/api/companies/{CNPJ}")
    disqualified = await api_client.get(
        f"/api/companies/{CNPJ}/participations", params={"status": "disqualified"}
    )
    active = await api_client.get(
        f"/api/companies/{CNPJ}/participations", params={"active_only": "true"}
    )
    events = await api_client.get(
        f"/api/companies/{CNPJ}/events", params={"event_type": "DISQUALIFIED"}
    )
    appeals = await api_client.get(
        f"/api/companies/{CNPJ}/events", params={"event_type": "APPEAL_SUBMITTED"}
    )
    missing = await api_client.get("/api/companies/11111111111111")

    assert listing.status_code == 200
    assert listing.json()["total"] == 1
    stats = listing.json()["items"][0]["stats"]
    assert stats["participations"] == 2
    assert stats["awarded"] == 1
    assert stats["disqualified"] == 1

    assert detail.status_code == 200
    assert detail.json()["company"]["cnpj"] == CNPJ
    assert detail.json()["stats"]["ineligible"] == 0

    assert disqualified.status_code == 200
    assert disqualified.json()["total"] == 1
    assert disqualified.json()["items"][0]["participant"]["status_code"] == "disqualified"

    assert active.status_code == 200
    assert [item["procurement"]["external_id"] for item in active.json()["items"]] == ["open-1"]

    assert events.status_code == 200
    assert events.json()["total"] == 1
    event = events.json()["items"][0]["event"]
    assert event["reason_category"] == "FAILURE_TO_RESPOND"
    assert event["requires_manual_review"] is True
    assert appeals.json()["total"] == 1

    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_company_pages_render_with_provenance(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    """The web pages show counters, participations and events without inventing data."""

    await _seed_company_history(database)

    listing = await api_client.get("/empresas", params={"search": "Histórica"})
    detail = await api_client.get(f"/empresas/{CNPJ}")
    missing = await api_client.get("/empresas/11111111111111")

    assert listing.status_code == 200
    assert "Empresa Histórica Ltda." in listing.text
    assert detail.status_code == 200
    assert "Desclassificada" in detail.text
    assert "Recurso apresentado" in detail.text
    assert "Ausência de resposta" in detail.text
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_company_list_pagination_is_stable(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    """Companies tied on name/counters keep a deterministic order across pages."""

    cnpjs = ["00000000000191", "11222333000181", "12345678000195"]
    async with database.sessions() as session, session.begin():
        for index, cnpj in enumerate(cnpjs):
            session.add(
                Company(
                    cnpj=cnpj,
                    legal_name="Empresa Igual Ltda.",
                    normalized_name=f"EMPRESA IGUAL {index}",
                    fingerprint=str(index) * 64,
                )
            )

    pages: list[str] = []
    for page in (1, 2, 3):
        response = await api_client.get("/api/companies", params={"page": page, "page_size": 1})
        assert response.status_code == 200
        pages.append(response.json()["items"][0]["company"]["cnpj"])

    assert sorted(pages) == sorted(cnpjs)


@pytest.mark.asyncio
async def test_company_active_only_includes_null_status_open_procurement(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    """A procurement without status text still counts as open by its proposal window."""

    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        company = Company(
            cnpj=CNPJ,
            legal_name="Empresa Sem Situação Ltda.",
            normalized_name="EMPRESA SEM SITUACAO",
            fingerprint="u" * 64,
        )
        procurement = Procurement(
            source="pncp",
            external_id="null-status-open",
            title="Pregão sem situação publicada",
            modality="Pregão - Eletrônico",
            publication_at=now,
            proposal_end_at=now + timedelta(days=2),
            status=None,
            fingerprint="v" * 64,
        )
        session.add_all([company, procurement])
        await session.flush()
        session.add(
            Participant(
                procurement_id=procurement.id,
                company_id=company.id,
                participation_role="participant",
                status_code=ParticipantStatus.PARTICIPANT,
                source="document",
                confidence=Decimal("0.90"),
                fingerprint="w" * 64,
            )
        )

    response = await api_client.get(
        f"/api/companies/{CNPJ}/participations", params={"active_only": "true"}
    )

    assert response.status_code == 200
    assert response.json()["total"] == 1
    assert response.json()["items"][0]["procurement"]["external_id"] == "null-status-open"


@pytest.mark.asyncio
async def test_company_empty_stats_and_invalid_identifiers(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    """Companies without participation facts report zeros, and bad CNPJs are rejected."""

    async with database.sessions() as session, session.begin():
        session.add(
            Company(
                cnpj=CNPJ,
                legal_name="Empresa Sem Histórico Ltda.",
                normalized_name="EMPRESA SEM HISTORICO",
                uf="SP",
                municipality="São Paulo",
                fingerprint="o" * 64,
            )
        )

    listing = await api_client.get("/api/companies", params={"uf": "SP"})
    detail = await api_client.get(f"/api/companies/{CNPJ}")
    invalid_api = await api_client.get("/api/companies/nao-e-cnpj")
    invalid_page = await api_client.get("/empresas/nao-e-cnpj")
    page = await api_client.get("/empresas", params={"uf": "SP"})

    assert listing.status_code == 200
    stats = listing.json()["items"][0]["stats"]
    assert stats == {
        "participations": 0,
        "awarded": 0,
        "disqualified": 0,
        "ineligible": 0,
        "last_participation_at": None,
    }
    assert detail.status_code == 200
    assert detail.json()["stats"]["participations"] == 0
    assert invalid_api.status_code == 404
    assert invalid_page.status_code == 404
    assert page.status_code == 200 and "Empresa Sem Histórico Ltda." in page.text


@pytest.mark.asyncio
async def test_procurement_page_accepts_advanced_filters(
    api_client: httpx.AsyncClient,
) -> None:
    """The web list accepts the new modality/route/SRP/value filters."""

    response = await api_client.get(
        "/procurements",
        params=[
            ("modality", "dispensa"),
            ("procurement_type", "contratacao_direta"),
            ("status_category", "aberta"),
            ("is_srp", "true"),
            ("value_min", "1000"),
            ("value_max", "900000"),
            ("procurement_type", "invalido"),
        ],
    )

    assert response.status_code == 200
    assert "Contratações" in response.text


@pytest.mark.asyncio
async def test_company_period_lead_and_active_filters(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    """Period, has_lead and active_only filters plus corporate contacts."""

    async with database.sessions() as session, session.begin():
        open_procurement = Procurement(
            source="pncp",
            external_id="open-2026",
            title="Pregão 2026",
            agency_name="Prefeitura de Exemplo",
            uf="MA",
            modality="Pregão - Eletrônico",
            publication_at=datetime(2026, 6, 1, tzinfo=UTC),
            proposal_end_at=datetime(2099, 1, 1, tzinfo=UTC),
            status="Divulgada no PNCP",
            fingerprint="a" * 64,
        )
        closed_procurement = Procurement(
            source="pncp",
            external_id="closed-2025",
            title="Pregão 2025",
            agency_name="Prefeitura de Exemplo",
            uf="MA",
            modality="Pregão - Eletrônico",
            publication_at=datetime(2025, 6, 1, tzinfo=UTC),
            proposal_end_at=datetime(2025, 7, 1, tzinfo=UTC),
            status="Homologada",
            fingerprint="b" * 64,
        )
        other_procurement = Procurement(
            source="pncp",
            external_id="other-2025",
            title="Concorrência 2025",
            agency_name="Secretaria Estadual",
            uf="PI",
            modality="Concorrência - Eletrônica",
            publication_at=datetime(2025, 8, 1, tzinfo=UTC),
            proposal_end_at=datetime(2025, 9, 1, tzinfo=UTC),
            status="Homologada",
            fingerprint="c" * 64,
        )
        company_a = Company(
            cnpj=CNPJ,
            legal_name="Empresa Alvo Ltda.",
            normalized_name="EMPRESA ALVO",
            uf="MA",
            fingerprint="d" * 64,
        )
        company_b = Company(
            cnpj="11222333000181",
            legal_name="Empresa com Lead Ltda.",
            normalized_name="EMPRESA COM LEAD",
            uf="PI",
            fingerprint="e" * 64,
        )
        session.add_all(
            [
                open_procurement,
                closed_procurement,
                other_procurement,
                company_a,
                company_b,
            ]
        )
        await session.flush()
        session.add_all(
            [
                Participant(
                    procurement_id=open_procurement.id,
                    company_id=company_a.id,
                    participation_role="awarded",
                    status_code=ParticipantStatus.AWARDED,
                    source="pncp",
                    confidence=Decimal("1"),
                    fingerprint="f" * 64,
                ),
                Participant(
                    procurement_id=closed_procurement.id,
                    company_id=company_a.id,
                    participation_role="awarded",
                    status_code=ParticipantStatus.AWARDED,
                    source="pncp",
                    confidence=Decimal("1"),
                    fingerprint="g" * 64,
                ),
                Participant(
                    procurement_id=other_procurement.id,
                    company_id=company_b.id,
                    participation_role="awarded",
                    status_code=ParticipantStatus.AWARDED,
                    source="pncp",
                    confidence=Decimal("1"),
                    fingerprint="h" * 64,
                ),
            ]
        )
        event = ProcurementEvent(
            procurement_id=other_procurement.id,
            company_id=company_b.id,
            event_type=ProcurementEventType.DISQUALIFIED,
            raw_description="Empresa desclassificada.",
            reason_category=ReasonCategory.OTHER,
            source="document",
            confidence=Decimal("0.90"),
            fingerprint="i" * 64,
        )
        session.add(event)
        await session.flush()
        lead = Lead(
            procurement_id=other_procurement.id,
            company_id=company_b.id,
            triggering_event_id=event.id,
            lead_status=LeadStatus.NEW,
            score=60,
            urgency_score=12,
            evidence_score=20,
            legal_relevance_score=20,
            contact_score=0,
            economic_value_score=8,
            fit_score=60,
            reason_summary="Evento comprovado.",
            recommended_action="Revisar evidência.",
            score_breakdown={"evidence": "20/25"},
            fingerprint="j" * 64,
        )
        session.add(lead)
        await session.flush()
        session.add_all(
            [
                CompanyContact(
                    company_id=company_a.id,
                    contact_type="email",
                    contact_value="contato@empresa-alvo.example",
                    source_url="https://empresa-alvo.example/contato",
                    is_corporate=True,
                    status="verified",
                    confidence=Decimal("0.90"),
                    fingerprint="k" * 64,
                ),
                CompanyContact(
                    company_id=company_b.id,
                    contact_type="phone",
                    contact_value="+55 98 90000-0000",
                    is_corporate=True,
                    status="discovered",
                    confidence=Decimal("0.70"),
                    fingerprint="l" * 64,
                ),
            ]
        )
        lead_id = lead.id

    by_period = await api_client.get(
        "/api/companies",
        params={
            "published_from": "2026-01-01T00:00:00Z",
            "published_to": "2026-12-31T23:59:59Z",
        },
    )
    active = await api_client.get("/api/companies", params={"active_only": "true"})
    with_lead = await api_client.get("/api/companies", params={"has_lead": "true"})
    period_page = await api_client.get(
        "/empresas",
        params={
            "published_from": "2026-01-01",
            "published_to": "2026-12-31",
            "active_only": "true",
        },
    )
    lead_page = await api_client.get("/empresas", params={"has_lead": "true"})
    company_page = await api_client.get(f"/empresas/{CNPJ}")
    lead_detail = await api_client.get(f"/leads/{lead_id}")

    assert by_period.status_code == 200
    assert [item["company"]["legal_name"] for item in by_period.json()["items"]] == [
        "Empresa Alvo Ltda."
    ]
    assert by_period.json()["items"][0]["stats"]["participations"] == 1
    assert active.status_code == 200
    assert [item["company"]["legal_name"] for item in active.json()["items"]] == [
        "Empresa Alvo Ltda."
    ]
    assert active.json()["items"][0]["stats"]["participations"] == 1
    assert with_lead.status_code == 200
    assert [item["company"]["legal_name"] for item in with_lead.json()["items"]] == [
        "Empresa com Lead Ltda."
    ]
    assert period_page.status_code == 200
    assert "Empresa Alvo Ltda." in period_page.text
    assert "Empresa com Lead Ltda." not in period_page.text
    assert "info-tip-button" in period_page.text
    assert "Em processo ativo" in period_page.text
    assert lead_page.status_code == 200 and "Empresa com Lead Ltda." in lead_page.text
    assert company_page.status_code == 200
    assert "contato@empresa-alvo.example" in company_page.text
    assert "Verificado" in company_page.text
    assert lead_detail.status_code == 200
    assert "+55 98 90000-0000" in lead_detail.text
    assert "/empresas/11222333000181" in lead_detail.text
