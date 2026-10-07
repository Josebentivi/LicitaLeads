"""Company history API and pages built on auditable participation facts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from app.models import Company, Participant, ParticipantStatus, Procurement, ProcurementEvent
from app.models.enums import ProcurementEventType, ReasonCategory

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
