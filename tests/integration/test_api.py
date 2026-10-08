"""End-to-end tests for the local REST surface."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select

from app.api.routes import health as health_route
from app.models import (
    Company,
    CompanyContact,
    CrawlRun,
    CrawlRunStatus,
    Participant,
    ParticipantStatus,
    PriceRegistry,
    PriceRegistryItem,
    Procurement,
)

from .conftest import DatabaseContext


@pytest.mark.asyncio
async def test_health_and_empty_paginated_lists(
    api_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Health and primary collection endpoints work without contacting upstream sources."""

    async def database_is_healthy(*_args: object, **_kwargs: object) -> bool:
        return True

    monkeypatch.setattr(health_route, "check_database", database_is_healthy)

    health = await api_client.get("/health")
    procurements = await api_client.get("/api/procurements")
    leads = await api_client.get("/api/leads")
    crawls = await api_client.get("/api/crawls")

    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    for response in (procurements, leads, crawls):
        assert response.status_code == 200
        assert response.json()["items"] == []
        assert response.json()["total"] == 0


@pytest.mark.asyncio
async def test_procurement_list_filters_and_paginates(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    """The public list uses snake-case filters and stable pagination metadata."""

    async with database.sessions() as session, session.begin():
        session.add_all(
            [
                Procurement(
                    source="pncp",
                    external_id="ma-1",
                    pncp_control_number="00000000000191-1-000001/2026",
                    title="Pregão de equipamentos",
                    agency_name="Prefeitura de Exemplo",
                    uf="MA",
                    municipality="São Luís",
                    modality="pregao_eletronico",
                    fingerprint="a" * 64,
                ),
                Procurement(
                    source="pncp",
                    external_id="pi-1",
                    title="Concorrência de obras",
                    agency_name="Secretaria Estadual",
                    uf="PI",
                    municipality="Teresina",
                    modality="concorrencia_eletronica",
                    fingerprint="b" * 64,
                ),
            ]
        )

    response = await api_client.get(
        "/api/procurements",
        params={"uf": "ma", "municipality": "são", "page": 1, "page_size": 1},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["page"] == 1
    assert payload["page_size"] == 1
    assert payload["items"][0]["external_id"] == "ma-1"


@pytest.mark.asyncio
async def test_procurement_advanced_filters(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    """Contracting route, SRP, value range, multi-modality and status category work."""

    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        session.add_all(
            [
                Procurement(
                    source="pncp",
                    external_id="open-srp",
                    title="Dispensa em aberto",
                    agency_name="Prefeitura de Exemplo",
                    uf="MA",
                    modality="Dispensa de Licitação",
                    procurement_type="contratacao_direta",
                    is_srp=True,
                    legal_basis="Lei 14.133/2021, art. 75, inciso I",
                    estimated_value=Decimal("50000.00"),
                    publication_at=now,
                    proposal_end_at=now + timedelta(days=5),
                    status="Divulgada no PNCP",
                    fingerprint="d" * 64,
                ),
                Procurement(
                    source="pncp",
                    external_id="closed-auction",
                    title="Pregão encerrado",
                    agency_name="Secretaria Estadual",
                    uf="PI",
                    modality="Pregão - Eletrônico",
                    procurement_type="licitacao",
                    is_srp=False,
                    estimated_value=Decimal("500000.00"),
                    publication_at=now,
                    proposal_end_at=now - timedelta(days=2),
                    status="Homologada",
                    fingerprint="e" * 64,
                ),
                Procurement(
                    source="pncp",
                    external_id="no-status-open",
                    title="Concorrência sem situação publicada",
                    agency_name="Prefeitura de Exemplo",
                    uf="MA",
                    modality="Concorrência - Eletrônica",
                    procurement_type="licitacao",
                    estimated_value=Decimal("75000.00"),
                    publication_at=now,
                    proposal_end_at=now + timedelta(days=10),
                    status=None,
                    fingerprint="f" * 64,
                ),
                Procurement(
                    source="pncp",
                    external_id="no-status-unknown",
                    title="Concorrência sem prazo",
                    agency_name="Prefeitura de Exemplo",
                    uf="MA",
                    modality="Concorrência - Eletrônica",
                    procurement_type="licitacao",
                    estimated_value=Decimal("25000.00"),
                    publication_at=now,
                    proposal_end_at=None,
                    status=None,
                    fingerprint="g" * 64,
                ),
            ]
        )

    direct = await api_client.get(
        "/api/procurements", params={"procurement_type": "contratacao_direta"}
    )
    srp = await api_client.get("/api/procurements", params={"is_srp": "true"})
    expensive = await api_client.get("/api/procurements", params={"value_min": "100000"})
    open_category = await api_client.get("/api/procurements", params={"status_category": "aberta"})
    closed_category = await api_client.get(
        "/api/procurements", params={"status_category": "encerrada"}
    )
    unknown_category = await api_client.get(
        "/api/procurements", params={"status_category": "desconhecida"}
    )
    multi = await api_client.get(
        "/api/procurements",
        params=[("modality", "dispensa"), ("modality", "pregao_eletronico")],
    )

    assert direct.status_code == 200
    assert [item["external_id"] for item in direct.json()["items"]] == ["open-srp"]
    assert srp.status_code == 200
    assert [item["external_id"] for item in srp.json()["items"]] == ["open-srp"]
    assert expensive.status_code == 200
    assert [item["external_id"] for item in expensive.json()["items"]] == ["closed-auction"]
    assert open_category.status_code == 200
    assert {item["external_id"] for item in open_category.json()["items"]} == {
        "open-srp",
        "no-status-open",
    }
    assert closed_category.status_code == 200
    assert [item["external_id"] for item in closed_category.json()["items"]] == ["closed-auction"]
    assert unknown_category.status_code == 200
    assert [item["external_id"] for item in unknown_category.json()["items"]] == [
        "no-status-unknown"
    ]
    assert multi.status_code == 200
    assert multi.json()["total"] == 2


@pytest.mark.asyncio
async def test_procurement_cnpj_filters(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    """Agency CNPJ and company CNPJ filters cover participations and ARP suppliers."""

    now = datetime.now(UTC)
    agency_cnpj = "00000000000191"
    company_cnpj = "11222333000181"
    async with database.sessions() as session, session.begin():
        agency_procurement = Procurement(
            source="pncp",
            external_id="agency-proc",
            agency_name="Órgão Alvo",
            agency_cnpj=agency_cnpj,
            modality="Pregão - Eletrônico",
            publication_at=now,
            fingerprint="h" * 64,
        )
        participant_procurement = Procurement(
            source="pncp",
            external_id="participant-proc",
            agency_name="Outro Órgão",
            modality="Pregão - Eletrônico",
            publication_at=now,
            fingerprint="i" * 64,
        )
        arp_procurement = Procurement(
            source="pncp",
            external_id="arp-proc",
            agency_name="Outro Órgão",
            pncp_control_number=f"{company_cnpj}-1-000009/2026",
            modality="Pregão - Eletrônico",
            publication_at=now,
            fingerprint="j" * 64,
        )
        other_procurement = Procurement(
            source="pncp",
            external_id="other-proc",
            agency_name="Outro Órgão",
            modality="Pregão - Eletrônico",
            publication_at=now,
            fingerprint="k" * 64,
        )
        company = Company(
            cnpj=company_cnpj,
            legal_name="Empresa Alvo Ltda.",
            normalized_name="EMPRESA ALVO",
            fingerprint="l" * 64,
        )
        session.add_all(
            [
                agency_procurement,
                participant_procurement,
                arp_procurement,
                other_procurement,
                company,
            ]
        )
        await session.flush()
        session.add(
            Participant(
                procurement_id=participant_procurement.id,
                company_id=company.id,
                participation_role="awarded",
                status_code=ParticipantStatus.AWARDED,
                source="pncp",
                confidence=Decimal("1"),
                fingerprint="m" * 64,
            )
        )
        registry = PriceRegistry(
            source="compras_gov",
            external_id="ata-1",
            linked_pncp_control_number=f"{company_cnpj}-1-000009/2026",
            fingerprint="n" * 64,
        )
        session.add(registry)
        await session.flush()
        session.add(
            PriceRegistryItem(
                price_registry_id=registry.id,
                item_number="1",
                supplier_cnpj=company_cnpj,
                fingerprint="o" * 64,
            )
        )

    by_agency = await api_client.get("/api/procurements", params={"agency_cnpj": agency_cnpj})
    by_agency_formatted = await api_client.get(
        "/api/procurements", params={"agency_cnpj": "00.000.000/0001-91"}
    )
    by_company = await api_client.get("/api/procurements", params={"company_cnpj": company_cnpj})
    by_company_formatted = await api_client.get(
        "/api/procurements", params={"company_cnpj": "11.222.333/0001-81"}
    )
    invalid_agency = await api_client.get("/api/procurements", params={"agency_cnpj": "123"})
    invalid_company = await api_client.get(
        "/api/procurements", params={"company_cnpj": "00000000000000"}
    )
    page = await api_client.get(
        "/procurements",
        params={"agency_cnpj": agency_cnpj, "company_cnpj": company_cnpj},
    )
    invalid_page = await api_client.get("/procurements", params={"company_cnpj": "123"})

    assert by_agency.status_code == 200
    assert [item["external_id"] for item in by_agency.json()["items"]] == ["agency-proc"]
    assert by_agency_formatted.status_code == 200
    assert by_agency_formatted.json()["total"] == 1
    assert by_company.status_code == 200
    assert {item["external_id"] for item in by_company.json()["items"]} == {
        "participant-proc",
        "arp-proc",
    }
    assert by_company_formatted.status_code == 200
    assert by_company_formatted.json()["total"] == 2
    assert invalid_agency.status_code == 422
    assert "CNPJ do órgão" in invalid_agency.json()["detail"]
    assert invalid_company.status_code == 422
    assert "CNPJ da empresa" in invalid_company.json()["detail"]
    assert page.status_code == 200
    assert "ver empresa" in page.text
    assert invalid_page.status_code == 200
    assert "CNPJ inválido informado" in invalid_page.text


@pytest.mark.asyncio
async def test_contact_csv_import_is_validated_and_idempotent(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    """Corporate CSV rows are upserted while unsafe or unknown rows stay auditable."""

    async with database.sessions() as session, session.begin():
        session.add(
            Company(
                cnpj="00000000000191",
                legal_name="Empresa Exemplo Ltda.",
                normalized_name="EMPRESA EXEMPLO",
                fingerprint="c" * 64,
            )
        )

    header = "cnpj,contact_type,contact_value,source_url,is_corporate\n"
    valid = header + (
        "00.000.000/0001-91,email,licitacoes@empresa.example,https://empresa.example/contato,true\n"
    )
    first = await api_client.post(
        "/api/contacts/import",
        files={"file": ("contacts.csv", valid.encode(), "text/csv")},
    )
    second = await api_client.post(
        "/api/contacts/import",
        files={"file": ("contacts.csv", valid.encode(), "text/csv")},
    )
    rejected = await api_client.post(
        "/api/contacts/import",
        files={
            "file": (
                "contacts.csv",
                (
                    header
                    + "00000000000191,email,pessoal@example.com,https://example.com,false\n"
                    + "11111111111111,email,invalido@example.com,https://example.com,true\n"
                ).encode(),
                "text/csv",
            )
        },
    )

    assert first.status_code == 200
    assert first.json() == {"created": 1, "updated": 0, "skipped": 0, "errors": []}
    assert second.status_code == 200
    assert second.json()["updated"] == 1
    assert rejected.status_code == 200
    assert rejected.json()["skipped"] == 2
    assert [item["row"] for item in rejected.json()["errors"]] == [2, 3]

    async with database.sessions() as session:
        count = await session.scalar(select(func.count()).select_from(CompanyContact))
        assert count == 1


@pytest.mark.asyncio
async def test_cancel_endpoint_requests_cancellation_and_rejects_terminal_runs(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    """Cancelling an active run persists the flag; terminal runs return 409."""

    async with database.sessions() as session, session.begin():
        active = CrawlRun(
            connector="pncp",
            status=CrawlRunStatus.RUNNING,
            started_at=datetime.now(UTC),
            filters={},
        )
        finished = CrawlRun(
            connector="pncp",
            status=CrawlRunStatus.COMPLETED,
            filters={},
        )
        session.add_all([active, finished])
        await session.flush()
        active_id, finished_id = active.id, finished.id

    accepted = await api_client.post(f"/api/crawls/{active_id}/cancel")
    conflict = await api_client.post(f"/api/crawls/{finished_id}/cancel")
    missing = await api_client.post(f"/api/crawls/{uuid4()}/cancel")
    web = await api_client.post(f"/crawls/{active_id}/cancel", follow_redirects=False)

    assert accepted.status_code == 202
    assert accepted.json()["status"] == "cancellation_requested"
    assert conflict.status_code == 409
    assert missing.status_code == 404
    assert web.status_code == 303
    async with database.sessions() as session:
        stored = await session.get(CrawlRun, active_id)
    assert stored is not None and stored.cancel_requested is True


@pytest.mark.asyncio
async def test_web_filter_forms_accept_blank_fields(
    api_client: httpx.AsyncClient,
) -> None:
    """Browsers submit empty inputs; the pages must treat them as omitted."""

    responses = {
        "empresas": await api_client.get(
            "/empresas",
            params={"uf": "", "search": "", "published_from": "", "published_to": ""},
        ),
        "procurements": await api_client.get(
            "/procurements",
            params={"uf": "", "municipality": "", "value_min": "", "value_max": ""},
        ),
        "leads": await api_client.get(
            "/leads",
            params={"min_score": "", "event_type": "", "created_from": "", "created_to": ""},
        ),
        "atas": await api_client.get("/atas", params={"search": "", "valid_on": ""}),
    }

    for name, response in responses.items():
        assert response.status_code == 200, name

    empresas = responses["empresas"].text
    assert '</a><span class="info-tip">' in empresas
    assert '<a href="/leads">Leads<button' not in empresas


@pytest.mark.asyncio
async def test_open_category_matches_active_trigger_semantics(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    """The SQL "aberta" category follows the same rule as the ingestion trigger."""

    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        closed_future = Procurement(
            source="pncp",
            external_id="closed-future",
            title="Encerrada com prazo futuro",
            modality="Pregão - Eletrônico",
            status="Encerrada",
            publication_at=now,
            proposal_end_at=now + timedelta(days=10),
            fingerprint="x" * 64,
        )
        open_no_deadline = Procurement(
            source="pncp",
            external_id="open-no-deadline",
            title="Divulgada sem prazo",
            modality="Pregão - Eletrônico",
            status="Divulgada no PNCP",
            publication_at=now,
            proposal_end_at=None,
            fingerprint="y" * 64,
        )
        company = Company(
            cnpj="12345678000195",
            legal_name="Empresa Sem Prazo Ltda.",
            normalized_name="EMPRESA SEM PRAZO",
            fingerprint="z" * 64,
        )
        session.add_all([closed_future, open_no_deadline, company])
        await session.flush()
        session.add(
            Participant(
                procurement_id=open_no_deadline.id,
                company_id=company.id,
                participation_role="awarded",
                status_code=ParticipantStatus.AWARDED,
                source="pncp",
                confidence=Decimal("1"),
                fingerprint="0" * 64,
            )
        )

    open_category = await api_client.get("/api/procurements", params={"status_category": "aberta"})
    active_companies = await api_client.get("/api/companies", params={"active_only": "true"})

    assert open_category.status_code == 200
    assert [item["external_id"] for item in open_category.json()["items"]] == ["open-no-deadline"]
    assert active_companies.status_code == 200
    assert [item["company"]["legal_name"] for item in active_companies.json()["items"]] == [
        "Empresa Sem Prazo Ltda."
    ]


@pytest.mark.asyncio
async def test_help_page_documents_the_lawyer_workflow(
    api_client: httpx.AsyncClient,
) -> None:
    """The manual explains the workflow in plain Portuguese and is in the menu."""

    response = await api_client.get("/ajuda")

    assert response.status_code == 200
    for text in (
        "Manual de uso",
        "O que é a plataforma",
        "Palavras que você vai ver",
        "Passo a passo por tela",
        "Como ler o score de um lead",
        "O que a plataforma não faz",
        "Perguntas frequentes",
        "Para quem opera as coletas",
        "não envia mensagens",
        "Participação comprovada",
    ):
        assert text in response.text
    assert '<a href="/ajuda">Ajuda</a>' in response.text
