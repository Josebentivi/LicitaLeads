"""End-to-end tests for the local REST surface."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select

from app.api.routes import health as health_route
from app.models import Company, CompanyContact, CrawlRun, CrawlRunStatus, Procurement

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
