"""Live-audit adapter and corporate-contact persistence tests."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx
from sqlalchemy import func, select

from app.config import Settings
from app.models import Company, CompanyContact
from app.services.contacts import CompanyContactCandidate
from app.services.contacts import ContactType as CandidateType
from app.services.ingestion.audit import audit_sources
from app.services.ingestion.contacts import ContactEnrichmentService

from .conftest import DatabaseContext


@pytest.mark.asyncio
@respx.mock
async def test_source_audit_merges_pncp_contracts_and_writes_dated_report(
    tmp_path: Path,
) -> None:
    respx.get("https://pncp.gov.br/api/consulta/v3/api-docs").mock(
        return_value=httpx.Response(
            200,
            json={
                "paths": {
                    "/v1/contratacoes/publicacao": {},
                    "/v1/contratacoes/atualizacao": {},
                    "/v1/contratacoes/proposta": {},
                    "/v1/atas": {},
                    "/v1/atas/atualizacao": {},
                    "/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}": {},
                }
            },
        )
    )
    respx.get("https://pncp.gov.br/pncp-api/v3/api-docs").mock(
        return_value=httpx.Response(
            200,
            json={
                "paths": {
                    "/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens": {},
                    "/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/arquivos": {},
                    "/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{numeroItem}/resultados": {},
                }
            },
        )
    )
    compras_paths = {
        "/modulo-contratacoes/1_consultarContratacoes_PNCP_14133": {},
        "/modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id": {},
        "/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id": {},
        "/modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id": {},
        "/modulo-arp/1_consultarARP": {},
        "/modulo-arp/1.1_consultarARP_Id": {},
        "/modulo-arp/2_consultarARPItem": {},
        "/modulo-arp/2.1_consultarARPItem_Id": {},
    }
    respx.get("https://dadosabertos.compras.gov.br/v3/api-docs").mock(
        return_value=httpx.Response(200, json={"paths": compras_paths})
    )
    respx.get("https://dadosabertos.compras.gov.br/v3/api-docs/swagger-config").mock(
        return_value=httpx.Response(404)
    )

    result = await audit_sources(
        settings=Settings(app_env="test"),
        output_directory=tmp_path,
    )

    assert result.checked_sources == 2
    assert result.missing_paths == 0
    assert result.report_path.is_file()
    text = result.report_path.read_text(encoding="utf-8")
    assert "compatível" in text
    assert "não altera automaticamente" in text


@pytest.mark.asyncio
@respx.mock
async def test_source_audit_records_unavailable_openapi_without_crashing(tmp_path: Path) -> None:
    for url in (
        "https://pncp.gov.br/api/consulta/v3/api-docs",
        "https://pncp.gov.br/pncp-api/v3/api-docs",
        "https://dadosabertos.compras.gov.br/v3/api-docs",
        "https://dadosabertos.compras.gov.br/v3/api-docs/swagger-config",
    ):
        respx.get(url).mock(return_value=httpx.Response(503))

    result = await audit_sources(
        settings=Settings(app_env="test"),
        output_directory=tmp_path,
    )

    assert len(result.warnings) == 2
    assert "não foi possível auditar" in result.report_path.read_text(encoding="utf-8")


class StaticContactProvider:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls = 0

    async def find_contacts(self, company):
        self.calls += 1
        if self.fail:
            raise RuntimeError("website unavailable")
        return [
            CompanyContactCandidate(
                CandidateType.EMAIL,
                "licitacoes@empresa.example",
                "https://empresa.example/contato",
                True,
                False,
                0.95,
                company.cnpj,
            ),
            CompanyContactCandidate(
                CandidateType.PHONE,
                "99999999999",
                "https://empresa.example/pessoa",
                False,
                True,
                0.5,
                company.cnpj,
            ),
        ]


@pytest.mark.asyncio
async def test_contact_enrichment_is_gated_and_idempotent(
    database: DatabaseContext,
) -> None:
    async with database.sessions() as session, session.begin():
        session.add(
            Company(
                cnpj="00000000000191",
                legal_name="Empresa Exemplo Ltda.",
                normalized_name="EMPRESA EXEMPLO",
                website="https://empresa.example",
                domain="empresa.example",
                fingerprint="f" * 64,
            )
        )
    provider = StaticContactProvider()
    service = ContactEnrichmentService(
        Settings(app_env="test", contact_search_enabled=False),
        session_factory=database.sessions,
        provider=provider,
    )

    disabled = await service.run()
    first = await service.run(force=True)
    second = await service.run(force=True)

    assert disabled.companies_checked == 0 and provider.calls == 2
    assert first.contacts_created == 1
    assert second.contacts_updated == 1
    async with database.sessions() as session:
        count = await session.scalar(select(func.count()).select_from(CompanyContact))
        contact = await session.scalar(select(CompanyContact))
    assert count == 1
    assert contact is not None and contact.is_corporate is True

    failing = ContactEnrichmentService(
        Settings(app_env="test", contact_search_enabled=True),
        session_factory=database.sessions,
        provider=StaticContactProvider(fail=True),
    )
    failed = await failing.run()
    assert failed.errors == 1
