"""Live-audit adapter and corporate-contact persistence tests."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
from sqlalchemy import event, func, select

from app.config import Settings
from app.models import Company, CompanyContact, ContactType, SourceRecord
from app.services.contacts import CompanyContactCandidate
from app.services.contacts import ContactType as CandidateType
from app.services.contacts.manual import begin_corporate_write, save_corporate_contact
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
                    "/v1/contratos": {},
                    "/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}": {},
                    "/v1/orgaos/{cnpj}/contratos/{ano}/{sequencial}/termos": {},
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
        "/modulo-arp/3_consultarUnidadesItem": {},
        "/modulo-arp/4_consultarEmpenhosSaldoItem": {},
        "/modulo-arp/5_consultarAdesoesItem": {},
        "/modulo-contratos/1_consultarContratos": {},
        "/modulo-contratos/2.1_consultarContratosItem_Id": {},
    }
    respx.get("https://dadosabertos.compras.gov.br/v3/api-docs").mock(
        return_value=httpx.Response(200, json={"paths": compras_paths})
    )
    respx.get("https://dadosabertos.compras.gov.br/v3/api-docs/swagger-config").mock(
        return_value=httpx.Response(404)
    )
    respx.get("https://api.portaldatransparencia.gov.br/v3/api-docs").mock(
        return_value=httpx.Response(
            200,
            json={
                "paths": {
                    "/api-de-dados/despesas/documentos-por-favorecido": {},
                }
            },
        )
    )

    result = await audit_sources(
        settings=Settings(app_env="test"),
        output_directory=tmp_path,
    )

    assert result.checked_sources == 4
    assert result.missing_paths == 0
    assert result.report_path.is_file()
    text = result.report_path.read_text(encoding="utf-8")
    assert "operação não verificada" in text
    assert "**compatível**" not in text
    assert "não altera automaticamente" in text


@pytest.mark.asyncio
@respx.mock
async def test_source_audit_records_unavailable_openapi_without_crashing(tmp_path: Path) -> None:
    for url in (
        "https://pncp.gov.br/api/consulta/v3/api-docs",
        "https://pncp.gov.br/pncp-api/v3/api-docs",
        "https://dadosabertos.compras.gov.br/v3/api-docs",
        "https://dadosabertos.compras.gov.br/v3/api-docs/swagger-config",
        "https://api.portaldatransparencia.gov.br/v3/api-docs",
    ):
        respx.get(url).mock(return_value=httpx.Response(503))

    result = await audit_sources(
        settings=Settings(app_env="test"),
        output_directory=tmp_path,
    )

    assert len(result.warnings) == 4
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


async def test_enrichment_waits_for_manual_contact_provenance(database: DatabaseContext):
    address = "licitacoes@empresa.example"
    manual_address = "licitacoes@EMPRESA.EXAMPLE"
    manual_origin = "https://empresa.example/verificado"
    async with database.sessions() as session, session.begin():
        company = Company(
            cnpj="00000000000191",
            normalized_name="EMPRESA",
            website="https://empresa.example",
            domain="empresa.example",
        )
        session.add(company)
        await session.flush()
        company_id = company.id
        session.add(
            CompanyContact(
                company_id=company_id,
                contact_type=ContactType.EMAIL,
                contact_value=address,
                source_url="https://empresa.example/antigo",
                is_corporate=True,
                confidence=Decimal("0.8"),
            )
        )

    service = ContactEnrichmentService(
        Settings(app_env="test"),
        session_factory=database.sessions,
        provider=StaticContactProvider(),
    )
    database_attempted = asyncio.Event()

    def before_write(connection, cursor, statement, parameters, context, executemany):
        if statement == "BEGIN IMMEDIATE":
            database_attempted.set()

    def after_read(connection, cursor, statement, parameters, context, executemany):
        if "FROM company_contacts" in statement:
            database_attempted.set()

    engine = database.engine.sync_engine
    async with database.sessions() as manual:
        await begin_corporate_write(manual)
        company = await manual.get_one(Company, company_id)
        assert not await save_corporate_contact(
            manual, company, ContactType.EMAIL, manual_address, manual_origin
        )
        event.listen(engine, "before_cursor_execute", before_write)
        event.listen(engine, "after_cursor_execute", after_read)
        try:
            automatic = asyncio.create_task(service.run(force=True))
            await asyncio.wait_for(database_attempted.wait(), timeout=5)
            await manual.commit()
            result = await asyncio.wait_for(automatic, timeout=5)
        finally:
            event.remove(engine, "before_cursor_execute", before_write)
            event.remove(engine, "after_cursor_execute", after_read)

    assert result.errors == result.contacts_updated == result.contacts_created == 0
    async with database.sessions() as session:
        contact = (await session.scalars(select(CompanyContact))).one()
        assert contact.source_record_id is not None
        source = await session.get_one(SourceRecord, contact.source_record_id)
        assert contact.source_url == source.endpoint == manual_origin
        assert contact.contact_value == source.raw_payload["contact_value"] == manual_address
        assert contact.confidence == 1


async def test_enrichment_discards_results_after_manual_website_change(database: DatabaseContext):
    async with database.sessions.begin() as db:
        company = Company(
            cnpj="00000000000191",
            normalized_name="EMPRESA",
            website="https://empresa.example",
            domain="empresa.example",
        )
        db.add(company)
        await db.flush()
        company_id = company.id

    class ProviderDuringEdit(StaticContactProvider):
        async def find_contacts(self, target):
            assert target.domain == "empresa.example"
            async with database.sessions() as manual:
                await begin_corporate_write(manual)
                company = await manual.get_one(Company, company_id)
                await save_corporate_contact(
                    manual,
                    company,
                    ContactType.WEBSITE,
                    "https://atual.example",
                    "https://atual.example/registro",
                )
                await manual.commit()
            return await super().find_contacts(target)

    result = await ContactEnrichmentService(
        Settings(app_env="test"), session_factory=database.sessions, provider=ProviderDuringEdit()
    ).run(force=True)

    assert result.companies_checked == 1
    assert result.contacts_created == result.contacts_updated == result.errors == 0
    async with database.sessions() as db:
        company = await db.get_one(Company, company_id)
        assert company.domain == "atual.example"
        contact = (await db.scalars(select(CompanyContact))).one()
        assert contact.contact_type == ContactType.WEBSITE
        assert contact.source_record_id is not None
