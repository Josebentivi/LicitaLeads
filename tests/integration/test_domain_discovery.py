"""Evidence-bound company domain discovery from official documents."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.models import (
    Company,
    Document,
    Evidence,
    ExtractionStatus,
    FieldObservation,
    FieldValueStatus,
    Participant,
    ParticipantRole,
    Procurement,
)
from app.services.ingestion.processor import DocumentProcessingService

from .conftest import DatabaseContext
from .test_document_processing import FakeDownloader

CNPJ = "00000000000191"
DOMAIN = "empresaexemplo.com.br"
EMAIL = "contato@" + DOMAIN


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        app_env="test",
        document_storage_path=tmp_path / "documents",
        raw_data_storage_path=tmp_path / "raw",
        holiday_calendar_path=tmp_path / "holidays.csv",
        min_lead_score=0,
    )


def _text(email: str) -> str:
    return (
        "ATA DE JULGAMENTO. A empresa Empresa Exemplo Ltda., CNPJ 00.000.000/0001-91, "
        f"apresentou proposta ao item 3 e indicou o e-mail {email} para comunicações "
        "oficiais. Nenhuma outra ocorrência relevante foi registrada no certame."
    )


async def _graph(
    database: DatabaseContext,
    *,
    existing_domain: str | None = None,
    external_id: str = "domain-process-1",
) -> UUID:
    async with database.sessions() as session, session.begin():
        procurement = Procurement(
            source="pncp",
            external_id=external_id,
            title="Pregão Eletrônico 30/2026",
            agency_name="Prefeitura Municipal de Exemplo",
            agency_cnpj=CNPJ,
            uf="MA",
            municipality="São Luís",
            estimated_value=180000,
            publication_at=datetime(2026, 9, 16, 14, 30, tzinfo=UTC),
            status="em julgamento",
            fingerprint="1" * 64,
        )
        company = Company(
            cnpj=CNPJ,
            legal_name="Empresa Exemplo Ltda.",
            normalized_name="EMPRESA EXEMPLO",
            domain=existing_domain,
            fingerprint="2" * 64,
        )
        session.add_all([procurement, company])
        await session.flush()
        participant = Participant(
            procurement_id=procurement.id,
            company_id=company.id,
            participation_role=ParticipantRole.PARTICIPANT,
            source="pncp",
            confidence=Decimal("1"),
            fingerprint="3" * 64,
        )
        document = Document(
            procurement_id=procurement.id,
            document_type="ata",
            title="Ata",
            original_url="https://pncp.gov.br/ata.txt",
            extraction_status=ExtractionStatus.PENDING,
            fingerprint="4" * 64,
        )
        session.add_all([participant, document])
        await session.flush()
        return company.id


def _service(database: DatabaseContext, tmp_path: Path, email: str) -> DocumentProcessingService:
    return DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(_text(email).encode(), filename="ata.txt", mime="text/plain"),
    )


@pytest.mark.asyncio
async def test_domain_is_discovered_with_evidence_and_is_idempotent(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    company_id = await _graph(database)
    service = _service(database, tmp_path, EMAIL)

    await service.process_pending()

    async with database.sessions() as session:
        company = await session.get(Company, company_id)
        observation = await session.scalar(select(FieldObservation))
        evidence = (
            await session.get(Evidence, observation.evidence_id)
            if observation is not None
            else None
        )
    assert company is not None
    assert company.domain == DOMAIN
    assert company.website == f"https://{DOMAIN}"
    assert observation is not None
    assert observation.entity_type == "company"
    assert observation.field_name == "domain"
    assert observation.value_status is FieldValueStatus.OBSERVED
    assert observation.evidence_id is not None
    assert evidence is not None and evidence.text_excerpt == EMAIL

    await service.detect_existing()
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(FieldObservation)) == 1


@pytest.mark.asyncio
async def test_existing_domain_is_not_overwritten_and_conflict_is_flagged(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    company_id = await _graph(database, existing_domain="antigo.com.br")
    service = _service(database, tmp_path, EMAIL)

    await service.process_pending()

    async with database.sessions() as session:
        company = await session.get(Company, company_id)
        observation = await session.scalar(select(FieldObservation))
    assert company is not None and company.domain == "antigo.com.br"
    assert observation is not None
    assert observation.value_status is FieldValueStatus.REQUIRES_MANUAL_REVIEW


@pytest.mark.asyncio
async def test_free_mail_domain_is_ignored(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    company_id = await _graph(database, external_id="domain-process-2")
    service = _service(database, tmp_path, "contato@" + "gmail.com")

    await service.process_pending()

    async with database.sessions() as session:
        company = await session.get(Company, company_id)
        observations = await session.scalar(select(func.count()).select_from(FieldObservation))
    assert company is not None and company.domain is None
    assert observations == 0
