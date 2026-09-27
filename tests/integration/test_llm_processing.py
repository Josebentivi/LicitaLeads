"""LLM-assisted detection: evidence-bound, source-marked, and idempotent."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.models import Document, ExtractionStatus, Lead, Procurement, ProcurementEvent
from app.services.ingestion.processor import DocumentProcessingService
from app.services.llm import EvidenceBoundLLMEventAnalyzer

from .conftest import DatabaseContext
from .test_document_processing import FakeDownloader

QUOTE = "registrou a proposta da empresa Empresa Exemplo Ltda."
TEXT = (
    "A Comissão de Licitação registrou a proposta da empresa Empresa Exemplo Ltda., "
    "CNPJ 00.000.000/0001-91, referente ao item 3 do certame. "
    "O processamento segue os termos do edital e a documentação de habilitação será "
    "analisada na sequência prevista, sem outras ocorrências relevantes para o julgamento."
)


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        app_env="test",
        document_storage_path=tmp_path / "documents",
        raw_data_storage_path=tmp_path / "raw",
        holiday_calendar_path=tmp_path / "holidays.csv",
        min_lead_score=0,
        llm_enabled=True,
        llm_provider="openai",
        llm_api_key="sk-test",
        llm_model="gpt-5.6-luna",
    )


async def _pending_document(database: DatabaseContext):
    async with database.sessions() as session, session.begin():
        procurement = Procurement(
            source="pncp",
            external_id="llm-process-1",
            title="Pregão Eletrônico 21/2026",
            agency_name="Prefeitura Municipal de Exemplo",
            agency_cnpj="00000000000191",
            uf="MA",
            municipality="São Luís",
            estimated_value=180000,
            publication_at=datetime(2026, 9, 16, 14, 30, tzinfo=UTC),
            status="em julgamento",
            fingerprint="1" * 64,
        )
        session.add(procurement)
        await session.flush()
        document = Document(
            procurement_id=procurement.id,
            document_type="ata",
            title="Ata",
            original_url="https://pncp.gov.br/ata.txt",
            extraction_status=ExtractionStatus.PENDING,
            fingerprint="2" * 64,
        )
        session.add(document)
        await session.flush()


def _service(database: DatabaseContext, tmp_path: Path, provider) -> DocumentProcessingService:
    return DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(TEXT.encode(), filename="ata.txt", mime="text/plain"),
        llm_analyzer=EvidenceBoundLLMEventAnalyzer(provider),
    )


@pytest.mark.asyncio
async def test_llm_finding_is_persisted_and_idempotent(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    await _pending_document(database)

    async def provider(text: str, pages: dict[int, str] | None) -> list[dict]:
        del text, pages
        return [
            {
                "event_type": "PROPOSAL_SUBMITTED",
                "company_name": "Empresa Exemplo Ltda.",
                "company_cnpj": "00.000.000/0001-91",
                "item_number": "3",
                "reason_summary": "Proposta registrada.",
                "reason_category": "UNKNOWN",
                "evidence_quotes": [{"text": QUOTE}],
                "confidence": 0.9,
                "requires_manual_review": False,
            }
        ]

    service = _service(database, tmp_path, provider)

    summary = await service.process_pending()

    assert summary.events_created == 1
    assert summary.leads_created == 1
    async with database.sessions() as session:
        event = await session.scalar(select(ProcurementEvent))
        lead = await session.scalar(select(Lead))
    assert event is not None and event.source == "llm"
    assert event.company_id is not None
    assert lead is not None and lead.company_id == event.company_id

    repeated = await service.detect_existing()
    assert repeated.events_created == 0
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ProcurementEvent)) == 1
        assert await session.scalar(select(func.count()).select_from(Lead)) == 1


@pytest.mark.asyncio
async def test_llm_hallucinated_quote_is_skipped(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    await _pending_document(database)

    async def provider(text: str, pages: dict[int, str] | None) -> list[dict]:
        del text, pages
        return [
            {
                "event_type": "PROPOSAL_SUBMITTED",
                "evidence_quotes": [{"text": "trecho que não existe no documento"}],
                "confidence": 0.4,
                "requires_manual_review": True,
            }
        ]

    service = _service(database, tmp_path, provider)

    summary = await service.process_pending()

    assert summary.events_created == 0
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ProcurementEvent)) == 0
