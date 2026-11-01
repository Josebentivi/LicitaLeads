"""Focused tests for deterministic deadline extraction in document processing."""

from __future__ import annotations

from datetime import UTC, datetime

from app.config import Settings
from app.models import ProcurementEventType
from app.services.documents.ocr import OCRPage
from app.services.documents.types import DocumentExtraction, DocumentExtractionStatus
from app.services.ingestion.processor import DocumentProcessingService


def test_relative_session_deadline_is_parsed_from_trigger() -> None:
    service = DocumentProcessingService(Settings(app_env="test"))
    trigger = datetime(2026, 9, 4, 14, 0, tzinfo=UTC)

    deadline = service._document_deadline(
        "A empresa foi convocada e tera o prazo de 2 horas para enviar a proposta.",
        trigger,
        ProcurementEventType.DILIGENCE_REQUESTED,
    )

    assert deadline == datetime(2026, 9, 4, 16, 0, tzinfo=UTC)


def test_relative_deadline_is_ignored_for_unrelated_events() -> None:
    service = DocumentProcessingService(Settings(app_env="test"))
    trigger = datetime(2026, 9, 4, 14, 0, tzinfo=UTC)

    deadline = service._document_deadline(
        "A empresa foi inabilitada e o edital previa o prazo de 2 horas.",
        trigger,
        ProcurementEventType.INELIGIBLE,
    )

    assert deadline is None


class FakeOCR:
    async def extract_text(self, content: bytes, *, mime_type: str):
        del content, mime_type
        return [OCRPage(page_number=1, text="Texto reconhecido da ata", confidence=0.93)]


class EmptyOCR:
    async def extract_text(self, content: bytes, *, mime_type: str):
        del content, mime_type
        return []


async def test_ocr_replaces_only_ocr_required_extraction() -> None:
    service = DocumentProcessingService(
        Settings(app_env="test", ocr_enabled=True, ocr_provider="tesseract"),
        ocr_provider=FakeOCR(),
    )
    extraction = DocumentExtraction(
        status=DocumentExtractionStatus.OCR_REQUIRED,
        mime_type="application/pdf",
        sha256="a" * 64,
    )

    result = await service._maybe_ocr(extraction, b"pdf")

    assert result.status is DocumentExtractionStatus.EXTRACTED
    assert result.text == "Texto reconhecido da ata"
    assert result.chunks[0].page_number == 1
    assert "ocr:tesseract" in result.warnings


async def test_ocr_keeps_required_status_when_provider_returns_nothing() -> None:
    service = DocumentProcessingService(
        Settings(app_env="test", ocr_enabled=True, ocr_provider="tesseract"),
        ocr_provider=EmptyOCR(),
    )
    extraction = DocumentExtraction(
        status=DocumentExtractionStatus.OCR_REQUIRED,
        mime_type="application/pdf",
        sha256="b" * 64,
    )

    result = await service._maybe_ocr(extraction, b"pdf")

    assert result.status is DocumentExtractionStatus.OCR_REQUIRED
