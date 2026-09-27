from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class DocumentExtractionStatus(StrEnum):
    EXTRACTED = "extracted"
    EMPTY = "empty"
    OCR_REQUIRED = "ocr_required"
    UNSUPPORTED = "unsupported"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class DocumentChunk:
    text: str
    page_number: int | None = None
    locator: str | None = None
    start_offset: int = 0
    end_offset: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ExtractedArchiveMember:
    filename: str
    extraction: DocumentExtraction


@dataclass(frozen=True, slots=True)
class DocumentExtraction:
    status: DocumentExtractionStatus
    mime_type: str
    sha256: str
    text: str = ""
    chunks: tuple[DocumentChunk, ...] = ()
    archive_members: tuple[ExtractedArchiveMember, ...] = ()
    warnings: tuple[str, ...] = ()
