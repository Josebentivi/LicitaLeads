"""Optional OCR contracts.

No concrete provider is required by the MVP.  A provider can be injected by the
processing job and its output is still treated as evidence requiring provenance.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class OCRPage:
    page_number: int
    text: str
    confidence: float | None = None


@runtime_checkable
class OCRProvider(Protocol):
    async def extract_text(self, content: bytes, *, mime_type: str) -> list[OCRPage]:
        """Return OCR text by one-based page number."""


class DisabledOCRProvider:
    async def extract_text(self, content: bytes, *, mime_type: str) -> list[OCRPage]:
        del content, mime_type
        return []
