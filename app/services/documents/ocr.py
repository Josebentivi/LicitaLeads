"""Optional OCR contracts.

No concrete provider is required by the MVP.  A provider can be injected by the
processing job and its output is still treated as evidence requiring provenance.
"""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Any, Protocol, runtime_checkable


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


class TesseractOCRProvider:
    """Optional local OCR. Requires the ``ocr`` extra and the Tesseract binary."""

    name = "tesseract"

    def __init__(
        self,
        *,
        language: str = "por",
        dpi: int = 200,
        min_confidence: float = 0.5,
    ) -> None:
        self.language = language
        self.dpi = dpi
        self.min_confidence = min_confidence

    @staticmethod
    def available() -> bool:
        import importlib.util
        import shutil

        return bool(
            importlib.util.find_spec("pytesseract")
            and importlib.util.find_spec("PIL")
            and shutil.which("tesseract")
        )

    async def extract_text(self, content: bytes, *, mime_type: str) -> list[OCRPage]:
        if not self.available():
            return []
        import asyncio

        return await asyncio.to_thread(self._extract_sync, content, mime_type)

    def _extract_sync(self, content: bytes, mime_type: str) -> list[OCRPage]:
        from PIL import Image

        results: list[OCRPage] = []
        if mime_type == "application/pdf":
            import fitz  # type: ignore[import-untyped]

            with fitz.open(stream=content, filetype="pdf") as document:
                for index, page in enumerate(document):
                    pixmap = page.get_pixmap(dpi=self.dpi)
                    image = Image.open(BytesIO(pixmap.tobytes("png")))
                    try:
                        result = self._ocr_image(image, index + 1)
                    finally:
                        image.close()
                    if result is not None:
                        results.append(result)
        else:
            image = Image.open(BytesIO(content))
            try:
                result = self._ocr_image(image, 1)
            finally:
                image.close()
            if result is not None:
                results.append(result)
        return results

    def _ocr_image(self, image: Any, page_number: int) -> OCRPage | None:
        import pytesseract  # type: ignore[import-untyped]

        text = pytesseract.image_to_string(image, lang=self.language).strip()
        if not text:
            return None
        data = pytesseract.image_to_data(
            image, lang=self.language, output_type=pytesseract.Output.DICT
        )
        scores = []
        for value in data.get("conf", []):
            try:
                score = float(value)
            except (TypeError, ValueError):
                continue
            if score >= 0:
                scores.append(score / 100)
        confidence = sum(scores) / len(scores) if scores else None
        if confidence is not None and confidence < self.min_confidence:
            return None
        return OCRPage(page_number=page_number, text=text, confidence=confidence)


def build_ocr_provider(settings: object | None = None) -> OCRProvider:
    """Build the configured provider without importing optional dependencies early."""

    from app.connectors.http import setting

    if not bool(setting(settings, "ocr_enabled", False)):
        return DisabledOCRProvider()
    provider = str(setting(settings, "ocr_provider", "none")).lower()
    if provider != "tesseract":
        return DisabledOCRProvider()
    return TesseractOCRProvider(
        language=str(setting(settings, "ocr_language", "por")),
        dpi=int(setting(settings, "ocr_dpi", 200)),
        min_confidence=float(setting(settings, "ocr_min_confidence", 0.5)),
    )
