"""Optional OCR provider tests without requiring the Tesseract binary."""

from __future__ import annotations

import io
import sys
from types import SimpleNamespace

import pytest

from app.config import Settings
from app.services.documents.ocr import (
    DisabledOCRProvider,
    TesseractOCRProvider,
    build_ocr_provider,
)

Image = pytest.importorskip("PIL.Image")


def _png() -> bytes:
    stream = io.BytesIO()
    Image.new("RGB", (8, 8), "white").save(stream, format="PNG")
    return stream.getvalue()


def test_factory_respects_disabled_setting() -> None:
    provider = build_ocr_provider(Settings(app_env="test", ocr_enabled=False))

    assert isinstance(provider, DisabledOCRProvider)


def test_factory_builds_tesseract_when_enabled() -> None:
    provider = build_ocr_provider(
        Settings(app_env="test", ocr_enabled=True, ocr_provider="tesseract")
    )

    assert isinstance(provider, TesseractOCRProvider)
    assert provider.language == "por"


@pytest.mark.asyncio
async def test_tesseract_provider_maps_text_and_confidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = SimpleNamespace(
        Output=SimpleNamespace(DICT="dict"),
        image_to_string=lambda image, lang: "Texto OCR",
        image_to_data=lambda image, lang, output_type: {
            "conf": ["90", "-1", "80"],
        },
    )
    monkeypatch.setitem(sys.modules, "pytesseract", fake)
    monkeypatch.setattr(TesseractOCRProvider, "available", staticmethod(lambda: True))
    provider = TesseractOCRProvider(language="por", dpi=200, min_confidence=0.5)

    pages = await provider.extract_text(_png(), mime_type="image/png")

    assert len(pages) == 1
    assert pages[0].page_number == 1
    assert pages[0].text == "Texto OCR"
    assert pages[0].confidence == pytest.approx(0.85)
