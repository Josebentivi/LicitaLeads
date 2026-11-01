"""A synthetic page exercises the installed Tesseract Portuguese model."""

import re

import pytest

pytestmark = pytest.mark.ocr_real


def test_real_portuguese_ocr_recognizes_procurement_identifiers(tmp_path):
    from app.services.test_suite import ocr_readiness

    ready = ocr_readiness()
    if not ready["available"]:
        pytest.skip(ready["detail"])
    import pytesseract
    from PIL import Image, ImageDraw, ImageFont

    pytesseract.pytesseract.tesseract_cmd = ready["executable"]
    try:
        font = ImageFont.truetype("C:/Windows/Fonts/arial.ttf", 48)
    except OSError:
        font = ImageFont.truetype("DejaVuSans.ttf", 48)
    page = Image.new("RGB", (1800, 420), "white")
    ImageDraw.Draw(page).multiline_text(
        (50, 50),
        "LICITAÇÃO PÚBLICA\nPARTICIPAÇÃO DOCUMENTADA\nCNPJ 11.222.333/0001-81\nPROCESSO 987654 2026",
        fill="black",
        font=font,
        spacing=25,
    )
    image = tmp_path / "portugues.png"
    page.save(image)
    text = pytesseract.image_to_string(str(image), lang="por", config="--psm 6", timeout=30)
    assert "LICITA" in text.upper() and "DOCUMENTADA" in text.upper()
    digits = re.sub(r"\D", "", text)
    assert "11222333000181" in digits and "9876542026" in digits
