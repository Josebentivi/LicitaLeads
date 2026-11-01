"""Safe document extraction primitives."""

from .extractor import DocumentExtractor
from .ocr import (
    DisabledOCRProvider,
    OCRPage,
    OCRProvider,
    TesseractOCRProvider,
    build_ocr_provider,
)
from .types import (
    DocumentChunk,
    DocumentExtraction,
    DocumentExtractionStatus,
    ExtractedArchiveMember,
)

__all__ = [
    "DocumentChunk",
    "DocumentExtraction",
    "DocumentExtractionStatus",
    "DocumentExtractor",
    "DisabledOCRProvider",
    "OCRPage",
    "OCRProvider",
    "TesseractOCRProvider",
    "build_ocr_provider",
    "ExtractedArchiveMember",
]
