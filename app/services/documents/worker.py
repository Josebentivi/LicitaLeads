"""Isolated extraction/OCR entrypoint. Receives paths and non-secret options only."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

from app.services.documents.errors import ExtractionFailure
from app.services.documents.extractor import DocumentExtractor
from app.services.documents.ocr import build_ocr_provider
from app.services.documents.security import ArchiveLimits
from app.services.documents.types import (
    DocumentChunk,
    DocumentExtraction,
    DocumentExtractionStatus,
    ExtractedArchiveMember,
)


def decode_extraction(data: dict[str, Any]) -> DocumentExtraction:
    return DocumentExtraction(
        status=DocumentExtractionStatus(data["status"]),
        mime_type=data["mime_type"],
        sha256=data["sha256"],
        text=data["text"],
        chunks=tuple(DocumentChunk(**chunk) for chunk in data["chunks"]),
        archive_members=tuple(
            ExtractedArchiveMember(
                filename=member["filename"],
                extraction=decode_extraction(member["extraction"]),
            )
            for member in data["archive_members"]
        ),
        warnings=tuple(data["warnings"]),
    )


async def extract_job(job: dict[str, Any]) -> DocumentExtraction:
    content = Path(job["input"]).read_bytes()
    job["_input_hash"] = hashlib.sha256(content).hexdigest()
    extractor = DocumentExtractor(
        archive_limits=ArchiveLimits(**job["archive_limits"]),
        max_nested_archives=job.get("archive_max_depth", 2),
    )
    extraction = extractor.extract(content, filename=job["filename"], mime_type=job["mime_type"])
    if extraction.status is not DocumentExtractionStatus.OCR_REQUIRED or not job["ocr_enabled"]:
        return extraction
    try:
        pages = await build_ocr_provider(job).extract_text(content, mime_type=extraction.mime_type)
    except Exception:
        return replace(extraction, warnings=(*extraction.warnings, "ocr:provider_failed"))
    chunks: list[DocumentChunk] = []
    parts: list[str] = []
    cursor = 0
    for page in pages:
        text = page.text.strip()
        if not text:
            continue
        if parts:
            cursor += 2
        chunks.append(
            DocumentChunk(
                text=text,
                page_number=page.page_number,
                locator=f"ocr:page:{page.page_number}",
                start_offset=cursor,
                end_offset=cursor + len(text),
                metadata={"ocr_confidence": page.confidence},
            )
        )
        cursor += len(text)
        parts.append(text)
    if not chunks:
        return extraction
    return replace(
        extraction,
        status=DocumentExtractionStatus.EXTRACTED,
        text="\n\n".join(parts),
        chunks=tuple(chunks),
        warnings=(*extraction.warnings, "ocr:tesseract"),
    )


def main() -> int:
    job_path, output_path = map(Path, sys.argv[1:3])
    job: dict[str, Any] = {}
    try:
        job = json.loads(job_path.read_text(encoding="utf-8"))
        extraction = asyncio.run(extract_job(job))
        output_path.write_text(json.dumps(asdict(extraction), ensure_ascii=False), encoding="utf-8")
        return 0
    except Exception as exc:
        failure = (
            ExtractionFailure(
                exc.code,
                exception_type=exc.exception_type,
                member_path=exc.member_path,
                input_hash=job.get("_input_hash"),
            )
            if isinstance(exc, ExtractionFailure)
            else ExtractionFailure.from_exception(exc, input_hash=job.get("_input_hash"))
        )
        output_path.write_text(
            json.dumps({"error": failure.as_dict()}, ensure_ascii=True), encoding="utf-8"
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
