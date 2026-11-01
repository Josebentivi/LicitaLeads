"""A PNCP ZIP version keeps the prior tree and withdraws facts only when complete."""

import hashlib
import io
from dataclasses import replace
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from sqlalchemy import select

from app.models import Document, Lead
from app.models.enums import ExtractionStatus
from app.services.documents.identity import canonical_document_ids
from app.services.documents.types import DocumentExtractionStatus
from app.services.ingestion.processor import DocumentProcessingService
from app.services.participation_integrity import lead_current
from tests.integration.test_document_processing import FakeDownloader
from tests.integration.test_participation_integrity import PROPOSALS, SUBJECT, URL, seed


def archive(text):
    out = io.BytesIO()
    with ZipFile(out, "w", ZIP_DEFLATED) as container:
        container.writestr("proposta.txt", text)
    return out.getvalue()


@pytest.mark.parametrize("complete", [False, True])
async def test_pncp_zip_versions_preserve_ancestors_and_last_verified_facts(
    database, tmp_path, monkeypatch, complete
):
    settings, ids = await seed(database, tmp_path)
    async with database.sessions() as db, db.begin():
        child = await db.get(Document, ids["document"])
        old_text, old_hash = child.extracted_text, child.file_hash
        body = archive(old_text)
        path = settings.document_storage_path / "old.zip"
        path.write_bytes(body)
        parent = Document(
            procurement_id=child.procurement_id,
            source_record_id=child.source_record_id,
            original_url=URL,
            local_path=str(path),
            file_hash=hashlib.sha256(body).hexdigest(),
            mime_type="application/zip",
            extraction_status=ExtractionStatus.EXTRACTED,
        )
        db.add(parent)
        await db.flush()
        parent_id = parent.id
        child.parent_document_id = parent.id
        child.original_url += "#proposta.txt"
        child.title = "proposta.txt"
        child.local_path = None
    service = DocumentProcessingService(
        settings,
        session_factory=database.sessions,
        downloader=FakeDownloader(
            archive(SUBJECT + PROPOSALS), filename="updated.zip", mime="application/zip"
        ),
    )
    if not complete:
        original = service._extract_download

        async def incomplete(*args, **kwargs):
            extraction, path = await original(*args, **kwargs)
            return replace(extraction, status=DocumentExtractionStatus.FAILED), path

        monkeypatch.setattr(service, "_extract_download", incomplete)
    try:
        await service.recheck_document(parent_id)
    finally:
        await service.aclose()
    async with database.sessions() as db:
        canonical = (await canonical_document_ids(db, [parent_id]))[0]
        assert canonical != parent_id
        old = await db.get(Document, ids["document"])
        assert (old.extracted_text, old.file_hash, old.parent_document_id) == (
            old_text,
            old_hash,
            parent_id,
        )
        eligible = await db.scalar(select(Lead.id).where(Lead.id == ids["lead"], lead_current()))
        assert (eligible is None) is complete
        lead = await db.get(Lead, ids["lead"])
        assert lead.score == 35 and lead.scoring_version == "1.0"
        assert (
            bool(
                (
                    await db.scalars(
                        select(Document.id).where(Document.parent_document_id == canonical)
                    )
                ).all()
            )
            is complete
        )
