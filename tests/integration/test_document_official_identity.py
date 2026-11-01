"""URL aliases never supply content/version evidence by themselves."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from app.connectors.base import RawDocument
from app.models import Document, DocumentAlias, DocumentIdentity, Procurement, SourceRecord
from app.models.enums import ExtractionStatus
from app.services.ingestion.pipeline import IngestionPipeline
from app.services.ingestion.processor import DocumentProcessingService
from tests.integration.test_participation_integrity import PUBLIC_URL, URL, seed


@pytest.mark.parametrize("versioned", [False, True])
async def test_changed_url_reuses_identity_but_only_verified_version_reuses_extraction(
    database, tmp_path, versioned
):
    settings, ids = await seed(database, tmp_path)
    pipeline = IngestionPipeline(settings, session_factory=database.sessions)
    async with database.sessions() as db, db.begin():
        document = await db.get(Document, ids["document"])
        old = await db.get(SourceRecord, document.source_record_id)
        row = {"url": PUBLIC_URL, "sequencialDocumento": 1, "statusAtivo": True}
        if versioned:
            original = dict(old.raw_payload[0], dataAtualizacao="2026-10-29T12:00:00")
            old.raw_payload = [original]
            row["dataAtualizacao"] = original["dataAtualizacao"]
        new = SourceRecord(
            source="pncp",
            endpoint=old.endpoint,
            request_parameters={},
            raw_payload=[row],
            response_hash="b" * 64,
            collection_complete=True,
            collected_at=datetime.now(UTC) + timedelta(seconds=1),
        )
        db.add(new)
        await db.flush()
        raw = RawDocument(
            source="pncp",
            external_id="file-1",
            procurement_external_id="01612325000198-1-000058/2026",
            download_url=PUBLIC_URL,
            raw_payload=row,
        )
        proc = await db.get(Procurement, ids["procurement"])
        await pipeline._upsert_documents(db, proc, [raw], {}, new.id)
        assert document.original_url == URL
        assert document.file_hash is not None and document.extracted_text is not None
        assert document.extraction_status == (
            ExtractionStatus.EXTRACTED if versioned else ExtractionStatus.PENDING
        )
        assert await db.scalar(select(func.count()).select_from(Document)) == 1
        identity = await db.scalar(select(DocumentIdentity))
        assert identity.canonical_document_id == document.id
        alias = await db.scalar(select(DocumentAlias).where(DocumentAlias.url == PUBLIC_URL))
        assert alias.equivalent is versioned
        assert alias.proof == ("official_version" if versioned else "verification_pending")
        await pipeline._upsert_documents(db, proc, [raw], {}, new.id)
        assert await db.scalar(select(func.count()).select_from(DocumentAlias)) == 2


async def test_aliases_do_not_consume_pending_document_capacity(database, tmp_path, monkeypatch):
    from app.services.documents.identity import document_resolution, reconcile_aliases

    settings, ids = await seed(database, tmp_path, alias=True)
    async with database.sessions() as db, db.begin():
        documents = list((await db.scalars(select(Document))).all())
        await reconcile_aliases(db, documents, apply=True)
        identity = await db.scalar(select(DocumentIdentity))
        canonical_id = identity.canonical_document_id
        primary = await db.get(Document, canonical_id)
        primary.extraction_status = ExtractionStatus.PENDING
        resolution = await document_resolution(db, primary)
        assert resolution is not None and ":62150" not in resolution.canonical_url
    selected = []

    async def prepare(document_id, **kwargs):
        selected.append(document_id)
        return None

    service = DocumentProcessingService(settings, session_factory=database.sessions)
    monkeypatch.setattr(service, "_prepare_document", prepare)
    summary = await service.process_pending(limit=2)
    assert selected == [canonical_id] and summary.documents_skipped == 1


async def test_changed_content_retains_files_and_evidence_until_complete(database, tmp_path):
    from app.models import Evidence, Lead
    from app.services.documents.identity import complete_content_version, fork_content_version
    from app.services.participation_integrity import lead_current

    settings, ids = await seed(database, tmp_path)
    async with database.sessions() as db, db.begin():
        original = await db.get(Document, ids["document"])
        old_text, old_path, old_hash = (
            original.extracted_text,
            original.local_path,
            original.file_hash,
        )
        evidence_ids = list(
            (await db.scalars(select(Evidence.id).where(Evidence.document_id == original.id))).all()
        )
        replacement = await fork_content_version(db, original, "b" * 64)
        replacement.file_hash = "b" * 64
        replacement.extracted_text = "nova versão"
        assert replacement.id != original.id
        # An incomplete replacement cannot retract facts of the last validated version.
        assert await db.scalar(select(Lead.id).where(lead_current())) is not None
        await complete_content_version(db, replacement)
        assert await db.scalar(select(Lead.id).where(lead_current())) is None
        assert (original.extracted_text, original.local_path, original.file_hash) == (
            old_text,
            old_path,
            old_hash,
        )
        assert (
            list(
                (
                    await db.scalars(select(Evidence.id).where(Evidence.document_id == original.id))
                ).all()
            )
            == evidence_ids
        )
        returned = await fork_content_version(db, replacement, old_hash)
        assert returned.id not in {replacement.id, original.id}


async def test_real_processing_forks_changed_download_and_keeps_old_content(database, tmp_path):
    from app.services.documents.identity import canonical_document_ids
    from tests.integration.test_document_processing import FakeDownloader

    settings, ids = await seed(database, tmp_path)
    async with database.sessions() as db:
        old = await db.get(Document, ids["document"])
        old_text, old_path, old_hash = old.extracted_text, old.local_path, old.file_hash
    service = DocumentProcessingService(
        settings,
        session_factory=database.sessions,
        downloader=FakeDownloader(
            b"Licitante: NOVA EMPRESA LTDA, CNPJ 00.000.000/0001-91. Proposta total R$ 10,00."
        ),
    )
    try:
        await service.recheck_document(ids["document"])
    finally:
        await service.aclose()
    async with database.sessions() as db:
        assert await db.scalar(select(func.count()).select_from(Document)) == 2
        old = await db.get(Document, ids["document"])
        assert (old.extracted_text, old.local_path, old.file_hash) == (old_text, old_path, old_hash)
        canonical = (await canonical_document_ids(db, [old.id]))[0]
        assert canonical != old.id
        assert (await db.get(Document, canonical)).extraction_status == ExtractionStatus.EXTRACTED


async def test_inactive_canonical_document_blocks_evidence_on_its_alias(database, tmp_path):
    from app.models import Evidence
    from app.repositories.current_evidence import evidence_current
    from app.services.documents.identity import reconcile_aliases

    settings, ids = await seed(database, tmp_path, alias=True)
    async with database.sessions() as db, db.begin():
        docs = list((await db.scalars(select(Document))).all())
        await reconcile_aliases(db, docs, apply=True)
        identity = await db.scalar(select(DocumentIdentity))
        canonical = await db.get(Document, identity.canonical_document_id)
        alias = next(d for d in docs if d.id != canonical.id)
        proof = Evidence(
            procurement_id=canonical.procurement_id,
            document_id=alias.id,
            text_excerpt="Referência histórica em alias",
        )
        db.add(proof)
        await db.flush()
        assert await db.scalar(
            select(Evidence.id).where(Evidence.id == proof.id, evidence_current(Evidence.id))
        )
        canonical.is_active = False
        await db.flush()
        db.add(
            Evidence(
                procurement_id=canonical.procurement_id,
                source_record_id=canonical.source_record_id,
                text_excerpt="Outra fonte vigente",
            )
        )
        await db.flush()
        assert (
            await db.scalar(
                select(Evidence.id).where(Evidence.id == proof.id, evidence_current(Evidence.id))
            )
            is None
        )
