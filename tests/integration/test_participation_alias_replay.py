"""Equal headers cannot override bytes; equivalent aliases replay idempotently."""

from datetime import UTC, datetime

from sqlalchemy import func, select

from app.models import Document, DocumentAlias, DocumentChunk, FieldChange, SourceRecord
from app.models.enums import ExtractionStatus
from app.services.documents.identity import register_alias
from tests.integration.test_participation_integrity import apply_repair, seed


async def test_conflicting_bytes_are_not_equivalent_even_when_header_is_equal(database, tmp_path):
    settings, ids = await seed(database, tmp_path, alias=True)
    async with database.sessions() as db, db.begin():
        docs = list((await db.scalars(select(Document))).all())
        original = await db.get(Document, ids["document"])
        other = next(d for d in docs if d.id != original.id)
        for doc in docs:
            source = await db.get(SourceRecord, doc.source_record_id)
            source.raw_payload = [
                dict(source.raw_payload[0], dataAtualizacao="2026-10-30T12:00:00")
            ]
        other.file_hash = "f" * 64
        await register_alias(
            db,
            original,
            url=original.original_url,
            record=await db.get(SourceRecord, original.source_record_id),
        )
        await register_alias(
            db,
            original,
            physical=other,
            url=other.original_url,
            record=await db.get(SourceRecord, other.source_record_id),
            allow_schedule=False,
        )
        alias = await db.scalar(select(DocumentAlias).where(DocumentAlias.document_id == other.id))
        assert not alias.equivalent


async def test_same_bytes_on_two_aliases_do_not_create_replay_history(database, tmp_path):
    settings, ids = await seed(database, tmp_path, alias=True)
    async with database.sessions() as db, db.begin():
        original = await db.get(Document, ids["document"])
        other = await db.scalar(select(Document).where(Document.id != original.id))
        other.file_hash, other.local_path = original.file_hash, original.local_path
        other.extracted_text, other.mime_type = original.extracted_text, original.mime_type
        other.extraction_status = ExtractionStatus.EXTRACTED
        other.last_checked_at = datetime.now(UTC)
        for chunk in (
            await db.scalars(select(DocumentChunk).where(DocumentChunk.document_id == original.id))
        ).all():
            db.add(
                DocumentChunk(
                    document_id=other.id,
                    sequence=chunk.sequence,
                    text=chunk.text,
                    page_number=chunk.page_number,
                    locator=chunk.locator,
                    start_offset=chunk.start_offset,
                    end_offset=chunk.end_offset,
                )
            )
    await apply_repair(database, settings)
    async with database.sessions() as db:
        before = await db.scalar(select(func.count()).select_from(FieldChange))
    await apply_repair(database, settings)
    async with database.sessions() as db:
        assert await db.scalar(select(func.count()).select_from(FieldChange)) == before
