import asyncio
import hashlib
from uuid import UUID

import pytest
from sqlalchemy import func, select, text

from app.config import Settings
from app.models import (
    Company,
    Document,
    DocumentChunk,
    Evidence,
    FactVerification,
    FieldChange,
    Lead,
    ProcurementEvent,
    ProcurementEventType,
    SourceRecord,
)
from app.services.backups import create_backup
from app.services.flow_reconciliation import reconcile_flow
from app.services.maintenance import exclusive_maintenance
from tests.integration.test_ai_central import CN_A
from tests.integration.test_document_processing import _pending_document


@pytest.fixture
def flow_settings(tmp_path):
    return Settings(
        _env_file=None,
        document_storage_path=tmp_path / "docs",
        raw_data_storage_path=tmp_path / "raw",
    )


async def test_flow_repair_is_backup_gated(database, flow_settings):
    with pytest.raises(ValueError, match="backup"):
        await reconcile_flow(database.sessions, flow_settings, apply=True)


async def test_flow_diagnostic_apply_resume_and_historical_score(database, flow_settings):
    settings = flow_settings.model_copy(update={"database_url": str(database.engine.url)})
    content = "Os preços registrados são os constantes da proposta adjudicada."
    doc_id = await _pending_document(database)
    settings.document_storage_path.mkdir(parents=True)
    path = settings.document_storage_path / "minuta.txt"
    path.write_text(content, encoding="utf-8")
    async with database.sessions() as db, db.begin():
        await db.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(80) NOT NULL)"))
        await db.execute(text("INSERT INTO alembic_version VALUES ('0027_ai_central')"))
        doc = await db.get(Document, doc_id)
        source = SourceRecord(
            source="compras_gov",
            endpoint="https://official.test/docs",
            raw_payload={"docs": [doc.original_url]},
            response_hash="1" * 64,
            fingerprint="source-minuta",
        )
        company = Company(
            cnpj=CN_A, legal_name="ALFA", normalized_name="alfa", fingerprint="flow-company"
        )
        db.add_all([source, company])
        await db.flush()
        doc.source_record_id, doc.title = source.id, "MINUTA-ATA-REGISTRO.docx"
        doc.file_hash, doc.local_path, doc.extracted_text = (
            hashlib.sha256(path.read_bytes()).hexdigest(),
            str(path),
            content,
        )
        chunk = DocumentChunk(document_id=doc_id, text=content, sequence=0)
        db.add(chunk)
        await db.flush()
        proof = Evidence(
            document_id=doc_id,
            procurement_id=doc.procurement_id,
            document_chunk_id=chunk.id,
            text_excerpt=content,
            fingerprint="flow-proof",
        )
        db.add(proof)
        await db.flush()
        event = ProcurementEvent(
            procurement_id=doc.procurement_id,
            company_id=company.id,
            event_type=ProcurementEventType.ADJUDICATED,
            source="document",
            evidence_id=proof.id,
            confidence=0.8,
            raw_description=content,
            fingerprint="flow-event",
        )
        db.add(event)
        await db.flush()
        lead = Lead(
            procurement_id=doc.procurement_id,
            company_id=company.id,
            triggering_event_id=event.id,
            score=77,
            urgency_score=15,
            evidence_score=20,
            contact_score=5,
            reason_summary="Histórico",
            fingerprint="flow-lead",
        )
        db.add(lead)
    impact = await reconcile_flow(database.sessions, settings)
    assert impact["result"]["events_requiring_invalidation"] == 1
    async with database.sessions() as db:
        assert await db.scalar(select(func.count()).select_from(FactVerification)) == 0
    async with exclusive_maintenance(database.sessions) as owner:
        backup = await asyncio.to_thread(
            create_backup,
            settings,
            settings.document_storage_path.parent / "backups",
            maintenance_owner=owner,
        )
        result = await reconcile_flow(
            database.sessions, settings, apply=True, backup=backup, maintenance_owner=owner
        )
        again = await reconcile_flow(
            database.sessions,
            settings,
            apply=True,
            backup=backup,
            maintenance_owner=owner,
            resume_id=UUID(result["run_id"]),
        )
        assert again["already_completed"]
    async with database.sessions() as db:
        lead = await db.scalar(select(Lead))
        assert lead.score == 77 and lead.scoring_version == "v1"
        assert (
            await db.scalar(select(FactVerification).where(FactVerification.entity_type == "lead"))
        ).status == "invalidated"
        assert await db.scalar(select(func.count()).select_from(FieldChange)) > 0
