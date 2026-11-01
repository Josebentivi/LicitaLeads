"""Prospective processing and maintenance repair use the same retained evidence."""

import asyncio
import hashlib
import sqlite3
from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.models import (
    Company,
    Document,
    DocumentAlias,
    DocumentChunk,
    Evidence,
    FactVerification,
    FieldChange,
    Lead,
    LeadReview,
    Participant,
    ParticipantValue,
    Procurement,
    ProcurementEvent,
    ProcurementItem,
    ReconciliationRun,
    SourceRecord,
)
from app.models.enums import ExtractionStatus, LeadStatus, ReviewDecision
from app.services.backups import create_backup
from app.services.maintenance import exclusive_maintenance
from app.services.participation_integrity import lead_current
from app.services.participation_reconciliation import reconcile_participation

SUBJECT = "Licitante: SÃO JORGE DISTRIBUIDORA HOSPITALAR LTDA, CNPJ 10.258.066/0001-30.\n"
PROPOSALS = "Lote 04, estimado em R$ 697.628,88 e proposto R$ 480.823,20; Lote 05, estimado em R$ 1.782.456,00 e proposto R$ 1.024.545,60."
COST = "Item 8 (enalapril 20 mg, custo R$ 0,0876). Proposta emitida pela DPROSMED Produtos Médico-Hospitalares Ltda (CNPJ 11.449.180/0001-00), fornecedor de custo, preço unitário R$ 43,7948 por caixa."
URL = "https://pncp.gov.br:62150/pncp-api/v1/orgaos/01612325000198/compras/2026/58/arquivos/1"
PUBLIC_URL = URL.replace(":62150", "")


async def seed(database, tmp_path, *, reviewed=False, alias=False):
    root = tmp_path / "documents"
    root.mkdir(exist_ok=True)
    text = SUBJECT + PROPOSALS + "\n" + COST
    content = text.encode()
    path = root / "retained.txt"
    path.write_bytes(content)
    settings = Settings(
        _env_file=None,
        database_url=str(database.engine.url),
        document_storage_path=root,
        raw_data_storage_path=tmp_path / "raw",
        holiday_calendar_path=tmp_path / "holidays.csv",
        llm_enabled=False,
        contact_search_enabled=False,
        ocr_enabled=False,
    )
    with sqlite3.connect(database.engine.url.database) as connection:
        connection.execute(
            "create table if not exists alembic_version (version_num varchar(64) primary key)"
        )
        connection.execute(
            "insert or ignore into alembic_version values ('0026_participation_integrity')"
        )
    async with database.sessions() as db, db.begin():
        proc = Procurement(
            source="pncp",
            external_id="01612325000198-1-000058/2026",
            pncp_control_number="01612325000198-1-000058/2026",
            status="Em andamento",
        )
        good = Company(cnpj="10258066000130", legal_name="São Jorge", normalized_name="SAO JORGE")
        bad = Company(cnpj="11449180000100", legal_name="DPROSMED", normalized_name="DPROSMED")
        record = SourceRecord(
            source="pncp",
            endpoint="https://pncp.gov.br/api/pncp/v1/orgaos/01612325000198/compras/2026/58/arquivos",
            request_parameters={},
            raw_payload=[{"url": URL, "sequencialDocumento": 1, "statusAtivo": True}],
            response_hash="a" * 64,
            collection_complete=True,
        )
        db.add_all([proc, good, bad, record])
        await db.flush()
        doc = Document(
            procurement_id=proc.id,
            source_record_id=record.id,
            original_url=URL,
            resolved_url=URL.replace("pncp-api", "api/pncp").replace(":62150", ""),
            title="Decisão do pregoeiro",
            file_hash=hashlib.sha256(content).hexdigest(),
            local_path=str(path),
            mime_type="text/plain",
            extraction_status=ExtractionStatus.EXTRACTED,
            extracted_text=text,
            last_checked_at=datetime(2026, 10, 29, tzinfo=UTC),
        )
        item = ProcurementItem(
            procurement_id=proc.id,
            item_number="8",
            description="Cloridrato de ciprofloxacino, 500mg.",
        )
        db.add_all([doc, item])
        await db.flush()
        chunks = [
            DocumentChunk(
                document_id=doc.id, sequence=i, text=t, page_number=i + 1, locator=f"page:{i + 1}"
            )
            for i, t in enumerate([SUBJECT + PROPOSALS, COST])
        ]
        db.add_all(chunks)
        await db.flush()
        evidence = [
            Evidence(
                procurement_id=proc.id,
                document_id=doc.id,
                source_record_id=record.id,
                document_chunk_id=ch.id,
                page_number=i + 1,
                locator=ch.locator,
                text_excerpt=ch.text,
                source_url=URL,
            )
            for i, ch in enumerate(chunks)
        ]
        db.add_all(evidence)
        await db.flush()
        participants = [
            Participant(
                procurement_id=proc.id,
                company_id=good.id,
                source="document",
                source_evidence_id=evidence[0].id,
            ),
            Participant(
                procurement_id=proc.id,
                company_id=bad.id,
                item_id=item.id,
                source="document",
                source_evidence_id=evidence[1].id,
            ),
        ]
        event = ProcurementEvent(
            procurement_id=proc.id,
            company_id=bad.id,
            event_type="PARTICIPATION_DETECTED",
            raw_description=COST,
            source="document",
            source_url=URL,
            evidence_id=evidence[1].id,
            confidence=0.76,
        )
        db.add_all([*participants, event])
        await db.flush()
        lead = Lead(
            procurement_id=proc.id,
            company_id=bad.id,
            triggering_event_id=event.id,
            score=35,
            fit_score=35,
            urgency_score=0,
            evidence_score=25,
            legal_relevance_score=8,
            contact_score=2,
            economic_value_score=0,
            reason_summary="Participação anteriormente detectada",
            recommended_action="Revisar a fonte",
            scoring_version="1.0",
            lead_status=LeadStatus.APPROVED if reviewed else LeadStatus.NEW,
            assigned_to="Analista",
        )
        db.add(lead)
        await db.flush()
        if reviewed:
            db.add(
                LeadReview(
                    lead_id=lead.id,
                    decision=ReviewDecision.APPROVED,
                    reviewer="Revisor",
                    notes="Decisão preservada",
                )
            )
        if alias:
            new = SourceRecord(
                source="pncp",
                endpoint=record.endpoint,
                request_parameters={},
                raw_payload=[{"url": PUBLIC_URL, "sequencialDocumento": 1, "statusAtivo": True}],
                response_hash="b" * 64,
                collection_complete=True,
            )
            db.add(new)
            await db.flush()
            db.add(
                Document(
                    procurement_id=proc.id,
                    source_record_id=new.id,
                    original_url=PUBLIC_URL,
                    extraction_status=ExtractionStatus.PENDING,
                )
            )
        ids = {
            "procurement": proc.id,
            "document": doc.id,
            "good": participants[0].id,
            "bad": participants[1].id,
            "company": good.id,
            "bad_company": bad.id,
            "event": event.id,
            "lead": lead.id,
        }
    return settings, ids


async def apply_repair(database, settings, *, resume=None):
    async with exclusive_maintenance(database.sessions) as owner:
        receipt = await asyncio.to_thread(create_backup, settings, maintenance_owner=owner)
        return await reconcile_participation(
            database.sessions,
            settings,
            apply=True,
            backup=receipt,
            maintenance_owner=owner,
            resume_id=resume,
        )


async def test_retained_archive_member_verified_without_new_extraction(database, tmp_path):
    import io
    from zipfile import ZIP_DEFLATED, ZipFile

    from app.services.participation_reconciliation import retained_pages

    settings, ids = await seed(database, tmp_path)
    async with database.sessions() as db, db.begin():
        child = await db.get(Document, ids["document"])
        content = (SUBJECT + PROPOSALS + "\n" + COST).encode()
        stream = io.BytesIO()
        with ZipFile(stream, "w", ZIP_DEFLATED) as archive:
            archive.writestr("proposta.txt", content)
        body = stream.getvalue()
        path = settings.document_storage_path / "retained.zip"
        path.write_bytes(body)
        parent = Document(
            procurement_id=child.procurement_id,
            source_record_id=child.source_record_id,
            original_url=URL,
            local_path=str(path),
            mime_type="application/zip",
            file_hash=hashlib.sha256(body).hexdigest(),
        )
        db.add(parent)
        await db.flush()
        child.parent_document_id = parent.id
        child.title = "proposta.txt"
        child.local_path = None
        pages, error = await retained_pages(db, child, settings, {})
        assert pages and error is None
        child.file_hash = "f" * 64
        pages, error = await retained_pages(db, child, settings, {})
        assert not pages and "hash" in error


async def test_maintenance_heartbeat_queues_before_reading_or_writing(database, monkeypatch):
    from sqlalchemy import event

    from app.database import write_session
    from app.models import JobLease
    from app.services import maintenance

    normal_sleep = asyncio.sleep

    async def short_heartbeat(seconds):
        await normal_sleep(0.005 if seconds == 30 else seconds)

    monkeypatch.setattr(maintenance.asyncio, "sleep", short_heartbeat)
    statements = []

    def record(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(database.engine.sync_engine, "before_cursor_execute", record)
    try:
        async with exclusive_maintenance(database.sessions) as owner:
            async with write_session(database.sessions) as writer:
                writer.info["maintenance_owner"] = owner
                await maintenance.fence_maintenance(writer, owner)
                before = (await writer.get(JobLease, "maintenance:exclusive")).heartbeat_at
                statements.clear()
                await normal_sleep(0.04)
                assert not statements  # The heartbeat waits before opening its transaction.
            for _ in range(30):
                await normal_sleep(0.005)
                async with database.sessions() as reader:
                    after = (await reader.get(JobLease, "maintenance:exclusive")).heartbeat_at
                if after > before:
                    break
            assert after > before
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", record)
    async with database.sessions() as reader:
        assert await reader.get(JobLease, "maintenance:exclusive") is None


@pytest.mark.parametrize("reviewed", [False, True])
async def test_repair_values_references_history_and_repetition(database, tmp_path, reviewed):
    settings, ids = await seed(database, tmp_path, reviewed=reviewed, alias=True)
    impact = await reconcile_participation(database.sessions, settings)
    assert impact["result"]["participants_invalidated"] == 1
    assert impact["result"]["unsupported_item_links"] == 1
    async with database.sessions() as db:
        assert await db.scalar(select(func.count()).select_from(FactVerification)) == 0
        assert await db.scalar(select(func.count()).select_from(ParticipantValue)) == 0
    result = await apply_repair(database, settings)
    assert result["result"]["monetary_values_created"] == 2
    async with database.sessions() as db:
        values = list(
            (await db.scalars(select(ParticipantValue).order_by(ParticipantValue.lot_number))).all()
        )
        assert [(v.lot_number, str(v.amount)) for v in values] == [
            ("4", "480823.2000"),
            ("5", "1024545.6000"),
        ]
        assert all(v.official_item_id is None and v.kind == "proposal" for v in values)
        assert all(v.observed_at.year == 2026 and v.observed_at.day == 29 for v in values)
        bad = await db.get(Participant, ids["bad"])
        assert bad.item_id is None
        lead = await db.get(Lead, ids["lead"])
        assert lead.score == 35 and lead.scoring_version == "1.0" and lead.assigned_to == "Analista"
        assert lead.lead_status == (LeadStatus.APPROVED if reviewed else LeadStatus.PENDING_REVIEW)
        assert await db.scalar(select(Lead.id).where(Lead.id == lead.id, lead_current())) is None
        assert await db.scalar(select(func.count()).select_from(LeadReview)) == int(reviewed)
        changes = await db.scalar(select(func.count()).select_from(FieldChange))
        assert await db.scalar(select(func.count()).select_from(DocumentAlias)) == 2
    repeated = await apply_repair(database, settings)
    assert repeated["result"]["monetary_values_created"] == 0
    async with database.sessions() as db:
        assert await db.scalar(select(func.count()).select_from(FieldChange)) == changes
        assert await db.scalar(select(func.count()).select_from(ParticipantValue)) == 2
    resumed = await apply_repair(database, settings, resume=result["run_id"])
    assert resumed["already_completed"] is True


async def test_repair_public_views_api_economic_reference_and_outreach_guard(
    api_client, database, tmp_path
):
    from app.services.lead_scoring.evaluation import economic_reference

    settings, ids = await seed(database, tmp_path)
    await apply_repair(database, settings)
    endpoint = f"/api/procurements/{ids['procurement']}/participants"
    current = (await api_client.get(endpoint)).json()
    historical = (await api_client.get(endpoint + "?include_history=true")).json()
    assert len(current) == 1 and len(historical) == 2
    assert current[0]["proposal_value"] is None and current[0]["final_value"] is None
    assert len(current[0]["monetary_values"]) == 2
    page = await api_client.get(f"/procurements/{ids['procurement']}")
    assert (
        page.status_code == 200 and "R$ 480.823,20" in page.text and "R$ 1.024.545,60" in page.text
    )
    assert "DPROSMED" not in page.text
    history_page = await api_client.get(f"/procurements/{ids['procurement']}?include_history=true")
    assert "DPROSMED" in history_page.text and "históricas ou invalidadas" in history_page.text
    blocked = await api_client.post(f"/api/leads/{ids['lead']}/generate-outreach", json={})
    assert blocked.status_code == 409
    detail = await api_client.get(f"/leads/{ids['lead']}")
    assert "Gerar rascunho" not in detail.text and "histórico" in detail.text
    async with database.sessions() as db:
        company = await db.get(Company, ids["company"])
        procurement = await db.get(Procurement, ids["procurement"])
        participant = await db.get(Participant, ids["good"])
        event = ProcurementEvent(
            procurement_id=procurement.id,
            company_id=company.id,
            source="document",
            event_type="PROPOSAL_ACCEPTED",
            raw_description="Propostas da licitante",
            evidence_id=participant.source_evidence_id,
        )
        amount, refs = await economic_reference(db, company, procurement, event)
        assert str(amount) == "1024545.6000" and any(
            r["entity_type"] == "participant_value" for r in refs
        )


async def test_missing_raw_material_and_corrupt_hash_are_reported(database, tmp_path):
    settings, ids = await seed(database, tmp_path)
    (settings.document_storage_path / "retained.txt").write_text("conteúdo alterado")
    impact = await reconcile_participation(database.sessions, settings)
    assert impact["result"]["documents_requiring_review"] == 1
    assert impact["result"].get("eligible_value_observations", 0) == 0
    assert any("hash" in d for d in impact["result"]["diagnostics"])
    with pytest.raises(ValueError, match="backup"):
        await reconcile_participation(database.sessions, settings, apply=True)


async def test_cancelled_repair_has_checkpoint_and_can_resume(database, tmp_path, monkeypatch):
    from app.services import participation_reconciliation as module

    settings, ids = await seed(database, tmp_path)
    original = module.inspect_procurement

    async def interrupted(*args, **kwargs):
        await original(*args, **kwargs)
        if kwargs["apply"]:
            raise asyncio.CancelledError

    monkeypatch.setattr(module, "inspect_procurement", interrupted)
    with pytest.raises(asyncio.CancelledError):
        await apply_repair(database, settings)
    async with database.sessions() as db:
        run = await db.scalar(select(ReconciliationRun))
        assert run.status == "running" and run.checkpoint is None
        assert await db.scalar(select(func.count()).select_from(ParticipantValue)) == 0
        run_id = run.id
    monkeypatch.setattr(module, "inspect_procurement", original)
    result = await apply_repair(database, settings, resume=run_id)
    assert result["result"]["monetary_values_created"] == 2
