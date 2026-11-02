"""Recover exact historical values without changing facts or human decisions."""

import asyncio
from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import func, select

from app.models import (
    Document,
    FactVerification,
    FieldChange,
    FieldState,
    Lead,
    LeadScoreRevision,
    ReconciliationRun,
)
from app.models.enums import LeadStatus
from app.services.backups import create_backup
from app.services.maintenance import exclusive_maintenance
from app.services.suspension_restoration import restore_suspended_state

from .test_participation_integrity import apply_repair, seed

REASON = "Fonte documental desativada; revisar os fatos antes de usar o lead."


async def suspended(database, tmp_path):
    settings, ids = await seed(database, tmp_path, reviewed=True)
    repaired = await apply_repair(database, settings)
    async with database.sessions() as db, db.begin():
        lead = await db.get(Lead, ids["lead"])
        lead.score = lead.fit_score = lead.evidence_score = lead.urgency_score = 0
        lead.recommended_action = REASON
        lead.score_breakdown = {**(lead.score_breakdown or {}), "source_state": REASON}
    return settings, ids, UUID(repaired["run_id"])


async def test_restore_exact_score_keeps_review_block_and_is_idempotent(database, tmp_path):
    settings, ids, baseline = await suspended(database, tmp_path)
    assert (await restore_suspended_state(database.sessions, settings, baseline))["restorable"] == 1
    async with exclusive_maintenance(database.sessions) as owner:
        backup = await asyncio.to_thread(create_backup, settings, maintenance_owner=owner)
        restored = await restore_suspended_state(
            database.sessions,
            settings,
            baseline,
            apply=True,
            backup=backup,
            maintenance_owner=owner,
        )
    assert restored["restored"] == 1
    async with database.sessions() as db:
        lead = await db.get(Lead, ids["lead"])
        assert lead.score == lead.fit_score == 35 and lead.evidence_score == 25
        assert lead.scoring_version == "1.0" and lead.score_breakdown == {}
        assert lead.recommended_action == "Revisar a fonte"
        assert lead.assigned_to == "Analista" and lead.lead_status is LeadStatus.APPROVED
        assert (
            await db.scalar(
                select(FactVerification.status).where(
                    FactVerification.entity_type == "lead", FactVerification.entity_id == lead.id
                )
            )
            == "invalidated"
        )
        revision = await db.scalar(select(LeadScoreRevision))
        assert revision.origin == "historical_restoration"
        assert revision.previous_score == 0 and revision.score == 35
        assert revision.inputs["restore_historical_snapshot"]
        assert (
            await db.scalar(
                select(func.count())
                .select_from(FieldChange)
                .where(FieldChange.field_name == "historical_score_restoration")
            )
            == 1
        )
        run = await db.get(ReconciliationRun, UUID(restored["run_id"]))
        assert run.status == "completed" and run.backup["baseline_run_id"] == str(baseline)
    assert (await restore_suspended_state(database.sessions, settings, baseline))["restorable"] == 0


@pytest.mark.parametrize("changed", ["later_evaluation", "breakdown", "identity"])
async def test_restore_skips_later_or_ambiguous_states(database, tmp_path, changed):
    settings, ids, baseline = await suspended(database, tmp_path)
    async with database.sessions() as db, db.begin():
        lead = await db.get(Lead, ids["lead"])
        if changed == "later_evaluation":
            db.add(
                LeadScoreRevision(
                    lead_id=lead.id,
                    scoring_version="1.0",
                    score=0,
                    components={},
                    inputs={},
                    references=[],
                    origin="manual",
                )
            )
        elif changed == "breakdown":
            lead.score_breakdown = {**lead.score_breakdown, "operator_change": 1}
        else:
            lead.company_id = ids["company"]
    assert (await restore_suspended_state(database.sessions, settings, baseline))["restorable"] == 0


async def test_restore_requires_verified_backups_and_exclusive_maintenance(database, tmp_path):
    settings, _, baseline = await suspended(database, tmp_path)
    with pytest.raises(ValueError, match="backup atual"):
        await restore_suspended_state(database.sessions, settings, baseline, apply=True)
    async with database.sessions() as db:
        run = await db.get(ReconciliationRun, baseline)
        path = run.backup["receipts"][0]["path"]
    from pathlib import Path

    selected = Path(path)
    await asyncio.to_thread(selected.write_bytes, selected.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="backup ausente, alterado"):
        await restore_suspended_state(database.sessions, settings, baseline)


@pytest.mark.parametrize("protection", ["accepted", "withdrawn", "hash_changed", "file_missing"])
async def test_restore_document_requires_accepted_state_and_unchanged_stored_bytes(
    database, tmp_path, protection
):
    settings, ids, baseline = await suspended(database, tmp_path)
    async with database.sessions() as db, db.begin():
        document = await db.get(Document, ids["document"])
        document.is_active = False
        document.deactivated_at = datetime.now(UTC)
        db.add(
            FieldState(
                entity_type="document",
                entity_id=document.id,
                field_name="is_active",
                value=protection != "withdrawn",
                source="pncp",
                source_record_id=document.source_record_id,
                observed_at=datetime.now(UTC),
            )
        )
        if protection == "hash_changed":
            document.file_hash = "f" * 64
        if protection == "file_missing":
            from pathlib import Path

            await asyncio.to_thread(Path(document.local_path).unlink)
    dry = await restore_suspended_state(database.sessions, settings, baseline)
    assert dry["documents_restorable"] == int(protection == "accepted")
    async with exclusive_maintenance(database.sessions) as owner:
        backup = await asyncio.to_thread(create_backup, settings, maintenance_owner=owner)
        result = await restore_suspended_state(
            database.sessions,
            settings,
            baseline,
            apply=True,
            backup=backup,
            maintenance_owner=owner,
        )
    assert result["documents_restored"] == int(protection == "accepted")
    async with database.sessions() as db:
        document = await db.get(Document, ids["document"])
        assert document.is_active == (protection == "accepted")
        assert (document.deactivated_at is None) == (protection == "accepted")


async def test_restore_resumes_committed_history_after_lost_completion(
    database, tmp_path, monkeypatch
):
    from app.services import suspension_restoration as service

    settings, _, baseline = await suspended(database, tmp_path)
    original = service.fence_maintenance
    calls = 0

    async def interrupted(db, owner):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise RuntimeError("stopped before completion")
        await original(db, owner)

    async with exclusive_maintenance(database.sessions) as owner:
        backup = await asyncio.to_thread(create_backup, settings, maintenance_owner=owner)
        monkeypatch.setattr(service, "fence_maintenance", interrupted)
        with pytest.raises(RuntimeError, match="before completion"):
            await restore_suspended_state(
                database.sessions,
                settings,
                baseline,
                apply=True,
                backup=backup,
                maintenance_owner=owner,
            )
        monkeypatch.setattr(service, "fence_maintenance", original)
        dry = await restore_suspended_state(database.sessions, settings, baseline)
        assert dry["restorable"] == 0 and dry["pending_run_id"]
        resumed = await restore_suspended_state(
            database.sessions,
            settings,
            baseline,
            apply=True,
            backup=backup,
            maintenance_owner=owner,
        )
    assert resumed["run_id"] == dry["pending_run_id"] and resumed["restored"] == 1
    async with database.sessions() as db:
        assert await db.scalar(select(func.count()).select_from(LeadScoreRevision)) == 1
        assert (await db.get(ReconciliationRun, UUID(resumed["run_id"]))).status == "completed"


async def test_restore_archive_member_requires_complete_unchanged_parent(database, tmp_path):
    import hashlib
    import io
    from zipfile import ZipFile

    from app.models import ExtractionStatus

    settings, ids = await seed(database, tmp_path)
    stream = io.BytesIO()
    content = b"Documento oficial armazenado"
    with ZipFile(stream, "w") as archive:
        archive.writestr("proposta.txt", content)
    body = stream.getvalue()
    path = tmp_path / "parent.zip"
    await asyncio.to_thread(path.write_bytes, body)
    async with database.sessions() as db, db.begin():
        parent = await db.get(Document, ids["document"])
        parent.file_hash = hashlib.sha256(body).hexdigest()
        parent.local_path = str(path)
        parent.mime_type = "application/zip"
        parent.extraction_status = ExtractionStatus.EXTRACTED
        child = Document(
            procurement_id=parent.procurement_id,
            parent_document_id=parent.id,
            title="proposta.txt",
            original_url=parent.original_url + "#member=proposta.txt",
            file_hash=hashlib.sha256(content).hexdigest(),
            mime_type="text/plain",
            extraction_status=ExtractionStatus.EXTRACTED,
            extracted_text=content.decode(),
        )
        db.add(child)
        await db.flush()
        child_id = child.id
    baseline = UUID((await apply_repair(database, settings))["run_id"])
    async with database.sessions() as db, db.begin():
        parent = await db.get(Document, ids["document"])
        child = await db.get(Document, child_id)
        parent.is_active = child.is_active = False
        db.add(
            FieldState(
                entity_type="document",
                entity_id=parent.id,
                field_name="is_active",
                value=True,
                source="pncp",
                source_record_id=parent.source_record_id,
                observed_at=datetime.now(UTC),
            )
        )
    assert (await restore_suspended_state(database.sessions, settings, baseline))[
        "documents_restorable"
    ] == 2
    async with database.sessions() as db, db.begin():
        parent = await db.get(Document, ids["document"])
        parent.extraction_status = ExtractionStatus.FAILED
    assert (await restore_suspended_state(database.sessions, settings, baseline))[
        "documents_restorable"
    ] == 1
