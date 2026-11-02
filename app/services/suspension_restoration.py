"""Restore proven history and accepted source state after erroneous suspension."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.database import write_session
from app.models import (
    Document,
    ExtractionStatus,
    FieldChange,
    FieldState,
    Lead,
    LeadScoreRevision,
    ProcurementEvent,
    ReconciliationRun,
    SourceRecord,
)
from app.services.backups import BackupReceipt, file_hash
from app.services.event_detection.participation_values import VERSION
from app.services.maintenance import fence_maintenance, run_blocking_maintenance

RESTORATION_VERSION = "source-suspension-restoration-1"
SUSPENSION_REASONS = {
    "Fonte documental desativada; revisar os fatos antes de usar o lead.",
    "Resultado oficial cancelado; revisar a atribuição da empresa ao evento.",
}
SCORE_FIELDS = (
    "score",
    "fit_score",
    "urgency_score",
    "evidence_score",
    "legal_relevance_score",
    "contact_score",
    "economic_value_score",
    "scoring_version",
    "score_breakdown",
    "recommended_action",
)
IDENTITY_FIELDS = ("procurement_id", "company_id", "triggering_event_id")


def _read_snapshots(
    receipt: BackupReceipt, settings: Settings
) -> tuple[list[dict[str, Any]], dict[UUID, dict[str, Any]]]:
    if receipt.backend != "sqlite":
        raise ValueError("recuperação de notas requer um backup SQLite verificado")
    receipt.verify(settings.database_url)
    with closing(sqlite3.connect(receipt.path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        rows = [
            dict(row)
            for row in db.execute(
                "select id,procurement_id,company_id,triggering_event_id,score,fit_score,"
                "urgency_score,evidence_score,legal_relevance_score,contact_score,"
                "economic_value_score,scoring_version,score_breakdown,recommended_action "
                "from leads where score > 0 order by id"
            )
        ]
        documents = {
            UUID(row["id"]): dict(row)
            for row in db.execute(
                "select id,procurement_id,parent_document_id,original_url,is_active,"
                "file_hash,deactivated_at from documents"
            )
        }
    for row in rows:
        row["score_breakdown"] = json.loads(row["score_breakdown"] or "{}")
    return rows, documents


async def _document_proof(
    db: AsyncSession,
    document: Document,
    snapshots: dict[UUID, dict[str, Any]],
) -> tuple[Document, FieldState] | None:
    current = document
    visited = set()
    while True:
        if current.id in visited:
            return None
        visited.add(current.id)
        previous = snapshots.get(current.id)
        if not previous or not previous["is_active"] or not previous["file_hash"]:
            return None
        if (
            current.file_hash != previous["file_hash"]
            or current.original_url != previous["original_url"]
            or str(current.procurement_id) != previous["procurement_id"]
            or (str(current.parent_document_id) if current.parent_document_id else None)
            != previous["parent_document_id"]
        ):
            return None
        if current.parent_document_id is None:
            break
        parent = await db.get(Document, current.parent_document_id)
        if parent is None:
            return None
        current = parent
    if len(visited) > 1 and (
        current.mime_type not in {"application/zip", "application/x-zip-compressed"}
        or current.extraction_status is not ExtractionStatus.EXTRACTED
    ):
        return None
    state = await db.scalar(
        select(FieldState).where(
            FieldState.entity_type == "document",
            FieldState.entity_id == current.id,
            FieldState.field_name == "is_active",
        )
    )
    if state is None or state.value is not True or state.source_record_id is None:
        return None
    record = await db.get(SourceRecord, state.source_record_id)
    if record is None or record.collection_complete is not True or record.source != state.source:
        return None
    return current, state


def _matches_suspension(lead: Lead, snapshot: dict[str, Any]) -> bool:
    current = dict(lead.score_breakdown or {})
    reason = current.pop("source_state", None)
    return bool(
        reason in SUSPENSION_REASONS
        and lead.recommended_action == reason
        and current == snapshot["score_breakdown"]
        and all(str(getattr(lead, key)) == str(snapshot[key]) for key in IDENTITY_FIELDS)
        and lead.scoring_version == snapshot["scoring_version"]
        and all(
            getattr(lead, key) == 0
            for key in ("score", "fit_score", "urgency_score", "evidence_score")
        )
        and all(
            getattr(lead, key) == snapshot[key]
            for key in ("legal_relevance_score", "contact_score", "economic_value_score")
        )
    )


async def restore_suspended_state(
    sessions: async_sessionmaker[AsyncSession],
    settings: Settings,
    baseline_run_id: UUID,
    *,
    apply: bool = False,
    backup: BackupReceipt | None = None,
    maintenance_owner: str | None = None,
) -> dict[str, Any]:
    """Use only the immutable backup bound to a completed participation repair."""
    if apply:
        if backup is None or maintenance_owner is None:
            raise ValueError("recuperação exige backup atual e manutenção exclusiva")
        await run_blocking_maintenance(
            lambda: backup.verify(settings.database_url, maintenance_owner)
        )
    async with sessions() as db:
        baseline = await db.get(ReconciliationRun, baseline_run_id)
        if baseline is None or baseline.status != "completed" or baseline.version != VERSION:
            raise ValueError("execução de referência ausente, incompleta ou incompatível")
        receipts = baseline.backup.get("receipts") or []
        if not receipts:
            raise ValueError("execução de referência sem backup")
        receipt = BackupReceipt(**{**receipts[0], "path": Path(receipts[0]["path"])})
        cutoff = baseline.created_at
        pending = await db.scalar(
            select(ReconciliationRun)
            .where(
                ReconciliationRun.version == RESTORATION_VERSION,
                ReconciliationRun.status == "running",
                ReconciliationRun.backup["baseline_run_id"].as_string() == str(baseline_run_id),
            )
            .order_by(ReconciliationRun.created_at)
            .limit(1)
        )
    snapshots, documents = await run_blocking_maintenance(
        lambda: _read_snapshots(receipt, settings)
    )
    result: dict[str, Any] = {
        "applied": apply,
        "baseline_run_id": str(baseline_run_id),
        "baseline_sha256": receipt.sha256,
        "restorable": 0,
        "restored": 0,
        "skipped_later_evaluation": 0,
        "documents_restorable": 0,
        "documents_restored": 0,
        "document_diagnostics": [],
        "run_id": None,
        "pending_run_id": str(pending.id) if pending else None,
    }
    run_id = pending.id if apply and pending else None
    if run_id:
        assert backup is not None and pending is not None
        result["run_id"] = str(run_id)
        result["restored"] = int(pending.totals.get("restored", 0))
        result["documents_restored"] = int(pending.totals.get("documents_restored", 0))
        async with write_session(sessions) as db:
            db.info["maintenance_owner"] = maintenance_owner
            await fence_maintenance(db, maintenance_owner or "")
            resumed = await db.get_one(ReconciliationRun, run_id)
            resumed.backup = {
                **resumed.backup,
                "receipts": [
                    *(resumed.backup.get("receipts") or []),
                    backup.as_dict(),
                ],
            }

    async def ensure_run(db: AsyncSession) -> UUID:
        nonlocal run_id
        if run_id is None:
            assert backup is not None
            run = ReconciliationRun(
                version=RESTORATION_VERSION,
                backup={
                    "receipts": [backup.as_dict()],
                    "baseline_run_id": str(baseline_run_id),
                    "baseline_sha256": receipt.sha256,
                },
            )
            db.add(run)
            await db.flush()
            run_id = run.id
            result["run_id"] = str(run_id)
        return run_id

    for offset in range(0, len(snapshots), 100):
        page = {UUID(row["id"]): row for row in snapshots[offset : offset + 100]}
        context = write_session(sessions) if apply else sessions.begin()
        async with context as db:
            if apply:
                db.info["maintenance_owner"] = maintenance_owner
                await fence_maintenance(db, maintenance_owner or "")
            leads = list((await db.scalars(select(Lead).where(Lead.id.in_(page)))).all())
            later = set(
                (
                    await db.scalars(
                        select(LeadScoreRevision.lead_id).where(
                            LeadScoreRevision.lead_id.in_(page),
                            LeadScoreRevision.calculated_at >= cutoff,
                        )
                    )
                ).all()
            )
            for lead in leads:
                snapshot = page[lead.id]
                if not _matches_suspension(lead, snapshot):
                    continue
                if lead.id in later:
                    result["skipped_later_evaluation"] += 1
                    continue
                result["restorable"] += 1
                if not apply:
                    continue
                await ensure_run(db)
                previous = {key: getattr(lead, key) for key in SCORE_FIELDS}
                event = await db.get(ProcurementEvent, lead.triggering_event_id)
                for key in SCORE_FIELDS:
                    setattr(lead, key, snapshot[key])
                references = [
                    {
                        "baseline_run_id": str(baseline_run_id),
                        "backup_sha256": receipt.sha256,
                        "source_record_id": str(event.source_record_id)
                        if event and event.source_record_id
                        else None,
                        "evidence_id": str(event.evidence_id)
                        if event and event.evidence_id
                        else None,
                    }
                ]
                db.add(
                    LeadScoreRevision(
                        lead_id=lead.id,
                        scoring_version=lead.scoring_version,
                        previous_score=previous["score"],
                        previous_version=previous["scoring_version"],
                        score=lead.score,
                        components=dict(lead.score_breakdown or {}),
                        inputs={
                            "restore_historical_snapshot": True,
                            "baseline_run_id": str(baseline_run_id),
                        },
                        references=references,
                        origin="historical_restoration",
                    )
                )
                db.add(
                    FieldChange(
                        entity_type="lead",
                        entity_id=lead.id,
                        parent_type="procurement",
                        parent_id=lead.procurement_id,
                        field_name="historical_score_restoration",
                        previous_value=previous,
                        received_value={key: snapshot[key] for key in SCORE_FIELDS},
                        decision="restored_history",
                        applied=True,
                        source="verified_score_backup",
                        source_record_id=event.source_record_id if event else None,
                        evidence_id=event.evidence_id if event else None,
                    )
                )
                result["restored"] += 1
            if apply and run_id:
                await fence_maintenance(db, maintenance_owner or "")
                run = await db.get_one(ReconciliationRun, run_id)
                run.checkpoint = str(max(page))
                run.totals = dict(result)
    # Prove stored bytes outside the writer transaction. A reviewed event may
    # never withdraw unrelated facts in the same accepted official document.
    verified_hashes: dict[UUID, str] = {}
    identifiers = list(documents)
    for offset in range(0, len(identifiers), 100):
        page_ids = identifiers[offset : offset + 100]
        candidates = []
        async with sessions() as db:
            rows = list(
                (
                    await db.scalars(
                        select(Document).where(
                            Document.id.in_(page_ids),
                            Document.is_active.is_(False),
                        )
                    )
                ).all()
            )
            for row in rows:
                proof = await _document_proof(db, row, documents)
                if proof is None:
                    if documents[row.id]["is_active"]:
                        result["document_diagnostics"].append(
                            {
                                "document_id": str(row.id),
                                "reason": "ativação ou versão sem comprovação suficiente",
                            }
                        )
                    continue
                root, _ = proof
                if root.id not in verified_hashes and root.local_path:
                    selected = Path(root.local_path)
                    if selected.is_file():
                        verified_hashes[root.id] = await run_blocking_maintenance(
                            partial(file_hash, selected)
                        )
                if verified_hashes.get(root.id) == root.file_hash:
                    candidates.append(row.id)
                else:
                    result["document_diagnostics"].append(
                        {
                            "document_id": str(row.id),
                            "reason": "arquivo armazenado ou hash não confirmado",
                        }
                    )
        if not candidates:
            continue
        context = write_session(sessions) if apply else sessions.begin()
        async with context as db:
            if apply:
                db.info["maintenance_owner"] = maintenance_owner
                await fence_maintenance(db, maintenance_owner or "")
            for identifier in candidates:
                document = await db.get(Document, identifier)
                if document is None or document.is_active:
                    continue
                proof = await _document_proof(db, document, documents)
                if proof is None or verified_hashes.get(proof[0].id) != proof[0].file_hash:
                    continue
                result["documents_restorable"] += 1
                if not apply:
                    continue
                await ensure_run(db)
                document.is_active = True
                prior_deactivation = documents[identifier]["deactivated_at"]
                document.deactivated_at = (
                    datetime.fromisoformat(prior_deactivation).replace(tzinfo=UTC)
                    if prior_deactivation
                    else None
                )
                db.add(
                    FieldChange(
                        entity_type="document",
                        entity_id=document.id,
                        parent_type="procurement",
                        parent_id=document.procurement_id,
                        field_name="is_active",
                        previous_value=False,
                        received_value=True,
                        decision="reconciled_state",
                        applied=True,
                        source="accepted_source_state",
                        source_record_id=proof[1].source_record_id,
                    )
                )
                result["documents_restored"] += 1
            if apply and run_id:
                await fence_maintenance(db, maintenance_owner or "")
                run = await db.get_one(ReconciliationRun, run_id)
                run.checkpoint, run.totals = str(max(candidates)), dict(result)
    if apply and run_id:
        async with write_session(sessions) as db:
            db.info["maintenance_owner"] = maintenance_owner
            await fence_maintenance(db, maintenance_owner or "")
            run = await db.get_one(ReconciliationRun, run_id)
            run.status, run.finished_at, run.totals = "completed", datetime.now(UTC), dict(result)
    return result
