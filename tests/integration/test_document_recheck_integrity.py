"""Rechecks preserve accepted versions and reconcile only verified ZIP membership."""

from __future__ import annotations

import asyncio
import hashlib
import io
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from sqlalchemy import inspect, select

from app.config import Settings
from app.models import (
    Deadline,
    DeadlineStatus,
    Document,
    DocumentChunk,
    Evidence,
    ExtractionStatus,
    FieldChange,
    FieldState,
    Lead,
    LeadReview,
    LeadStatus,
    Procurement,
    ProcurementEvent,
    ProcurementPhase,
    ReviewDecision,
)
from app.services.documents import DocumentExtractor
from app.services.documents.security import ArchiveLimits
from app.services.documents.types import DocumentExtraction, DocumentExtractionStatus
from app.services.ingestion import processor
from app.services.ingestion.documents import DocumentDownloadError, DownloadedDocument
from app.services.ingestion.pipeline import IngestionPipeline
from app.services.ingestion.processor import DocumentProcessingService

from .conftest import DatabaseContext
from .test_document_processing import FakeDownloader, _pending_document, _settings
from .test_document_update_ordering import _observe, _seed_derivatives


def _event_text(item: int) -> str:
    return (
        f"EMPRESA EXEMPLO LTDA., CNPJ 00.000.000/0001-91, licitante do item {item}, "
        "foi inabilitada por não apresentou a certidão exigida no item 8.4. "
        "Prazo até 20/09/2026 às 18:00."
    )


def _zip(members: dict[str, str]) -> bytes:
    content = io.BytesIO()
    with ZipFile(content, "w", ZIP_DEFLATED) as archive:
        for name, text in members.items():
            archive.writestr(name, text)
    return content.getvalue()


@dataclass
class _PausedDownloader(FakeDownloader):
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)

    async def download(self, url: str) -> DownloadedDocument:
        self.entered.set()
        await self.release.wait()
        return await super().download(url)


@pytest.fixture
def thread_extraction(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise real extraction without subprocess startup in version-ordering races."""

    async def extract(
        input_path: Path,
        *,
        filename: str,
        mime_type: str | None,
        settings: Settings,
        archive_limits: ArchiveLimits,
    ) -> DocumentExtraction:
        def run() -> DocumentExtraction:
            return DocumentExtractor(archive_limits=archive_limits).extract(
                input_path.read_bytes(), filename=filename, mime_type=mime_type
            )

        return await asyncio.to_thread(run)

    monkeypatch.setattr(processor, "extract_in_process", extract)


async def _snapshot(database: DatabaseContext, *, exclude_parent: UUID | None = None) -> dict:
    """Include accepted metadata and historical facts, independent of ORM identity maps."""
    result = {}
    async with database.sessions() as session:
        for model in (
            Document,
            DocumentChunk,
            Evidence,
            ProcurementEvent,
            Deadline,
            Lead,
            LeadReview,
            Procurement,
            FieldChange,
            FieldState,
        ):
            rows = (await session.scalars(select(model))).all()
            result[model.__name__] = {
                row.id: {
                    column.key: deepcopy(getattr(row, column.key))
                    for column in inspect(model).column_attrs
                }
                for row in rows
                if not (
                    (model is Document and row.id == exclude_parent)
                    or (model is DocumentChunk and row.document_id == exclude_parent)
                )
            }
    return result


async def _requeue(database: DatabaseContext, document_id: UUID) -> None:
    async with database.sessions() as session, session.begin():
        document = await session.get_one(Document, document_id)
        document.extraction_status = ExtractionStatus.PENDING


async def _run_update(
    service: DocumentProcessingService, document_id: UUID, entry_point: str
) -> None:
    if entry_point == "pending":
        summary = await service.process_pending(document_ids=(document_id,))
        assert summary.documents_processed == 1 and summary.documents_failed == 0
    else:
        await service.recheck_document(document_id)


@pytest.mark.parametrize("stage", ["download", "extraction"])
async def test_late_pending_failure_preserves_newly_rechecked_content_and_facts(
    database: DatabaseContext,
    tmp_path: Path,
    thread_extraction: None,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    document_id = await _pending_document(database)
    entered, release = asyncio.Event(), asyncio.Event()

    class LateFailure(FakeDownloader):
        async def download(self, url):
            entered.set()
            await release.wait()
            raise DocumentDownloadError("Falha temporária da consulta anterior")

    async def late_extraction(*args, **kwargs):
        from app.services.documents.errors import ExtractionFailure

        entered.set()
        await release.wait()
        raise ExtractionFailure("archive_members_limit")

    stale = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=LateFailure(b"Versao anterior")
        if stage == "download"
        else FakeDownloader(b"Versao anterior"),
    )
    fresh = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(_event_text(9).encode()),
    )
    if stage == "extraction":
        monkeypatch.setattr(stale, "_extract_download", late_extraction)
    task = asyncio.create_task(stale.process_pending(document_ids=(document_id,)))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        await fresh.recheck_document(document_id)
        accepted = await _snapshot(database)
        assert accepted["Document"][document_id]["extraction_status"] is ExtractionStatus.EXTRACTED
        assert accepted["Document"][document_id]["extraction_attempts"] == 0
        assert len(accepted["ProcurementEvent"]) == len(accepted["Lead"]) == 1
        release.set()
        summary = await asyncio.wait_for(task, 15)
        assert summary.documents_failed == summary.documents_processed == 0
        assert summary.documents_skipped == 1
        assert await _snapshot(database) == accepted
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await stale.aclose()
        await fresh.aclose()


@pytest.mark.parametrize("mutation", ["none", "url", "activity-cycle", "inactive"])
async def test_pending_failure_only_updates_the_attempted_version(
    database: DatabaseContext, tmp_path: Path, mutation: str
) -> None:
    document_id = await _pending_document(database)
    entered, release = asyncio.Event(), asyncio.Event()

    class LateFailure(FakeDownloader):
        async def download(self, url):
            entered.set()
            await release.wait()
            raise DocumentDownloadError("Falha temporária preservada")

    service = DocumentProcessingService(
        _settings(tmp_path), session_factory=database.sessions, downloader=LateFailure(b"conteudo")
    )
    task = asyncio.create_task(service.process_pending(document_ids=(document_id,)))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        if mutation in {"url", "inactive"}:
            async with database.sessions() as session, session.begin():
                document = await session.get_one(Document, document_id)
                if mutation == "url":
                    document.original_url = "https://official.test/outra-versao.txt"
                else:
                    document.is_active = False
        elif mutation == "activity-cycle":
            start = datetime.now(UTC)
            async with database.sessions() as session, session.begin():
                for offset, active in enumerate([False, True]):
                    session.add(
                        FieldChange(
                            entity_type="document",
                            entity_id=document_id,
                            parent_type="procurement",
                            parent_id=(await session.get_one(Document, document_id)).procurement_id,
                            field_name="is_active",
                            previous_value=not active,
                            received_value=active,
                            applied=True,
                            decision="updated",
                            source="pncp",
                            observed_at=start + timedelta(seconds=offset),
                        )
                    )
        before = await _snapshot(database)
        release.set()
        summary = await asyncio.wait_for(task, 15)
        if mutation == "none":
            assert summary.documents_failed == 1 and summary.documents_skipped == 0
            async with database.sessions() as session:
                document = await session.get_one(Document, document_id)
                assert document.extraction_status is ExtractionStatus.FAILED
                assert document.extraction_attempts == 1
                assert document.next_attempt_at > datetime.now(UTC)
                assert "Falha temporária preservada" in document.extraction_error
        else:
            assert summary.documents_failed == 0 and summary.documents_skipped == 1
            assert await _snapshot(database) == before
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await service.aclose()


@pytest.mark.parametrize("unchanged_response", [False, True], ids=["changed-hash", "same-hash"])
async def test_late_recheck_cannot_overwrite_newly_processed_version(
    database: DatabaseContext, tmp_path: Path, unchanged_response: bool, thread_extraction: None
) -> None:
    document_id = await _pending_document(database)
    initial = b"Versao inicial sem evento."
    initial_service = DocumentProcessingService(
        _settings(tmp_path), session_factory=database.sessions, downloader=FakeDownloader(initial)
    )
    assert (await initial_service.process_pending()).documents_processed == 1
    downloader = _PausedDownloader(initial if unchanged_response else _event_text(1).encode())
    stale_service = DocumentProcessingService(
        _settings(tmp_path), session_factory=database.sessions, downloader=downloader
    )
    stale = asyncio.create_task(stale_service.recheck_document(document_id))
    try:
        await asyncio.wait_for(downloader.entered.wait(), timeout=10)
        await _requeue(database, document_id)
        fresh_content = _event_text(9).encode()
        fresh_service = DocumentProcessingService(
            _settings(tmp_path),
            session_factory=database.sessions,
            downloader=FakeDownloader(fresh_content),
        )
        summary = await fresh_service.process_pending(document_ids=(document_id,))
        assert summary.events_created == summary.leads_created == 1
        before = await _snapshot(database)
        accepted = before["Document"][document_id]
        assert accepted["file_hash"] == hashlib.sha256(fresh_content).hexdigest()
        assert accepted["extracted_text"] == _event_text(9)
        assert len(before["DocumentChunk"]) == 1
        downloader.release.set()
        result = await asyncio.wait_for(stale, timeout=30)
        assert result.events_created == result.leads_created == 0
        assert await _snapshot(database) == before
    finally:
        stale.cancel()
        await asyncio.gather(stale, return_exceptions=True)
        await stale_service.aclose()


async def test_failure_snapshot_prevents_duplicate_attempt_and_still_checks_cancellation(
    database: DatabaseContext, tmp_path: Path
) -> None:
    from app.services.ingestion.pipeline import PipelineCancelled

    document_id = await _pending_document(database)
    service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(b"conteudo"),
    )
    try:
        async with database.sessions() as session, session.begin():
            document = await session.get_one(Document, document_id)
            document.extraction_status = ExtractionStatus.FAILED
            document.extraction_attempts = 1
            version = await service._document_version(session, document)
        failure = DocumentDownloadError("mesma tentativa")
        assert await service._mark_failed(document_id, failure, version=version)
        after_first = await _snapshot(database)
        assert after_first["Document"][document_id]["extraction_attempts"] == 2
        assert not await service._mark_failed(document_id, failure, version=version)
        assert await _snapshot(database) == after_first

        async def cancelled(session):
            raise PipelineCancelled("Cancelamento solicitado")

        with pytest.raises(PipelineCancelled):
            await service._mark_failed(document_id, failure, version=version, checkpoint=cancelled)
        assert await _snapshot(database) == after_first
    finally:
        await service.aclose()


@pytest.mark.parametrize("mutation", ["url", "activity-cycle"])
@pytest.mark.parametrize("unchanged_response", [False, True], ids=["changed-hash", "same-hash"])
async def test_recheck_revalidates_url_and_accepted_activity_revision(
    database: DatabaseContext,
    tmp_path: Path,
    mutation: str,
    unchanged_response: bool,
    thread_extraction: None,
) -> None:
    settings = _settings(tmp_path)
    pipeline = IngestionPipeline(settings, session_factory=database.sessions)
    start = datetime(2026, 10, 20, tzinfo=UTC)
    await _observe(database, pipeline, active=True, stamp=start, observed=start)
    await _seed_derivatives(database)
    original = b"Conteudo anterior."
    async with database.sessions() as session, session.begin():
        document = await session.scalar(
            select(Document).where(Document.parent_document_id.is_(None))
        )
        document_id = document.id
        document.file_hash = hashlib.sha256(original).hexdigest()
        document.mime_type = "text/plain"
        document.extracted_text = original.decode()
        document.downloaded_at = document.last_checked_at = start
        # A requeued document can be rechecked while awaiting normal processing.
        assert document.extraction_status is ExtractionStatus.PENDING

    downloader = _PausedDownloader(original if unchanged_response else b"Conteudo obsoleto novo.")
    service = DocumentProcessingService(
        settings, session_factory=database.sessions, downloader=downloader
    )
    operation = asyncio.create_task(service.recheck_document(document_id))
    try:
        await asyncio.wait_for(downloader.entered.wait(), timeout=10)
        if mutation == "url":
            async with database.sessions() as session, session.begin():
                document = await session.get_one(Document, document_id)
                document.original_url = "https://official.test/ata-nova.txt"
        else:
            for offset, active in [(1, False), (2, True)]:
                stamp = start + timedelta(days=offset)
                await _observe(database, pipeline, active=active, stamp=stamp, observed=stamp)
            async with database.sessions() as session:
                document = await session.get_one(Document, document_id)
                assert document.is_active and document.deactivated_at is None
                assert document.file_hash == hashlib.sha256(original).hexdigest()
                assert document.extraction_status is ExtractionStatus.PENDING
                assert document.downloaded_at == document.last_checked_at == start
        before = await _snapshot(database)
        downloader.release.set()
        await asyncio.wait_for(operation, timeout=30)
        assert await _snapshot(database) == before
    finally:
        operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)
        await service.aclose()


async def _seed_archive(database: DatabaseContext, tmp_path: Path) -> tuple[UUID, dict[str, UUID]]:
    document_id = await _pending_document(database, url="https://pncp.gov.br/atas.zip")
    service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(
            _zip(
                {
                    "keep.txt": _event_text(3),
                    "replace.txt": _event_text(4),
                    "remove.txt": _event_text(5),
                }
            ),
            filename="atas.zip",
            mime="application/zip",
        ),
    )
    summary = await service.process_pending()
    assert summary.events_created == summary.leads_created == 3
    async with database.sessions() as session, session.begin():
        children = {
            child.title: child.id
            for child in await session.scalars(
                select(Document).where(Document.parent_document_id == document_id)
            )
        }
        for title, identifier in children.items():
            event = await session.scalar(
                select(ProcurementEvent)
                .join(Evidence, ProcurementEvent.evidence_id == Evidence.id)
                .where(Evidence.document_id == identifier)
            )
            assert event is not None
            event.requires_manual_review = False
            deadline = await session.scalar(select(Deadline).where(Deadline.event_id == event.id))
            assert deadline is not None
            deadline.status = DeadlineStatus.OPEN
            deadline.remaining_seconds = 3600
            deadline.requires_manual_review = False
            lead = await session.scalar(select(Lead).where(Lead.triggering_event_id == event.id))
            assert lead is not None
            lead.score = lead.fit_score = 60
            lead.urgency_score = lead.evidence_score = 20
            lead.lead_status = {
                "keep.txt": LeadStatus.NEW,
                "replace.txt": LeadStatus.REJECTED,
                "remove.txt": LeadStatus.ARCHIVED,
            }[title]
            if title != "keep.txt":
                session.add(
                    LeadReview(
                        lead_id=lead.id,
                        decision=ReviewDecision.REJECTED,
                        notes=f"Decisão humana sobre {title}",
                        reviewer="Operadora",
                        previous_status=LeadStatus.NEW.value,
                    )
                )
    return document_id, children


def _zip_service(
    database: DatabaseContext, tmp_path: Path, content: bytes
) -> DocumentProcessingService:
    return DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(content, filename="atas.zip", mime="application/zip"),
    )


async def _assert_suspended(
    database: DatabaseContext, document_ids: set[UUID], before: dict
) -> None:
    after = await _snapshot(database)
    assert after["LeadReview"] == before["LeadReview"]
    event_ids = {
        event_id
        for event_id, event in before["ProcurementEvent"].items()
        if before["Evidence"][event["evidence_id"]]["document_id"] in document_ids
    }
    assert len(event_ids) == len(document_ids)
    for document_id in document_ids:
        assert not after["Document"][document_id]["is_active"]
        assert after["Document"][document_id]["deactivated_at"] is not None
    for evidence_id, evidence in before["Evidence"].items():
        if evidence["document_id"] in document_ids:
            assert after["Evidence"][evidence_id] == evidence
    for chunk_id, chunk in before["DocumentChunk"].items():
        if chunk["document_id"] in document_ids:
            assert after["DocumentChunk"][chunk_id] == chunk
    for event_id in event_ids:
        assert after["ProcurementEvent"][event_id]["requires_manual_review"]
    for deadline in after["Deadline"].values():
        if deadline["event_id"] in event_ids:
            assert deadline["status"] is DeadlineStatus.REQUIRES_REVIEW
            assert deadline["requires_manual_review"]
            assert deadline["remaining_seconds"] is None
    for lead_id, lead in after["Lead"].items():
        if lead["triggering_event_id"] in event_ids:
            previous = before["Lead"][lead_id]["lead_status"]
            expected = (
                previous
                if previous
                in {
                    LeadStatus.APPROVED,
                    LeadStatus.NEEDS_CHANGES,
                    LeadStatus.REJECTED,
                    LeadStatus.ARCHIVED,
                }
                else LeadStatus.PENDING_REVIEW
            )
            assert lead["lead_status"] is expected
            assert lead["score"] == before["Lead"][lead_id]["score"]
            assert lead["fit_score"] == before["Lead"][lead_id]["fit_score"]
            for field in (
                "urgency_score",
                "evidence_score",
                "recommended_action",
                "score_breakdown",
            ):
                assert lead[field] == before["Lead"][lead_id][field]


@pytest.mark.parametrize("entry_point", ["pending", "recheck"])
async def test_zip_update_retires_removed_and_replaced_members_only(
    database: DatabaseContext, tmp_path: Path, entry_point: str
) -> None:
    parent_id, previous = await _seed_archive(database, tmp_path)
    before = await _snapshot(database)
    if entry_point == "pending":
        await _requeue(database, parent_id)
    service = _zip_service(
        database, tmp_path, _zip({"keep.txt": _event_text(3), "replace.txt": _event_text(6)})
    )
    try:
        await _run_update(service, parent_id, entry_point)
    finally:
        await service.aclose()
    after = await _snapshot(database)
    active_children = {
        document_id: document
        for document_id, document in after["Document"].items()
        if document["parent_document_id"] == parent_id and document["is_active"]
    }
    assert {document["title"] for document in active_children.values()} == {
        "keep.txt",
        "replace.txt",
    }
    assert previous["keep.txt"] in active_children
    replacement_id = next(
        key for key, doc in active_children.items() if doc["title"] == "replace.txt"
    )
    assert replacement_id != previous["replace.txt"]
    assert active_children[replacement_id]["extracted_text"] == _event_text(6)
    await _assert_suspended(database, {previous["remove.txt"], previous["replace.txt"]}, before)
    keep_evidence = {
        key
        for key, evidence in before["Evidence"].items()
        if evidence["document_id"] == previous["keep.txt"]
    }
    for event in after["ProcurementEvent"].values():
        if event["id"] in before["ProcurementEvent"] and event["evidence_id"] in keep_evidence:
            assert "source_state" not in next(
                lead["score_breakdown"]
                for lead in after["Lead"].values()
                if lead["triggering_event_id"] == event["id"]
            )


@pytest.mark.parametrize("entry_point", ["pending", "recheck"])
async def test_complete_empty_zip_retires_all_previous_members(
    database: DatabaseContext, tmp_path: Path, entry_point: str
) -> None:
    parent_id, children = await _seed_archive(database, tmp_path)
    before = await _snapshot(database)
    if entry_point == "pending":
        await _requeue(database, parent_id)
    service = _zip_service(database, tmp_path, _zip({}))
    try:
        await _run_update(service, parent_id, entry_point)
    finally:
        await service.aclose()
    after = await _snapshot(database)
    parent = after["Document"][parent_id]
    assert parent["is_active"] and parent["extraction_status"] is ExtractionStatus.EXTRACTED
    assert parent["extracted_text"] is None
    assert not any(chunk["document_id"] == parent_id for chunk in after["DocumentChunk"].values())
    await _assert_suspended(database, set(children.values()), before)


@pytest.mark.parametrize("entry_point", ["pending", "recheck"])
@pytest.mark.parametrize("unverified", ["unsafe-path", "incomplete"])
async def test_unverified_zip_cannot_retract_or_reactivate_members(
    database: DatabaseContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    entry_point: str,
    unverified: str,
) -> None:
    parent_id, children = await _seed_archive(database, tmp_path)
    async with database.sessions() as session, session.begin():
        await processor.suspend_document_derivatives(session, children["replace.txt"])
    if entry_point == "pending":
        await _requeue(database, parent_id)
    before = await _snapshot(database, exclude_parent=parent_id)
    members = {"replace.txt": _event_text(4)}
    if unverified == "unsafe-path":
        members["../escape.txt"] = _event_text(8)
    service = _zip_service(database, tmp_path, _zip(members))
    if unverified == "incomplete":
        extract = service._extract_download

        async def incomplete(*args, **kwargs):
            extraction, path = await extract(*args, **kwargs)
            return replace(extraction, status=DocumentExtractionStatus.FAILED), path

        monkeypatch.setattr(service, "_extract_download", incomplete)
    try:
        if unverified == "unsafe-path" and entry_point == "recheck":
            with pytest.raises(RuntimeError, match="archive_unsafe_path"):
                await service.recheck_document(parent_id)
        elif unverified == "unsafe-path":
            summary = await service.process_pending(document_ids=(parent_id,))
            assert summary.documents_failed == 1 and summary.documents_processed == 0
        else:
            await _run_update(service, parent_id, entry_point)
    finally:
        await service.aclose()
    assert await _snapshot(database, exclude_parent=parent_id) == before
    assert not (tmp_path / "escape.txt").exists()


@pytest.mark.parametrize("entry_point", ["pending", "recheck"])
async def test_failed_zip_reconciliation_rolls_back_version_and_derived_changes(
    database: DatabaseContext, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, entry_point: str
) -> None:
    parent_id, _children = await _seed_archive(database, tmp_path)
    if entry_point == "pending":
        await _requeue(database, parent_id)
    before = await _snapshot(database)
    original = processor.suspend_document_derivatives
    suspended = []

    async def fail_after_suspension(session, document_id):
        await original(session, document_id)
        suspended.append(document_id)
        raise RuntimeError("Falha depois de suspender derivados")

    monkeypatch.setattr(processor, "suspend_document_derivatives", fail_after_suspension)
    service = _zip_service(database, tmp_path, _zip({"keep.txt": _event_text(3)}))
    try:
        if entry_point == "recheck":
            with pytest.raises(RuntimeError, match="Falha depois de suspender derivados"):
                await service.recheck_document(parent_id)
        else:
            summary = await service.process_pending(document_ids=(parent_id,))
            assert summary.documents_failed == 1 and summary.documents_processed == 0
    finally:
        await service.aclose()
    assert suspended
    after = await _snapshot(database)
    if entry_point == "pending":
        parent = after["Document"][parent_id]
        assert parent["extraction_status"] is ExtractionStatus.FAILED
        assert "Falha depois de suspender derivados" in parent["extraction_error"]
        assert (
            parent["extraction_attempts"]
            == before["Document"][parent_id]["extraction_attempts"] + 1
        )
        assert parent["next_attempt_at"] is not None
        # Normal batch processing records a retry failure after rolling back the content transaction.
        for field_name in (
            "extraction_status",
            "extraction_error",
            "extraction_attempts",
            "next_attempt_at",
        ):
            parent.pop(field_name)
            before["Document"][parent_id].pop(field_name)
    assert after == before


async def test_session_poll_and_direct_recheck_download_only_the_official_zip_parent(
    database: DatabaseContext, tmp_path: Path
) -> None:
    parent_url = "https://pncp.gov.br/atas.zip"
    parent_id = await _pending_document(database, url=parent_url)
    content = _zip({"sessao.txt": _event_text(3)})
    start = datetime(2026, 10, 20, tzinfo=UTC)
    async with database.sessions() as session, session.begin():
        parent = await session.get_one(Document, parent_id)
        parent.file_hash = hashlib.sha256(content).hexdigest()
        parent.extraction_status = ExtractionStatus.EXTRACTED
        parent.last_checked_at = parent.downloaded_at = start + timedelta(days=1)
        procurement = await session.get_one(Procurement, parent.procurement_id)
        procurement.current_phase = ProcurementPhase.JUDGMENT
        child = Document(
            procurement_id=parent.procurement_id,
            parent_document_id=parent_id,
            title="Ata da sessão interna",
            original_url=f"{parent_url}#sessao.txt",
            file_hash=hashlib.sha256(_event_text(3).encode()).hexdigest(),
            extraction_status=ExtractionStatus.EXTRACTED,
            last_checked_at=start,
            downloaded_at=start,
            fingerprint="session-poll-member",
        )
        session.add(child)
        await session.flush()
        child_id = child.id

    requests = []

    class RecordingDownloader(FakeDownloader):
        async def download(self, url: str) -> DownloadedDocument:
            requests.append(url)
            return await super().download(url)

    service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=RecordingDownloader(content, filename="atas.zip", mime="application/zip"),
    )
    before = await _snapshot(database)
    try:
        await service.recheck_document(child_id)
        assert requests == []
        assert await _snapshot(database) == before
        summary = await service.recheck_stale_session_documents(limit=1)
        assert summary.documents_processed == 1 and summary.documents_failed == 0
        assert requests == [parent_url]
        after = await _snapshot(database)
        assert after["Document"][child_id] == before["Document"][child_id]
    finally:
        await service.aclose()


@pytest.mark.parametrize(
    "download_fails", [False, True], ids=["stale-response", "late-download-error"]
)
async def test_session_poll_cannot_mark_a_new_version_checked_after_an_obsolete_attempt(
    database: DatabaseContext, tmp_path: Path, download_fails: bool, thread_extraction: None
) -> None:
    document_id = await _pending_document(database)
    initial_service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(b"Versao inicial sem evento."),
    )
    assert (await initial_service.process_pending()).documents_processed == 1
    async with database.sessions() as session, session.begin():
        document = await session.get_one(Document, document_id)
        procurement = await session.get_one(Procurement, document.procurement_id)
        procurement.current_phase = ProcurementPhase.JUDGMENT
    downloader = _PausedDownloader(
        _event_text(1).encode(),
        error=DocumentDownloadError("download obsoleto falhou") if download_fails else None,
    )
    stale_service = DocumentProcessingService(
        _settings(tmp_path), session_factory=database.sessions, downloader=downloader
    )
    stale_poll = asyncio.create_task(stale_service.recheck_stale_session_documents(limit=1))
    try:
        await asyncio.wait_for(downloader.entered.wait(), timeout=10)
        await _requeue(database, document_id)
        fresh_content = _event_text(9).encode()
        fresh_service = DocumentProcessingService(
            _settings(tmp_path),
            session_factory=database.sessions,
            downloader=FakeDownloader(fresh_content),
        )
        summary = await fresh_service.process_pending(document_ids=(document_id,))
        assert summary.documents_processed == 1
        assert summary.events_created == summary.leads_created == 1
        before = await _snapshot(database)
        accepted = before["Document"][document_id]
        assert accepted["file_hash"] == hashlib.sha256(fresh_content).hexdigest()
        assert accepted["downloaded_at"] is not None and accepted["last_checked_at"] is not None
        downloader.release.set()
        result = await asyncio.wait_for(stale_poll, timeout=30)
        assert result.documents_failed == int(download_fails)
        assert result.events_created == result.leads_created == 0
        assert await _snapshot(database) == before
    finally:
        stale_poll.cancel()
        await asyncio.gather(stale_poll, return_exceptions=True)
        await stale_service.aclose()
