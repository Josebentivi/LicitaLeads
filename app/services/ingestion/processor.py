"""Persist extracted documents, evidence, events, deadlines, and lead candidates."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from app.config import Settings, get_settings
from app.connectors.http import AsyncHTTPClient
from app.connectors.progress import ActivityCallback, ProgressEvent
from app.database import async_session_factory, write_session
from app.models import (
    Company,
    CrawlRun,
    Deadline,
    DeadlineCalculationMethod,
    DeadlineStatus,
    Document,
    Evidence,
    ExtractionStatus,
    FieldChange,
    FieldObservation,
    FieldValueStatus,
    Lead,
    LeadStatus,
    Participant,
    ParticipantRole,
    ParticipantStatus,
    Procurement,
    ProcurementEvent,
    ProcurementEventType,
    ProcurementItem,
    ProcurementPhase,
    ReasonCategory,
    SourceRecord,
)
from app.models import (
    DocumentChunk as StoredDocumentChunk,
)
from app.services.contacts.domains import (
    EmailCandidate,
    context_window,
    derive_company_domain,
    extract_email_candidates,
)
from app.services.deadlines import BrazilBusinessCalendar, DeadlineEngine, DeadlineRequest
from app.services.documents import DocumentExtractor
from app.services.documents.identity import (
    complete_content_version,
    document_resolution,
    fork_content_version,
    is_canonical,
    noncanonical_document_ids,
)
from app.services.documents.ocr import OCRProvider, build_ocr_provider
from app.services.documents.processes import extract_in_process
from app.services.documents.security import ArchiveLimits
from app.services.documents.types import (
    DocumentChunk,
    DocumentExtraction,
    DocumentExtractionStatus,
)
from app.services.event_detection import (
    CompanyReference,
    DetectedEvent,
    DetectedParticipant,
    DocumentEvidence,
    EventDetector,
    ParticipantDetector,
    associate_company,
)
from app.services.identifiers import (
    is_valid_cnpj,
    normalize_cnpj,
    normalize_company_name,
    participant_status_code,
)
from app.services.ingestion.documents import (
    DownloadedDocument,
    SecureDocumentDownloader,
    store_by_hash,
)
from app.services.ingestion.pipeline import PipelineCancelled, stable_fingerprint
from app.services.lead_scoring import LeadScorer
from app.services.llm import DisabledLLMEventAnalyzer, LLMEventAnalyzer
from app.services.llm.analyzer import LLMEventAnalysis
from app.services.parallel import bounded_map, joined_thread
from app.services.participation_integrity import (
    event_verified,
    participant_current,
    persist_values,
    verify_participant_reference,
)
from app.services.procurement_phase import (
    latest_procurement_event,
    procurement_accepts_participation_leads,
    refresh_procurement_phase,
)

logger = logging.getLogger(__name__)

_EXPLICIT_DATE = re.compile(
    r"\b(?:at[eé]|prazo(?:\s+final)?(?:\s+em|\s+at[eé])?|vence(?:\s+em)?)\s*"
    r"(?P<day>[0-3]?\d)[/.-](?P<month>[01]?\d)[/.-](?P<year>20\d{2})"
    r"(?:\s*(?:[àa]s?|,)?\s*(?P<hour>[0-2]?\d)(?::|h)(?P<minute>[0-5]\d)?)?\b",
    re.IGNORECASE,
)
_RELATIVE_DEADLINE = re.compile(
    r"\b(?:prazo|dentro\s+de|em\s+at[e\u00e9]|no\s+prazo\s+de)\s*(?:de\s*)?"
    r"(?P<amount>\d{1,3})\s*(?P<unit>minutos?|mins?|horas?|h)\b",
    re.IGNORECASE,
)
_SAME_DAY_TIME = re.compile(
    r"\bat[e\u00e9]\s+[a\u00e0]s?\s*(?P<hour>[0-2]?\d)(?::|h)(?P<minute>[0-5]\d)\b",
    re.IGNORECASE,
)
_RELATIVE_DEADLINE_EVENTS = {
    ProcurementEventType.BEST_OFFER_DECLARED,
    ProcurementEventType.NEGOTIATION_REQUESTED,
    ProcurementEventType.DILIGENCE_REQUESTED,
    ProcurementEventType.DOCUMENTATION_REQUESTED,
}

# Events that describe the winning/awarded side and can be attributed to the
# single adjudicated company without guessing which loser they refer to.
_AUTO_LINK_EVENT_TYPES = {
    ProcurementEventType.HOMOLOGATED,
    ProcurementEventType.ADJUDICATED,
    ProcurementEventType.WINNER_DECLARED,
    ProcurementEventType.QUALIFIED,
}
_SESSION_PHASES = (
    ProcurementPhase.DISPUTE,
    ProcurementPhase.DILIGENCE,
    ProcurementPhase.JUDGMENT,
    ProcurementPhase.APPEAL,
    ProcurementPhase.COUNTERARGUMENT,
    ProcurementPhase.QUALIFICATION,
    ProcurementPhase.ADJUDICATION,
)
_SESSION_DOCUMENT_PATTERNS = (
    "%sess%",
    "%julgamento%",
    "%recurso%",
    "%dilig%",
    "%habilita%",
    "%decis%",
)


@dataclass(slots=True)
class ProcessingSummary:
    documents_processed: int = 0
    documents_failed: int = 0
    documents_skipped: int = 0
    events_created: int = 0
    participants_created: int = 0
    leads_created: int = 0


@dataclass(frozen=True, slots=True)
class _DocumentVersion:
    original_url: str
    resolved_url: str | None
    file_hash: str | None
    deactivated_at: datetime | None
    activity_revision: int
    extraction_status: ExtractionStatus
    downloaded_at: datetime | None
    last_checked_at: datetime | None
    extraction_attempts: int
    next_attempt_at: datetime | None


@dataclass(frozen=True, slots=True)
class _PreparedDocument:
    document_id: UUID
    original_url: str
    previous_hash: str | None
    deactivated_at: datetime | None
    activity_revision: int
    downloaded: DownloadedDocument
    extraction: DocumentExtraction
    local_path: Path
    version: _DocumentVersion


ProgressCallback = Callable[[int, int], Awaitable[None]]
ProcessingCheckpoint = Callable[[AsyncSession], Awaitable[None]]


async def event_document_is_active(session: AsyncSession, event: ProcurementEvent) -> bool:
    """Historical evidence remains readable, but inactive documents cannot drive work."""
    if (
        await session.scalar(
            select(ProcurementEvent.id).where(ProcurementEvent.id == event.id, event_verified())
        )
        is None
    ):
        return False
    if event.evidence_id is None:
        return True
    evidence = await session.get(Evidence, event.evidence_id)
    if evidence is None or evidence.document_id is None:
        return evidence is not None
    document_id: UUID | None = evidence.document_id
    visited: set[UUID] = set()
    while document_id is not None:
        if document_id in visited:
            return False
        visited.add(document_id)
        document = await session.get(Document, document_id, populate_existing=True)
        if document is None or not document.is_active:
            return False
        document_id = document.parent_document_id
    return True


async def event_award_inference_is_current(session: AsyncSession, event: ProcurementEvent) -> bool:
    inferred = await session.scalar(
        select(FieldObservation.id)
        .where(
            FieldObservation.entity_type == "procurement_event",
            FieldObservation.entity_id == event.id,
            FieldObservation.field_name == "company_id",
            FieldObservation.source == "inferred_single_winner",
        )
        .limit(1)
    )
    if inferred is None:
        return True
    return (
        await session.scalar(
            select(Participant.id)
            .where(
                Participant.procurement_id == event.procurement_id,
                Participant.company_id == event.company_id,
                or_(
                    Participant.status_code.in_(
                        [ParticipantStatus.WINNER, ParticipantStatus.AWARDED]
                    ),
                    and_(
                        Participant.status_code.is_(None),
                        Participant.participation_role.in_(
                            [ParticipantRole.WINNER, ParticipantRole.AWARDED]
                        ),
                    ),
                ),
            )
            .limit(1)
        )
        is not None
    )


async def suspend_event_work(
    session: AsyncSession, events: list[ProcurementEvent], reason: str
) -> None:
    event_ids = [event.id for event in events]
    for event in events:
        event.requires_manual_review = True
    if not event_ids:
        return
    for deadline in (
        await session.scalars(select(Deadline).where(Deadline.event_id.in_(event_ids)))
    ).all():
        deadline.status = DeadlineStatus.REQUIRES_REVIEW
        deadline.requires_manual_review = True
        deadline.remaining_seconds = None
    for lead in (
        await session.scalars(select(Lead).where(Lead.triggering_event_id.in_(event_ids)))
    ).all():
        if lead.lead_status not in {LeadStatus.REJECTED, LeadStatus.ARCHIVED}:
            lead.lead_status = LeadStatus.PENDING_REVIEW
        lead.score = lead.fit_score = lead.evidence_score = lead.urgency_score = 0
        lead.recommended_action = reason
        lead.score_breakdown = {**(lead.score_breakdown or {}), "source_state": reason}


async def review_cancelled_award_derivatives(
    session: AsyncSession, participant: Participant
) -> None:
    events = list(
        (
            await session.scalars(
                select(ProcurementEvent).where(
                    ProcurementEvent.procurement_id == participant.procurement_id,
                    ProcurementEvent.company_id == participant.company_id,
                )
            )
        ).all()
    )
    invalid = []
    for event in events:
        if not await event_award_inference_is_current(session, event):
            invalid.append(event)
    await suspend_event_work(
        session, invalid, "Resultado oficial cancelado; revisar a atribuição da empresa ao evento."
    )


async def suspend_document_derivatives(session: AsyncSession, document_id: UUID) -> None:
    """Suspend operational uses of a withdrawn source without deleting its history."""
    identifiers = {document_id}
    pending = [document_id]
    procurement_ids: set[UUID] = set()
    while pending:
        current = pending.pop()
        document = await session.get(Document, current)
        if document is not None:
            document.is_active = False
            document.deactivated_at = document.deactivated_at or datetime.now(UTC)
            procurement_ids.add(document.procurement_id)
        children = list(
            (
                await session.scalars(
                    select(Document.id).where(Document.parent_document_id == current)
                )
            ).all()
        )
        for child_id in children:
            if child_id not in identifiers:
                identifiers.add(child_id)
                pending.append(child_id)
    evidence_ids = select(Evidence.id).where(Evidence.document_id.in_(identifiers))
    events = list(
        (
            await session.scalars(
                select(ProcurementEvent).where(ProcurementEvent.evidence_id.in_(evidence_ids))
            )
        ).all()
    )
    await suspend_event_work(
        session, events, "Fonte documental desativada; revisar os fatos antes de usar o lead."
    )
    await session.flush()
    for procurement_id in procurement_ids:
        procurement = await session.get(Procurement, procurement_id)
        if procurement is not None:
            await refresh_procurement_phase(session, procurement)


class DocumentProcessingService:
    """Turn downloaded official documents into traceable, reviewable facts."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        downloader: SecureDocumentDownloader | None = None,
        llm_analyzer: LLMEventAnalyzer | None = None,
        ocr_provider: OCRProvider | None = None,
        http_budgets: dict[str, AsyncHTTPClient] | None = None,
        crawl_run_id: UUID | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.session_factory = session_factory or async_session_factory
        allowed_hosts = {
            urlparse(self.settings.pncp_base_url).hostname or "pncp.gov.br",
            urlparse(self.settings.pncp_integration_base_url).hostname or "pncp.gov.br",
            urlparse(self.settings.compras_gov_base_url).hostname or "dadosabertos.compras.gov.br",
        }
        self.downloader = downloader or SecureDocumentDownloader(
            allowed_hosts=allowed_hosts,
            max_bytes=self.settings.document_max_bytes,
            timeout=self.settings.http_timeout_seconds,
            user_agent=self.settings.http_user_agent,
        )
        self._owns_downloader = downloader is None
        self.extractor = DocumentExtractor(
            max_nested_archives=self.settings.archive_max_depth,
            archive_limits=ArchiveLimits(
                max_members=self.settings.archive_max_members,
                max_uncompressed_bytes=self.settings.archive_max_bytes,
                max_compression_ratio=100,
                max_member_bytes=self.settings.document_max_bytes,
            ),
        )
        self.event_detector = EventDetector()
        self.participant_detector = ParticipantDetector()
        if self.settings.holiday_calendar_path.exists():
            calendar = BrazilBusinessCalendar.from_csv(self.settings.holiday_calendar_path)
        else:
            calendar = BrazilBusinessCalendar()
        self.deadline_engine = DeadlineEngine(calendar)
        self.scorer = LeadScorer()
        self.llm_analyzer = llm_analyzer or DisabledLLMEventAnalyzer()
        self.crawl_run_id = crawl_run_id
        self.ocr_provider = ocr_provider or build_ocr_provider(self.settings)
        self._http_budgets = dict(http_budgets or {})
        self._owned_budgets: list[AsyncHTTPClient] = []
        self._budget_lock = asyncio.Lock()
        self._download_slots = asyncio.Semaphore(self.settings.document_download_concurrency)
        self._process_slots = asyncio.Semaphore(self.settings.document_process_concurrency)
        self._downloads_active = self._processes_active = self._documents_ready = 0

    async def aclose(self) -> None:
        for budget in self._owned_budgets:
            await budget.aclose()
        self._owned_budgets.clear()
        if self._owns_downloader:
            await self.downloader.aclose()
        provider = getattr(self.llm_analyzer, "provider", None)
        provider_close = getattr(provider, "aclose", None)
        if callable(provider_close):
            await provider_close()

    async def process_pending(
        self,
        *,
        limit: int | None = None,
        on_progress: ProgressCallback | None = None,
        checkpoint: ProcessingCheckpoint | None = None,
        on_activity: ActivityCallback | None = None,
        document_ids: tuple[UUID, ...] | None = None,
        failed_only: bool = False,
        on_selected: Callable[[list[UUID]], Awaitable[None]] | None = None,
    ) -> ProcessingSummary:
        from app.services.ingestion.document_retry import document_processing_lease

        try:
            if checkpoint is None:
                async with document_processing_lease(self.session_factory) as fence:
                    return await self._process_pending(
                        limit=limit,
                        on_progress=on_progress,
                        checkpoint=fence,
                        on_activity=on_activity,
                        document_ids=document_ids,
                        failed_only=failed_only,
                        on_selected=on_selected,
                    )
            return await self._process_pending(
                limit=limit,
                on_progress=on_progress,
                checkpoint=checkpoint,
                on_activity=on_activity,
                document_ids=document_ids,
                failed_only=failed_only,
                on_selected=on_selected,
            )
        finally:
            await self.aclose()

    async def _process_pending(
        self,
        *,
        limit: int | None = None,
        on_progress: ProgressCallback | None = None,
        checkpoint: ProcessingCheckpoint | None = None,
        on_activity: ActivityCallback | None = None,
        document_ids: tuple[UUID, ...] | None = None,
        failed_only: bool = False,
        on_selected: Callable[[list[UUID]], Awaitable[None]] | None = None,
    ) -> ProcessingSummary:
        """Process a stable snapshot of pending documents, one transaction each."""

        now = datetime.now(UTC)
        from app.services.ingestion.document_retry import corrective_scope

        async with self.session_factory() as session:
            corrections = await corrective_scope(session, self.crawl_run_id)
        statement = (
            select(Document.id)
            .where(
                Document.id.not_in(noncanonical_document_ids()),
                Document.is_active.is_(True),
                Document.parent_document_id.is_(None),
                or_(
                    Document.extraction_status == ExtractionStatus.PENDING,
                    and_(
                        Document.extraction_status == ExtractionStatus.FAILED,
                        Document.extraction_attempts < self.settings.document_max_attempts,
                        or_(
                            Document.next_attempt_at.is_(None),
                            Document.next_attempt_at <= now,
                            *[
                                and_(
                                    Document.id == identifier,
                                    Document.extraction_attempts == attempts,
                                )
                                for identifier, attempts in corrections.items()
                            ],
                        ),
                    ),
                ),
            )
            .order_by(Document.created_at, Document.id)
        )
        if document_ids is not None:
            from app.services.documents.identity import canonical_document_ids

            async with self.session_factory() as session:
                normalized = await canonical_document_ids(session, document_ids)
            statement = statement.where(Document.id.in_(normalized))
        if failed_only:
            statement = statement.where(Document.extraction_status == ExtractionStatus.FAILED)
        if limit is not None:
            statement = statement.limit(limit)
        async with self.session_factory() as session:
            identifiers = list((await session.scalars(statement)).all())

        if on_selected is not None:
            await on_selected(identifiers)
        summary = ProcessingSummary()
        if on_activity is not None:
            await on_activity(
                ProgressEvent(
                    stage="documents",
                    processed=0,
                    total=len(identifiers),
                    message="Processando documentos pendentes deste lote",
                )
            )

        async def prepare(
            document_id: UUID,
        ) -> tuple[_PreparedDocument | None, Exception | None, _DocumentVersion | None]:
            # Each task owns its snapshot, including when preparation raises before
            # producing a result. No shared "current document" pointer is used.
            version: _DocumentVersion | None = None

            def captured(snapshot: _DocumentVersion) -> None:
                nonlocal version
                version = snapshot

            try:
                return (
                    await self._prepare_document(
                        document_id,
                        checkpoint=checkpoint,
                        on_activity=on_activity,
                        on_version=captured,
                    ),
                    None,
                    version,
                )
            except (asyncio.CancelledError, PipelineCancelled):
                raise
            except Exception as exc:
                return None, exc, version

        stream = bounded_map(
            identifiers,
            prepare,
            capacity=2
            * max(
                self.settings.document_download_concurrency,
                self.settings.document_process_concurrency,
            ),
        )
        position = 0
        try:
            async for document_id, (prepared, error, version) in stream:
                try:
                    if error is not None:
                        raise error
                    if prepared is not None:
                        self._documents_ready -= 1
                    await self._document_activity(on_activity)
                    processed = await self._persist_prepared(
                        prepared, summary, checkpoint=checkpoint
                    )
                    if processed:
                        summary.documents_processed += 1
                    else:
                        summary.documents_skipped += 1
                except (asyncio.CancelledError, PipelineCancelled):
                    raise
                except Exception as exc:
                    if version is None:
                        # Infrastructure failure before the document was observed:
                        # report the attempt, but do not change an unknown version.
                        summary.documents_failed += 1
                    elif await self._mark_failed(
                        document_id, exc, version=version, checkpoint=checkpoint
                    ):
                        summary.documents_failed += 1
                    else:
                        summary.documents_skipped += 1
                position += 1
                if on_progress is not None:
                    await on_progress(position, len(identifiers))
        finally:
            await stream.aclose()
        return summary

    async def detect_existing(self, *, limit: int | None = None) -> ProcessingSummary:
        """Idempotently rerun deterministic detection on already-extracted text."""

        statement = (
            select(Document.id)
            .where(
                Document.is_active.is_(True),
                Document.extraction_status == ExtractionStatus.EXTRACTED,
                or_(Document.mime_type.is_(None), Document.mime_type != "application/zip"),
            )
            .order_by(Document.created_at, Document.id)
        )
        if limit is not None:
            statement = statement.limit(limit)
        async with self.session_factory() as session:
            identifiers = list((await session.scalars(statement)).all())
        summary = ProcessingSummary()
        for document_id in identifiers:
            async with write_session(self.session_factory) as session:
                document = await session.get(Document, document_id)
                if document is None or not document.is_active:
                    continue
                events, participants, leads = await self._detect_and_persist(session, document)
                summary.documents_processed += 1
                summary.events_created += events
                summary.participants_created += participants
                summary.leads_created += leads
        return summary

    async def recalculate_deadlines_and_leads(self) -> ProcessingSummary:
        """Refresh volatile deadline state and deterministic scores for all events."""

        async with self.session_factory() as session:
            identifiers = list(
                (
                    await session.scalars(
                        select(ProcurementEvent.id).where(ProcurementEvent.company_id.is_not(None))
                    )
                ).all()
            )
        summary = ProcessingSummary()
        procurement_ids: set[UUID] = set()
        for event_id in identifiers:
            async with write_session(self.session_factory) as session:
                event = await session.get(ProcurementEvent, event_id)
                if event is None:
                    continue
                procurement = await session.get(Procurement, event.procurement_id)
                if procurement is None:
                    continue
                procurement_ids.add(procurement.id)
                summary.leads_created += int(
                    await self._deadline_and_lead(session, procurement, event)
                )
        async with write_session(self.session_factory) as session:
            for procurement_id in procurement_ids:
                procurement = await session.get(Procurement, procurement_id)
                if procurement is not None:
                    await refresh_procurement_phase(session, procurement)
        return summary

    async def _maybe_ocr(
        self, extraction: DocumentExtraction, content: bytes
    ) -> DocumentExtraction:
        """Replace an OCR_REQUIRED extraction when a provider returns usable text."""

        if extraction.status is not DocumentExtractionStatus.OCR_REQUIRED:
            return extraction
        if not bool(getattr(self.settings, "ocr_enabled", False)):
            return extraction
        try:
            pages = await self.ocr_provider.extract_text(content, mime_type=extraction.mime_type)
        except Exception:
            logger.exception("ocr provider failed")
            return extraction
        if not pages:
            return extraction
        parts: list[str] = []
        chunks: list[DocumentChunk] = []
        cursor = 0
        for page in pages:
            text = page.text.strip()
            if not text:
                continue
            if parts:
                parts.append("\n\n")
                cursor += 2
            start = cursor
            parts.append(text)
            cursor += len(text)
            chunks.append(
                DocumentChunk(
                    text=text,
                    page_number=page.page_number,
                    locator=f"ocr:page:{page.page_number}",
                    start_offset=start,
                    end_offset=cursor,
                    metadata={"ocr_confidence": page.confidence},
                )
            )
        if not chunks:
            return extraction
        return replace(
            extraction,
            status=DocumentExtractionStatus.EXTRACTED,
            text="".join(parts),
            chunks=tuple(chunks),
            warnings=(*extraction.warnings, "ocr:tesseract"),
        )

    async def _document_activity(self, callback: ActivityCallback | None) -> None:
        if callback is not None:
            await callback(
                ProgressEvent(
                    source="documents",
                    download_active=self._downloads_active,
                    download_limit=self.settings.document_download_concurrency,
                    process_active=self._processes_active,
                    process_limit=self.settings.document_process_concurrency,
                    queue_ready=self._documents_ready,
                )
            )

    @staticmethod
    async def _activity_revision(session: AsyncSession, document_id: UUID) -> int:
        # Counting accepted append-only transitions also detects active → inactive
        # → active while a worker prepares content. Rejected observations do not
        # invalidate the accepted version, and no legacy history is invented.
        return int(
            await session.scalar(
                select(func.count())
                .select_from(FieldChange)
                .where(
                    FieldChange.entity_type == "document",
                    FieldChange.entity_id == document_id,
                    FieldChange.field_name == "is_active",
                    FieldChange.applied.is_(True),
                )
            )
            or 0
        )

    async def _document_version(
        self, session: AsyncSession, document: Document
    ) -> _DocumentVersion:
        return _DocumentVersion(
            original_url=document.original_url,
            resolved_url=document.resolved_url,
            file_hash=document.file_hash,
            deactivated_at=document.deactivated_at,
            activity_revision=await self._activity_revision(session, document.id),
            extraction_status=document.extraction_status,
            downloaded_at=document.downloaded_at,
            last_checked_at=document.last_checked_at,
            extraction_attempts=document.extraction_attempts or 0,
            next_attempt_at=document.next_attempt_at,
        )

    async def _extract_download(
        self,
        downloaded: DownloadedDocument,
        declared_mime: str | None,
        on_activity: ActivityCallback | None = None,
    ) -> tuple[DocumentExtraction, Path]:
        def store() -> Path:
            return store_by_hash(
                downloaded.content,
                hashlib.sha256(downloaded.content).hexdigest(),
                self.settings.document_storage_path,
                downloaded.filename,
            )

        path = await joined_thread(store)
        async with self._process_slots:
            self._processes_active += 1
            try:
                await self._document_activity(on_activity)
                extraction = await extract_in_process(
                    path,
                    filename=downloaded.filename,
                    mime_type=downloaded.declared_mime or declared_mime,
                    settings=self.settings,
                    archive_limits=self.extractor.archive_limits,
                )
                if extraction.sha256 != path.stem:
                    raise ValueError("conteúdo do documento mudou durante a extração")
            finally:
                self._processes_active -= 1
                await self._document_activity(on_activity)
        return extraction, path

    async def _prepare_document(
        self,
        document_id: UUID,
        *,
        checkpoint: ProcessingCheckpoint | None = None,
        on_activity: ActivityCallback | None = None,
        on_version: Callable[[_DocumentVersion], None] | None = None,
    ) -> _PreparedDocument | None:
        async with write_session(self.session_factory) as session:
            if checkpoint is not None:
                await checkpoint(session)
            document = await session.get(Document, document_id)
            if (
                document is None
                or not document.is_active
                or document.parent_document_id is not None
                or document.extraction_status
                not in {
                    ExtractionStatus.PENDING,
                    ExtractionStatus.FAILED,
                }
            ):
                return None
            original_url, previous_hash = document.original_url, document.file_hash
            if document.extraction_status == ExtractionStatus.FAILED:
                if document.extraction_attempts >= self.settings.document_max_attempts:
                    return None
                from app.services.ingestion.document_retry import claim_corrective_attempt

                corrected = await claim_corrective_attempt(session, self.crawl_run_id, document)
                if (
                    not corrected
                    and document.next_attempt_at is not None
                    and document.next_attempt_at > datetime.now(UTC)
                ):
                    return None
            deactivated_at, declared_mime = document.deactivated_at, document.mime_type
            activity_revision = await self._activity_revision(session, document_id)
            version = await self._document_version(session, document)
            if on_version is not None:
                on_version(version)
            subject = document.title or f"Documento {document.id}"
            record = (
                await session.get(SourceRecord, document.source_record_id)
                if document.source_record_id
                else None
            )
            resolution = await document_resolution(session, document)
            procurement = await session.get(Procurement, document.procurement_id)
            source = (
                record.source
                if record is not None
                else procurement.source
                if procurement
                else "documents"
            )
        if on_activity is not None:
            await on_activity(
                ProgressEvent(
                    stage="documents",
                    source="documents",
                    operation="download",
                    subject=subject,
                    message="Baixando documento oficial",
                )
            )

        async def download_checkpoint() -> None:
            if checkpoint is not None:
                async with write_session(self.session_factory) as session:
                    await checkpoint(session)

        budget: AsyncHTTPClient | None = None
        if isinstance(self.downloader, SecureDocumentDownloader):
            async with self._budget_lock:
                if source not in self._http_budgets:
                    budget = await joined_thread(AsyncHTTPClient, self.settings)
                    self._http_budgets[source] = budget
                    self._owned_budgets.append(budget)
                budget = self._http_budgets[source]

        @asynccontextmanager
        async def network_slot(url: str) -> AsyncIterator[int]:
            assert budget is not None
            async with budget.request_slot(url, extra_semaphore=self._download_slots) as generation:
                self._downloads_active += 1
                try:
                    await self._document_activity(on_activity)
                    yield generation
                finally:
                    self._downloads_active -= 1
                    await self._document_activity(on_activity)

        if isinstance(self.downloader, SecureDocumentDownloader):
            downloaded = await self.downloader.download(
                original_url,
                resolution=resolution,
                on_activity=on_activity,
                checkpoint=download_checkpoint,
                http_budget=budget,
                network_slot=network_slot,
                max_retries=self.settings.http_max_retries,
            )
        else:
            async with self._download_slots:
                self._downloads_active += 1
                try:
                    await self._document_activity(on_activity)
                    downloaded = await self.downloader.download(original_url)
                finally:
                    self._downloads_active -= 1
                    await self._document_activity(on_activity)
        extraction, local_path = await self._extract_download(
            downloaded, declared_mime, on_activity
        )
        self._documents_ready += 1
        await self._document_activity(on_activity)
        return _PreparedDocument(
            document_id,
            original_url,
            previous_hash,
            deactivated_at,
            activity_revision,
            replace(downloaded, content=b""),
            extraction,
            local_path,
            version,
        )

    async def _process_one(
        self,
        document_id: UUID,
        summary: ProcessingSummary,
        *,
        checkpoint: ProcessingCheckpoint | None = None,
        on_activity: ActivityCallback | None = None,
    ) -> bool:
        prepared = await self._prepare_document(
            document_id, checkpoint=checkpoint, on_activity=on_activity
        )
        if prepared is not None:
            self._documents_ready -= 1
        return await self._persist_prepared(prepared, summary, checkpoint=checkpoint)

    async def _persist_prepared(
        self,
        prepared: _PreparedDocument | None,
        summary: ProcessingSummary,
        *,
        checkpoint: ProcessingCheckpoint | None = None,
    ) -> bool:
        if prepared is None:
            return False
        document_id, downloaded = prepared.document_id, prepared.downloaded
        extraction, local_path = prepared.extraction, prepared.local_path
        counts = ProcessingSummary()
        async with write_session(self.session_factory) as session:
            if checkpoint is not None:
                await checkpoint(session)
            document = await session.scalar(
                select(Document)
                .where(Document.id == document_id)
                .with_for_update()
                .options(selectinload(Document.procurement))
            )
            if (
                document is None
                or not document.is_active
                or document.original_url != prepared.original_url
                or document.file_hash != prepared.previous_hash
                or document.deactivated_at != prepared.deactivated_at
                or await self._document_version(session, document) != prepared.version
                or await self._activity_revision(session, document_id) != prepared.activity_revision
                or not await is_canonical(session, document_id)
                or document.extraction_status
                not in {ExtractionStatus.PENDING, ExtractionStatus.FAILED}
            ):
                return False
            document = await fork_content_version(session, document, extraction.sha256)
            document.resolved_url = downloaded.url
            document.file_hash = extraction.sha256
            document.mime_type = extraction.mime_type
            document.local_path = str(local_path.resolve())
            now = datetime.now(UTC)
            document.downloaded_at = now
            document.last_checked_at = now
            document.extracted_text = extraction.text or None
            document.extraction_attempts = 0
            document.next_attempt_at = None
            document.extraction_status = self._stored_status(extraction.status)
            document.extraction_error = "; ".join(extraction.warnings) or None
            await self._replace_chunks(session, document, extraction)

            targets = await self._extraction_targets(session, document, extraction)
            if extraction.status in {
                DocumentExtractionStatus.EXTRACTED,
                DocumentExtractionStatus.EMPTY,
            }:
                await complete_content_version(session, document)
            for target in targets:
                events, participants, leads = await self._detect_and_persist(session, target)
                counts.events_created += events
                counts.participants_created += participants
                counts.leads_created += leads

        summary.events_created += counts.events_created
        summary.participants_created += counts.participants_created
        summary.leads_created += counts.leads_created
        return True

    async def recheck_stale_session_documents(
        self, *, limit: int | None = None
    ) -> ProcessingSummary:
        """Re-download session documents that may have changed in place.

        PNCP documents can be updated without receiving a new URL, so a normal
        "pending only" pass would miss later session facts.  The query is
        deliberately scoped to processes in a dispute/judgment phase.
        """

        selected_limit = limit or self.settings.session_poll_document_limit
        async with self.session_factory() as session:
            identifiers = list(
                (
                    await session.scalars(
                        select(Document.id)
                        .join(Procurement, Procurement.id == Document.procurement_id)
                        .where(
                            Document.is_active.is_(True),
                            Document.id.not_in(noncanonical_document_ids()),
                            Document.parent_document_id.is_(None),
                            Procurement.current_phase.in_(_SESSION_PHASES),
                            or_(
                                *(
                                    Document.document_type.ilike(pattern)
                                    for pattern in _SESSION_DOCUMENT_PATTERNS
                                ),
                                *(
                                    Document.title.ilike(pattern)
                                    for pattern in _SESSION_DOCUMENT_PATTERNS
                                ),
                            ),
                        )
                        .order_by(
                            Document.last_checked_at.asc().nullsfirst(),
                            Document.downloaded_at.asc().nullsfirst(),
                            Document.created_at,
                            Document.id,
                        )
                        .limit(selected_limit)
                    )
                ).all()
            )
        summary = ProcessingSummary()
        for document_id in identifiers:
            async with self.session_factory() as session:
                document = await session.get(Document, document_id)
                if document is None or not document.is_active:
                    continue
                version = await self._document_version(session, document)
            try:
                changed = await self.recheck_document(document_id)
            except (asyncio.CancelledError, PipelineCancelled):
                raise
            except Exception:
                logger.exception(
                    "session document recheck failed", extra={"document_id": str(document_id)}
                )
                summary.documents_failed += 1
                # Failed documents must rotate to the end of the recheck queue,
                # but a stale attempt must not mark a newer version as checked.
                await self._mark_checked(document_id, version=version)
                continue
            summary.documents_processed += 1
            summary.events_created += changed.events_created
            summary.participants_created += changed.participants_created
            summary.leads_created += changed.leads_created
        return summary

    async def recheck_document(self, document_id: UUID) -> ProcessingSummary:
        """Download one document again and reprocess only when its hash changed."""

        summary = ProcessingSummary()
        async with self.session_factory() as session:
            document = await session.get(Document, document_id)
            if (
                document is None
                or not document.is_active
                or document.parent_document_id is not None
            ):
                return summary
            original_url = document.original_url
            declared_mime = document.mime_type
            previous_hash = document.file_hash
            version = await self._document_version(session, document)
            resolution = await document_resolution(session, document)
            if (
                await session.scalar(
                    select(Document.id).where(
                        Document.id == document.id, Document.id.in_(noncanonical_document_ids())
                    )
                )
                is not None
            ):
                return summary

        if isinstance(self.downloader, SecureDocumentDownloader):
            downloaded = await self.downloader.download(original_url, resolution=resolution)
        else:
            downloaded = await self.downloader.download(original_url)
        content_hash = hashlib.sha256(downloaded.content).hexdigest()
        if previous_hash and content_hash == previous_hash:
            async with write_session(self.session_factory) as session:
                document = await session.scalar(
                    select(Document).where(Document.id == document_id).with_for_update()
                )
                if (
                    document is not None
                    and document.is_active
                    and await self._document_version(session, document) == version
                    and await is_canonical(session, document_id)
                ):
                    now = datetime.now(UTC)
                    document.downloaded_at = now
                    document.last_checked_at = now
                    document.resolved_url = downloaded.url
            return summary
        extraction, local_path = await self._extract_download(downloaded, declared_mime)
        async with write_session(self.session_factory) as session:
            document = await session.scalar(
                select(Document).where(Document.id == document_id).with_for_update()
            )
            if (
                document is None
                or not document.is_active
                or await self._document_version(session, document) != version
                or not await is_canonical(session, document_id)
            ):
                return summary
            document = await fork_content_version(session, document, extraction.sha256)
            now = datetime.now(UTC)
            document.resolved_url = downloaded.url
            document.file_hash = extraction.sha256
            document.mime_type = extraction.mime_type
            document.local_path = str(local_path.resolve())
            document.downloaded_at = now
            document.last_checked_at = now
            document.extracted_text = extraction.text or None
            document.extraction_attempts = 0
            document.next_attempt_at = None
            document.extraction_status = self._stored_status(extraction.status)
            document.extraction_error = "; ".join(extraction.warnings) or None
            await self._replace_chunks(session, document, extraction)
            targets = await self._extraction_targets(session, document, extraction)
            if extraction.status in {
                DocumentExtractionStatus.EXTRACTED,
                DocumentExtractionStatus.EMPTY,
            }:
                await complete_content_version(session, document)
            for target in targets:
                events, participants, leads = await self._detect_and_persist(session, target)
                summary.events_created += events
                summary.participants_created += participants
                summary.leads_created += leads
            return summary

    async def _mark_checked(
        self,
        document_id: UUID,
        *,
        version: _DocumentVersion | None = None,
    ) -> None:
        async with write_session(self.session_factory) as session:
            document = await session.scalar(
                select(Document).where(Document.id == document_id).with_for_update()
            )
            if (
                document is not None
                and document.is_active
                and (version is None or await self._document_version(session, document) == version)
            ):
                document.last_checked_at = datetime.now(UTC)

    async def _mark_failed(
        self,
        document_id: UUID,
        exc: Exception,
        *,
        version: _DocumentVersion,
        checkpoint: ProcessingCheckpoint | None = None,
    ) -> bool:
        async with write_session(self.session_factory) as session:
            if checkpoint is not None:
                await checkpoint(session)
            document = await session.scalar(
                select(Document).where(Document.id == document_id).with_for_update()
            )
            if (
                document is not None
                and document.is_active
                and await self._document_version(session, document) == version
                and await is_canonical(session, document_id)
            ):
                document.extraction_attempts = (document.extraction_attempts or 0) + 1
                delay = min(
                    self.settings.document_retry_base_minutes
                    * (2 ** max(document.extraction_attempts - 1, 0)),
                    24 * 60,
                )
                document.next_attempt_at = datetime.now(UTC) + timedelta(minutes=delay)
                document.extraction_status = ExtractionStatus.FAILED
                document.extraction_error = f"{type(exc).__name__}: {exc}"[:4000]
                from app.services.documents.errors import ExtractionFailure

                failure = (
                    exc
                    if isinstance(exc, ExtractionFailure)
                    else ExtractionFailure("worker_timeout", exception_type=type(exc).__name__)
                    if isinstance(exc, TimeoutError)
                    else None
                )
                if self.crawl_run_id and failure is not None:
                    run = await session.get(CrawlRun, self.crawl_run_id)
                    if run is not None:
                        cursor = dict(run.cursor or {})
                        cursor["document_errors"] = {
                            **cursor.get("document_errors", {}),
                            str(document_id): failure.as_dict(),
                        }
                        run.cursor = cursor
                return True
            return False

    @staticmethod
    def _stored_status(status: DocumentExtractionStatus) -> ExtractionStatus:
        if status in {DocumentExtractionStatus.EXTRACTED, DocumentExtractionStatus.EMPTY}:
            return ExtractionStatus.EXTRACTED
        if status is DocumentExtractionStatus.OCR_REQUIRED:
            return ExtractionStatus.OCR_REQUIRED
        if status is DocumentExtractionStatus.UNSUPPORTED:
            return ExtractionStatus.UNSUPPORTED
        return ExtractionStatus.FAILED

    async def _replace_chunks(
        self,
        session: AsyncSession,
        document: Document,
        extraction: DocumentExtraction,
    ) -> None:
        existing = list(
            (
                await session.scalars(
                    select(StoredDocumentChunk).where(
                        StoredDocumentChunk.document_id == document.id
                    )
                )
            ).all()
        )
        for stored_chunk in existing:
            await session.delete(stored_chunk)
        await session.flush()
        for sequence, extracted_chunk in enumerate(extraction.chunks):
            metadata: dict[str, object] = extracted_chunk.metadata or {}
            session.add(
                StoredDocumentChunk(
                    document_id=document.id,
                    sequence=sequence,
                    chunk_type="text",
                    page_number=extracted_chunk.page_number,
                    section=metadata.get("section"),
                    sheet_name=metadata.get("sheet"),
                    cell_range=metadata.get("cell_range"),
                    locator=extracted_chunk.locator,
                    text=extracted_chunk.text,
                    start_offset=extracted_chunk.start_offset,
                    end_offset=extracted_chunk.end_offset,
                    fingerprint=stable_fingerprint(
                        "chunk", document.id, sequence, extracted_chunk.text
                    ),
                )
            )
        await session.flush()

    async def _extraction_targets(
        self,
        session: AsyncSession,
        document: Document,
        extraction: DocumentExtraction,
    ) -> list[Document]:
        """Reconcile verified archive membership in the extraction transaction."""
        complete = extraction.status in {
            DocumentExtractionStatus.EXTRACTED,
            DocumentExtractionStatus.EMPTY,
        }
        if extraction.mime_type == "application/zip":
            if not complete:
                # A rejected/truncated archive cannot withdraw or reactivate
                # members whose membership was not fully verified.
                return []
            targets = []
            current_ids = set()
            for member in extraction.archive_members:
                child = await self._persist_archive_member(
                    session, document, member.filename, member.extraction
                )
                current_ids.add(child.id)
                targets.extend(await self._extraction_targets(session, child, member.extraction))
        else:
            targets = [document]
            current_ids = set()
        if complete:
            previous = (
                await session.scalars(
                    select(Document).where(
                        Document.parent_document_id == document.id,
                        Document.is_active.is_(True),
                    )
                )
            ).all()
            for child in previous:
                if child.id not in current_ids:
                    await suspend_document_derivatives(session, child.id)
        return targets

    async def _persist_archive_member(
        self,
        session: AsyncSession,
        parent: Document,
        filename: str,
        extraction: DocumentExtraction,
    ) -> Document:
        fingerprint = stable_fingerprint("archive_member", parent.id, filename, extraction.sha256)
        child = await session.scalar(select(Document).where(Document.fingerprint == fingerprint))
        if child is None:
            child = Document(
                procurement_id=parent.procurement_id,
                parent_document_id=parent.id,
                document_type=parent.document_type,
                title=filename,
                original_url=f"{parent.original_url}#{filename}",
                resolved_url=f"{parent.resolved_url}#{filename}" if parent.resolved_url else None,
                file_hash=extraction.sha256,
                mime_type=extraction.mime_type,
                extracted_text=extraction.text or None,
                extraction_status=self._stored_status(extraction.status),
                extraction_error="; ".join(extraction.warnings) or None,
                published_at=parent.published_at,
                downloaded_at=parent.downloaded_at,
                last_checked_at=parent.last_checked_at,
                fingerprint=fingerprint,
            )
            session.add(child)
            await session.flush()
        child.is_active = parent.is_active
        child.deactivated_at = None if parent.is_active else parent.deactivated_at
        child.source_record_id = parent.source_record_id
        child.mime_type = extraction.mime_type
        child.extracted_text = extraction.text or None
        child.extraction_status = self._stored_status(extraction.status)
        child.extraction_error = "; ".join(extraction.warnings) or None
        child.downloaded_at, child.last_checked_at = parent.downloaded_at, parent.last_checked_at
        await self._replace_chunks(session, child, extraction)
        return child

    async def _detect_and_persist(
        self,
        session: AsyncSession,
        document: Document,
    ) -> tuple[int, int, int]:
        if (
            not document.is_active
            or document.extraction_status != ExtractionStatus.EXTRACTED
            or document.mime_type == "application/zip"
        ):
            return 0, 0, 0
        if (
            await session.scalar(
                select(Document.id).where(
                    Document.id == document.id, Document.id.in_(noncanonical_document_ids())
                )
            )
            is not None
        ):
            return 0, 0, 0
        procurement = await session.get(Procurement, document.procurement_id)
        if procurement is None:
            return 0, 0, 0
        companies = list(
            (
                await session.scalars(
                    select(Company)
                    .join(Participant, Participant.company_id == Company.id)
                    .where(Participant.procurement_id == procurement.id, participant_current())
                )
            )
            .unique()
            .all()
        )
        company_refs = [
            CompanyReference(
                str(company.id), company.legal_name or company.normalized_name, company.cnpj
            )
            for company in companies
        ]
        chunks = list(
            (
                await session.scalars(
                    select(StoredDocumentChunk)
                    .where(StoredDocumentChunk.document_id == document.id)
                    .order_by(StoredDocumentChunk.sequence)
                )
            ).all()
        )
        latest_event = await latest_procurement_event(session, procurement.id)
        event_count = participant_count = lead_count = 0
        pending_evaluations: dict[UUID, ProcurementEvent] = {}
        pages: list[DocumentEvidence] = []
        for chunk in chunks:
            source_evidence = DocumentEvidence(
                text=chunk.text,
                source_url=document.resolved_url or document.original_url,
                page_number=chunk.page_number,
                locator=chunk.locator,
                document_type=document.document_type,
                published_at=document.published_at,
                start_offset=chunk.start_offset,
                end_offset=chunk.end_offset,
            )
            pages.append(source_evidence)
            participants = await joined_thread(
                self.participant_detector.detect,
                source_evidence,
                known_companies=company_refs,
            )
            for detected_participant in participants:
                created = await self._persist_participant(
                    session,
                    procurement,
                    document,
                    chunk,
                    detected_participant,
                )
                participant_count += int(created)
                if (
                    not detected_participant.requires_manual_review
                    and procurement_accepts_participation_leads(
                        procurement, last_event=latest_event
                    )
                ):
                    event, event_created = await self._persist_participation_event(
                        session,
                        procurement,
                        document,
                        chunk,
                        detected_participant,
                    )
                    if event is not None and event_created:
                        event_count += 1
                        await refresh_procurement_phase(session, procurement)
                        if event.company_id:
                            pending_evaluations[event.id] = event
            # Refresh company_refs so an event in the same chunk can bind to a just-seen CNPJ.
            companies = list(
                (
                    await session.scalars(
                        select(Company)
                        .join(Participant, Participant.company_id == Company.id)
                        .where(Participant.procurement_id == procurement.id)
                        .where(participant_current())
                    )
                )
                .unique()
                .all()
            )
            company_refs = [
                CompanyReference(
                    str(company.id), company.legal_name or company.normalized_name, company.cnpj
                )
                for company in companies
            ]
            events = await joined_thread(
                self.event_detector.detect, source_evidence, known_companies=company_refs
            )
            for detected_event in events:
                event, created = await self._persist_event(
                    session,
                    procurement,
                    document,
                    chunk,
                    detected_event,
                )
                event_count += int(created)
                if event.company_id is None:
                    await self._auto_link_single_winner(session, procurement, event)
                await refresh_procurement_phase(session, procurement)
                if event.company_id:
                    pending_evaluations[event.id] = event

            if self.settings.contact_domain_discovery_enabled:
                await self._discover_company_domains(
                    session, procurement, document, chunk, company_refs
                )
        from app.services.llm.queue import enqueue_document

        await persist_values(session, document, pages)
        for event in pending_evaluations.values():
            lead_count += int(await self._deadline_and_lead(session, procurement, event))
        await enqueue_document(session, document, self.settings, run_id=self.crawl_run_id)
        return event_count, participant_count, lead_count

    async def _discover_company_domains(
        self,
        session: AsyncSession,
        procurement: Procurement,
        document: Document,
        chunk: StoredDocumentChunk,
        known_companies: list[CompanyReference],
    ) -> int:
        """Fill a company domain from an e-mail evidenced in an official document.

        The domain is only stored when the receipt is attributed to a company by
        exact CNPJ or unambiguous name and the company name is inside the
        domain. An existing different domain is never overwritten; the conflict
        is recorded for human review instead.
        """

        discovered = 0
        for candidate in extract_email_candidates(chunk.text):
            window, _ = context_window(chunk.text, candidate.start, candidate.end)
            reference, method, ambiguous = associate_company(window, known_companies)
            if reference is None or ambiguous or not reference.external_id:
                continue
            if method not in {"exact_cnpj", "exact_normalized_name"}:
                continue
            try:
                company = await session.get(Company, UUID(reference.external_id))
            except ValueError:
                continue
            if company is None:
                continue
            domain = derive_company_domain(candidate, normalize_company_name(company.legal_name))
            if domain is None:
                continue
            if company.domain and company.domain != domain:
                await self._observe_company_domain(
                    session, procurement, document, chunk, company, candidate, domain, conflict=True
                )
                continue
            if company.domain == domain:
                continue
            company.domain = domain
            if not company.website:
                company.website = f"https://{domain}"
            await self._observe_company_domain(
                session, procurement, document, chunk, company, candidate, domain, conflict=False
            )
            discovered += 1
        return discovered

    async def _observe_company_domain(
        self,
        session: AsyncSession,
        procurement: Procurement,
        document: Document,
        chunk: StoredDocumentChunk,
        company: Company,
        candidate: EmailCandidate,
        domain: str,
        *,
        conflict: bool,
    ) -> None:
        evidence = await self._evidence(
            session,
            procurement,
            document,
            chunk,
            DocumentEvidence(
                text=candidate.email,
                source_url=document.resolved_url or document.original_url,
                page_number=chunk.page_number,
                locator=chunk.locator,
                document_type=document.document_type,
                published_at=document.published_at,
                start_offset=candidate.start,
                end_offset=candidate.end,
            ),
        )
        fingerprint = stable_fingerprint("company_domain", company.id, domain, candidate.email)
        exists = await session.scalar(
            select(FieldObservation.id).where(FieldObservation.fingerprint == fingerprint)
        )
        if exists is not None:
            return
        session.add(
            FieldObservation(
                entity_type="company",
                entity_id=company.id,
                field_name="domain",
                value={
                    "domain": domain,
                    "website": f"https://{domain}",
                    "email": candidate.email,
                    "conflict": conflict,
                },
                value_status=(
                    FieldValueStatus.REQUIRES_MANUAL_REVIEW
                    if conflict
                    else FieldValueStatus.OBSERVED
                ),
                source="document",
                evidence_id=evidence.id,
                confidence=Decimal("0.50") if conflict else Decimal("0.85"),
                collected_at=datetime.now(UTC),
                fingerprint=fingerprint,
            )
        )
        await session.flush()

    async def _detect_llm_events(
        self,
        session: AsyncSession,
        procurement: Procurement,
        document: Document,
        chunk: StoredDocumentChunk,
        *,
        analyses: list[LLMEventAnalysis],
    ) -> tuple[int, int]:
        """Run the evidence-bound LLM analyzer on one chunk and persist findings.

        Any provider or schema failure is logged and skipped so a single bad
        response never blocks deterministic detection or the document.
        """

        event_count = lead_count = 0
        for analysis in analyses:
            if not analysis.evidence_quotes:
                continue
            quote = analysis.evidence_quotes[0]
            quote_start = chunk.start_offset
            quote_end = chunk.end_offset
            if chunk.text:
                position = chunk.text.find(quote.text)
                if position >= 0:
                    quote_start = (chunk.start_offset or 0) + position
                    quote_end = quote_start + len(quote.text)
            page_number = (
                quote.page
                if quote.page is not None and quote.page == chunk.page_number
                else chunk.page_number
            )
            detected = DetectedEvent(
                event_type=analysis.event_type,
                raw_description=quote.text,
                normalized_reason=analysis.reason_summary,
                reason_category=analysis.reason_category,
                evidence=DocumentEvidence(
                    text=quote.text,
                    source_url=document.resolved_url or document.original_url,
                    page_number=page_number,
                    locator=chunk.locator,
                    document_type=document.document_type,
                    published_at=document.published_at,
                    start_offset=quote_start,
                    end_offset=quote_end,
                ),
                confidence=min(analysis.confidence, 0.90),
                requires_manual_review=True,
                company_name=analysis.company_name,
                company_cnpj=analysis.company_cnpj,
                item_number=analysis.item_number,
                occurred_at=analysis.event_date or document.published_at,
            )
            if await self._event_already_exists(session, procurement, detected):
                continue
            event, created = await self._persist_event(
                session, procurement, document, chunk, detected, source="llm"
            )
            event_count += int(created)
            if event.company_id is None:
                await self._auto_link_single_winner(session, procurement, event)
            await refresh_procurement_phase(session, procurement)
            if event.company_id:
                lead_count += int(await self._deadline_and_lead(session, procurement, event))
        return event_count, lead_count

    async def _event_already_exists(
        self,
        session: AsyncSession,
        procurement: Procurement,
        detected: DetectedEvent,
    ) -> bool:
        """Skip an LLM finding equivalent to an event already stored for the process."""

        company = await self._company(session, detected.company_name, detected.company_cnpj)
        item = await self._item(
            session, procurement.id, detected.item_number, context=detected.evidence.text
        )
        conditions = [
            ProcurementEvent.procurement_id == procurement.id,
            ProcurementEvent.event_type == ProcurementEventType(detected.event_type.value),
            (
                ProcurementEvent.company_id == company.id
                if company is not None
                else ProcurementEvent.company_id.is_(None)
            ),
            (
                ProcurementEvent.item_id == item.id
                if item is not None
                else ProcurementEvent.item_id.is_(None)
            ),
        ]
        existing = await session.scalar(select(ProcurementEvent.id).where(*conditions).limit(1))
        return existing is not None

    async def _auto_link_single_winner(
        self,
        session: AsyncSession,
        procurement: Procurement,
        event: ProcurementEvent,
    ) -> bool:
        """Attribute an unattributed winner event when only one company won.

        This is deliberately conservative: it only applies to winner-side event
        types and only when the procurement has exactly one adjudicated company
        across all its items. The link stays flagged for human review and is
        recorded as an inference, never as a confirmed source fact.
        """

        if event.company_id is not None or event.event_type not in _AUTO_LINK_EVENT_TYPES:
            return False
        if (
            await session.scalar(
                select(ProcurementEvent.id).where(ProcurementEvent.id == event.id, event_verified())
            )
            is None
        ):
            return False
        company_ids = list(
            (
                await session.scalars(
                    select(Participant.company_id)
                    .where(
                        Participant.procurement_id == procurement.id,
                        Participant.participation_role.in_(
                            [ParticipantRole.AWARDED, ParticipantRole.WINNER]
                        ),
                        or_(
                            Participant.status_code.in_(
                                [ParticipantStatus.WINNER, ParticipantStatus.AWARDED]
                            ),
                            Participant.status_code.is_(None),
                        ),
                    )
                    .distinct()
                )
            ).all()
        )
        if len(company_ids) != 1:
            return False
        company_id = company_ids[0]
        event.company_id = company_id
        event.requires_manual_review = True
        if event.confidence > Decimal("0.60"):
            event.confidence = Decimal("0.60")
        await self._record_link_observation(
            session,
            event,
            company_id,
            source="inferred_single_winner",
            confidence=Decimal("0.60"),
        )
        await session.flush()
        return True

    async def _record_link_observation(
        self,
        session: AsyncSession,
        event: ProcurementEvent,
        company_id: UUID,
        *,
        source: str,
        confidence: Decimal,
        reviewer: str | None = None,
        note: str | None = None,
    ) -> None:
        """Append the company/event attribution to the audit trail once."""

        fingerprint = stable_fingerprint(
            "event_company_link",
            event.id,
            str(company_id),
            source,
            reviewer or "",
            note or "",
        )
        exists = await session.scalar(
            select(FieldObservation.id).where(FieldObservation.fingerprint == fingerprint)
        )
        if exists is not None:
            return
        value: dict[str, object] = {"company_id": str(company_id)}
        if reviewer:
            value["reviewer"] = reviewer
        if note:
            value["note"] = note
        session.add(
            FieldObservation(
                entity_type="procurement_event",
                entity_id=event.id,
                field_name="company_id",
                value=value,
                value_status=FieldValueStatus.REQUIRES_MANUAL_REVIEW,
                source=source,
                evidence_id=event.evidence_id,
                source_record_id=event.source_record_id,
                confidence=confidence,
                collected_at=datetime.now(UTC),
                fingerprint=fingerprint,
            )
        )

    async def link_event_company(
        self,
        *,
        event_id: UUID,
        company_id: UUID,
        reviewer: str | None = None,
        note: str | None = None,
        expected_procurement_id: UUID | None = None,
        session: AsyncSession | None = None,
    ) -> bool:
        """Auditable manual attribution of an event to an existing participant.

        The company must already participate in the procurement, so the action
        links evidence-backed entities instead of inventing a relationship. When
        a session is supplied the caller owns the transaction boundary.
        """

        if session is not None:
            return await self._link_event_company(
                session,
                event_id=event_id,
                company_id=company_id,
                reviewer=reviewer,
                note=note,
                expected_procurement_id=expected_procurement_id,
            )
        async with write_session(self.session_factory) as owned_session:
            return await self._link_event_company(
                owned_session,
                event_id=event_id,
                company_id=company_id,
                reviewer=reviewer,
                note=note,
                expected_procurement_id=expected_procurement_id,
            )

    async def _link_event_company(
        self,
        session: AsyncSession,
        *,
        event_id: UUID,
        company_id: UUID,
        reviewer: str | None,
        note: str | None,
        expected_procurement_id: UUID | None,
    ) -> bool:
        event = await session.get(ProcurementEvent, event_id)
        if event is None:
            raise LookupError("event not found")
        if expected_procurement_id is not None and event.procurement_id != expected_procurement_id:
            raise LookupError("event does not belong to the informed procurement")
        procurement = await session.get(Procurement, event.procurement_id)
        if procurement is None:
            raise LookupError("procurement not found")
        company = await session.get(Company, company_id)
        if company is None:
            raise LookupError("company not found")
        participates = await session.scalar(
            select(Participant.id)
            .where(
                Participant.procurement_id == procurement.id,
                Participant.company_id == company_id,
            )
            .limit(1)
        )
        if participates is None:
            raise ValueError("a empresa não participa desta contratação")
        event.company_id = company_id
        # Decision: the explicit human action *is* the review of the attribution,
        # so the review flag is cleared to let the lead leave "pending review".
        # The original evidence and the reviewer/note remain in the audit trail
        # (FieldObservation), so nothing is silently lost.
        event.requires_manual_review = False
        await self._record_link_observation(
            session,
            event,
            company_id,
            source="manual",
            confidence=Decimal("1"),
            reviewer=reviewer,
            note=note,
        )
        await session.flush()
        await self._deadline_and_lead(session, procurement, event)
        return True

    async def _evidence(
        self,
        session: AsyncSession,
        procurement: Procurement,
        document: Document,
        chunk: StoredDocumentChunk,
        detected: DocumentEvidence,
    ) -> Evidence:
        fingerprint = stable_fingerprint(
            "evidence",
            procurement.id,
            document.id,
            detected.page_number,
            detected.locator,
            detected.start_offset,
            detected.end_offset,
            detected.text,
        )
        evidence = await session.scalar(select(Evidence).where(Evidence.fingerprint == fingerprint))
        if evidence is not None:
            evidence.document_chunk_id = chunk.id
            evidence.page_number = detected.page_number
            evidence.locator = detected.locator
            evidence.start_offset = detected.start_offset
            evidence.end_offset = detected.end_offset
            evidence.text_excerpt = detected.text
            evidence.source_url = detected.source_url
            return evidence
        if evidence is None:
            evidence = Evidence(
                procurement_id=procurement.id,
                document_id=document.id,
                document_chunk_id=chunk.id,
                page_number=detected.page_number,
                locator=detected.locator,
                text_excerpt=detected.text,
                start_offset=detected.start_offset,
                end_offset=detected.end_offset,
                source_url=detected.source_url,
                fingerprint=fingerprint,
            )
            session.add(evidence)
            await session.flush()
        return evidence

    async def _company(
        self,
        session: AsyncSession,
        name: str | None,
        cnpj_value: str | None,
    ) -> Company | None:
        cnpj = normalize_cnpj(cnpj_value)
        if cnpj and is_valid_cnpj(cnpj):
            company = await session.scalar(select(Company).where(Company.cnpj == cnpj))
            if company is None:
                company = Company(
                    cnpj=cnpj,
                    legal_name=name,
                    normalized_name=normalize_company_name(name) or cnpj,
                    fingerprint=stable_fingerprint("company", cnpj),
                )
                session.add(company)
                await session.flush()
            elif name and not company.legal_name:
                company.legal_name = name
            return company
        if not name:
            return None
        normalized = normalize_company_name(name)
        if not normalized:
            return None
        candidates = list(
            (
                await session.scalars(select(Company).where(Company.normalized_name == normalized))
            ).all()
        )
        return candidates[0] if len(candidates) == 1 else None

    async def _item(
        self,
        session: AsyncSession,
        procurement_id: UUID,
        number: str | None,
        *,
        context: str | None = None,
    ) -> ProcurementItem | None:
        if not number:
            return None
        item = await session.scalar(
            select(ProcurementItem).where(
                ProcurementItem.procurement_id == procurement_id,
                ProcurementItem.item_number == number,
            )
        )
        if item is not None and context is not None:
            from app.services.identifiers import normalize_company_name

            description = normalize_company_name(item.description, strip_legal_suffix=False) or ""
            published = normalize_company_name(context, strip_legal_suffix=False) or ""
            if len(description) < 8 or description not in published:
                return None
        return item

    async def _persist_participant(
        self,
        session: AsyncSession,
        procurement: Procurement,
        document: Document,
        chunk: StoredDocumentChunk,
        detected: DetectedParticipant,
    ) -> bool:
        from app.services.participation_integrity import is_procurement_buyer

        if is_procurement_buyer(detected.company_cnpj, procurement):
            return False
        company = await self._company(
            session,
            detected.company_name,
            detected.company_cnpj,
        )
        if company is None or is_procurement_buyer(company.cnpj, procurement):
            return False
        evidence = await self._evidence(
            session,
            procurement,
            document,
            chunk,
            detected.evidence,
        )
        item = await self._item(
            session, procurement.id, detected.item_number, context=detected.evidence.text
        )
        role = ParticipantRole(detected.role.value)
        fingerprint = stable_fingerprint(
            "participant_document",
            procurement.id,
            company.id,
            item.id if item else None,
            role.value,
            detected.status,
        )
        exists = await session.scalar(
            select(Participant).where(Participant.fingerprint == fingerprint)
        )
        if exists is None:
            exists = await session.scalar(
                select(Participant)
                .where(
                    Participant.procurement_id == procurement.id,
                    Participant.company_id == company.id,
                    Participant.source == "document",
                    Participant.participation_role == role,
                    Participant.status == detected.status,
                    Participant.item_id == (item.id if item else None),
                )
                .order_by(Participant.created_at)
                .limit(1)
            )
        if exists is not None:
            from app.services.participation_integrity import audit_change

            await audit_change(
                session,
                entity_type="participant",
                entity_id=exists.id,
                procurement_id=procurement.id,
                field="source_evidence_id",
                previous=exists.source_evidence_id,
                received=evidence.id,
                evidence_id=evidence.id,
                source_record_id=document.source_record_id,
                decision="documentary_observation",
            )
            exists.source_evidence_id = evidence.id
            exists.status = detected.status
            exists.status_code = participant_status_code(detected.role.value, detected.status)
            exists.confidence = Decimal(str(detected.confidence))
            await verify_participant_reference(session, exists, chunk.text, document.document_type)
            return False
        participant = Participant(
            procurement_id=procurement.id,
            company_id=company.id,
            item_id=item.id if item else None,
            participation_role=role,
            status=detected.status,
            status_code=participant_status_code(detected.role.value, detected.status),
            source="document",
            source_evidence_id=evidence.id,
            confidence=Decimal(str(detected.confidence)),
            fingerprint=fingerprint,
        )
        session.add(participant)
        await session.flush()
        await verify_participant_reference(session, participant, chunk.text, document.document_type)
        return True

    async def _persist_event(
        self,
        session: AsyncSession,
        procurement: Procurement,
        document: Document,
        chunk: StoredDocumentChunk,
        detected: DetectedEvent,
        *,
        source: str = "document",
    ) -> tuple[ProcurementEvent, bool]:
        from app.services.event_detection.participation_values import reference_status
        from app.services.participation_integrity import is_procurement_buyer, verify_fact

        ancillary = bool(
            is_procurement_buyer(detected.company_cnpj, procurement)
            or (
                detected.company_cnpj
                and reference_status(detected.evidence.text, detected.company_cnpj) == "invalidated"
            )
        )
        if ancillary:
            detected = replace(
                detected,
                company_name=None,
                company_cnpj=None,
                company_external_id=None,
                requires_manual_review=True,
            )
        company: Company | None = None
        external_id = detected.company_external_id
        if external_id:
            try:
                company = await session.get(Company, UUID(external_id))
            except ValueError:
                company = None
        company = company or await self._company(
            session,
            detected.company_name,
            detected.company_cnpj,
        )
        if company and is_procurement_buyer(company.cnpj, procurement):
            ancillary, company = True, None
        item = await self._item(
            session, procurement.id, detected.item_number, context=detected.evidence.text
        )
        evidence = await self._evidence(
            session,
            procurement,
            document,
            chunk,
            detected.evidence,
        )
        event_type = ProcurementEventType(detected.event_type.value)
        fingerprint = stable_fingerprint(
            "event",
            procurement.id,
            event_type.value,
            company.id if company else None,
            item.id if item else None,
            detected.evidence.text,
        )
        event = await session.scalar(
            select(ProcurementEvent).where(ProcurementEvent.fingerprint == fingerprint)
        )
        if event is not None:
            event.evidence_id = evidence.id
            event.raw_description = detected.raw_description
            event.normalized_reason = detected.normalized_reason
            event.occurred_at = detected.occurred_at
            event.confidence = Decimal(str(detected.confidence))
            event.requires_manual_review = detected.requires_manual_review
            if ancillary:
                await verify_fact(
                    session,
                    "procurement_event",
                    event.id,
                    procurement.id,
                    "review",
                    "A atribuição da empresa não tem comprovação documental; revisar o evento.",
                    evidence_id=evidence.id,
                )
            return event, False
        event = ProcurementEvent(
            procurement_id=procurement.id,
            company_id=company.id if company else None,
            item_id=item.id if item else None,
            event_type=event_type,
            occurred_at=detected.occurred_at,
            published_at=document.published_at,
            raw_description=detected.raw_description,
            normalized_reason=detected.normalized_reason,
            reason_category=ReasonCategory(detected.reason_category.value),
            source=source,
            source_url=document.resolved_url or document.original_url,
            evidence_id=evidence.id,
            confidence=Decimal(str(detected.confidence)),
            requires_manual_review=detected.requires_manual_review,
            fingerprint=fingerprint,
        )
        session.add(event)
        await session.flush()
        if ancillary:
            await verify_fact(
                session,
                "procurement_event",
                event.id,
                procurement.id,
                "review",
                "A atribuição da empresa não tem comprovação documental; revisar o evento.",
                evidence_id=evidence.id,
            )
        return event, True

    async def _persist_participation_event(
        self,
        session: AsyncSession,
        procurement: Procurement,
        document: Document,
        chunk: StoredDocumentChunk,
        detected: DetectedParticipant,
    ) -> tuple[ProcurementEvent | None, bool]:
        """Create one evidence-bound event per company detected in an open process.

        The fingerprint ignores the evidence on purpose: the same company must
        not generate a new lead for every chunk/document that mentions it.
        """

        from app.services.participation_integrity import is_procurement_buyer

        if is_procurement_buyer(detected.company_cnpj, procurement):
            return None, False
        company = await self._company(session, detected.company_name, detected.company_cnpj)
        if company is None or is_procurement_buyer(company.cnpj, procurement):
            return None, False
        item = await self._item(
            session, procurement.id, detected.item_number, context=detected.evidence.text
        )
        fingerprint = stable_fingerprint("participation_event", procurement.id, company.id)
        event = await session.scalar(
            select(ProcurementEvent).where(ProcurementEvent.fingerprint == fingerprint)
        )
        if (
            event is not None
            and await session.scalar(
                select(ProcurementEvent.id).where(ProcurementEvent.id == event.id, event_verified())
            )
            is None
        ):
            fingerprint = stable_fingerprint(
                "participation_event_verified",
                procurement.id,
                company.id,
                document.file_hash or document.id,
                detected.evidence.text,
            )
            event = await session.scalar(
                select(ProcurementEvent).where(ProcurementEvent.fingerprint == fingerprint)
            )
        if event is not None:
            return event, False
        evidence = await self._evidence(
            session,
            procurement,
            document,
            chunk,
            detected.evidence,
        )
        event = ProcurementEvent(
            procurement_id=procurement.id,
            company_id=company.id,
            item_id=item.id if item else None,
            event_type=ProcurementEventType.PARTICIPATION_DETECTED,
            occurred_at=None,
            published_at=document.published_at,
            raw_description=" ".join(detected.evidence.text.split())[:2000],
            normalized_reason=None,
            reason_category=ReasonCategory.UNKNOWN,
            source="document",
            source_url=document.resolved_url or document.original_url,
            evidence_id=evidence.id,
            confidence=Decimal(str(detected.confidence)),
            requires_manual_review=detected.requires_manual_review,
            fingerprint=fingerprint,
        )
        session.add(event)
        await session.flush()
        return event, True

    def _document_deadline(
        self,
        text: str,
        trigger: datetime | None,
        event_type: ProcurementEventType | None = None,
    ) -> datetime | None:
        match = _EXPLICIT_DATE.search(text)
        if match is not None:
            hour = int(match.group("hour") or 23)
            minute = int(match.group("minute") or 59)
            try:
                local = datetime(
                    int(match.group("year")),
                    int(match.group("month")),
                    int(match.group("day")),
                    hour,
                    minute,
                    59 if match.group("hour") is None else 0,
                    tzinfo=ZoneInfo(self.settings.timezone),
                )
            except ValueError:
                return None
            if trigger and local.year < trigger.astimezone(ZoneInfo(self.settings.timezone)).year:
                return None
            return local.astimezone(UTC)
        if trigger is None or event_type not in _RELATIVE_DEADLINE_EVENTS:
            return None
        timezone = ZoneInfo(self.settings.timezone)
        local_trigger = trigger.astimezone(timezone)
        relative = _RELATIVE_DEADLINE.search(text)
        if relative is not None:
            amount = int(relative.group("amount"))
            unit = relative.group("unit").lower()
            delta = (
                timedelta(minutes=amount)
                if unit.startswith(("min", "m"))
                else timedelta(hours=amount)
            )
            return (local_trigger + delta).astimezone(UTC)
        same_day = _SAME_DAY_TIME.search(text)
        if same_day is not None:
            try:
                local = local_trigger.replace(
                    hour=int(same_day.group("hour")),
                    minute=int(same_day.group("minute")),
                    second=0,
                    microsecond=0,
                )
            except ValueError:
                return None
            if local <= local_trigger:
                local += timedelta(days=1)
            return local.astimezone(UTC)
        return None

    async def _deadline_and_lead(
        self,
        session: AsyncSession,
        procurement: Procurement,
        event: ProcurementEvent,
    ) -> bool:
        if not await event_document_is_active(session, event):
            evidence = await session.get(Evidence, event.evidence_id) if event.evidence_id else None
            if evidence is not None and evidence.document_id is not None:
                await suspend_document_derivatives(session, evidence.document_id)
            return False
        if not await event_award_inference_is_current(session, event):
            await suspend_event_work(
                session,
                [event],
                "Resultado oficial cancelado; revisar a atribuição da empresa ao evento.",
            )
            return False
        company = await session.get(Company, event.company_id) if event.company_id else None
        if company is None:
            return False
        trigger = event.occurred_at or event.published_at
        explicit = self._document_deadline(event.raw_description, trigger, event.event_type)
        result = self.deadline_engine.calculate(
            DeadlineRequest(
                event_type=event.event_type.value,
                trigger_at=trigger,
                document_deadline_at=explicit,
                legal_business_days=3,
                legal_estimate_applicable=True,
                legal_basis="Lei 14.133/2021; estimativa operacional sujeita a conferência",
                trigger_source=event.source_url,
                uf=procurement.uf,
                timezone=self.settings.timezone,
                timezone_inferred=False,
                local_holiday_calendar_complete=self.settings.holiday_calendar_path.exists(),
            )
        )
        deadline_fingerprint = stable_fingerprint("deadline", event.id, "event_response")
        deadline = await session.scalar(
            select(Deadline).where(Deadline.fingerprint == deadline_fingerprint)
        )
        if deadline is None:
            deadline = Deadline(
                procurement_id=procurement.id,
                company_id=company.id,
                event_id=event.id,
                deadline_type="event_response",
                legal_basis=result.legal_basis,
                trigger_at=result.trigger_at,
                explicit_deadline_at=result.explicit_deadline_at,
                estimated_deadline_at=result.estimated_deadline_at,
                calculation_method=DeadlineCalculationMethod(result.method.value),
                remaining_seconds=result.remaining_seconds,
                status=DeadlineStatus(result.status.value),
                confidence=Decimal(str(result.confidence)),
                requires_manual_review=result.requires_manual_review,
                calculation_explanation=result.calculation_explanation,
                fingerprint=deadline_fingerprint,
            )
            session.add(deadline)
            await session.flush()
        else:
            deadline.company_id = company.id
            deadline.legal_basis = result.legal_basis
            deadline.trigger_at = result.trigger_at
            deadline.explicit_deadline_at = result.explicit_deadline_at
            deadline.estimated_deadline_at = result.estimated_deadline_at
            deadline.calculation_method = DeadlineCalculationMethod(result.method.value)
            deadline.remaining_seconds = result.remaining_seconds
            deadline.status = DeadlineStatus(result.status.value)
            deadline.confidence = Decimal(str(result.confidence))
            deadline.requires_manual_review = result.requires_manual_review
            deadline.calculation_explanation = result.calculation_explanation

        fingerprint = stable_fingerprint("lead", procurement.id, company.id, event.id)
        lead = await session.scalar(select(Lead).where(Lead.fingerprint == fingerprint))
        created = lead is None
        if lead is None:
            lead = Lead(
                procurement_id=procurement.id,
                company_id=company.id,
                triggering_event_id=event.id,
                fingerprint=fingerprint,
                score=0,
                urgency_score=0,
                evidence_score=0,
                legal_relevance_score=0,
                contact_score=0,
                economic_value_score=0,
                fit_score=0,
                scoring_version="2.0",
                reason_summary="pending",
            )
            session.add(lead)
        lead.deadline_id = deadline.id
        lead.reason_summary = event.normalized_reason or event.raw_description[:1000]
        if created:
            lead.recommended_action = "Revisar a evidência e confirmar o prazo na fonte oficial."
        await session.flush()
        from app.services.lead_scoring.evaluation import evaluate_lead

        evaluation = await evaluate_lead(
            session,
            lead,
            origin="processing",
            minimum_score=self.settings.min_lead_score,
            initial=created,
        )
        if created and "skipped" in evaluation:
            await session.delete(lead)
            return False
        return created
