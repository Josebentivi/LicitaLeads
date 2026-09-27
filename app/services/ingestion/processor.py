"""Persist extracted documents, evidence, events, deadlines, and lead candidates."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import urlparse
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from app.config import Settings, get_settings
from app.database import async_session_factory
from app.models import (
    Company,
    CompanyContact,
    Deadline,
    DeadlineCalculationMethod,
    DeadlineStatus,
    Document,
    Evidence,
    ExtractionStatus,
    FieldObservation,
    FieldValueStatus,
    Lead,
    LeadStatus,
    Participant,
    ParticipantRole,
    Procurement,
    ProcurementEvent,
    ProcurementEventType,
    ProcurementItem,
    ReasonCategory,
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
from app.services.documents.security import ArchiveLimits
from app.services.documents.types import DocumentExtraction, DocumentExtractionStatus
from app.services.event_detection import (
    CompanyReference,
    DetectedEvent,
    DetectedParticipant,
    DocumentEvidence,
    EventDetector,
    ParticipantDetector,
    associate_company,
)
from app.services.identifiers import is_valid_cnpj, normalize_cnpj, normalize_company_name
from app.services.ingestion.documents import SecureDocumentDownloader, store_by_hash
from app.services.ingestion.pipeline import stable_fingerprint
from app.services.lead_scoring import LeadScoreInput, LeadScorer
from app.services.llm import LLMEventAnalyzer, build_event_analyzer

logger = logging.getLogger(__name__)

_EXPLICIT_DATE = re.compile(
    r"\b(?:at[eé]|prazo(?:\s+final)?(?:\s+em|\s+at[eé])?|vence(?:\s+em)?)\s*"
    r"(?P<day>[0-3]?\d)[/.-](?P<month>[01]?\d)[/.-](?P<year>20\d{2})"
    r"(?:\s*(?:[àa]s?|,)?\s*(?P<hour>[0-2]?\d)(?::|h)(?P<minute>[0-5]\d)?)?\b",
    re.IGNORECASE,
)

# Events that describe the winning/awarded side and can be attributed to the
# single adjudicated company without guessing which loser they refer to.
_AUTO_LINK_EVENT_TYPES = {
    ProcurementEventType.HOMOLOGATED,
    ProcurementEventType.ADJUDICATED,
    ProcurementEventType.WINNER_DECLARED,
    ProcurementEventType.QUALIFIED,
}


@dataclass(slots=True)
class ProcessingSummary:
    documents_processed: int = 0
    documents_failed: int = 0
    events_created: int = 0
    participants_created: int = 0
    leads_created: int = 0


class DocumentProcessingService:
    """Turn downloaded official documents into traceable, reviewable facts."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        downloader: SecureDocumentDownloader | None = None,
        llm_analyzer: LLMEventAnalyzer | None = None,
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
            archive_limits=ArchiveLimits(
                max_members=self.settings.archive_max_members,
                max_uncompressed_bytes=self.settings.archive_max_bytes,
                max_compression_ratio=100,
                max_member_bytes=self.settings.document_max_bytes,
            )
        )
        self.event_detector = EventDetector()
        self.participant_detector = ParticipantDetector()
        if self.settings.holiday_calendar_path.exists():
            calendar = BrazilBusinessCalendar.from_csv(self.settings.holiday_calendar_path)
        else:
            calendar = BrazilBusinessCalendar()
        self.deadline_engine = DeadlineEngine(calendar)
        self.scorer = LeadScorer()
        self.llm_analyzer = llm_analyzer or build_event_analyzer(self.settings)

    async def aclose(self) -> None:
        if self._owns_downloader:
            await self.downloader.aclose()
        provider = getattr(self.llm_analyzer, "provider", None)
        provider_close = getattr(provider, "aclose", None)
        if callable(provider_close):
            await provider_close()

    async def process_pending(self, *, limit: int | None = None) -> ProcessingSummary:
        """Process a stable snapshot of pending documents, one transaction each."""

        statement = (
            select(Document.id)
            .where(Document.extraction_status == ExtractionStatus.PENDING)
            .order_by(Document.created_at, Document.id)
        )
        if limit is not None:
            statement = statement.limit(limit)
        async with self.session_factory() as session:
            identifiers = list((await session.scalars(statement)).all())

        summary = ProcessingSummary()
        try:
            for document_id in identifiers:
                try:
                    await self._process_one(document_id, summary)
                    summary.documents_processed += 1
                except Exception as exc:
                    summary.documents_failed += 1
                    await self._mark_failed(document_id, exc)
        finally:
            await self.aclose()
        return summary

    async def detect_existing(self, *, limit: int | None = None) -> ProcessingSummary:
        """Idempotently rerun deterministic detection on already-extracted text."""

        statement = (
            select(Document.id)
            .where(Document.extraction_status == ExtractionStatus.EXTRACTED)
            .order_by(Document.created_at, Document.id)
        )
        if limit is not None:
            statement = statement.limit(limit)
        async with self.session_factory() as session:
            identifiers = list((await session.scalars(statement)).all())
        summary = ProcessingSummary()
        for document_id in identifiers:
            async with self.session_factory() as session, session.begin():
                document = await session.get(Document, document_id)
                if document is None:
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
        for event_id in identifiers:
            async with self.session_factory() as session, session.begin():
                event = await session.get(ProcurementEvent, event_id)
                if event is None:
                    continue
                procurement = await session.get(Procurement, event.procurement_id)
                if procurement is None:
                    continue
                summary.leads_created += int(
                    await self._deadline_and_lead(session, procurement, event)
                )
        return summary

    async def _process_one(self, document_id: UUID, summary: ProcessingSummary) -> None:
        async with self.session_factory() as session:
            document = await session.get(Document, document_id)
            if document is None or document.extraction_status != ExtractionStatus.PENDING:
                return
            original_url = document.original_url
            declared_mime = document.mime_type

        downloaded = await self.downloader.download(original_url)
        extraction = self.extractor.extract(
            downloaded.content,
            filename=downloaded.filename,
            mime_type=downloaded.declared_mime or declared_mime,
        )
        local_path = store_by_hash(
            downloaded.content,
            extraction.sha256,
            self.settings.document_storage_path,
            downloaded.filename,
        )
        async with self.session_factory() as session, session.begin():
            document = await session.scalar(
                select(Document)
                .where(Document.id == document_id)
                .options(selectinload(Document.procurement))
            )
            if document is None:
                return
            document.original_url = downloaded.url
            document.file_hash = extraction.sha256
            document.mime_type = extraction.mime_type
            document.local_path = str(local_path.resolve())
            document.downloaded_at = datetime.now(UTC)
            document.extracted_text = extraction.text or None
            document.extraction_status = self._stored_status(extraction.status)
            document.extraction_error = "; ".join(extraction.warnings) or None
            await self._replace_chunks(session, document, extraction)

            targets = [document]
            if extraction.archive_members:
                targets = []
                for member in extraction.archive_members:
                    child = await self._persist_archive_member(
                        session,
                        document,
                        member.filename,
                        member.extraction,
                    )
                    targets.append(child)
            for target in targets:
                events, participants, leads = await self._detect_and_persist(session, target)
                summary.events_created += events
                summary.participants_created += participants
                summary.leads_created += leads

    async def _mark_failed(self, document_id: UUID, exc: Exception) -> None:
        async with self.session_factory() as session, session.begin():
            document = await session.get(Document, document_id)
            if document is not None:
                document.extraction_status = ExtractionStatus.FAILED
                document.extraction_error = f"{type(exc).__name__}: {exc}"[:4000]

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
                file_hash=extraction.sha256,
                mime_type=extraction.mime_type,
                extracted_text=extraction.text or None,
                extraction_status=self._stored_status(extraction.status),
                extraction_error="; ".join(extraction.warnings) or None,
                published_at=parent.published_at,
                downloaded_at=parent.downloaded_at,
                fingerprint=fingerprint,
            )
            session.add(child)
            await session.flush()
        await self._replace_chunks(session, child, extraction)
        return child

    async def _detect_and_persist(
        self,
        session: AsyncSession,
        document: Document,
    ) -> tuple[int, int, int]:
        if document.extraction_status != ExtractionStatus.EXTRACTED:
            return 0, 0, 0
        procurement = await session.get(Procurement, document.procurement_id)
        if procurement is None:
            return 0, 0, 0
        companies = list(
            (
                await session.scalars(
                    select(Company)
                    .join(Participant, Participant.company_id == Company.id)
                    .where(Participant.procurement_id == procurement.id)
                )
            )
            .unique()
            .all()
        )
        references = [
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
        event_count = participant_count = lead_count = 0
        llm_chunks_used = 0
        for chunk in chunks:
            source_evidence = DocumentEvidence(
                text=chunk.text,
                source_url=document.original_url,
                page_number=chunk.page_number,
                locator=chunk.locator,
                document_type=document.document_type,
                published_at=document.published_at,
                start_offset=chunk.start_offset,
                end_offset=chunk.end_offset,
            )
            participants = self.participant_detector.detect(
                source_evidence,
                known_companies=references,
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
            # Refresh references so an event in the same chunk can bind to a just-seen CNPJ.
            companies = list(
                (
                    await session.scalars(
                        select(Company)
                        .join(Participant, Participant.company_id == Company.id)
                        .where(Participant.procurement_id == procurement.id)
                    )
                )
                .unique()
                .all()
            )
            references = [
                CompanyReference(
                    str(company.id), company.legal_name or company.normalized_name, company.cnpj
                )
                for company in companies
            ]
            events = self.event_detector.detect(source_evidence, known_companies=references)
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
                if event.company_id:
                    lead_count += int(await self._deadline_and_lead(session, procurement, event))

            if self._llm_should_run(chunk, llm_chunks_used):
                llm_chunks_used += 1
                llm_events, llm_leads = await self._detect_llm_events(
                    session, procurement, document, chunk
                )
                event_count += llm_events
                lead_count += llm_leads

            if self.settings.contact_domain_discovery_enabled:
                await self._discover_company_domains(
                    session, procurement, document, chunk, references
                )
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
                source_url=document.original_url,
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

    def _llm_should_run(self, chunk: StoredDocumentChunk, used: int) -> bool:
        if not self.settings.llm_enabled:
            return False
        if used >= self.settings.llm_max_chunks_per_document:
            return False
        return len(chunk.text) >= self.settings.llm_min_chunk_characters

    async def _detect_llm_events(
        self,
        session: AsyncSession,
        procurement: Procurement,
        document: Document,
        chunk: StoredDocumentChunk,
    ) -> tuple[int, int]:
        """Run the evidence-bound LLM analyzer on one chunk and persist findings.

        Any provider or schema failure is logged and skipped so a single bad
        response never blocks deterministic detection or the document.
        """

        pages = {chunk.page_number: chunk.text} if chunk.page_number is not None else None
        try:
            analyses = await self.llm_analyzer.analyze(chunk.text, pages=pages)
        except Exception as exc:  # provider/schema/evidence failures are non-fatal
            logger.warning(
                "llm_analysis_skipped",
                extra={"document_id": str(document.id), "error": type(exc).__name__},
            )
            return 0, 0

        event_count = lead_count = 0
        for analysis in analyses:
            if not analysis.evidence_quotes:
                continue
            quote = analysis.evidence_quotes[0]
            detected = DetectedEvent(
                event_type=analysis.event_type,
                raw_description=quote.text,
                normalized_reason=analysis.reason_summary,
                reason_category=analysis.reason_category,
                evidence=DocumentEvidence(
                    text=quote.text,
                    source_url=document.original_url,
                    page_number=quote.page if quote.page is not None else chunk.page_number,
                    locator=chunk.locator,
                    document_type=document.document_type,
                    published_at=document.published_at,
                    start_offset=chunk.start_offset,
                    end_offset=chunk.end_offset,
                ),
                confidence=analysis.confidence,
                requires_manual_review=analysis.requires_manual_review,
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
        item = await self._item(session, procurement.id, detected.item_number)
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
        company_ids = list(
            (
                await session.scalars(
                    select(Participant.company_id)
                    .where(
                        Participant.procurement_id == procurement.id,
                        Participant.participation_role.in_(
                            [ParticipantRole.AWARDED, ParticipantRole.WINNER]
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

        fingerprint = stable_fingerprint("event_company_link", event.id, str(company_id), source)
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
        async with self.session_factory() as owned_session, owned_session.begin():
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
            chunk.id,
            detected.start_offset,
            detected.end_offset,
            detected.text,
        )
        evidence = await session.scalar(select(Evidence).where(Evidence.fingerprint == fingerprint))
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
    ) -> ProcurementItem | None:
        if not number:
            return None
        return await session.scalar(
            select(ProcurementItem).where(
                ProcurementItem.procurement_id == procurement_id,
                ProcurementItem.item_number == number,
            )
        )

    async def _persist_participant(
        self,
        session: AsyncSession,
        procurement: Procurement,
        document: Document,
        chunk: StoredDocumentChunk,
        detected: DetectedParticipant,
    ) -> bool:
        company = await self._company(
            session,
            detected.company_name,
            detected.company_cnpj,
        )
        if company is None:
            return False
        evidence = await self._evidence(
            session,
            procurement,
            document,
            chunk,
            detected.evidence,
        )
        item = await self._item(session, procurement.id, detected.item_number)
        role = ParticipantRole(detected.role.value)
        fingerprint = stable_fingerprint(
            "participant_document",
            procurement.id,
            company.id,
            item.id if item else None,
            role.value,
            evidence.id,
        )
        exists = await session.scalar(
            select(Participant).where(Participant.fingerprint == fingerprint)
        )
        if exists is not None:
            return False
        session.add(
            Participant(
                procurement_id=procurement.id,
                company_id=company.id,
                item_id=item.id if item else None,
                participation_role=role,
                status=detected.status,
                source="document",
                source_evidence_id=evidence.id,
                confidence=Decimal(str(detected.confidence)),
                fingerprint=fingerprint,
            )
        )
        await session.flush()
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
        item = await self._item(session, procurement.id, detected.item_number)
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
            evidence.id,
        )
        event = await session.scalar(
            select(ProcurementEvent).where(ProcurementEvent.fingerprint == fingerprint)
        )
        if event is not None:
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
            source_url=document.original_url,
            evidence_id=evidence.id,
            confidence=Decimal(str(detected.confidence)),
            requires_manual_review=detected.requires_manual_review,
            fingerprint=fingerprint,
        )
        session.add(event)
        await session.flush()
        return event, True

    def _document_deadline(self, text: str, reference: datetime | None) -> datetime | None:
        match = _EXPLICIT_DATE.search(text)
        if match is None:
            return None
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
        if reference and local.year < reference.astimezone(ZoneInfo(self.settings.timezone)).year:
            return None
        return local.astimezone(UTC)

    async def _deadline_and_lead(
        self,
        session: AsyncSession,
        procurement: Procurement,
        event: ProcurementEvent,
    ) -> bool:
        company = await session.get(Company, event.company_id) if event.company_id else None
        if company is None:
            return False
        trigger = event.occurred_at or event.published_at
        explicit = self._document_deadline(event.raw_description, trigger)
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

        contact_types = tuple(
            item.value
            for item in (
                await session.scalars(
                    select(CompanyContact.contact_type).where(
                        CompanyContact.company_id == company.id,
                        CompanyContact.is_corporate.is_(True),
                    )
                )
            ).all()
        )
        closed = any(
            token in (procurement.status or "").lower()
            for token in ("encerr", "cancel", "anulad", "revogad")
        )
        score = self.scorer.score(
            LeadScoreInput(
                event_type=event.event_type.value,
                deadline_status=deadline.status.value,
                hours_remaining=(
                    deadline.remaining_seconds / 3600
                    if deadline.remaining_seconds is not None
                    else None
                ),
                evidence_confidence=float(event.confidence),
                has_direct_evidence=event.evidence_id is not None,
                contact_types=contact_types,
                estimated_value=procurement.estimated_value,
                company_confirmed=True,
                cnpj_present=bool(company.cnpj),
                reason_known=event.reason_category != ReasonCategory.UNKNOWN,
                deadline_estimated=deadline.estimated_deadline_at is not None,
                process_closed=closed,
                deadline_expired=deadline.status == DeadlineStatus.EXPIRED,
            )
        )
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
                reason_summary="pending",
            )
            session.add(lead)
        lead.deadline_id = deadline.id
        lead.score = score.score
        lead.urgency_score = score.urgency_score
        lead.evidence_score = score.evidence_score
        lead.legal_relevance_score = score.legal_relevance_score
        lead.contact_score = score.contact_score
        lead.economic_value_score = score.economic_value_score
        lead.fit_score = score.score
        lead.scoring_version = score.scoring_version
        lead.score_breakdown = dict(score.breakdown)
        lead.reason_summary = event.normalized_reason or event.raw_description[:1000]
        lead.recommended_action = "Revisar a evidência e confirmar o prazo na fonte oficial."
        if score.score < self.settings.min_lead_score:
            lead.lead_status = LeadStatus.BELOW_THRESHOLD
        elif event.requires_manual_review or deadline.requires_manual_review:
            lead.lead_status = LeadStatus.PENDING_REVIEW
        elif lead.lead_status in {LeadStatus.BELOW_THRESHOLD, LeadStatus.PENDING_REVIEW}:
            lead.lead_status = LeadStatus.NEW
        await session.flush()
        return created
