"""Relational domain model for procurements, evidence, deadlines, and leads."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates

from app.models.base import (
    GUID,
    Base,
    FingerprintMixin,
    TimestampMixin,
    UTCDateTime,
    UUIDPrimaryKeyMixin,
    utc_now,
)
from app.models.enums import (
    ContactStatus,
    ContactType,
    CrawlRunStatus,
    DataAvailability,
    DeadlineCalculationMethod,
    DeadlineStatus,
    ExtractionStatus,
    FieldValueStatus,
    LeadStatus,
    OutreachChannel,
    ParticipantRole,
    ProcurementEventType,
    ReasonCategory,
    ReviewDecision,
    database_enum,
)


class Procurement(UUIDPrimaryKeyMixin, TimestampMixin, FingerprintMixin, Base):
    __tablename__ = "procurements"
    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_procurements_source_external_id"),
        CheckConstraint(
            "estimated_value IS NULL OR estimated_value >= 0",
            name="estimated_value_nonnegative",
        ),
        Index("ix_procurements_location", "uf", "municipality"),
        Index(
            "ix_procurements_purchase_identity",
            "agency_cnpj",
            "uasg",
            "purchase_number",
            "purchase_year",
            "modality_key",
        ),
        Index("ix_procurements_dates", "publication_at", "proposal_end_at"),
    )

    source: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    pncp_control_number: Mapped[str | None] = mapped_column(String(100), unique=True, index=True)
    uasg: Mapped[str | None] = mapped_column(String(30), index=True)
    purchase_number: Mapped[str | None] = mapped_column(String(100), index=True)
    purchase_year: Mapped[int | None] = mapped_column(Integer, index=True)
    modality: Mapped[str | None] = mapped_column(String(100), index=True)
    modality_key: Mapped[str | None] = mapped_column(String(100), index=True)
    procurement_type: Mapped[str | None] = mapped_column(String(100))
    title: Mapped[str | None] = mapped_column(String(500))
    object_description: Mapped[str | None] = mapped_column(Text)
    agency_name: Mapped[str | None] = mapped_column(String(500), index=True)
    agency_cnpj: Mapped[str | None] = mapped_column(String(14), index=True)
    government_sphere: Mapped[str | None] = mapped_column(String(100))
    government_branch: Mapped[str | None] = mapped_column(String(100))
    uf: Mapped[str | None] = mapped_column(String(2), index=True)
    municipality: Mapped[str | None] = mapped_column(String(255), index=True)
    estimated_value: Mapped[Decimal | None] = mapped_column(Numeric(19, 2))
    proposal_start_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    proposal_end_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), index=True)
    session_start_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    publication_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), index=True)
    last_source_update_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    status: Mapped[str | None] = mapped_column(String(100), index=True)
    source_url: Mapped[str | None] = mapped_column(Text)

    @validates("modality")
    def _sync_modality_key(self, _key: str, value: str | None) -> str | None:
        """Keep the filterable canonical key aligned with any modality assignment."""

        from app.services.identifiers import canonical_modality

        self.modality_key = canonical_modality(value)
        return value

    sources: Mapped[list[ProcurementSource]] = relationship(
        back_populates="procurement",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="selectin",
    )
    items: Mapped[list[ProcurementItem]] = relationship(
        back_populates="procurement", cascade="all, delete-orphan", passive_deletes=True
    )
    participants: Mapped[list[Participant]] = relationship(
        back_populates="procurement", cascade="all, delete-orphan", passive_deletes=True
    )
    events: Mapped[list[ProcurementEvent]] = relationship(
        back_populates="procurement", cascade="all, delete-orphan", passive_deletes=True
    )
    documents: Mapped[list[Document]] = relationship(
        back_populates="procurement", cascade="all, delete-orphan", passive_deletes=True
    )
    evidence: Mapped[list[Evidence]] = relationship(
        back_populates="procurement", cascade="all, delete-orphan", passive_deletes=True
    )
    deadlines: Mapped[list[Deadline]] = relationship(
        back_populates="procurement", cascade="all, delete-orphan", passive_deletes=True
    )
    leads: Mapped[list[Lead]] = relationship(
        back_populates="procurement", cascade="all, delete-orphan", passive_deletes=True
    )


class CrawlRun(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "crawl_runs"
    __table_args__ = (
        CheckConstraint("records_found >= 0", name="records_found_nonnegative"),
        CheckConstraint("records_created >= 0", name="records_created_nonnegative"),
        CheckConstraint("records_updated >= 0", name="records_updated_nonnegative"),
        Index("ix_crawl_runs_connector_started", "connector", "started_at"),
    )

    connector: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), index=True)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    status: Mapped[CrawlRunStatus] = mapped_column(
        database_enum(CrawlRunStatus), default=CrawlRunStatus.PENDING, nullable=False, index=True
    )
    records_found: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_created: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    records_updated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    errors: Mapped[list[Any]] = mapped_column(
        MutableList.as_mutable(JSON), default=list, nullable=False
    )
    cursor: Mapped[dict[str, Any] | None] = mapped_column(MutableDict.as_mutable(JSON))
    filters: Mapped[dict[str, Any]] = mapped_column(
        MutableDict.as_mutable(JSON), default=dict, nullable=False
    )
    diagnostic: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)

    source_records: Mapped[list[SourceRecord]] = relationship(
        back_populates="crawl_run", passive_deletes=True
    )


class SourceRecord(UUIDPrimaryKeyMixin, FingerprintMixin, Base):
    __tablename__ = "source_records"
    __table_args__ = (
        Index("ix_source_records_source_collected", "source", "collected_at"),
        Index("ix_source_records_response_hash", "response_hash"),
    )

    crawl_run_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("crawl_runs.id", ondelete="SET NULL"), index=True
    )
    source: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    endpoint: Mapped[str] = mapped_column(Text, nullable=False)
    request_parameters: Mapped[dict[str, Any]] = mapped_column(
        MutableDict.as_mutable(JSON), default=dict, nullable=False
    )
    response_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    raw_payload: Mapped[Any | None] = mapped_column(JSON)
    http_status: Mapped[int | None] = mapped_column(Integer)
    availability: Mapped[DataAvailability] = mapped_column(
        database_enum(DataAvailability), default=DataAvailability.AVAILABLE, nullable=False
    )
    collected_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)
    diagnostic: Mapped[str | None] = mapped_column(Text)

    crawl_run: Mapped[CrawlRun | None] = relationship(back_populates="source_records")
    procurement_sources: Mapped[list[ProcurementSource]] = relationship(
        back_populates="source_record"
    )
    observations: Mapped[list[FieldObservation]] = relationship(back_populates="source_record")
    evidence: Mapped[list[Evidence]] = relationship(back_populates="source_record")


class ProcurementSource(UUIDPrimaryKeyMixin, FingerprintMixin, Base):
    __tablename__ = "procurement_sources"
    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_procurement_sources_source_external_id"),
        Index("ix_procurement_sources_procurement_source", "procurement_id", "source"),
    )

    procurement_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("procurements.id", ondelete="CASCADE"), nullable=False, index=True
    )
    source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL"), index=True
    )
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    pncp_control_number: Mapped[str | None] = mapped_column(String(100), index=True)
    endpoint: Mapped[str | None] = mapped_column(Text)
    source_url: Mapped[str | None] = mapped_column(Text)
    collected_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)
    is_primary: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    procurement: Mapped[Procurement] = relationship(back_populates="sources")
    source_record: Mapped[SourceRecord | None] = relationship(back_populates="procurement_sources")


class FieldObservation(UUIDPrimaryKeyMixin, FingerprintMixin, Base):
    __tablename__ = "field_observations"
    __table_args__ = (
        CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name="confidence_range",
        ),
        Index("ix_field_observations_entity", "entity_type", "entity_id", "field_name"),
    )

    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)
    entity_id: Mapped[UUID] = mapped_column(GUID(), nullable=False)
    field_name: Mapped[str] = mapped_column(String(100), nullable=False)
    value: Mapped[Any | None] = mapped_column(JSON)
    value_status: Mapped[FieldValueStatus] = mapped_column(
        database_enum(FieldValueStatus), default=FieldValueStatus.OBSERVED, nullable=False
    )
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL"), index=True
    )
    evidence_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("evidence.id", ondelete="SET NULL"), index=True
    )
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    collected_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)

    source_record: Mapped[SourceRecord | None] = relationship(back_populates="observations")
    evidence: Mapped[Evidence | None] = relationship(back_populates="observations")


class ProcurementItem(UUIDPrimaryKeyMixin, FingerprintMixin, Base):
    __tablename__ = "procurement_items"
    __table_args__ = (
        UniqueConstraint(
            "procurement_id",
            "item_number",
            name="uq_procurement_items_procurement_item_number",
        ),
        CheckConstraint("quantity IS NULL OR quantity >= 0", name="quantity_nonnegative"),
        CheckConstraint(
            "estimated_unit_value IS NULL OR estimated_unit_value >= 0",
            name="unit_value_nonnegative",
        ),
        CheckConstraint(
            "estimated_total_value IS NULL OR estimated_total_value >= 0",
            name="total_value_nonnegative",
        ),
    )

    procurement_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("procurements.id", ondelete="CASCADE"), nullable=False, index=True
    )
    source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL"), index=True
    )
    external_item_id: Mapped[str | None] = mapped_column(String(255))
    item_number: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(19, 4))
    unit: Mapped[str | None] = mapped_column(String(100))
    estimated_unit_value: Mapped[Decimal | None] = mapped_column(Numeric(19, 4))
    estimated_total_value: Mapped[Decimal | None] = mapped_column(Numeric(19, 2))
    result_status: Mapped[str | None] = mapped_column(String(100))

    procurement: Mapped[Procurement] = relationship(back_populates="items")
    participants: Mapped[list[Participant]] = relationship(back_populates="item")
    events: Mapped[list[ProcurementEvent]] = relationship(back_populates="item")


class Company(UUIDPrimaryKeyMixin, TimestampMixin, FingerprintMixin, Base):
    __tablename__ = "companies"
    __table_args__ = (Index("ix_companies_location", "uf", "municipality"),)

    cnpj: Mapped[str | None] = mapped_column(String(14), unique=True, index=True)
    legal_name: Mapped[str | None] = mapped_column(String(500))
    trade_name: Mapped[str | None] = mapped_column(String(500))
    normalized_name: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    uf: Mapped[str | None] = mapped_column(String(2))
    municipality: Mapped[str | None] = mapped_column(String(255))
    website: Mapped[str | None] = mapped_column(Text)
    domain: Mapped[str | None] = mapped_column(String(255), index=True)

    participants: Mapped[list[Participant]] = relationship(back_populates="company")
    events: Mapped[list[ProcurementEvent]] = relationship(back_populates="company")
    deadlines: Mapped[list[Deadline]] = relationship(back_populates="company")
    contacts: Mapped[list[CompanyContact]] = relationship(
        back_populates="company", cascade="all, delete-orphan", passive_deletes=True
    )
    leads: Mapped[list[Lead]] = relationship(back_populates="company")


class Document(UUIDPrimaryKeyMixin, FingerprintMixin, Base):
    __tablename__ = "documents"
    __table_args__ = (
        Index("ix_documents_procurement_type", "procurement_id", "document_type"),
        Index("ix_documents_file_hash", "file_hash"),
    )

    procurement_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("procurements.id", ondelete="CASCADE"), nullable=False, index=True
    )
    parent_document_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("documents.id", ondelete="CASCADE"), index=True
    )
    source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL"), index=True
    )
    document_type: Mapped[str | None] = mapped_column(String(100), index=True)
    title: Mapped[str | None] = mapped_column(String(500))
    original_url: Mapped[str] = mapped_column(Text, nullable=False)
    file_hash: Mapped[str | None] = mapped_column(String(64))
    mime_type: Mapped[str | None] = mapped_column(String(255))
    local_path: Mapped[str | None] = mapped_column(Text)
    extracted_text: Mapped[str | None] = mapped_column(Text)
    extraction_status: Mapped[ExtractionStatus] = mapped_column(
        database_enum(ExtractionStatus),
        default=ExtractionStatus.PENDING,
        nullable=False,
        index=True,
    )
    extraction_error: Mapped[str | None] = mapped_column(Text)
    published_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    downloaded_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)

    procurement: Mapped[Procurement] = relationship(back_populates="documents")
    parent: Mapped[Document | None] = relationship(
        back_populates="children", remote_side="Document.id"
    )
    children: Mapped[list[Document]] = relationship(
        back_populates="parent", cascade="all, delete-orphan", single_parent=True
    )
    chunks: Mapped[list[DocumentChunk]] = relationship(
        back_populates="document",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="DocumentChunk.sequence",
    )
    evidence: Mapped[list[Evidence]] = relationship(back_populates="document")


class DocumentChunk(UUIDPrimaryKeyMixin, FingerprintMixin, Base):
    __tablename__ = "document_chunks"
    __table_args__ = (
        UniqueConstraint("document_id", "sequence", name="uq_document_chunks_document_sequence"),
        CheckConstraint("sequence >= 0", name="sequence_nonnegative"),
        CheckConstraint("page_number IS NULL OR page_number >= 1", name="page_number_positive"),
        CheckConstraint(
            "start_offset IS NULL OR start_offset >= 0", name="start_offset_nonnegative"
        ),
        CheckConstraint("end_offset IS NULL OR end_offset >= 0", name="end_offset_nonnegative"),
    )

    document_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False, index=True
    )
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    chunk_type: Mapped[str] = mapped_column(String(50), default="text", nullable=False)
    page_number: Mapped[int | None] = mapped_column(Integer)
    section: Mapped[str | None] = mapped_column(String(500))
    sheet_name: Mapped[str | None] = mapped_column(String(255))
    cell_range: Mapped[str | None] = mapped_column(String(100))
    locator: Mapped[str | None] = mapped_column(String(500))
    text: Mapped[str] = mapped_column(Text, nullable=False)
    start_offset: Mapped[int | None] = mapped_column(Integer)
    end_offset: Mapped[int | None] = mapped_column(Integer)
    extracted_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)

    document: Mapped[Document] = relationship(back_populates="chunks")
    evidence: Mapped[list[Evidence]] = relationship(back_populates="document_chunk")


class Evidence(UUIDPrimaryKeyMixin, FingerprintMixin, Base):
    __tablename__ = "evidence"
    __table_args__ = (
        CheckConstraint("page_number IS NULL OR page_number >= 1", name="page_number_positive"),
        CheckConstraint(
            "start_offset IS NULL OR start_offset >= 0", name="start_offset_nonnegative"
        ),
        CheckConstraint("end_offset IS NULL OR end_offset >= 0", name="end_offset_nonnegative"),
        CheckConstraint(
            "document_id IS NOT NULL OR source_record_id IS NOT NULL OR source_url IS NOT NULL",
            name="traceable_source_required",
        ),
        Index("ix_evidence_procurement_document", "procurement_id", "document_id"),
    )

    procurement_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("procurements.id", ondelete="CASCADE"), nullable=False, index=True
    )
    document_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("documents.id", ondelete="CASCADE"), index=True
    )
    document_chunk_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("document_chunks.id", ondelete="SET NULL"), index=True
    )
    source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL"), index=True
    )
    page_number: Mapped[int | None] = mapped_column(Integer)
    locator: Mapped[str | None] = mapped_column(String(500))
    text_excerpt: Mapped[str] = mapped_column(Text, nullable=False)
    start_offset: Mapped[int | None] = mapped_column(Integer)
    end_offset: Mapped[int | None] = mapped_column(Integer)
    source_url: Mapped[str | None] = mapped_column(Text)
    collected_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)

    procurement: Mapped[Procurement] = relationship(back_populates="evidence")
    document: Mapped[Document | None] = relationship(back_populates="evidence")
    document_chunk: Mapped[DocumentChunk | None] = relationship(back_populates="evidence")
    source_record: Mapped[SourceRecord | None] = relationship(back_populates="evidence")
    observations: Mapped[list[FieldObservation]] = relationship(back_populates="evidence")


class Participant(UUIDPrimaryKeyMixin, FingerprintMixin, Base):
    __tablename__ = "participants"
    __table_args__ = (
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="confidence_range"),
        CheckConstraint("rank IS NULL OR rank >= 1", name="rank_positive"),
        CheckConstraint(
            "proposal_value IS NULL OR proposal_value >= 0", name="proposal_value_nonnegative"
        ),
        CheckConstraint("final_value IS NULL OR final_value >= 0", name="final_value_nonnegative"),
        Index("ix_participants_procurement_company", "procurement_id", "company_id"),
    )

    procurement_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("procurements.id", ondelete="CASCADE"), nullable=False, index=True
    )
    company_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True
    )
    item_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("procurement_items.id", ondelete="CASCADE"), index=True
    )
    participation_role: Mapped[ParticipantRole] = mapped_column(
        database_enum(ParticipantRole), default=ParticipantRole.PARTICIPANT, nullable=False
    )
    proposal_value: Mapped[Decimal | None] = mapped_column(Numeric(19, 2))
    final_value: Mapped[Decimal | None] = mapped_column(Numeric(19, 2))
    rank: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str | None] = mapped_column(String(100))
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    source_evidence_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("evidence.id", ondelete="SET NULL"), index=True
    )
    source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL"), index=True
    )
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), default=Decimal("1"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)

    procurement: Mapped[Procurement] = relationship(back_populates="participants")
    company: Mapped[Company] = relationship(back_populates="participants")
    item: Mapped[ProcurementItem | None] = relationship(back_populates="participants")
    source_evidence: Mapped[Evidence | None] = relationship(foreign_keys=[source_evidence_id])
    source_record: Mapped[SourceRecord | None] = relationship(foreign_keys=[source_record_id])


class ProcurementEvent(UUIDPrimaryKeyMixin, FingerprintMixin, Base):
    __tablename__ = "procurement_events"
    __table_args__ = (
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="confidence_range"),
        Index("ix_procurement_events_procurement_type", "procurement_id", "event_type"),
        Index("ix_procurement_events_company_type", "company_id", "event_type"),
    )

    procurement_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("procurements.id", ondelete="CASCADE"), nullable=False, index=True
    )
    company_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("companies.id", ondelete="SET NULL"), index=True
    )
    item_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("procurement_items.id", ondelete="SET NULL"), index=True
    )
    event_type: Mapped[ProcurementEventType] = mapped_column(
        database_enum(ProcurementEventType),
        default=ProcurementEventType.UNKNOWN,
        nullable=False,
        index=True,
    )
    occurred_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    published_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    raw_description: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_reason: Mapped[str | None] = mapped_column(Text)
    reason_category: Mapped[ReasonCategory] = mapped_column(
        database_enum(ReasonCategory), default=ReasonCategory.UNKNOWN, nullable=False
    )
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text)
    evidence_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("evidence.id", ondelete="SET NULL"), index=True
    )
    source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL"), index=True
    )
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    requires_manual_review: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)

    procurement: Mapped[Procurement] = relationship(back_populates="events")
    company: Mapped[Company | None] = relationship(back_populates="events")
    item: Mapped[ProcurementItem | None] = relationship(back_populates="events")
    evidence: Mapped[Evidence | None] = relationship(foreign_keys=[evidence_id])
    source_record: Mapped[SourceRecord | None] = relationship(foreign_keys=[source_record_id])
    deadlines: Mapped[list[Deadline]] = relationship(back_populates="event")
    leads: Mapped[list[Lead]] = relationship(back_populates="triggering_event")


class Deadline(UUIDPrimaryKeyMixin, TimestampMixin, FingerprintMixin, Base):
    __tablename__ = "deadlines"
    __table_args__ = (
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="confidence_range"),
        Index("ix_deadlines_procurement_status", "procurement_id", "status"),
        Index("ix_deadlines_effective", "explicit_deadline_at", "estimated_deadline_at"),
    )

    procurement_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("procurements.id", ondelete="CASCADE"), nullable=False, index=True
    )
    company_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("companies.id", ondelete="SET NULL"), index=True
    )
    event_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("procurement_events.id", ondelete="CASCADE"), index=True
    )
    deadline_type: Mapped[str] = mapped_column(String(100), nullable=False)
    legal_basis: Mapped[str | None] = mapped_column(Text)
    trigger_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    explicit_deadline_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    estimated_deadline_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    calculation_method: Mapped[DeadlineCalculationMethod] = mapped_column(
        database_enum(DeadlineCalculationMethod), nullable=False
    )
    remaining_seconds: Mapped[int | None] = mapped_column(BigInteger)
    status: Mapped[DeadlineStatus] = mapped_column(
        database_enum(DeadlineStatus), default=DeadlineStatus.UNKNOWN, nullable=False, index=True
    )
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    requires_manual_review: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    calculation_explanation: Mapped[str] = mapped_column(Text, nullable=False)

    procurement: Mapped[Procurement] = relationship(back_populates="deadlines")
    company: Mapped[Company | None] = relationship(back_populates="deadlines")
    event: Mapped[ProcurementEvent | None] = relationship(back_populates="deadlines")
    leads: Mapped[list[Lead]] = relationship(back_populates="deadline")


class CompanyContact(UUIDPrimaryKeyMixin, FingerprintMixin, Base):
    __tablename__ = "company_contacts"
    __table_args__ = (
        UniqueConstraint(
            "company_id", "contact_type", "contact_value", name="uq_company_contacts_identity"
        ),
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="confidence_range"),
        CheckConstraint("NOT (is_corporate AND is_personal)", name="not_corporate_and_personal"),
        Index("ix_company_contacts_company_status", "company_id", "status"),
    )

    company_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True
    )
    contact_type: Mapped[ContactType] = mapped_column(database_enum(ContactType), nullable=False)
    contact_value: Mapped[str] = mapped_column(String(1000), nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text)
    is_corporate: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_personal: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    verified_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    status: Mapped[ContactStatus] = mapped_column(
        database_enum(ContactStatus), default=ContactStatus.DISCOVERED, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, onupdate=utc_now, nullable=False
    )

    company: Mapped[Company] = relationship(back_populates="contacts")


class Lead(UUIDPrimaryKeyMixin, TimestampMixin, FingerprintMixin, Base):
    __tablename__ = "leads"
    __table_args__ = (
        CheckConstraint("score >= 0 AND score <= 100", name="score_range"),
        CheckConstraint("urgency_score >= 0 AND urgency_score <= 30", name="urgency_score_range"),
        CheckConstraint(
            "evidence_score >= 0 AND evidence_score <= 25", name="evidence_score_range"
        ),
        CheckConstraint(
            "legal_relevance_score >= 0 AND legal_relevance_score <= 20",
            name="legal_score_range",
        ),
        CheckConstraint("contact_score >= 0 AND contact_score <= 15", name="contact_score_range"),
        CheckConstraint(
            "economic_value_score >= 0 AND economic_value_score <= 10",
            name="value_score_range",
        ),
        CheckConstraint("fit_score >= 0 AND fit_score <= 100", name="fit_score_range"),
        Index("ix_leads_status_score", "lead_status", "score"),
        Index("ix_leads_procurement_company", "procurement_id", "company_id"),
    )

    procurement_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("procurements.id", ondelete="CASCADE"), nullable=False, index=True
    )
    company_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True
    )
    triggering_event_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("procurement_events.id", ondelete="CASCADE"), nullable=False, index=True
    )
    deadline_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("deadlines.id", ondelete="SET NULL"), index=True
    )
    lead_status: Mapped[LeadStatus] = mapped_column(
        database_enum(LeadStatus), default=LeadStatus.NEW, nullable=False, index=True
    )
    score: Mapped[int] = mapped_column(Integer, nullable=False)
    urgency_score: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_score: Mapped[int] = mapped_column(Integer, nullable=False)
    legal_relevance_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    contact_score: Mapped[int] = mapped_column(Integer, nullable=False)
    economic_value_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    fit_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    scoring_version: Mapped[str] = mapped_column(String(50), default="v1", nullable=False)
    score_breakdown: Mapped[dict[str, Any]] = mapped_column(
        MutableDict.as_mutable(JSON), default=dict, nullable=False
    )
    reason_summary: Mapped[str] = mapped_column(Text, nullable=False)
    recommended_action: Mapped[str | None] = mapped_column(Text)
    assigned_to: Mapped[str | None] = mapped_column(String(255), index=True)

    procurement: Mapped[Procurement] = relationship(back_populates="leads")
    company: Mapped[Company] = relationship(back_populates="leads")
    triggering_event: Mapped[ProcurementEvent] = relationship(back_populates="leads")
    deadline: Mapped[Deadline | None] = relationship(back_populates="leads")
    reviews: Mapped[list[LeadReview]] = relationship(
        back_populates="lead",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="LeadReview.reviewed_at",
    )
    outreach_drafts: Mapped[list[OutreachDraft]] = relationship(
        back_populates="lead", cascade="all, delete-orphan", passive_deletes=True
    )


class LeadReview(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "lead_reviews"
    __table_args__ = (Index("ix_lead_reviews_lead_reviewed", "lead_id", "reviewed_at"),)

    lead_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("leads.id", ondelete="CASCADE"), nullable=False, index=True
    )
    decision: Mapped[ReviewDecision] = mapped_column(database_enum(ReviewDecision), nullable=False)
    notes: Mapped[str | None] = mapped_column(Text)
    reviewer: Mapped[str | None] = mapped_column(String(255))
    previous_status: Mapped[str | None] = mapped_column(String(50))
    reviewed_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)

    lead: Mapped[Lead] = relationship(back_populates="reviews")


class OutreachDraft(UUIDPrimaryKeyMixin, FingerprintMixin, Base):
    __tablename__ = "outreach_drafts"
    __table_args__ = (
        CheckConstraint("NOT sent OR approved", name="sent_requires_approval"),
        Index("ix_outreach_drafts_lead_created", "lead_id", "created_at"),
    )

    lead_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("leads.id", ondelete="CASCADE"), nullable=False, index=True
    )
    channel: Mapped[OutreachChannel] = mapped_column(database_enum(OutreachChannel), nullable=False)
    subject: Mapped[str | None] = mapped_column(String(500))
    message: Mapped[str] = mapped_column(Text, nullable=False)
    generation_method: Mapped[str] = mapped_column(String(100), nullable=False)
    facts_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    template_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    approved: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    sent: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)

    lead: Mapped[Lead] = relationship(back_populates="outreach_drafts")


class JobLease(Base):
    __tablename__ = "job_leases"
    __table_args__ = (Index("ix_job_leases_expires_at", "expires_at"),)

    name: Mapped[str] = mapped_column(String(255), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(255), nullable=False)
    acquired_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)
    heartbeat_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        MutableDict.as_mutable(JSON), default=dict, nullable=False, server_default=text("'{}'")
    )
