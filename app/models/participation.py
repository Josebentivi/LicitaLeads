"""Evidence-bound participation facts, document aliases and repair checkpoints."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import (
    GUID,
    Base,
    FingerprintMixin,
    TimestampMixin,
    UTCDateTime,
    UUIDPrimaryKeyMixin,
    utc_now,
)


class FactVerification(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "fact_verifications"
    __table_args__ = (
        UniqueConstraint("entity_type", "entity_id"),
        CheckConstraint(
            "status IN ('confirmed','review','invalidated')", name="verification_status"
        ),
    )
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False)
    entity_id: Mapped[UUID] = mapped_column(GUID(), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    rule_version: Mapped[str] = mapped_column(String(60), nullable=False)
    evidence_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("evidence.id", ondelete="SET NULL")
    )
    source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL")
    )


class ParticipantValue(UUIDPrimaryKeyMixin, FingerprintMixin, Base):
    __tablename__ = "participant_values"
    __table_args__ = (
        CheckConstraint("amount >= 0", name="amount_nonnegative"),
        CheckConstraint(
            "kind IN ('proposal','initial_proposal','revised_proposal','final_result')",
            name="value_kind",
        ),
        CheckConstraint("scope IN ('procurement','lot','item','lot_item')", name="value_scope"),
        CheckConstraint("verification_status IN ('confirmed','review')", name="value_verification"),
        CheckConstraint(
            "scope NOT IN ('lot','lot_item') OR lot_number IS NOT NULL", name="lot_scope_number"
        ),
        Index("ix_participant_values_company_procurement", "company_id", "procurement_id"),
    )
    procurement_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("procurements.id", ondelete="CASCADE"), nullable=False
    )
    company_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    participant_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("participants.id", ondelete="SET NULL")
    )
    document_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False
    )
    evidence_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("evidence.id", ondelete="CASCADE"), nullable=False
    )
    subject_evidence_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("evidence.id", ondelete="CASCADE"), nullable=False
    )
    source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL")
    )
    official_item_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("procurement_items.id", ondelete="SET NULL")
    )
    kind: Mapped[str] = mapped_column(String(30), nullable=False)
    scope: Mapped[str] = mapped_column(String(20), nullable=False)
    lot_number: Mapped[str | None] = mapped_column(String(100))
    local_item_number: Mapped[str | None] = mapped_column(String(100))
    amount: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    verification_status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="confirmed"
    )
    document_hash: Mapped[str | None] = mapped_column(String(64))
    rule_version: Mapped[str] = mapped_column(String(60), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, default=utc_now)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, default=utc_now)


class DocumentIdentity(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "document_identities"
    __table_args__ = (UniqueConstraint("source", "official_key"),)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    official_key: Mapped[str] = mapped_column(String(200), nullable=False)
    procurement_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("procurements.id", ondelete="CASCADE"), nullable=False
    )
    canonical_document_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False
    )
    header_hash: Mapped[str | None] = mapped_column(String(64))
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, default=utc_now)


class DocumentAlias(UUIDPrimaryKeyMixin, FingerprintMixin, Base):
    __tablename__ = "document_aliases"
    identity_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("document_identities.id", ondelete="CASCADE"), nullable=False, index=True
    )
    document_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False, index=True
    )
    source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL")
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    equivalent: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    proof: Mapped[str] = mapped_column(String(60), nullable=False)


class ReconciliationRun(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "reconciliation_runs"
    version: Mapped[str] = mapped_column(String(80), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="running")
    checkpoint: Mapped[str | None] = mapped_column(String(36))
    totals: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    backup: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, default=utc_now)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
