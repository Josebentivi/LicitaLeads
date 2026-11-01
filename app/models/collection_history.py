"""Incremental collection state and append-only, field-level provenance."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import JSON, Boolean, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import GUID, Base, TimestampMixin, UTCDateTime, UUIDPrimaryKeyMixin, utc_now


class FieldState(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "field_states"
    __table_args__ = (UniqueConstraint("entity_type", "entity_id", "field_name"),)

    entity_type: Mapped[str] = mapped_column(String(60), nullable=False)
    entity_id: Mapped[UUID] = mapped_column(GUID(), nullable=False)
    field_name: Mapped[str] = mapped_column(String(100), nullable=False)
    value: Mapped[Any | None] = mapped_column(JSON)
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL")
    )
    source_updated_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    timestamp_origin: Mapped[str | None] = mapped_column(String(100))
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    uncertain: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)


class FieldChange(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "field_changes"
    __table_args__ = (
        Index("ix_field_changes_parent_time", "parent_type", "parent_id", "observed_at"),
        Index("ix_field_changes_entity", "entity_type", "entity_id"),
    )

    entity_type: Mapped[str] = mapped_column(String(60), nullable=False)
    entity_id: Mapped[UUID] = mapped_column(GUID(), nullable=False)
    parent_type: Mapped[str] = mapped_column(String(60), nullable=False)
    parent_id: Mapped[UUID] = mapped_column(GUID(), nullable=False)
    field_name: Mapped[str] = mapped_column(String(100), nullable=False)
    previous_value: Mapped[Any | None] = mapped_column(JSON)
    received_value: Mapped[Any | None] = mapped_column(JSON)
    decision: Mapped[str] = mapped_column(String(40), nullable=False)
    applied: Mapped[bool] = mapped_column(Boolean, nullable=False)
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    previous_source: Mapped[str | None] = mapped_column(String(100))
    previous_source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL")
    )
    source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL")
    )
    evidence_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("evidence.id", ondelete="SET NULL")
    )
    crawl_run_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("crawl_runs.id", ondelete="SET NULL")
    )
    previous_source_updated_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    source_updated_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    timestamp_origin: Mapped[str | None] = mapped_column(String(100))
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)


class CollectionBatch(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "collection_batches"
    __table_args__ = (Index("ix_collection_batches_stream", "stream_key", "status"),)

    stream_key: Mapped[str] = mapped_column(String(64), nullable=False)
    filters: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="pending", nullable=False)
    cursors: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    last_run_id: Mapped[UUID | None] = mapped_column(GUID())


class CollectionWorkItem(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "collection_work_items"
    __table_args__ = (
        UniqueConstraint("batch_id", "source", "kind", "identity"),
        Index("ix_collection_work_pending", "batch_id", "source", "status"),
    )

    batch_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("collection_batches.id", ondelete="CASCADE"), nullable=False
    )
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    kind: Mapped[str] = mapped_column(String(60), default="record", nullable=False)
    identity: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL")
    )
    status: Mapped[str] = mapped_column(String(30), default="pending", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    position: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)
