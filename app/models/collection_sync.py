"""Prospective coverage and immutable evidence for verified source versions."""

from datetime import date, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import JSON, Date, ForeignKey, Index, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import GUID, Base, UTCDateTime, UUIDPrimaryKeyMixin, utc_now


class CollectionSyncState(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "collection_sync_states"

    stream_key: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    filters: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    covered_until: Mapped[date | None] = mapped_column(Date)
    bootstrap_started_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)
    last_reconciliation_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    pending_request: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    pending_batch_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("collection_batches.id", ondelete="SET NULL")
    )


class VerifiedCollectionVersion(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "verified_collection_versions"
    __table_args__ = (
        Index("ix_verified_collection_identity", "source", "mode", "external_id", "verified_at"),
    )

    source: Mapped[str] = mapped_column(String(50), nullable=False)
    mode: Mapped[str] = mapped_column(String(40), nullable=False)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    adapter_key: Mapped[str] = mapped_column(String(64), nullable=False)
    header_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source_updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    timestamp_origin: Mapped[str] = mapped_column(String(100), nullable=False)
    operations: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    source_records: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    verified_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)
    crawl_run_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("crawl_runs.id", ondelete="SET NULL")
    )


class SourceResponseValidator(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "source_response_validators"

    validator_key: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    source_record_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("source_records.id", ondelete="SET NULL")
    )
    etag: Mapped[str | None] = mapped_column(Text)
    last_modified: Mapped[str | None] = mapped_column(Text)
    validated_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)


class CollectionRecheckState(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "collection_recheck_states"
    __table_args__ = (UniqueConstraint("stream_key", "external_id"),)

    stream_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    attempted_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
