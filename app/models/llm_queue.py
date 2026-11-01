"""Prospective, durable document analysis and per-request accounting."""

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import JSON, Boolean, ForeignKey, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import GUID, Base, UTCDateTime, UUIDPrimaryKeyMixin, utc_now


class LLMBatch(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "llm_batches"
    crawl_run_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("crawl_runs.id", ondelete="CASCADE"), unique=True
    )
    label: Mapped[str] = mapped_column(String(100), nullable=False)
    cancelled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)


class LLMResult(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "llm_results"
    cache_key: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    analyses: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)


class LLMWorkItem(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "llm_work_items"
    job_key: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    cache_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    config_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    document_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False
    )
    chunk_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("document_chunks.id", ondelete="SET NULL")
    )
    snapshot: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="pending", nullable=False, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    next_attempt_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    result_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("llm_results.id", ondelete="SET NULL")
    )
    reused: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class LLMDemand(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "llm_demands"
    __table_args__ = (UniqueConstraint("batch_id", "work_id"),)
    reused: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    batch_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("llm_batches.id", ondelete="CASCADE"), nullable=False, index=True
    )
    work_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("llm_work_items.id", ondelete="CASCADE"), nullable=False, index=True
    )


class LLMProviderState(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "llm_provider_states"
    config_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    blocked_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    cooldown_until: Mapped[datetime | None] = mapped_column(UTCDateTime())
    error: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class LLMAttempt(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "llm_attempts"
    result_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("llm_results.id", ondelete="SET NULL")
    )
    work_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("llm_work_items.id", ondelete="SET NULL")
    )
    batch_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("llm_batches.id", ondelete="SET NULL")
    )
    started_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, nullable=False, index=True
    )
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    model: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="reserved", nullable=False)
    diagnostic: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    input_tokens: Mapped[int | None] = mapped_column(Integer)
    output_tokens: Mapped[int | None] = mapped_column(Integer)
    input_price: Mapped[Decimal] = mapped_column(Numeric(24, 12), nullable=False)
    output_price: Mapped[Decimal] = mapped_column(Numeric(24, 12), nullable=False)
    cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(30, 15))
