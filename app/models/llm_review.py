"""AI suggestions are observations, never operational facts until reviewed."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import JSON, ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import GUID, Base, UTCDateTime, UUIDPrimaryKeyMixin, utc_now


class LLMSuggestion(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "llm_suggestions"
    fingerprint: Mapped[str] = mapped_column(String(64), unique=True)
    procurement_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("procurements.id", ondelete="CASCADE"), index=True
    )
    document_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("documents.id", ondelete="CASCADE"), index=True
    )
    chunk_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("document_chunks.id", ondelete="SET NULL")
    )
    event_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("procurement_events.id", ondelete="SET NULL")
    )
    company_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("companies.id", ondelete="SET NULL"), index=True
    )
    analysis: Mapped[dict[str, Any]] = mapped_column(JSON)
    snapshot: Mapped[dict[str, Any]] = mapped_column(JSON)
    evidence_ids: Mapped[list[str]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(20), default="pending", index=True)
    reason: Mapped[str] = mapped_column(String(100))
    reviewer: Mapped[str | None] = mapped_column(String(200))
    note: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)
    reviewed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class LLMEventCitation(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "llm_event_citations"
    __table_args__ = (UniqueConstraint("event_id", "evidence_id"),)
    event_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("procurement_events.id", ondelete="CASCADE"), index=True
    )
    evidence_id: Mapped[UUID] = mapped_column(GUID(), ForeignKey("evidence.id", ondelete="CASCADE"))
