"""Append-only snapshots of prospectively calculated lead priorities."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import JSON, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import GUID, Base, UTCDateTime, UUIDPrimaryKeyMixin, utc_now


class LeadScoreRevision(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "lead_score_revisions"
    __table_args__ = (
        Index("ix_lead_score_revisions_lead_time", "lead_id", "calculated_at"),
        UniqueConstraint("lead_id", "request_id", name="uq_lead_score_revision_request"),
    )

    lead_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("leads.id", ondelete="CASCADE"), nullable=False
    )
    request_id: Mapped[UUID | None] = mapped_column(GUID())
    scoring_version: Mapped[str] = mapped_column(String(50), nullable=False)
    previous_score: Mapped[int | None] = mapped_column(Integer)
    previous_version: Mapped[str | None] = mapped_column(String(50))
    score: Mapped[int] = mapped_column(Integer, nullable=False)
    components: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    inputs: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    references: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    origin: Mapped[str] = mapped_column(String(60), nullable=False)
    calculated_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)


class LeadScoreRequest(Base):
    """Idempotency receipt, including selections with ineligible historical leads."""

    __tablename__ = "lead_score_requests"
    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True)
    company_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    lead_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    results: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)
