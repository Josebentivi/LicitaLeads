"""Shared SQLAlchemy primitives for the persistence layer."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import CHAR, DateTime, MetaData, String
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import TypeDecorator

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


def utc_now() -> datetime:
    """Return an aware UTC timestamp suitable for application-side defaults."""

    return datetime.now(UTC)


class GUID(TypeDecorator[UUID]):
    """A UUID type that uses PostgreSQL UUID and canonical strings elsewhere."""

    impl = CHAR
    cache_ok = True

    def load_dialect_impl(self, dialect: Any) -> Any:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PostgreSQLUUID(as_uuid=True))
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value: UUID | str | None, dialect: Any) -> UUID | str | None:
        if value is None:
            return None
        parsed = value if isinstance(value, UUID) else UUID(str(value))
        if dialect.name == "postgresql":
            return parsed
        return str(parsed)

    def process_result_value(self, value: UUID | str | None, dialect: Any) -> UUID | None:
        if value is None:
            return None
        return value if isinstance(value, UUID) else UUID(str(value))


class UTCDateTime(TypeDecorator[datetime]):
    """Persist UTC and always return timezone-aware values, including on SQLite."""

    impl = DateTime
    cache_ok = True

    def load_dialect_impl(self, dialect: Any) -> Any:
        return dialect.type_descriptor(DateTime(timezone=True))

    def process_bind_param(self, value: datetime | None, dialect: Any) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def process_result_value(self, value: datetime | None, dialect: Any) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


class Base(DeclarativeBase):
    """Declarative base with deterministic constraint names for Alembic."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class UUIDPrimaryKeyMixin:
    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), default=utc_now, onupdate=utc_now, nullable=False
    )


class FingerprintMixin:
    """Content-derived identity populated by ingestion services when available."""

    fingerprint: Mapped[str | None] = mapped_column(String(64), unique=True, index=True)
