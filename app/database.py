"""Async SQLAlchemy engine, sessions, and SQLite connection hardening."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings
from app.models import Base


def normalize_async_database_url(database_url: str) -> str:
    """Translate user-friendly sync URLs to their async SQLAlchemy variants."""

    if database_url.startswith("sqlite+aiosqlite:"):
        return database_url
    if database_url.startswith("sqlite:"):
        return database_url.replace("sqlite:", "sqlite+aiosqlite:", 1)
    if database_url.startswith("postgresql+psycopg:"):
        return database_url
    if database_url.startswith("postgresql:"):
        return database_url.replace("postgresql:", "postgresql+psycopg:", 1)
    if database_url.startswith("postgres:"):
        return database_url.replace("postgres:", "postgresql+psycopg:", 1)
    return database_url


def _ensure_sqlite_parent(database_url: str) -> None:
    """Create the parent of a file-backed SQLite database, if applicable."""

    if not database_url.startswith("sqlite") or ":memory:" in database_url:
        return
    path_fragment = database_url.split("///", 1)
    if len(path_fragment) != 2 or not path_fragment[1]:
        return
    # Query parameters are not part of the filesystem path.
    database_path = Path(path_fragment[1].split("?", 1)[0])
    database_path.parent.mkdir(parents=True, exist_ok=True)


def _apply_sqlite_pragmas(dbapi_connection: Any, connection_record: Any) -> None:
    """Enable integrity and bounded locking behavior on each SQLite connection."""

    del connection_record
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.execute("PRAGMA synchronous=NORMAL")
        # WAL is persistent for file databases and gracefully falls back for :memory:.
        cursor.execute("PRAGMA journal_mode=WAL")
    finally:
        cursor.close()


def create_database_engine(database_url: str, *, echo: bool = False) -> AsyncEngine:
    """Build a configured async engine for SQLite or PostgreSQL."""

    async_url = normalize_async_database_url(database_url)
    _ensure_sqlite_parent(async_url)
    kwargs: dict[str, Any] = {"echo": echo, "pool_pre_ping": True}
    if async_url.startswith("sqlite"):
        kwargs["connect_args"] = {"timeout": 30, "check_same_thread": False}
    created_engine = create_async_engine(async_url, **kwargs)
    if async_url.startswith("sqlite"):
        event.listen(created_engine.sync_engine, "connect", _apply_sqlite_pragmas)
    return created_engine


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create the short-lived session factory used by API, CLI, and jobs."""

    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        autoflush=False,
        expire_on_commit=False,
    )


settings = get_settings()
engine = create_database_engine(settings.database_url, echo=settings.app_env == "development")
async_session_factory = create_session_factory(engine)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI-compatible session dependency with rollback on failure."""

    async with async_session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """Provide an explicit transaction boundary for CLI and background jobs."""

    async with async_session_factory() as session:
        async with session.begin():
            yield session


async def create_schema(target_engine: AsyncEngine | None = None) -> None:
    """Create all tables for isolated tests; deployed databases should use Alembic."""

    selected_engine = target_engine or engine
    async with selected_engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)


async def drop_schema(target_engine: AsyncEngine | None = None) -> None:
    """Drop all tables, intended only for isolated test databases."""

    selected_engine = target_engine or engine
    async with selected_engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)


async def check_database(
    raise_on_error: bool = True,
    target_engine: AsyncEngine | None = None,
) -> bool:
    """Check connectivity, optionally propagating the original driver error."""

    selected_engine = target_engine or engine
    try:
        async with selected_engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except Exception:
        if raise_on_error:
            raise
        return False
    return True


async def database_is_ready(target_engine: AsyncEngine | None = None) -> bool:
    """Backward-compatible non-raising readiness check."""

    return await check_database(raise_on_error=False, target_engine=target_engine)


async def dispose_database(target_engine: AsyncEngine | None = None) -> None:
    """Release pooled connections during application shutdown or tests."""

    await (target_engine or engine).dispose()


def is_sqlite(target_engine: AsyncEngine | Engine | None = None) -> bool:
    """Identify the active dialect without opening an additional connection."""

    selected_engine = target_engine or engine
    return selected_engine.dialect.name == "sqlite"
