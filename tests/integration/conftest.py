"""Isolated SQLite and ASGI fixtures for integration tests."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass

import httpx
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.database import create_database_engine, create_session_factory
from app.dependencies import get_db
from app.main import app
from app.models import Base


@dataclass(frozen=True, slots=True)
class DatabaseContext:
    """Resources owned by one test database."""

    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]


@pytest_asyncio.fixture
async def database(tmp_path) -> AsyncIterator[DatabaseContext]:
    """Create a fresh file-backed SQLite schema for every integration test."""

    database_path = (tmp_path / "integration.sqlite3").as_posix()
    engine = create_database_engine(f"sqlite+aiosqlite:///{database_path}")
    sessions = create_session_factory(engine)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield DatabaseContext(engine=engine, sessions=sessions)
    finally:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
        await engine.dispose()


@pytest_asyncio.fixture
async def api_client(database: DatabaseContext) -> AsyncIterator[httpx.AsyncClient]:
    """Serve the FastAPI app while routing request sessions to the isolated database."""

    async def override_get_db() -> AsyncIterator[AsyncSession]:
        async with database.sessions() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = override_get_db
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client
    finally:
        app.dependency_overrides.pop(get_db, None)
