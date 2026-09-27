"""FastAPI dependency helpers."""

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session_factory


async def get_db() -> AsyncIterator[AsyncSession]:
    """Yield a transaction-safe database session per request."""
    async with async_session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
