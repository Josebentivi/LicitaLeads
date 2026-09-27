"""Reusable asynchronous repository primitives."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Base


@dataclass(frozen=True, slots=True)
class Page[ModelT: Base]:
    """Stable pagination result shared by API and UI services."""

    items: Sequence[ModelT]
    page: int
    page_size: int
    total: int

    @property
    def pages(self) -> int:
        return (self.total + self.page_size - 1) // self.page_size if self.total else 0


def pagination_bounds(page: int, page_size: int, *, maximum: int = 500) -> tuple[int, int]:
    """Validate public pagination parameters and return offset and bounded size."""

    if page < 1:
        raise ValueError("page must be greater than or equal to 1")
    if page_size < 1 or page_size > maximum:
        raise ValueError(f"page_size must be between 1 and {maximum}")
    return (page - 1) * page_size, page_size


class BaseRepository[ModelT: Base]:
    """Small unit-of-work friendly repository; methods never commit implicitly."""

    def __init__(self, session: AsyncSession, model: type[ModelT]) -> None:
        self.session = session
        self.model = model

    async def get(self, entity_id: UUID | str, *, for_update: bool = False) -> ModelT | None:
        statement = select(self.model).where(self.model.id == entity_id)  # type: ignore[attr-defined]
        if for_update:
            statement = statement.with_for_update()
        return await self.session.scalar(statement)

    async def get_required(self, entity_id: UUID | str) -> ModelT:
        entity = await self.get(entity_id)
        if entity is None:
            raise LookupError(f"{self.model.__name__} {entity_id} was not found")
        return entity

    async def get_by_fingerprint(self, fingerprint: str) -> ModelT | None:
        if not hasattr(self.model, "fingerprint"):
            raise TypeError(f"{self.model.__name__} does not define a fingerprint")
        statement = select(self.model).where(
            self.model.fingerprint == fingerprint  # type: ignore[attr-defined]
        )
        return await self.session.scalar(statement)

    async def add(self, entity: ModelT, *, flush: bool = True) -> ModelT:
        self.session.add(entity)
        if flush:
            await self.session.flush()
        return entity

    async def add_all(self, entities: Iterable[ModelT], *, flush: bool = True) -> list[ModelT]:
        materialized = list(entities)
        self.session.add_all(materialized)
        if flush:
            await self.session.flush()
        return materialized

    async def delete(self, entity: ModelT, *, flush: bool = True) -> None:
        await self.session.delete(entity)
        if flush:
            await self.session.flush()

    async def count(self, statement: Select[tuple[ModelT]] | None = None) -> int:
        filtered = statement if statement is not None else select(self.model)
        count_statement = select(func.count()).select_from(filtered.order_by(None).subquery())
        return int(await self.session.scalar(count_statement) or 0)

    async def page(
        self,
        statement: Select[tuple[ModelT]] | None = None,
        *,
        page: int = 1,
        page_size: int = 50,
    ) -> Page[ModelT]:
        offset, limit = pagination_bounds(page, page_size)
        filtered = statement if statement is not None else select(self.model)
        total = await self.count(filtered)
        result = await self.session.scalars(filtered.offset(offset).limit(limit))
        return Page(items=result.unique().all(), page=page, page_size=limit, total=total)

    async def upsert_by_fingerprint(
        self,
        fingerprint: str,
        values: Mapping[str, Any],
    ) -> tuple[ModelT, bool]:
        """Insert or update by a service-generated content identity.

        Returns ``(entity, created)`` and intentionally leaves commit control to
        the caller's transaction.
        """

        entity = await self.get_by_fingerprint(fingerprint)
        if entity is None:
            entity = self.model(fingerprint=fingerprint, **dict(values))
            await self.add(entity)
            return entity, True
        for key, value in values.items():
            if key in {"id", "fingerprint", "created_at"}:
                continue
            if not hasattr(entity, key):
                raise AttributeError(f"{self.model.__name__} has no attribute {key!r}")
            setattr(entity, key, value)
        await self.session.flush()
        return entity, False
