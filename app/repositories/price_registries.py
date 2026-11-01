"""Repositories for official price registries (ARP/atas) and their items."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from uuid import UUID

from sqlalchemy import Select, exists, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import (
    PriceRegistry,
    PriceRegistryItem,
)
from app.repositories.base import BaseRepository, Page


class PriceRegistryRepository(BaseRepository[PriceRegistry]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, PriceRegistry)

    async def get_detail(self, registry_id: UUID | str) -> PriceRegistry | None:
        return await self.session.scalar(
            select(PriceRegistry)
            .where(PriceRegistry.id == registry_id)
            .options(
                selectinload(PriceRegistry.items).selectinload(PriceRegistryItem.company),
                selectinload(PriceRegistry.units),
                selectinload(PriceRegistry.commitments),
                selectinload(PriceRegistry.adhesions),
                selectinload(PriceRegistry.procurement),
            )
        )

    async def list_filtered(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        search: str | None = None,
        agency: str | None = None,
        agency_cnpj: str | None = None,
        registry_number: str | None = None,
        status: str | None = None,
        supplier_cnpj: str | None = None,
        valid_on: date | None = None,
        expiring_within_days: int | None = None,
        allows_adhesion: bool | None = None,
    ) -> Page[PriceRegistry]:
        statement: Select[tuple[PriceRegistry]] = select(PriceRegistry)
        if search:
            term = f"%{search}%"
            statement = statement.where(
                or_(
                    PriceRegistry.object_description.ilike(term),
                    PriceRegistry.registry_number.ilike(term),
                    PriceRegistry.agency_name.ilike(term),
                    exists(
                        select(PriceRegistryItem.id).where(
                            PriceRegistryItem.price_registry_id == PriceRegistry.id,
                            or_(
                                PriceRegistryItem.description.ilike(term),
                                PriceRegistryItem.catalog_code.ilike(term),
                                PriceRegistryItem.supplier_name.ilike(term),
                            ),
                        )
                    ),
                )
            )
        if agency:
            statement = statement.where(PriceRegistry.agency_name.ilike(f"%{agency}%"))
        if agency_cnpj:
            statement = statement.where(PriceRegistry.agency_cnpj == agency_cnpj)
        if registry_number:
            statement = statement.where(PriceRegistry.registry_number == registry_number)
        if status:
            statement = statement.where(PriceRegistry.status == status)
        if supplier_cnpj:
            statement = statement.where(
                exists(
                    select(PriceRegistryItem.id).where(
                        PriceRegistryItem.price_registry_id == PriceRegistry.id,
                        PriceRegistryItem.supplier_cnpj == supplier_cnpj,
                    )
                )
            )
        if valid_on:
            day_start = datetime.combine(valid_on, time.min, tzinfo=UTC)
            day_end = datetime.combine(valid_on, time.max, tzinfo=UTC)
            statement = statement.where(
                PriceRegistry.valid_from <= day_end,
                PriceRegistry.valid_until >= day_start,
            )
        if expiring_within_days is not None:
            now = datetime.now(UTC)
            statement = statement.where(
                PriceRegistry.valid_until >= now,
                PriceRegistry.valid_until <= now + timedelta(days=expiring_within_days),
            )
        if allows_adhesion is not None:
            statement = statement.where(PriceRegistry.allows_adhesion.is_(allows_adhesion))
        statement = statement.order_by(
            PriceRegistry.published_at.desc().nullslast(),
            PriceRegistry.valid_from.desc().nullslast(),
            PriceRegistry.id,
        )
        return await self.page(statement, page=page, page_size=page_size)
