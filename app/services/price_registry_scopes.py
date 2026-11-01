"""Verified agency/unit choices for manual ARP collection, derived from stored sources."""

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Procurement, ProcurementSource
from app.services.identifiers import is_valid_cnpj, normalize_cnpj

SOURCES = ("pncp", "compras_gov")


@dataclass(frozen=True)
class PriceRegistryScope:
    key: str
    uf: str
    agency_cnpj: str
    unit_code: str
    agency_name: str
    sources: tuple[str, ...]

    @property
    def label(self) -> str:
        return f"{self.agency_name} · unidade {self.unit_code} · {self.agency_cnpj}"


def restore_price_registry_scopes(filters: dict[str, Any]) -> tuple[PriceRegistryScope, ...]:
    """Restore the frozen unit selection saved with a crawl, without expanding it."""
    return tuple(
        PriceRegistryScope(**{**item, "sources": tuple(item["sources"])})
        for item in filters.get("price_registry_scopes", ())
    )


async def known_price_registry_scopes(session: AsyncSession, uf: str) -> list[PriceRegistryScope]:
    columns = (Procurement.agency_cnpj, Procurement.uasg, Procurement.agency_name)
    conditions = (
        Procurement.uf == uf,
        Procurement.agency_cnpj.is_not(None),
        Procurement.uasg.is_not(None),
    )
    primary = select(*columns, Procurement.source).where(*conditions)
    linked = (
        select(*columns, ProcurementSource.source)
        .join(ProcurementSource, ProcurementSource.procurement_id == Procurement.id)
        .where(*conditions)
    )
    groups: dict[str, PriceRegistryScope] = {}
    for cnpj, unit, name, source in (await session.execute(primary.union(linked))).all():
        normalized = normalize_cnpj(cnpj)
        unit = str(unit or "").strip()
        if source not in SOURCES or not is_valid_cnpj(normalized) or not unit or len(unit) > 30:
            continue
        assert normalized is not None
        key = f"{uf}:{normalized}:{unit}"
        previous = groups.get(key)
        sources = set(previous.sources if previous else ()) | {source}
        groups[key] = PriceRegistryScope(
            key=key,
            uf=uf,
            agency_cnpj=normalized,
            unit_code=unit,
            agency_name=(previous.agency_name if previous else name) or "Órgão sem nome informado",
            sources=tuple(item for item in SOURCES if item in sources),
        )
    return sorted(
        groups.values(),
        key=lambda item: (item.agency_name.casefold(), item.unit_code, item.agency_cnpj),
    )
