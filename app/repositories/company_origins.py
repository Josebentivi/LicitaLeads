"""Observed company activity, using the same exact supplier identity as the dossier."""

from typing import TypedDict
from uuid import UUID

from sqlalchemy import and_, case, func, literal, or_, select, union_all
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.municipal_payments.sources import MUNICIPAL_SOURCES
from app.models import (
    Company,
    Contract,
    Participant,
    Payment,
    PriceRegistry,
    PriceRegistryItem,
    Procurement,
)
from app.services.company_overview import (
    contract_procurement_link,
    registry_procurement_link,
    supplier_identity,
)
from app.services.participation_integrity import participant_current

ORIGIN_LABELS = {
    "contracts": "Contratos",
    "payments": "Execução financeira",
    "price_registries": "Atas de registro de preços",
    "participations": "Participações documentadas",
}


def activity_records():
    portal = case(
        {f"municipal:{s.id}": f"{s.municipality}/{s.uf}" for s in MUNICIPAL_SOURCES},
        value=Payment.source,
        else_=None,
    )
    procurement_place = case(
        (Procurement.municipality.is_(None), Procurement.uf),
        (Procurement.uf.is_(None), Procurement.municipality),
        else_=Procurement.municipality + "/" + Procurement.uf,
    )
    return union_all(
        select(
            Company.id.label("company_id"),
            Contract.id.label("record_id"),
            literal("contracts").label("origin"),
            Contract.agency_name.label("agency"),
            procurement_place.label("locality"),
            literal(0).label("aggregate"),
        )
        .join_from(Company, Contract, supplier_identity(Contract, Company))
        .outerjoin(
            Procurement,
            and_(
                contract_procurement_link(),
                or_(
                    Contract.agency_cnpj.is_(None),
                    Procurement.agency_cnpj.is_(None),
                    Contract.agency_cnpj == Procurement.agency_cnpj,
                ),
            ),
        ),
        select(
            Company.id,
            Payment.id,
            literal("payments"),
            Payment.agency_name,
            portal,
            case(
                (
                    or_(Payment.status == "agregado_por_empenho", Payment.phase == "pago_agregado"),
                    1,
                ),
                else_=0,
            ),
        )
        .join_from(Company, Payment, supplier_identity(Payment, Company))
        .where(Payment.is_current),
        select(
            Company.id,
            PriceRegistry.id,
            literal("price_registries"),
            PriceRegistry.agency_name,
            procurement_place,
            literal(0),
        )
        .join_from(Company, PriceRegistryItem, supplier_identity(PriceRegistryItem, Company))
        .join(PriceRegistry, PriceRegistry.id == PriceRegistryItem.price_registry_id)
        .outerjoin(
            Procurement,
            and_(
                registry_procurement_link(),
                or_(
                    PriceRegistry.agency_cnpj.is_(None),
                    Procurement.agency_cnpj.is_(None),
                    PriceRegistry.agency_cnpj == Procurement.agency_cnpj,
                ),
            ),
        ),
        select(
            Participant.company_id,
            Procurement.id,
            literal("participations"),
            Procurement.agency_name,
            procurement_place,
            literal(0),
        )
        .join_from(Participant, Procurement)
        .where(participant_current()),
    ).subquery()


def companies_with_activity(origin: str | None, location: str | None):
    records = activity_records().c
    statement = select(records.company_id)
    if origin:
        statement = statement.where(records.origin == origin)
    if location:
        statement = statement.where(
            or_(
                records.locality.icontains(location, autoescape=True),
                records.agency.icontains(location, autoescape=True),
            )
        )
    return statement


class ActivitySummary(TypedDict):
    counts: dict[str, int]
    locations: list[str]
    aggregates: int


async def page_activity(
    session: AsyncSession, company_ids: list[UUID]
) -> dict[UUID, ActivitySummary]:
    summaries: dict[UUID, ActivitySummary] = {
        identifier: {"counts": {}, "locations": [], "aggregates": 0} for identifier in company_ids
    }
    if not summaries:
        return summaries
    r = activity_records().c
    rows = await session.execute(
        select(
            r.company_id,
            r.origin,
            r.agency,
            r.locality,
            func.count(func.distinct(r.record_id)).label("record_count"),
            func.sum(r.aggregate).label("aggregate_count"),
        )
        .where(r.company_id.in_(company_ids))
        .group_by(r.company_id, r.origin, r.agency, r.locality)
    )
    for row in rows:
        summary = summaries[row.company_id]
        counts = summary["counts"]
        counts[row.origin] = counts.get(row.origin, 0) + row.record_count
        summary["locations"].extend(label for label in (row.locality, row.agency) if label)
        summary["aggregates"] += row.aggregate_count
    for summary in summaries.values():
        summary["locations"] = sorted(set(summary["locations"]))
    return summaries
