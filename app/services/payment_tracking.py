"""Select federal payment targets from the operator list and legacy settings."""

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Company, PaymentTrackingTarget
from app.services.identifiers import is_valid_cnpj, normalize_cnpj


@dataclass(frozen=True, slots=True)
class PaymentTrackingEntry:
    cnpj: str
    company_name: str | None
    active: bool
    configured: bool


async def _tracking_state(
    session: AsyncSession, configured: list[str]
) -> tuple[dict[str, bool], set[str]]:
    statuses = {
        cnpj: True
        for value in configured
        if (cnpj := normalize_cnpj(value)) is not None and is_valid_cnpj(cnpj)
    }
    configured_cnpjs = set(statuses)
    overrides = await session.execute(
        select(PaymentTrackingTarget.cnpj, PaymentTrackingTarget.active).order_by(
            PaymentTrackingTarget.cnpj
        )
    )
    statuses.update((cnpj, active) for cnpj, active in overrides)
    return statuses, configured_cnpjs


async def list_payment_tracking(
    session: AsyncSession, configured: list[str]
) -> list[PaymentTrackingEntry]:
    statuses, configured_cnpjs = await _tracking_state(session, configured)
    if not statuses:
        return []
    names = {
        cnpj: name
        for cnpj, name in await session.execute(
            select(Company.cnpj, Company.legal_name).where(Company.cnpj.in_(statuses))
        )
    }
    return [
        PaymentTrackingEntry(cnpj, names.get(cnpj), active, cnpj in configured_cnpjs)
        for cnpj, active in sorted(statuses.items())
    ]


async def tracked_payment_cnpjs(session: AsyncSession, configured: list[str]) -> list[str]:
    statuses, _ = await _tracking_state(session, configured)
    return [cnpj for cnpj, active in statuses.items() if active]


async def set_payment_tracking(session: AsyncSession, cnpj: str, *, active: bool) -> str:
    canonical = normalize_cnpj(cnpj)
    if canonical is None or not is_valid_cnpj(canonical):
        raise ValueError("Informe um CNPJ válido.")

    insert = postgresql_insert if session.get_bind().dialect.name == "postgresql" else sqlite_insert
    statement = insert(PaymentTrackingTarget).values(cnpj=canonical, active=active)
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=[PaymentTrackingTarget.cnpj],
            set_={"active": active},
            where=PaymentTrackingTarget.active != active,
        )
    )
    return canonical
