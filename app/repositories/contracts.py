"""Read repositories for contracts and payment documents."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

from sqlalchemy import Select, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import Contract, Payment
from app.repositories.base import BaseRepository, Page

_PAYMENT_PHASES = ("pagamento", "pago", "pago_agregado", "3")


def contract_value(contract: Contract) -> Decimal | None:
    """Choose a published contract value without treating an official zero as absent."""

    return next(
        (
            value
            for value in (contract.global_value, contract.accumulated_value, contract.initial_value)
            if value is not None
        ),
        None,
    )


def _actual_payment_phase():
    """Return a SQL condition for payments, excluding commitments/liquidations."""

    phase = func.lower(func.trim(func.coalesce(Payment.phase, "")))
    return or_(
        phase.in_(_PAYMENT_PHASES),
        phase.like("pagamento%"),
    )


class ContractRepository(BaseRepository[Contract]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Contract)

    async def get_detail(self, contract_id: UUID | str) -> Contract | None:
        return await self.session.scalar(
            select(Contract)
            .where(Contract.id == contract_id)
            .options(
                selectinload(Contract.company),
                selectinload(Contract.procurement),
                selectinload(Contract.price_registry),
                selectinload(Contract.payments.and_(Payment.is_current.is_(True))),
                selectinload(Contract.items),
                selectinload(Contract.terms),
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
        supplier_cnpj: str | None = None,
        contract_number: str | None = None,
        valid_on: date | None = None,
        status: str | None = None,
        source: str | None = None,
    ) -> Page[Contract]:
        statement: Select[tuple[Contract]] = select(Contract)
        if search:
            term = f"%{search}%"
            statement = statement.where(
                or_(
                    Contract.object_description.ilike(term),
                    Contract.contract_number.ilike(term),
                    Contract.agency_name.ilike(term),
                    Contract.supplier_name.ilike(term),
                    Contract.process_number.ilike(term),
                )
            )
        if agency:
            statement = statement.where(Contract.agency_name.ilike(f"%{agency}%"))
        if agency_cnpj:
            statement = statement.where(Contract.agency_cnpj == agency_cnpj)
        if supplier_cnpj:
            statement = statement.where(Contract.supplier_cnpj == supplier_cnpj)
        if contract_number:
            statement = statement.where(Contract.contract_number == contract_number)
        if status:
            statement = statement.where(Contract.status == status)
        if source:
            statement = statement.where(Contract.source == source)
        if valid_on:
            day_start = datetime.combine(valid_on, time.min, tzinfo=UTC)
            day_end = datetime.combine(valid_on, time.max, tzinfo=UTC)
            statement = statement.where(
                Contract.valid_from <= day_end,
                Contract.valid_until >= day_start,
            )
        statement = statement.order_by(
            Contract.published_at.desc().nullslast(),
            Contract.signed_at.desc().nullslast(),
            Contract.id,
        )
        return await self.page(statement, page=page, page_size=page_size)

    async def payment_total(self, contract_id: UUID) -> Decimal:
        """Sum only documents that represent an actual payment.

        The federal connector can also import commitments (empenho) and
        liquidations for the same contract.  Adding those rows to ``PAGO``
        would turn budget execution into payment evidence.
        """

        value = await self.session.scalar(
            select(func.coalesce(func.sum(Payment.amount), 0)).where(
                Payment.contract_id == contract_id,
                Payment.is_current.is_(True),
                _actual_payment_phase(),
            )
        )
        return Decimal(value or 0)

    async def payment_summary(
        self, contract: Contract, *, grace_days: int = 60
    ) -> dict[str, object]:
        """Summarize located payments without asserting default.

        "sem_pagamento_localizado" only appears after the grace window; a young
        contract or a missing contract value is "nao_avaliado".
        """

        paid = await self.payment_total(contract.id)
        contracted = contract_value(contract)
        if contracted is None:
            status = "nao_avaliado"
            balance = None
        else:
            balance = contracted - paid
            if paid >= contracted:
                status = "pago"
            elif paid > 0:
                status = "parcial"
            else:
                start = contract.signed_at or contract.published_at or contract.valid_from
                if start is not None and datetime.now(UTC) - start >= timedelta(days=grace_days):
                    status = "sem_pagamento_localizado"
                else:
                    status = "nao_avaliado"
        return {
            "paid_value": float(paid),
            "contracted_value": float(contracted) if contracted is not None else None,
            "balance": float(balance) if balance is not None else None,
            "status": status,
            "grace_days": grace_days,
            "disclaimer": (
                "Pagamento nao localizado na fonte consultada nao prova inadimplencia; "
                "confirme a execucao financeira antes de qualquer cobranca."
            ),
        }


class PaymentRepository(BaseRepository[Payment]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Payment)

    async def list_filtered(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        supplier_cnpj: str | None = None,
        agency_cnpj: str | None = None,
        contract_id: UUID | None = None,
        phase: str | None = None,
        match_status: str | None = None,
        source: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> Page[Payment]:
        statement = (
            select(Payment)
            .where(Payment.is_current.is_(True))
            .options(selectinload(Payment.contract), selectinload(Payment.company))
        )
        if supplier_cnpj:
            statement = statement.where(Payment.supplier_cnpj == supplier_cnpj)
        if source:
            statement = statement.where(Payment.source == source)
        if agency_cnpj:
            statement = statement.where(Payment.agency_cnpj == agency_cnpj)
        if contract_id:
            statement = statement.where(Payment.contract_id == contract_id)
        if phase:
            statement = statement.where(Payment.phase == phase)
        if match_status:
            statement = statement.where(Payment.match_status == match_status)
        if date_from:
            statement = statement.where(Payment.payment_date >= date_from)
        if date_to:
            statement = statement.where(Payment.payment_date <= date_to)
        statement = statement.order_by(
            Payment.payment_date.desc().nullslast(), Payment.created_at.desc(), Payment.id
        )
        return await self.page(statement, page=page, page_size=page_size)
