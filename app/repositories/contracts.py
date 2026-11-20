"""Read repositories for contracts and payment documents."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import Select, and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from app.models import Company, Contract, ContractTerm, FieldObservation, Payment
from app.models.entities import MunicipalBudgetEntry
from app.repositories.base import BaseRepository, Page
from app.services.municipal_budget import budget_function_label, public_budget_values

_PAYMENT_PHASES = ("pagamento", "pago", "pago_agregado", "3")
CONTRACT_TERMS_AVAILABILITY_FIELD = "terms_availability"


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


def published_term_dates(contract: Contract) -> list[ContractTerm]:
    return sorted(
        (term for term in contract.terms if term.new_valid_until is not None),
        key=lambda term: (term.new_valid_until, term.id),
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

    async def latest_terms_observation(self, contract_id: UUID) -> FieldObservation | None:
        return await self.session.scalar(
            select(FieldObservation)
            .options(joinedload(FieldObservation.source_record, innerjoin=True))
            .where(
                FieldObservation.entity_type == "contract",
                FieldObservation.entity_id == contract_id,
                FieldObservation.field_name == CONTRACT_TERMS_AVAILABILITY_FIELD,
                FieldObservation.source == "pncp",
            )
            .order_by(FieldObservation.collected_at.desc(), FieldObservation.id.desc())
            .limit(1)
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
        expiring_within_days: int | None = None,
        status: str | None = None,
        source: str | None = None,
    ) -> Page[Contract]:
        statement = select(Contract).options(
            selectinload(Contract.terms).load_only(
                ContractTerm.term_number,
                ContractTerm.new_valid_until,
                ContractTerm.status,
                ContractTerm.source_record_id,
                ContractTerm.source_url,
            )
        )
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
        if expiring_within_days is not None:
            timezone = ZoneInfo("America/Sao_Paulo")
            today = datetime.now(timezone).date()
            start = datetime.combine(today, time.min, timezone)
            stop = datetime.combine(
                today + timedelta(days=expiring_within_days + 1), time.min, timezone
            )
            dates = (
                select(Contract.id.label("contract_id"), Contract.valid_until.label("expiry"))
                .where(Contract.valid_until >= start, Contract.valid_until < stop)
                .union_all(
                    select(ContractTerm.contract_id, ContractTerm.new_valid_until).where(
                        ContractTerm.new_valid_until >= start, ContractTerm.new_valid_until < stop
                    )
                )
                .subquery()
            )
            upcoming = (
                select(dates.c.contract_id, func.min(dates.c.expiry).label("first_date"))
                .group_by(dates.c.contract_id)
                .subquery()
            )
            statement = statement.join(upcoming, upcoming.c.contract_id == Contract.id).order_by(
                upcoming.c.first_date
            )
        statement = statement.order_by(
            Contract.published_at.desc().nullslast(),
            Contract.signed_at.desc().nullslast(),
            Contract.id,
        )
        return await self.page(statement, page=page, page_size=page_size)

    async def _payment_totals(self, contract_id: UUID) -> tuple[int, int, Decimal | None]:
        row = await self.session.execute(
            select(
                func.count(Payment.id), func.count(Payment.amount), func.sum(Payment.amount)
            ).where(
                Payment.contract_id == contract_id,
                Payment.is_current.is_(True),
                _actual_payment_phase(),
            )
        )
        count, valued_count, total = row.one()
        return count, valued_count, total

    async def payment_total(self, contract_id: UUID) -> Decimal:
        """Sum published amounts for current payments, excluding other financial phases."""
        _, _, total = await self._payment_totals(contract_id)
        return total if total is not None else Decimal(0)

    async def payment_summary(
        self, contract: Contract, *, grace_days: int = 60
    ) -> dict[str, object]:
        """Compare published amounts only for payment records linked in the local collection."""
        count, valued_count, subtotal = await self._payment_totals(contract.id)
        paid = subtotal if count == valued_count else None
        contracted = contract_value(contract)
        balance = contracted - paid if contracted is not None and paid is not None else None
        status = "nao_avaliado"
        if contracted is not None and contracted > 0:
            if paid is not None and paid > 0:
                status = "pago" if paid >= contracted else "parcial"
            elif count == 0:
                start = contract.signed_at or contract.published_at or contract.valid_from
                if start is not None and datetime.now(UTC) - start >= timedelta(days=grace_days):
                    status = "sem_pagamento_localizado"
        return {
            "payment_count": count,
            "paid_value": float(paid) if paid is not None else None,
            "contracted_value": float(contracted) if contracted is not None else None,
            "balance": float(balance) if balance is not None else None,
            "status": status,
            "grace_days": grace_days,
            "disclaimer": (
                "Resumo dos registros de pagamento vigentes vinculados ao contrato no acervo. "
                "A cobertura das fontes pode ser parcial. Confirme a execução na fonte oficial."
            ),
        }


class PaymentRepository(BaseRepository[Payment]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Payment)

    @staticmethod
    def _classification_join():
        entry = MunicipalBudgetEntry
        aggregate = Payment.status == "agregado_por_empenho"
        # Older aggregates may only carry the structured external ID from the adapter.
        identity = case(
            (
                aggregate & Payment.external_id.endswith(":agregado"),
                func.substr(Payment.external_id, 1, func.length(Payment.external_id) - 9),
            ),
            (Payment.status == "detalhe_empenho", Payment.commitment_external_id),
        )
        return and_(
            Payment.source_record_id == entry.source_record_id,
            Payment.source == "municipal:" + entry.municipality,
            identity == entry.commitment_identity,
            or_(
                Payment.commitment_external_id.is_(None),
                Payment.commitment_external_id == entry.commitment_identity,
            ),
        )

    async def budget_entries(self, payments: list[Payment]) -> dict[UUID, dict[str, Any]]:
        if not payments:
            return {}
        rows = await self.session.execute(
            select(Payment.id, MunicipalBudgetEntry)
            .join(MunicipalBudgetEntry, self._classification_join())
            .where(Payment.id.in_([payment.id for payment in payments]))
        )
        return {payment_id: public_budget_values(entry) for payment_id, entry in rows}

    async def budget_options(self) -> tuple[list[str], list[int]]:
        entry = MunicipalBudgetEntry
        rows = (
            await self.session.execute(
                select(entry.function_name, entry.fiscal_year)
                .select_from(Payment)
                .join(entry, self._classification_join())
                .where(Payment.is_current.is_(True))
                .distinct()
            )
        ).all()
        return sorted({name for raw, _ in rows if (name := budget_function_label(raw))}), sorted(
            {year for _, year in rows}, reverse=True
        )

    @classmethod
    def filtered_query(
        cls,
        *,
        supplier_cnpj: str | None = None,
        agency_cnpj: str | None = None,
        contract_id: UUID | None = None,
        phase: str | None = None,
        match_status: str | None = None,
        source: str | None = None,
        municipality: str | None = None,
        budget_function: str | None = None,
        fiscal_year: int | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> Select[Any]:
        statement = (
            select(Payment)
            .where(Payment.is_current.is_(True))
            .options(selectinload(Payment.contract), selectinload(Payment.company))
        )
        if supplier_cnpj:
            statement = statement.where(Payment.supplier_cnpj == supplier_cnpj)
        if source:
            statement = statement.where(Payment.source == source)
        if municipality:
            statement = statement.where(
                Payment.source.like("municipal:%")
                if municipality == "all"
                else Payment.source == f"municipal:{municipality}"
            )
        if budget_function or fiscal_year is not None:
            entry = MunicipalBudgetEntry
            statement = statement.join(entry, cls._classification_join())
            if budget_function:
                statement = statement.where(entry.function_name == budget_function)
            if fiscal_year is not None:
                statement = statement.where(entry.fiscal_year == fiscal_year)
        if agency_cnpj:
            statement = statement.where(Payment.agency_cnpj == agency_cnpj)
        if contract_id:
            statement = statement.where(Payment.contract_id == contract_id)
        if phase:
            statement = statement.where(Payment.phase == phase)
        if match_status:
            statement = statement.where(
                Payment.requires_manual_review.is_(True)
                if match_status == "review"
                else Payment.match_status == match_status
            )
        if date_from:
            statement = statement.where(Payment.payment_date >= date_from)
        if date_to:
            statement = statement.where(Payment.payment_date <= date_to)
        return statement.order_by(
            Payment.payment_date.desc().nullslast(), Payment.created_at.desc(), Payment.id
        )

    async def list_filtered(
        self, *, page: int = 1, page_size: int = 50, **filters: Any
    ) -> Page[Payment]:
        return await self.page(self.filtered_query(**filters), page=page, page_size=page_size)

    async def suppliers_in_scope(
        self, scope: Select[Any], page: int | None, page_size: int = 50
    ) -> tuple[int, list[dict[str, Any]]]:
        # The stored CNPJ and the linked company's identity must agree.
        statement = (
            scope.order_by(None)
            .join(
                Company,
                and_(Company.id == Payment.company_id, Company.cnpj == Payment.supplier_cnpj),
            )
            .where(func.length(Company.cnpj) == 14)
        )
        count = int(
            await self.session.scalar(
                statement.with_only_columns(
                    func.count(func.distinct(Company.cnpj)), maintain_column_froms=True
                )
            )
            or 0
        )
        if page is None:
            return count, []
        records = func.count(Payment.id)
        label = func.coalesce(Company.legal_name, Company.normalized_name, Company.cnpj)
        grouped = statement.with_only_columns(
            Company.cnpj, label, records, maintain_column_froms=True
        )
        rows = await self.session.execute(
            grouped.group_by(Company.cnpj, label)
            .order_by(records.desc(), label, Company.cnpj)
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        return count, [dict(cnpj=cnpj, name=name, records=n) for cnpj, name, n in rows]
