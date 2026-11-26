"""Read federal collection coverage and distinguish the reported financial amounts."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal
from typing import Any, cast
from uuid import UUID

from sqlalchemy import Select, and_, case, func, literal, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import CollectionBatch, CollectionWorkItem, CrawlRun, CrawlRunStatus, Payment
from app.services.ingestion.federal_payments import FEDERAL_BATCH_MODE

FEDERAL_PAYMENT_LABELS = {
    "issued": "Documentos emitidos",
    "intermediary": "Documentos emitidos a intermediários",
    "final": "Repasses a favorecidos finais",
}
_RESUMABLE_BATCHES = {"limited", "failed", "suspended"}
_ACTIVE_RUNS = {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}


@dataclass(frozen=True)
class FederalCoverageCounts:
    total: int = 0
    completed: int = 0
    empty: int = 0
    pending: int = 0
    waiting: int = 0
    failed: int = 0
    review_required: int = 0


@dataclass(frozen=True)
class FederalBatchView:
    id: UUID
    latest_run_id: UUID | None
    status: str
    can_continue: bool
    start_date: str | None
    end_date: str | None
    updated_at: datetime | None
    archives: FederalCoverageCounts
    api: FederalCoverageCounts
    api_notice: str | None = None
    records_created: int = 0
    records_updated: int = 0
    beneficiaries_found: int | None = None

    @property
    def status_label(self) -> str:
        if self.status == "partial" and not (self.archives.failed or self.api.failed):
            if self.archives.pending or self.api.pending:
                return "Lote a continuar"
            if self.archives.waiting or self.api.waiting:
                return "Aguardando nova tentativa"
        return {
            "pending": "Aguardando",
            "running": "Em andamento",
            "completed": "Concluída",
            "partial": "Com pendências",
            "failed": "Falhou",
            "cancelled": "Interrompida",
        }.get(self.status, "Situação indisponível")


def _counts(stats: dict[str, Any]) -> FederalCoverageCounts:
    return FederalCoverageCounts(
        total=stats.get("total", 0),
        completed=stats.get("completed", 0),
        empty=stats.get("empty", 0),
        pending=stats.get("pending", 0),
        waiting=stats.get("waiting", 0),
        failed=stats.get("failed", 0),
        review_required=stats.get("review_required", 0),
    )


def federal_day_label(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value).strftime("%d/%m/%Y")
    except ValueError:
        return None


def _in_cohort(column: Any, cnpj: str) -> Any:
    # The Python and JSON representations both store normalized CNPJs as strings.
    # Matching the JSON quotes keeps membership exact on SQLite and PostgreSQL.
    return column.as_string().contains(f'"{cnpj}"', autoescape=True)


def _batch_query() -> Select[CollectionBatch, CrawlRun]:
    return (
        select(CollectionBatch, CrawlRun)
        .outerjoin(CrawlRun, CrawlRun.id == CollectionBatch.last_run_id)
        .where(CollectionBatch.filters["mode"].as_string() == FEDERAL_BATCH_MODE)
    )


def _batch_view(batch: CollectionBatch, run: CrawlRun | None) -> FederalBatchView:
    stored = batch.cursors.get("federal", {})
    active = run is not None and run.status in _ACTIVE_RUNS
    status = (
        run.status.value
        if run is not None and (active or batch.status == "failed")
        else {
            "complete": "completed",
            "limited": "partial",
            "suspended": "cancelled",
        }.get(batch.status, batch.status)
    )
    return FederalBatchView(
        id=batch.id,
        latest_run_id=batch.last_run_id,
        status=status,
        can_continue=(run is not None and not active and batch.status in _RESUMABLE_BATCHES),
        start_date=federal_day_label(batch.filters.get("start_date")),
        end_date=federal_day_label(batch.filters.get("end_date")),
        updated_at=(run.finished_at or run.heartbeat_at if run is not None else None)
        or batch.updated_at,
        archives=_counts(stored.get("archives", {})),
        api=_counts(stored.get("api", {})),
        api_notice=stored.get("api_notice"),
        records_created=stored.get("records_created", 0),
        records_updated=stored.get("records_updated", 0),
        beneficiaries_found=stored.get("found"),
    )


async def federal_batch_views(
    db: AsyncSession, batch_ids: set[UUID]
) -> dict[UUID, FederalBatchView]:
    if not batch_ids:
        return {}
    rows = await db.execute(_batch_query().where(CollectionBatch.id.in_(batch_ids)))
    return {batch.id: _batch_view(batch, run) for batch, run in rows}


async def latest_federal_batch(
    db: AsyncSession, *, cnpj: str | None = None
) -> FederalBatchView | None:
    statement = _batch_query().order_by(CollectionBatch.created_at.desc(), CollectionBatch.id)
    if cnpj is not None:
        statement = statement.where(
            or_(
                _in_cohort(CollectionBatch.filters["cnpjs"], cnpj),
                _in_cohort(CollectionBatch.filters["tracked_cnpjs"], cnpj),
            )
        )
    row = (await db.execute(statement.limit(1))).first()
    if row is None:
        return None
    batch, run = row
    if cnpj is None:
        return _batch_view(batch, run)

    # An archive covers the frozen cohort even when one particular CNPJ has no
    # matches. A beneficiary query covers only its own CNPJ and exercise.
    item = CollectionWorkItem
    company_has_match = _in_cohort(item.payload["found_cnpjs"], cnpj)
    archived = cnpj in batch.filters.get("cnpjs", [])
    rows = await db.execute(
        select(
            item.kind,
            item.status,
            func.count(),
            func.sum(case((company_has_match, 1), else_=0)),
        )
        .where(
            item.batch_id == batch.id,
            or_(
                and_(item.kind == "archive", literal(archived)),
                and_(item.kind == "beneficiary", item.payload["cnpj"].as_string() == cnpj),
            ),
        )
        .group_by(item.kind, item.status)
    )
    stats: dict[str, dict[str, Any]] = {"archive": {}, "beneficiary": {}}
    for kind, status, count, found in rows:
        group = stats[kind]
        group["total"] = group.get("total", 0) + count
        if status in {"succeeded", "empty"}:
            group["completed"] = group.get("completed", 0) + count
            empty = count - found if kind == "archive" else count if status == "empty" else 0
            group["empty"] = group.get("empty", 0) + empty
        else:
            group[status] = count

    view = _batch_view(batch, run)
    return replace(
        view,
        archives=_counts(stats["archive"]),
        api=_counts(stats["beneficiary"]),
        api_notice=view.api_notice if cnpj in batch.filters.get("tracked_cnpjs", []) else None,
        records_created=0,
        records_updated=0,
        beneficiaries_found=None,
    )


def federal_payment_category(payment: Payment) -> str | None:
    if payment.federal_beneficiary_role == "issued" and payment.federal_intermediary:
        return "intermediary"
    return payment.federal_beneficiary_role


def federal_payment_filter(category: str) -> Any:
    if category == "intermediary":
        return and_(
            Payment.federal_beneficiary_role == "issued", Payment.federal_intermediary.is_(True)
        )
    if category == "issued":
        return and_(
            Payment.federal_beneficiary_role == "issued", Payment.federal_intermediary.is_not(True)
        )
    return Payment.federal_beneficiary_role == "final"


async def federal_payment_totals(
    db: AsyncSession, scope: Select[Any]
) -> dict[str, tuple[int, Decimal | None]]:
    payments = scope.order_by(None).subquery()
    role = case(
        (
            and_(
                payments.c.federal_beneficiary_role == "issued",
                payments.c.federal_intermediary.is_(True),
            ),
            "intermediary",
        ),
        else_=payments.c.federal_beneficiary_role,
    )
    rows = await db.execute(
        select(role, func.count(), func.sum(payments.c.amount))
        .where(
            payments.c.is_current.is_(True),
            payments.c.federal_beneficiary_role.in_(("issued", "final")),
            payments.c.federal_identity_conflict.is_not(True),
        )
        .group_by(role)
    )
    totals = {}
    for row in rows:
        category, count, amount = cast(tuple[str, int, Decimal | None], tuple(row))
        totals[category] = (count, amount)
    return totals
