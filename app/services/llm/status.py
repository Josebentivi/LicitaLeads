"""Read-only queue summaries with bounded result sets and shared SQL aggregates."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import cast, func, literal, or_, select, union_all
from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from app.config import Settings
from app.models import LLMAttempt, LLMBatch, LLMDemand, LLMProviderState, LLMWorkItem
from app.models.base import GUID
from app.services.llm.queue import config_key, next_request_at

_STATUSES = ("pending", "running", "succeeded", "failed", "cancelled", "obsolete")


async def queue_statuses(
    session: AsyncSession,
    settings: Settings,
    *,
    run_ids: Iterable[UUID] = (),
    include_global: bool = False,
) -> dict[UUID | None, dict[str, Any] | None]:
    """Summarize every requested scope in eight queries, without loading jobs.

    A job shared by batches is counted once in a scope. Reuse retains the
    existing demand-based count, and cost belongs to the attempt's batch.
    Nothing survives this call: cancellations are visible on the next request.
    """
    identifiers = tuple(dict.fromkeys(run_ids))
    scopes: list[UUID | None] = list(identifiers)
    if include_global:
        scopes.append(None)
    output: dict[UUID | None, dict[str, Any] | None] = dict.fromkeys(scopes)
    if not scopes:
        return output
    selected: ColumnElement[bool] = LLMBatch.crawl_run_id.in_(identifiers)
    if include_global:
        recent = select(LLMBatch.id).order_by(LLMBatch.created_at.desc()).limit(20)
        selected = or_(selected, LLMBatch.id.in_(recent))
    batches = list(
        (
            await session.execute(
                select(
                    LLMBatch.id,
                    LLMBatch.crawl_run_id,
                    LLMBatch.label,
                    LLMBatch.cancelled,
                    LLMBatch.created_at,
                )
                .where(selected)
                .order_by(LLMBatch.created_at.desc())
            )
        ).all()
    )
    if not batches:
        return output

    parts = []
    if identifiers:
        parts.append(
            select(LLMBatch.crawl_run_id.label("scope"), LLMBatch.id.label("batch_id")).where(
                LLMBatch.crawl_run_id.in_(identifiers)
            )
        )
    if include_global:
        parts.append(
            select(cast(literal(None), GUID()).label("scope"), LLMBatch.id.label("batch_id"))
        )
    batch_scopes = union_all(*parts).cte("queue_batch_scopes")
    members = (
        select(batch_scopes.c.scope, LLMDemand.work_id)
        .join(LLMDemand, LLMDemand.batch_id == batch_scopes.c.batch_id)
        .distinct()
        .cte("queue_scope_members")
    )
    state_rows = list(
        (
            await session.scalars(
                select(LLMProviderState).where(
                    or_(
                        LLMProviderState.blocked_at.is_not(None),
                        LLMProviderState.config_key == config_key(settings),
                    )
                )
            )
        ).all()
    )
    blocked = [state for state in state_rows if state.blocked_at is not None]
    blocks = {state.config_key: state.error for state in blocked}
    current_config = config_key(settings)
    due = await next_request_at(session, settings, datetime.now(UTC))
    for state in state_rows:
        if state.config_key == current_config and state.cooldown_until:
            due = max(due, state.cooldown_until)
    for scope in scopes:
        exists = bool(batches) if scope is None else any(b.crawl_run_id == scope for b in batches)
        if exists:
            output[scope] = {
                **dict.fromkeys(_STATUSES, 0),
                "blocked": 0,
                "configuration_changed": 0,
                "reused": 0,
                "total": 0,
                "known_cost_usd": "0",
                "unknown_cost_attempts": 0,
                "requests": 0,
                "limit_per_hour": settings.llm_max_requests_per_hour,
                "next_request_at": due.isoformat(),
                "enabled": settings.llm_enabled,
                "diagnostics": [],
                "retry_available": False,
                "batches": [],
            }

    rows: Result[Any, Any, Any, Any, Any] = await session.execute(
        select(
            members.c.scope,
            LLMWorkItem.status,
            LLMWorkItem.config_key,
            func.count(),
            func.min(LLMWorkItem.next_attempt_at),
        )
        .join(LLMWorkItem, LLMWorkItem.id == members.c.work_id)
        .group_by(members.c.scope, LLMWorkItem.status, LLMWorkItem.config_key)
    )
    diagnostic_keys: dict[UUID | None, set[str]] = {scope: set() for scope in scopes}
    deadlines = dict.fromkeys(scopes, due)
    pending_dates: dict[UUID | None, datetime] = {}
    for scope, status, config, count, pending_at in rows:
        entry = output[scope]
        assert entry is not None
        entry["total"] += count
        if status in _STATUSES:
            entry[status] += count
        if status == "pending":
            entry["blocked"] += count if config in blocks else 0
            entry["configuration_changed"] += count if config != current_config else 0
            if pending_at:
                pending_dates[scope] = min(pending_dates.get(scope, pending_at), pending_at)
        if status in {"pending", "failed", "running"}:
            diagnostic_keys[scope].add(config)
        entry["retry_available"] |= (
            status in {"failed", "cancelled"}
            or config in blocks
            or (status == "pending" and config != current_config)
        )
    for scope, entry in output.items():
        if entry is not None:
            entry["diagnostics"] = [
                state.error for state in blocked if state.config_key in diagnostic_keys[scope]
            ]
            entry["next_request_at"] = max(
                deadlines[scope], pending_dates.get(scope, deadlines[scope])
            ).isoformat()

    reused: Result[Any, Any] = await session.execute(
        select(batch_scopes.c.scope, func.count())
        .join(LLMDemand, LLMDemand.batch_id == batch_scopes.c.batch_id)
        .join(LLMWorkItem, LLMWorkItem.id == LLMDemand.work_id)
        .where(or_(LLMDemand.reused.is_(True), LLMWorkItem.reused.is_(True)))
        .group_by(batch_scopes.c.scope)
    )
    for scope, count in reused:
        entry = output[scope]
        assert entry is not None
        entry["reused"] = count

    costs: dict[UUID | None, Decimal] = dict.fromkeys(scopes, Decimal(0))
    cost_rows: Result[Any, Any, Any] = await session.execute(
        select(batch_scopes.c.scope, LLMAttempt.cost_usd, func.count())
        .join(LLMAttempt, LLMAttempt.batch_id == batch_scopes.c.batch_id)
        .group_by(batch_scopes.c.scope, LLMAttempt.cost_usd)
    )
    for scope, cost, count in cost_rows:
        entry = output[scope]
        assert entry is not None
        entry["requests"] += count
        if cost is None:
            entry["unknown_cost_attempts"] += count
        else:
            # SQLite SUM(NUMERIC) uses floating point. Retrieve each distinct
            # typed Decimal once and multiply its count without losing precision.
            costs[scope] += cost * count
    for scope, entry in output.items():
        if entry is not None:
            entry["known_cost_usd"] = str(costs[scope])

    batch_rows = await session.execute(
        select(LLMDemand.batch_id, LLMWorkItem.status, LLMWorkItem.config_key, func.count())
        .join(LLMWorkItem, LLMWorkItem.id == LLMDemand.work_id)
        .where(LLMDemand.batch_id.in_([batch.id for batch in batches]))
        .group_by(LLMDemand.batch_id, LLMWorkItem.status, LLMWorkItem.config_key)
    )
    flags: dict[UUID, dict[str, bool]] = {
        batch.id: {"pending": False, "retry_available": False} for batch in batches
    }
    for batch_id, status, config, _ in batch_rows:
        flags[batch_id]["pending"] |= status in {"pending", "running"}
        flags[batch_id]["retry_available"] |= status in {"failed", "cancelled"} or (
            status == "pending" and (config in blocks or config != current_config)
        )
    for scope, entry in output.items():
        if entry is not None:
            entry["batches"] = [
                {
                    "id": str(batch.id),
                    "label": batch.label,
                    "cancelled": batch.cancelled,
                    **flags[batch.id],
                }
                for batch in batches
                if scope is None or batch.crawl_run_id == scope
            ][:20]
    return output
