"""Project recorded causes and the current state of the exact batches a collection used."""

from collections import defaultdict
from collections.abc import Sequence
from types import SimpleNamespace
from typing import Any
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models import (
    CollectionBatch,
    CrawlRun,
    DataAvailability,
    FieldObservation,
    SourceRecord,
    TceMaVerifiedLink,
)
from app.services.scheduler_control import scheduler_status
from app.services.tce_ma_review import IDENTITY_REVIEW, active_tce_links

_ISSUES = {
    DataAvailability.ACCESS_RESTRICTED: ("Portal indisponível", "acesso restrito"),
    DataAvailability.TEMPORARY_ERROR: ("Falha na consulta", "erro temporário da fonte"),
    DataAvailability.NOT_PUBLISHED: ("Aguardando publicação", ""),
}
_ORIGINAL_ONLY = {"pending", "running", "skipped", "not_started"}
_CAN_CONTINUE = {"limited", "failed", "suspended"}


def _identifiers(values: Sequence[Any]) -> list[UUID]:
    identifiers = []
    for value in values:
        try:
            identifiers.append(UUID(str(value)))
        except ValueError:
            continue
    return identifiers


def _run_url(identifier: UUID) -> str:
    return f"/crawls?run={identifier}#execucao-{identifier}"


def _effective_status(children: list[CrawlRun], batches: dict[UUID, CollectionBatch]) -> str:
    states = {
        "partial"
        if child.batch_id in batches
        and batches[child.batch_id].status == "limited"
        and child.status.value not in {"running", "pending"}
        else child.status.value
        for child in children
    }
    if states == {"completed"}:
        return "completed"
    if "running" in states or "pending" in states:
        return "running"
    if len(states) == 1:
        return states.pop()
    return "partial"


async def attach_complete_stages(
    db: AsyncSession,
    views: list[SimpleNamespace],
    settings: Settings,
    source_labels: dict[str, str],
) -> None:
    roots = [view for view in views if view.complete and view.complete_stages]
    if not roots:
        return
    steps = [(root, step) for root in roots for step in root.complete_stages]
    child_ids = {
        identifier for _, step in steps for identifier in _identifiers(step.get("run_ids", []))
    }
    children = {
        run.id: run for run in await db.scalars(select(CrawlRun).where(CrawlRun.id.in_(child_ids)))
    }
    batch_ids = {run.batch_id for run in children.values() if run.batch_id}
    batch_ids.update(
        identifier for _, step in steps for identifier in _identifiers([step.get("batch_id")])
    )
    batches = {
        batch.id: batch
        for batch in await db.scalars(
            select(CollectionBatch).where(CollectionBatch.id.in_(batch_ids))
        )
    }
    latest_ids = {batch.last_run_id for batch in batches.values() if batch.last_run_id} - child_ids
    children.update(
        {
            run.id: run
            for run in await db.scalars(select(CrawlRun).where(CrawlRun.id.in_(latest_ids)))
        }
    )
    root_ids = {root.id for root in roots}
    issue_runs = children.keys() | root_ids
    source_issues: dict[UUID, set[tuple[str, DataAvailability, int | None]]] = defaultdict(set)
    for run_id, source, availability, http_status in await db.execute(
        select(
            SourceRecord.crawl_run_id,
            SourceRecord.source,
            SourceRecord.availability,
            SourceRecord.http_status,
        )
        .where(
            SourceRecord.crawl_run_id.in_(issue_runs),
            or_(
                SourceRecord.availability.in_(_ISSUES),
                SourceRecord.http_status == 429,
                SourceRecord.http_status >= 500,
            ),
        )
        .distinct()
    ):
        if run_id is not None:
            source_issues[run_id].add((source, availability, http_status))
    tce_targets: dict[UUID, set[UUID]] = defaultdict(set)
    for run_id, procurement_id in await db.execute(
        select(SourceRecord.crawl_run_id, FieldObservation.entity_id)
        .join(FieldObservation, FieldObservation.source_record_id == SourceRecord.id)
        .where(
            SourceRecord.crawl_run_id.in_(root_ids),
            FieldObservation.field_name == IDENTITY_REVIEW,
        )
        .distinct()
    ):
        if run_id is not None:
            tce_targets[run_id].add(procurement_id)
    for root, step in steps:
        tce_targets[root.id].update(_identifiers(step.get("review_targets", [])))
    reviewed = (
        {
            identifier
            for identifier in await db.scalars(
                active_tce_links()
                .with_only_columns(TceMaVerifiedLink.procurement_id)
                .where(TceMaVerifiedLink.procurement_id.in_(set().union(*tce_targets.values())))
            )
        }
        if any(tce_targets.values())
        else set()
    )
    scheduler = (
        await scheduler_status(db, settings)
        if "transparencia" in settings.payment_sources and any(root.federal_batch for root in roots)
        else {}
    )

    for root in roots:
        root.complete_stages = [
            _stage_view(
                root,
                step,
                children,
                batches,
                source_issues,
                tce_targets[root.id] - reviewed,
                source_labels,
                scheduler,
            )
            for step in root.complete_stages
        ]


def _stage_view(
    root: SimpleNamespace,
    step: dict,
    children: dict[UUID, CrawlRun],
    batches: dict[UUID, CollectionBatch],
    source_issues: dict[UUID, set[tuple[str, DataAvailability, int | None]]],
    review_targets: set[UUID],
    source_labels: dict[str, str],
    scheduler: dict[str, object],
) -> dict:
    latest = {}
    for identifier in _identifiers(step.get("run_ids", [])):
        child = children.get(identifier)
        if child is not None:
            batch = batches.get(child.batch_id) if child.batch_id else None
            refreshed = children.get(batch.last_run_id) if batch and batch.last_run_id else None
            latest[batch.id if batch else child.id] = refreshed or child
    status = original = step.get("status")
    key = step.get("key")
    federal = root.federal_batch if key == "federal" else None
    if federal:
        status = federal.status
    elif latest and original not in _ORIGINAL_ONLY:
        status = _effective_status(list(latest.values()), batches)
    reasons: list[tuple[str, str, str | None, str | None]] = []
    if step.get("reason") == "configuration":
        reasons.append(
            ("Etapa não configurada", "", "/pagamentos#acompanhamento-federal", "Ver configuração")
        )
    elif step.get("reason") == "excluded":
        reasons.append(("Etapa fora do escopo", "", None, None))
    elif step.get("reason") == "failure":
        reasons.append(("Falha registrada", "", _run_url(root.id), "Ver execução"))
    if key == "tce_ma":
        for procurement_id in sorted(review_targets):
            reasons.append(
                (
                    "Revisão necessária",
                    "Identificação do pregão no TCE-MA",
                    f"/procurements/{procurement_id}#tce-title",
                    "Revisar vínculo",
                )
            )
    if federal:
        if federal.api_notice:
            reasons.append(
                (
                    "Etapa não configurada",
                    federal.api_notice,
                    "/pagamentos#acompanhamento-federal",
                    "Ver acompanhamento",
                )
            )
        if federal.archives.waiting:
            reasons.append(
                (
                    "Aguardando publicação",
                    f"{federal.archives.waiting} arquivo(s) ausentes do catálogo oficial",
                    None,
                    None,
                )
            )
        if federal.archives.review_required and federal.latest_run_id:
            reasons.append(
                (
                    "Revisão necessária",
                    f"{federal.archives.review_required} arquivo(s) com divergências",
                    f"/crawls/{federal.latest_run_id}/federal",
                    "Ver evidências",
                )
            )
        if federal.archives.pending or federal.api.pending:
            automatic = (
                federal.status == "partial"
                and federal.can_continue
                and scheduler.get("alive")
                and not scheduler.get("paused")
            )
            reasons.append(
                (
                    "Limite atingido" if federal.status == "partial" else "Trabalho pendente",
                    "Retomada automática programada" if automatic else "",
                    None,
                    None,
                )
            )
    resume = []
    unresolved = [child for child in latest.values() if child.status.value != "completed"]
    for child in latest.values():
        batch = batches.get(child.batch_id) if child.batch_id else None
        if (
            not federal
            and batch
            and batch.status in _CAN_CONTINUE
            and child.status.value not in {"running", "pending"}
        ):
            resume.append(child.id)
            if batch.status == "limited":
                reasons.append(("Limite atingido", "", None, None))
        if child.records_conflicted:
            reasons.append(
                (
                    "Revisão necessária",
                    "Identidades em conflito",
                    _run_url(child.id),
                    "Ver detalhes",
                )
            )
        documents = (child.cursor or {}).get("sources", {}).get("documents", {})
        if documents.get("documents_failed"):
            reasons.append(
                (
                    "Falha em documentos",
                    f"{documents['documents_failed']} falha(s) de processamento",
                    f"/crawls/{child.id}/documents",
                    "Ver documentos",
                )
            )
    references = [child.id for child in unresolved]
    if status in {"partial", "failed"} and not references:
        references = [child.id for child in latest.values()]
    if key == "tce_ma":
        references.append(root.id)
    for identifier in references:
        for source, availability, http_status in sorted(
            source_issues.get(identifier, set()), key=lambda item: (item[0], item[1], item[2] or 0)
        ):
            if federal and availability == DataAvailability.NOT_PUBLISHED:
                continue
            label, description = _ISSUES.get(availability, ("Falha na consulta", ""))
            if http_status and (http_status in {403, 429} or http_status >= 500):
                label, description = "Portal indisponível", f"HTTP {http_status}"
            action = (
                "/pagamentos#importar-sao-luis"
                if source == "municipal:sao_luis"
                else _run_url(identifier)
            )
            name = source_labels.get(source, source)
            reasons.append(
                (
                    label,
                    " · ".join(filter(None, (name, description))),
                    action,
                    "Revisar CSV de São Luís" if source == "municipal:sao_luis" else "Ver execução",
                )
            )
    if status in {"failed", "partial"} and not reasons and not resume:
        reasons.append(("Motivo não registrado", "", None, None))
    return {
        **step,
        "effective_status": status,
        "latest_runs": [child.id for child in latest.values()] or step.get("run_ids", []),
        "reasons": list(dict.fromkeys(reasons)),
        "resume": resume,
    }
