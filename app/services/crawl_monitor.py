"""Presentation of optional crawl telemetry, including runs created by older versions."""

import math
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

from app.config import get_settings
from app.models import CrawlRun
from app.services.crawl_progress import STAGE_LABELS, safe_label


def nonnegative_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value)) if value is not None else 0
    except (ValueError, TypeError, OverflowError):
        return 0


def seconds_label(value: Any) -> str:
    try:
        number = float(value)
    except (ValueError, TypeError, OverflowError):
        number = 0.0
    if not math.isfinite(number):
        number = 0.0
    return f"{max(0.0, number):.2f}".rstrip("0").rstrip(".").replace(".", ",")


def timestamp(value: Any) -> datetime | None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def elapsed_label(value: Any, now: datetime) -> str:
    moment = timestamp(value)
    if moment is None:
        return "Não informado"
    seconds = max(0, int((now - moment).total_seconds()))
    if seconds < 60:
        return f"{seconds} s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes} min {seconds:02d} s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours} h {minutes:02d} min"


def monitor_view(run: CrawlRun, base: SimpleNamespace, *, now: datetime) -> SimpleNamespace:
    cursor = run.cursor if isinstance(run.cursor, dict) else {}
    progress = cursor.get("progress")
    progress = progress if isinstance(progress, dict) else {}
    if not progress and "stage" in cursor:
        progress = cursor  # Previous municipal collectors used the root cursor.
    stage = str(
        progress.get("stage") or ("queued" if run.status.value == "pending" else "discovery")
    )
    if not base.active:
        stage = run.status.value
    sources = cursor.get("sources")
    sources = sources if isinstance(sources, dict) else {}
    records = [
        entry for key, entry in sources.items() if key != "documents" and isinstance(entry, dict)
    ]
    examined = sum(
        nonnegative_int(
            entry.get("progress", {}).get("processed")
            if isinstance(entry.get("progress"), dict)
            else entry.get("records_found")
            if entry.get("status") not in {"pending", "running"}
            else 0
        )
        for entry in records
    )
    failed = sum(nonnegative_int(entry.get("records_failed")) for entry in records)
    documents = sources.get("documents")
    documents = documents if isinstance(documents, dict) else {}
    if isinstance(run.filters, dict) and run.filters.get("mode") == "documents":
        failed = nonnegative_int(documents.get("documents_failed"))
        examined = nonnegative_int(documents.get("documents_processed")) + (
            failed + nonnegative_int(documents.get("documents_skipped")) if not base.active else 0
        )
    processed = nonnegative_int(progress.get("processed"))
    total = nonnegative_int(progress.get("total")) if progress.get("total") is not None else None
    percentage = min(100, int(100 * processed / total)) if total and base.active else None
    atomic = stage == "persisting" and run.connector.startswith("municipal:")
    if atomic:
        percentage = None
    heartbeat = timestamp(run.heartbeat_at) or timestamp(run.started_at or run.created_at)
    stale = base.active and heartbeat is not None and (now - heartbeat).total_seconds() > 150
    activities = cursor.get("activity")
    activities = activities[-20:] if isinstance(activities, list) else []
    history = []
    for activity in reversed(activities):
        if not isinstance(activity, dict):
            continue
        when = timestamp(activity.get("at"))
        history.append(
            SimpleNamespace(
                at=when.isoformat() if when else "",
                label=when.astimezone(ZoneInfo(get_settings().timezone)).strftime("%H:%M:%S")
                if when
                else "—",
                message=safe_label(str(activity.get("message") or "Atualização de andamento")),
            )
        )
    data = vars(base).copy()
    parallel = cursor.get("parallel")
    parallel = parallel if isinstance(parallel, dict) else {}
    parallel_view = []
    for source, stats in parallel.items():
        if not isinstance(stats, dict) or not stats:
            continue
        entry = sources.get(source, {})
        source_progress = entry.get("progress") if isinstance(entry, dict) else None
        ready = source_progress.get("ready", 0) if isinstance(source_progress, dict) else 0
        parallel_view.append(
            SimpleNamespace(
                label={
                    "pncp": "PNCP",
                    "compras_gov": "Compras.gov.br",
                    "documents": "Documentos",
                }.get(source, safe_label(str(source))),
                http_limit=nonnegative_int(stats.get("http_limit")),
                http_active=nonnegative_int(stats.get("http_active")) if base.active else 0,
                http_peak=nonnegative_int(stats.get("http_peak")),
                http_waiting=nonnegative_int(stats.get("http_waiting")) if base.active else 0,
                detailed_waiting="http_waiting_rate" in stats,
                http_waiting_rate=nonnegative_int(stats.get("http_waiting_rate"))
                if base.active
                else 0,
                http_waiting_capacity=nonnegative_int(stats.get("http_waiting_capacity"))
                if base.active
                else 0,
                http_waiting_checkpoint=nonnegative_int(stats.get("http_waiting_checkpoint"))
                if base.active
                else 0,
                http_waiting_retry=nonnegative_int(stats.get("http_waiting_retry"))
                if base.active
                else 0,
                interval=seconds_label(stats.get("http_rate_interval_seconds")),
                cooldown=seconds_label(stats.get("http_cooldown_seconds")) if base.active else "0",
                download_active=nonnegative_int(stats.get("download_active")) if base.active else 0,
                download_limit=nonnegative_int(stats.get("download_limit")),
                process_active=nonnegative_int(stats.get("process_active")) if base.active else 0,
                process_limit=nonnegative_int(stats.get("process_limit")),
                ready=nonnegative_int(stats.get("queue_ready", ready)),
            )
        )
    data.update(
        collection=cursor.get("collection") if isinstance(cursor.get("collection"), dict) else None,
        collection_strategy={
            "bootstrap": "Carga inicial",
            "reconciliation": "Revisão semanal",
            "incremental": "Incremental",
            "manual_complete": "Consulta completa",
            "manual_versions": "Manual com versões verificadas",
        }.get(
            str((cursor.get("collection") or {}).get("strategy") or "")
            if isinstance(cursor.get("collection"), dict)
            else "",
            "Coleta",
        ),
        records_reused=sum(nonnegative_int(entry.get("records_reused")) for entry in records)
        if any("records_reused" in entry for entry in records)
        else None,
        operations_reused=sum(nonnegative_int(entry.get("operations_reused")) for entry in records)
        if any("operations_reused" in entry for entry in records)
        else None,
        parallel=parallel_view,
        stage=stage,
        stage_label=STAGE_LABELS.get(stage, "Coleta em andamento"),
        message="Gravando lote — aguardando confirmação"
        if atomic
        else safe_label(
            str(
                progress.get("message")
                or STAGE_LABELS.get(stage, "Aguardando atualização do coletor")
            )
        ),
        subject=safe_label(str(progress.get("subject") or "")),
        elapsed=elapsed_label(run.started_at or run.created_at, now),
        last_activity=elapsed_label(progress.get("updated_at"), now),
        last_heartbeat=elapsed_label(run.heartbeat_at, now),
        stale=stale,
        processed=processed,
        total=total,
        percentage=percentage,
        page=nonnegative_int(progress.get("page")),
        total_pages=nonnegative_int(progress.get("total_pages")),
        page_label="Arquivo" if progress.get("operation") == "files" else "Página",
        retry_at=timestamp(progress.get("retry_at")),
        attempt=nonnegative_int(progress.get("attempt")),
        max_attempts=nonnegative_int(progress.get("max_attempts")),
        retry_seconds=max(
            0, int(((timestamp(progress.get("retry_at")) or now) - now).total_seconds())
        ),
        created=nonnegative_int(run.records_created),
        updated=nonnegative_int(run.records_updated),
        examined=examined,
        failed=failed,
        history=history,
        documents_count=nonnegative_int(documents.get("documents_processed")),
        documents_total=documents.get("documents_total"),
        events_count=nonnegative_int(documents.get("events_created")),
        leads_count=nonnegative_int(documents.get("leads_created")),
    )
    return SimpleNamespace(**data)
