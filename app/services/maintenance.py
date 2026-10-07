"""Destructive maintenance operations guarded by explicit confirmation."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.models import Base, CrawlRun, CrawlRunStatus, JobLease

COUNT_TABLE_LABELS: dict[str, str] = {
    "procurements": "Contratações",
    "documents": "Documentos",
    "document_chunks": "Trechos extraídos",
    "procurement_events": "Eventos",
    "deadlines": "Prazos",
    "leads": "Leads",
    "lead_reviews": "Revisões de lead",
    "outreach_drafts": "Rascunhos de contato",
    "companies": "Empresas",
    "company_contacts": "Contatos",
    "participants": "Participantes",
    "price_registries": "Atas de registro de preços",
    "price_registry_items": "Itens de ata",
    "evidence": "Evidências",
    "source_records": "Registros de fonte",
    "field_observations": "Observações de campo",
    "crawl_runs": "Coletas",
}


class MaintenanceBlocked(RuntimeError):
    """Raised when active work prevents a destructive maintenance operation."""


@dataclass(slots=True)
class ClearDataResult:
    """Rows and files removed by the reset."""

    counts: dict[str, int] = field(default_factory=dict)
    files_removed: int = 0

    @property
    def total_records(self) -> int:
        return sum(self.counts.values())


async def maintenance_blocked_reason(session: AsyncSession) -> str | None:
    """Explain why the reset cannot run right now, or return ``None``."""

    active = int(
        await session.scalar(
            select(func.count())
            .select_from(CrawlRun)
            .where(CrawlRun.status.in_([CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING]))
        )
        or 0
    )
    if active:
        return f"{active} coleta(s) em andamento — encerre em /crawls antes de apagar os dados"
    leases = int(await session.scalar(select(func.count()).select_from(JobLease)) or 0)
    if leases:
        return "um job do scheduler está em andamento — tente novamente em alguns minutos"
    return None


async def data_counts(session: AsyncSession) -> dict[str, int]:
    """Count the user-visible rows shown in the settings danger zone."""

    counts: dict[str, int] = {}
    for table_name in COUNT_TABLE_LABELS:
        table = Base.metadata.tables[table_name]
        counts[table_name] = int(await session.scalar(select(func.count()).select_from(table)) or 0)
    return counts


async def clear_all_data(
    session: AsyncSession,
    *,
    settings: Settings | None = None,
) -> ClearDataResult:
    """Delete every collected row and stored file, preserving configuration.

    Refuses to run while a crawl or scheduler job is active.
    """

    selected = settings or get_settings()
    reason = await maintenance_blocked_reason(session)
    if reason is not None:
        raise MaintenanceBlocked(reason)

    result = ClearDataResult()
    for table in reversed(Base.metadata.sorted_tables):
        count = int(await session.scalar(select(func.count()).select_from(table)) or 0)
        if not count:
            continue
        result.counts[table.name] = count
        await session.execute(delete(table))
    await session.commit()

    result.files_removed = await asyncio.to_thread(
        _remove_stored_files,
        [selected.document_storage_path, selected.raw_data_storage_path],
    )
    await asyncio.to_thread(
        _truncate_scheduler_log,
        selected.document_storage_path.resolve().parent / "scheduler.log",
    )
    return result


def _remove_stored_files(roots: list[Path]) -> int:
    """Remove the contents of the configured storage roots, keeping .gitkeep."""

    removed = 0
    for root in roots:
        resolved = root.resolve()
        if not resolved.is_dir() or len(resolved.parts) <= 1:
            continue
        for entry in resolved.iterdir():
            if entry.name == ".gitkeep":
                continue
            removed += _remove_entry(entry)
    return removed


def _remove_entry(entry: Path) -> int:
    """Delete a file, symlink, or directory tree and count removed files."""

    try:
        if entry.is_dir() and not entry.is_symlink():
            removed = 0
            for child in entry.iterdir():
                removed += _remove_entry(child)
            entry.rmdir()
            return removed
        entry.unlink()
        return 1
    except OSError:
        return 0


def _truncate_scheduler_log(path: Path) -> None:
    """Best-effort truncation; the scheduler may hold the file open."""

    if not path.is_file():
        return
    with suppress(OSError):
        path.write_text("", encoding="utf-8")
