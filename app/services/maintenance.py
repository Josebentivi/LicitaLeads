"""Destructive maintenance operations guarded by explicit confirmation."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.database import write_session
from app.models import Base, CrawlRun, CrawlRunStatus, JobLease
from app.repositories.crawls import MAINTENANCE_LEASE, JobLeaseRepository
from app.services.scheduler_control import set_scheduler_paused

COUNT_TABLE_LABELS: dict[str, str] = {
    "participant_values": "Valores documentais das participantes",
    "fact_verifications": "Verificação dos fatos",
    "document_identities": "Identidades oficiais de documentos",
    "document_aliases": "Endereços e versões documentais",
    "reconciliation_runs": "Execuções de reconciliação",
    "lead_score_requests": "Solicitações de recálculo dos leads",
    "lead_score_revisions": "Histórico de pontuação dos leads",
    "llm_batches": "Lotes de análise de IA",
    "llm_demands": "Solicitações de análise de IA",
    "llm_work_items": "Fila de análise de IA",
    "llm_results": "Resultados de IA validados",
    "llm_attempts": "Chamadas e custos de IA",
    "llm_provider_states": "Bloqueios da análise de IA",
    "collection_recheck_states": "Rotação de conferências",
    "collection_sync_states": "Cobertura incremental",
    "verified_collection_versions": "Versões verificadas",
    "source_response_validators": "Validadores de respostas oficiais",
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
    "price_registry_units": "Unidades/saldos de ata",
    "price_registry_commitments": "Empenhos de ata",
    "price_registry_adhesions": "Adesões de ata",
    "contracts": "Contratos",
    "contract_items": "Itens de contrato",
    "contract_terms": "Termos/aditivos de contrato",
    "payments": "Pagamentos",
    "evidence": "Evidências",
    "source_records": "Registros de fonte",
    "field_observations": "Observações de campo",
    "crawl_runs": "Coletas",
    "field_states": "Proveniência atual dos campos",
    "field_changes": "Histórico de alterações",
    "collection_batches": "Lotes de coleta",
    "collection_work_items": "Fila de continuação",
}


async def run_blocking_maintenance[T](operation: Callable[[], T]) -> T:
    """Do not release the maintenance gate while its filesystem thread survives."""
    task = asyncio.create_task(asyncio.to_thread(operation))
    cancellation: asyncio.CancelledError | None = None
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError as exc:
            if task.cancelled():
                raise
            cancellation = exc
    if cancellation is not None:
        raise cancellation
    return result


class MaintenanceBlocked(RuntimeError):
    """Raised when active work prevents a destructive maintenance operation."""


@asynccontextmanager
async def exclusive_maintenance(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[str]:
    """Exclude collectors and other maintenance from backup through final commit."""
    owner = str(uuid4())
    async with write_session(session_factory) as session:
        now = datetime.now(UTC)
        repository = JobLeaseRepository(session)
        if not await repository.acquire(
            name=MAINTENANCE_LEASE,
            owner_id=owner,
            now=now,
            expires_at=now + timedelta(minutes=5),
        ):
            reason = await maintenance_blocked_reason(session)
            raise MaintenanceBlocked(reason or "outra manutenção está em andamento")
        lease = await session.get(JobLease, MAINTENANCE_LEASE)
        assert lease is not None
        generation = lease.version

    async def heartbeat() -> None:
        while True:
            await asyncio.sleep(30)
            async with write_session(session_factory) as session:
                moment = datetime.now(UTC)
                if not await JobLeaseRepository(session).renew(
                    name=MAINTENANCE_LEASE,
                    owner_id=owner,
                    generation=generation,
                    now=moment,
                    expires_at=moment + timedelta(minutes=5),
                ):
                    raise MaintenanceBlocked("a manutenção perdeu a propriedade da lease")

    task = asyncio.create_task(heartbeat(), name="maintenance-heartbeat")
    try:
        yield owner
        if task.done():
            await task
    finally:
        task.cancel()
        try:
            with suppress(asyncio.CancelledError):
                await task
        finally:

            async def release() -> None:
                async with write_session(session_factory) as session:
                    await JobLeaseRepository(session).release(
                        name=MAINTENANCE_LEASE, owner_id=owner, generation=generation
                    )

            release_task = asyncio.create_task(release())
            try:
                await asyncio.shield(release_task)
            except asyncio.CancelledError:
                await release_task
                raise


async def fence_maintenance(session: AsyncSession, owner: str) -> None:
    lease = await session.get(JobLease, MAINTENANCE_LEASE, populate_existing=True)
    if lease is None or not await JobLeaseRepository(session).fence(
        name=MAINTENANCE_LEASE,
        owner_id=owner,
        generation=lease.version,
        now=datetime.now(UTC),
    ):
        raise MaintenanceBlocked("manutenção exclusiva ausente ou expirada")


@dataclass(slots=True)
class ClearDataResult:
    """Rows and files removed by the reset."""

    counts: dict[str, int] = field(default_factory=dict)
    files_removed: int = 0
    scheduler_paused: bool = False

    @property
    def total_records(self) -> int:
        return sum(self.counts.values())


async def maintenance_blocked_reason(
    session: AsyncSession,
    *,
    now: datetime | None = None,
) -> str | None:
    """Explain why the reset cannot run right now, or return ``None``.

    Only unexpired leases count: rows left behind by a killed process are
    inert and must not block the operator forever. Crawl leases are covered by
    the active-run check, so they never block by themselves.
    """

    moment = now or datetime.now(UTC)
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
    leases = list(
        (await session.scalars(select(JobLease).where(JobLease.expires_at > moment))).all()
    )
    if any(not lease.name.startswith("crawl:") for lease in leases):
        return "um job do scheduler está em andamento — tente novamente em alguns minutos"
    return None


async def active_leases(
    session: AsyncSession,
    *,
    now: datetime | None = None,
) -> list[tuple[str, datetime]]:
    """Return unexpired leases as ``(name, expires_at)`` for diagnosis."""

    moment = now or datetime.now(UTC)
    rows = await session.scalars(
        select(JobLease)
        .where(JobLease.expires_at > moment)
        .order_by(JobLease.expires_at, JobLease.name)
    )
    return [(lease.name, lease.expires_at) for lease in rows.all()]


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
    await JobLeaseRepository(session).purge_stale(now=datetime.now(UTC))
    await session.commit()
    from app.database import create_session_factory

    assert session.bind is not None
    bound_engine = (
        session.bind.engine if isinstance(session.bind, AsyncConnection) else session.bind
    )
    factory = create_session_factory(bound_engine)
    async with exclusive_maintenance(factory) as owner:
        session.info["maintenance_owner"] = owner
        try:
            await fence_maintenance(session, owner)
            return await _clear_under_maintenance(session, selected)
        finally:
            await session.rollback()
            session.info.pop("maintenance_owner", None)


async def _clear_under_maintenance(session: AsyncSession, selected: Settings) -> ClearDataResult:

    result = ClearDataResult()
    set_scheduler_paused(True, settings=selected)
    result.scheduler_paused = True
    for table in reversed(Base.metadata.sorted_tables):
        count_query = select(func.count()).select_from(table)
        deletion = delete(table)
        if table.name == "job_leases":
            count_query = count_query.where(table.c.name != MAINTENANCE_LEASE)
            deletion = deletion.where(table.c.name != MAINTENANCE_LEASE)
        count = int(await session.scalar(count_query) or 0)
        if not count:
            continue
        result.counts[table.name] = count
        await session.execute(deletion)
    await session.commit()

    def remove_files_and_log() -> int:
        removed = _remove_stored_files(
            [selected.document_storage_path, selected.raw_data_storage_path],
            settings=selected,
        )
        _truncate_scheduler_log(selected.document_storage_path.resolve().parent / "scheduler.log")
        return removed

    result.files_removed = await run_blocking_maintenance(remove_files_and_log)
    return result


def _sqlite_path(database_url: str) -> Path | None:
    if not database_url.startswith("sqlite") or ":memory:" in database_url:
        return None
    fragment = database_url.split("///", 1)
    if len(fragment) != 2 or not fragment[1]:
        return None
    return Path(fragment[1].split("?", 1)[0]).resolve()


def _safe_storage_root(root: Path, settings: Settings) -> Path | None:
    resolved = root.expanduser().resolve()
    if not resolved.is_dir() or len(resolved.parts) <= 1:
        return None
    repo = Path.cwd().resolve()
    home = Path.home().resolve()
    if repo.is_relative_to(resolved) or home.is_relative_to(resolved):
        return None
    if any(
        (resolved / marker).exists()
        for marker in (".git", ".env", "pyproject.toml", "alembic.ini", "app", "migrations")
    ):
        return None
    database_path = _sqlite_path(settings.database_url)
    if database_path is not None and (
        database_path == resolved or database_path.is_relative_to(resolved)
    ):
        return None
    return resolved


def _remove_stored_files(roots: list[Path], *, settings: Settings) -> int:
    """Remove only validated storage roots, keeping .gitkeep."""

    removed = 0
    for root in roots:
        resolved = _safe_storage_root(root, settings)
        if resolved is None:
            continue
        for entry in resolved.iterdir():
            if entry.name == ".gitkeep":
                continue
            removed += _remove_entry(entry)
    return removed


def _remove_entry(entry: Path) -> int:
    """Delete a file, symlink, or directory tree and count removed files."""

    try:
        is_junction = getattr(entry, "is_junction", lambda: False)()
        if entry.is_symlink() or is_junction:
            entry.unlink()
            return 1
        if entry.is_dir():
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
