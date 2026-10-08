"""Destructive reset must wipe data atomically and never touch config."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.models import (
    CrawlRun,
    CrawlRunStatus,
    Document,
    ExtractionStatus,
    JobLease,
    PriceRegistry,
    PriceRegistryItem,
    Procurement,
)
from app.services import maintenance, scheduler_control

from .conftest import DatabaseContext


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        app_env="test",
        document_storage_path=tmp_path / "documents",
        raw_data_storage_path=tmp_path / "raw",
        holiday_calendar_path=tmp_path / "holidays.csv",
    )


async def _seed(database: DatabaseContext) -> None:
    async with database.sessions() as session, session.begin():
        procurement = Procurement(
            source="pncp",
            external_id="wipe-1",
            title="Pregão de equipamentos",
            fingerprint="a" * 64,
        )
        session.add(procurement)
        await session.flush()
        session.add(
            Document(
                procurement_id=procurement.id,
                document_type="ata",
                title="Ata da sessão",
                original_url="https://pncp.gov.br/ata.pdf",
                extraction_status=ExtractionStatus.PENDING,
                fingerprint="b" * 64,
            )
        )
        session.add(CrawlRun(connector="pncp", status=CrawlRunStatus.COMPLETED, filters={}))


@pytest.mark.asyncio
async def test_clear_data_endpoint_wipes_database_and_files(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr(maintenance, "get_settings", lambda: settings)
    documents_root = settings.document_storage_path
    (documents_root / "ab").mkdir(parents=True)
    (documents_root / "ab" / "ata.pdf").write_bytes(b"%PDF-1.4")
    (documents_root / ".gitkeep").write_text("", encoding="utf-8")
    raw_root = settings.raw_data_storage_path
    raw_root.mkdir(parents=True)
    (raw_root / "payload.json").write_text("{}", encoding="utf-8")
    scheduler_log = tmp_path / "scheduler.log"
    scheduler_log.write_text("log antigo\n", encoding="utf-8")
    await _seed(database)

    response = await api_client.post("/api/maintenance/clear-data", json={"confirm": True})

    assert response.status_code == 200
    payload = response.json()
    assert payload["counts"]["procurements"] == 1
    assert payload["counts"]["documents"] == 1
    assert payload["counts"]["crawl_runs"] == 1
    assert payload["files_removed"] == 2
    assert payload["scheduler_paused"] is True
    assert not (documents_root / "ab").exists()
    assert (documents_root / ".gitkeep").exists()
    assert not (raw_root / "payload.json").exists()
    assert scheduler_log.read_text(encoding="utf-8") == ""
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Procurement)) == 0
        assert await session.scalar(select(func.count()).select_from(Document)) == 0
        assert await session.scalar(select(func.count()).select_from(CrawlRun)) == 0


@pytest.mark.asyncio
async def test_maintenance_counts_and_clears_price_registries(
    database: DatabaseContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reset danger zone reports and wipes ARP/ata rows."""

    settings = _settings(tmp_path)
    monkeypatch.setattr(maintenance, "get_settings", lambda: settings)
    async with database.sessions() as session, session.begin():
        registry = PriceRegistry(source="compras_gov", external_id="ata-1", fingerprint="p" * 64)
        session.add(registry)
        await session.flush()
        session.add(
            PriceRegistryItem(
                price_registry_id=registry.id,
                item_number="1",
                fingerprint="q" * 64,
            )
        )

    async with database.sessions() as session:
        counts = await maintenance.data_counts(session)
    assert counts["price_registries"] == 1
    assert counts["price_registry_items"] == 1

    async with database.sessions() as session:
        result = await maintenance.clear_all_data(session, settings=settings)
    assert result.counts["price_registries"] == 1
    assert result.counts["price_registry_items"] == 1
    assert result.scheduler_paused is True
    assert (tmp_path / "scheduler.paused").is_file()
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(PriceRegistry)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceRegistryItem)) == 0


@pytest.mark.asyncio
async def test_clear_data_endpoint_requires_confirmation(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    await _seed(database)

    response = await api_client.post("/api/maintenance/clear-data", json={"confirm": False})

    assert response.status_code == 422
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Procurement)) == 1


@pytest.mark.asyncio
async def test_clear_data_blocks_active_crawl_and_scheduler_job(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    await _seed(database)
    async with database.sessions() as session, session.begin():
        session.add(CrawlRun(connector="all", status=CrawlRunStatus.RUNNING, filters={}))
    blocked = await api_client.post("/api/maintenance/clear-data", json={"confirm": True})
    assert blocked.status_code == 409
    assert "coleta" in blocked.json()["detail"]

    async with database.sessions() as session, session.begin():
        run = await session.scalar(
            select(CrawlRun).where(CrawlRun.status == CrawlRunStatus.RUNNING)
        )
        assert run is not None
        run.status = CrawlRunStatus.COMPLETED
        now = datetime.now(UTC)
        session.add(
            JobLease(
                name="scheduler:test",
                owner_id="test",
                acquired_at=now,
                heartbeat_at=now,
                expires_at=now + timedelta(minutes=5),
            )
        )
    lease_blocked = await api_client.post("/api/maintenance/clear-data", json={"confirm": True})
    assert lease_blocked.status_code == 409
    assert "scheduler" in lease_blocked.json()["detail"]
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Procurement)) == 1


@pytest.mark.asyncio
async def test_settings_page_offers_reset_and_explains_blocks(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(maintenance, "get_settings", lambda: _settings(tmp_path))
    await _seed(database)

    page = await api_client.get("/settings")
    assert page.status_code == 200
    assert "Zona de risco" in page.text
    assert "Apagar todos os dados" in page.text

    without_confirmation = await api_client.post("/settings/clear-data", follow_redirects=False)
    assert without_confirmation.status_code == 303
    assert without_confirmation.headers["location"] == "/settings?reset=sem-confirmacao"

    accepted = await api_client.post(
        "/settings/clear-data",
        data={"confirmo": "on"},
        follow_redirects=False,
    )
    assert accepted.status_code == 303
    assert accepted.headers["location"] == "/settings?reset=ok"
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Procurement)) == 0

    async with database.sessions() as session, session.begin():
        session.add(CrawlRun(connector="all", status=CrawlRunStatus.RUNNING, filters={}))
    blocked_page = await api_client.get("/settings")
    assert "Bloqueado agora" in blocked_page.text
    blocked_reset = await api_client.post(
        "/settings/clear-data",
        data={"confirmo": "on"},
        follow_redirects=False,
    )
    assert blocked_reset.headers["location"] == "/settings?reset=bloqueado"


@pytest.mark.asyncio
async def test_maintenance_service_helpers_directly(
    database: DatabaseContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr(maintenance, "get_settings", lambda: settings)
    await _seed(database)

    async with database.sessions() as session:
        assert await maintenance.maintenance_blocked_reason(session) is None
        counts = await maintenance.data_counts(session)
    assert counts["procurements"] == 1
    assert counts["documents"] == 1

    async with database.sessions() as session, session.begin():
        session.add(CrawlRun(connector="all", status=CrawlRunStatus.RUNNING, filters={}))

    async with database.sessions() as session:
        reason = await maintenance.maintenance_blocked_reason(session)
    assert reason is not None and "coleta" in reason
    async with database.sessions() as session:
        with pytest.raises(maintenance.MaintenanceBlocked):
            await maintenance.clear_all_data(session, settings=settings)

    async with database.sessions() as session, session.begin():
        run = await session.scalar(
            select(CrawlRun).where(CrawlRun.status == CrawlRunStatus.RUNNING)
        )
        assert run is not None
        run.status = CrawlRunStatus.COMPLETED
        now = datetime.now(UTC)
        session.add(
            JobLease(
                name="scheduler:direct-test",
                owner_id="test",
                acquired_at=now,
                heartbeat_at=now,
                expires_at=now + timedelta(minutes=5),
            )
        )

    async with database.sessions() as session:
        reason = await maintenance.maintenance_blocked_reason(session)
    assert reason is not None and "scheduler" in reason

    async with database.sessions() as session, session.begin():
        lease = await session.get(JobLease, "scheduler:direct-test")
        assert lease is not None
        await session.delete(lease)

    async with database.sessions() as session:
        result = await maintenance.clear_all_data(session, settings=settings)
    assert result.counts["procurements"] == 1
    assert result.total_records >= 3


@pytest.mark.asyncio
async def test_expired_and_orphan_leases_do_not_block_reset(
    database: DatabaseContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leases left by dead processes are cleaned instead of blocking the operator."""

    settings = _settings(tmp_path)
    monkeypatch.setattr(maintenance, "get_settings", lambda: settings)
    await _seed(database)
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        session.add_all(
            [
                JobLease(
                    name="scheduler:stale_job",
                    owner_id="host:dead",
                    acquired_at=now - timedelta(hours=4),
                    heartbeat_at=now - timedelta(hours=4),
                    expires_at=now - timedelta(hours=2),
                ),
                JobLease(
                    name="crawl:all:MA",
                    owner_id="pipeline:dead",
                    acquired_at=now - timedelta(hours=4),
                    heartbeat_at=now - timedelta(hours=4),
                    expires_at=now + timedelta(hours=1),
                ),
            ]
        )

    async with database.sessions() as session:
        reason = await maintenance.maintenance_blocked_reason(session)
    assert reason is None

    async with database.sessions() as session:
        result = await maintenance.clear_all_data(session, settings=settings)
    assert result.counts["procurements"] == 1
    async with database.sessions() as session:
        remaining = await session.scalar(select(func.count()).select_from(JobLease))
    assert remaining == 0


@pytest.mark.asyncio
async def test_valid_scheduler_lease_blocks_and_is_listed_with_details(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unexpired scheduler lease still blocks, and the page shows how to recover."""

    settings = _settings(tmp_path)
    monkeypatch.setattr(maintenance, "get_settings", lambda: settings)
    await _seed(database)
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        session.add(
            JobLease(
                name="scheduler:enrich_pending_companies",
                owner_id="host:alive",
                acquired_at=now,
                heartbeat_at=now,
                expires_at=now + timedelta(minutes=30),
            )
        )

    page = await api_client.get("/settings")

    assert page.status_code == 200
    assert "Bloqueado agora" in page.text
    assert "scheduler" in page.text
    assert "scheduler:enrich_pending_companies" in page.text
    assert "Detalhes técnicos" in page.text
    assert "Liberar travas antigas" in page.text
    async with database.sessions() as session:
        reason = await maintenance.maintenance_blocked_reason(session)
    assert reason is not None and "scheduler" in reason


@pytest.mark.asyncio
async def test_release_stale_leases_endpoint_purges_orphans(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The operator action removes expired and orphan crawl leases."""

    settings = _settings(tmp_path)
    monkeypatch.setattr(maintenance, "get_settings", lambda: settings)
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        session.add_all(
            [
                JobLease(
                    name="scheduler:stale_job",
                    owner_id="host:dead",
                    acquired_at=now - timedelta(hours=4),
                    heartbeat_at=now - timedelta(hours=4),
                    expires_at=now - timedelta(hours=2),
                ),
                JobLease(
                    name="crawl:all:MA",
                    owner_id="pipeline:dead",
                    acquired_at=now - timedelta(hours=4),
                    heartbeat_at=now - timedelta(hours=4),
                    expires_at=now + timedelta(hours=1),
                ),
            ]
        )

    response = await api_client.post("/settings/release-stale-leases", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?reset=livre"
    async with database.sessions() as session:
        remaining = await session.scalar(select(func.count()).select_from(JobLease))
    assert remaining == 0


@pytest.mark.asyncio
async def test_settings_page_controls_scheduler_pause(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The settings page shows scheduler status and toggles the pause flag."""

    settings = _settings(tmp_path)
    monkeypatch.setattr(maintenance, "get_settings", lambda: settings)
    monkeypatch.setattr(scheduler_control, "get_settings", lambda: settings)
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        session.add(
            JobLease(
                name="scheduler:recalculate_deadlines",
                owner_id="host:alive",
                acquired_at=now,
                heartbeat_at=now,
                expires_at=now + timedelta(minutes=30),
            )
        )

    page = await api_client.get("/settings")
    assert page.status_code == 200
    assert "Coletas automáticas (scheduler)" in page.text
    assert "Ativo" in page.text
    assert "Última atividade" in page.text

    paused = await api_client.post("/settings/pause-scheduler", follow_redirects=False)
    assert paused.status_code == 303
    assert paused.headers["location"] == "/settings?scheduler=pausado"
    assert (tmp_path / "scheduler.paused").is_file()

    after_pause = await api_client.get("/settings")
    assert "Pausado" in after_pause.text
    assert "Retomar coletas automáticas" in after_pause.text

    resumed = await api_client.post("/settings/resume-scheduler", follow_redirects=False)
    assert resumed.status_code == 303
    assert resumed.headers["location"] == "/settings?scheduler=ativo"
    assert not (tmp_path / "scheduler.paused").exists()
