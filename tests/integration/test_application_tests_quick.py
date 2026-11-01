"""Quick diagnostics against the isolated integration app/database."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text

from app.config import Settings
from app.main import app as asgi_app
from app.models import CrawlRun, CrawlRunStatus, JobLease
from app.services.application_tests import ApplicationTestService
from app.services.scheduler_control import set_scheduler_paused, write_scheduler_heartbeat

from .conftest import DatabaseContext


@pytest.mark.asyncio
async def test_quick_diagnostics_run_against_isolated_database(
    database: DatabaseContext,
    api_client: httpx.AsyncClient,
    tmp_path: Path,
) -> None:
    documents = tmp_path / "documents"
    raw = tmp_path / "raw"
    documents.mkdir()
    raw.mkdir()
    head = ScriptDirectory.from_config(Config("alembic.ini")).get_current_head()
    async with database.engine.begin() as connection:
        await connection.execute(
            text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
        )
        await connection.execute(
            text("INSERT INTO alembic_version (version_num) VALUES (:version)"),
            {"version": head},
        )
    settings = Settings(
        app_env="test",
        document_storage_path=documents,
        raw_data_storage_path=raw,
        municipal_payment_sources=["imperatriz", "timon"],
        payment_sources=["manual"],
    )
    service = ApplicationTestService(
        settings,
        session_factory=database.sessions,
        report_path=tmp_path / "report.json",
    )

    results = await service._run_quick(asgi_app, include_live=False)

    by_key = {result.key: result for result in results}
    assert set(by_key) == {
        "environment",
        "database",
        "web",
        "api",
        "security",
        "domain",
        "sources",
        "scheduler",
    }
    assert all(result.status != "failed" for result in results), {
        key: (result.status, result.detail) for key, result in by_key.items()
    }
    assert by_key["database"].detail.startswith("Banco conectado")
    assert "páginas responderam" in by_key["web"].detail
    assert "endpoints" in by_key["api"].detail


@pytest.mark.asyncio
async def test_database_check_rejects_missing_or_wrong_revision(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    settings = Settings(
        app_env="test",
        document_storage_path=tmp_path / "documents",
        raw_data_storage_path=tmp_path / "raw",
        municipal_payment_sources=[],
        payment_sources=["manual"],
    )
    service = ApplicationTestService(
        settings, session_factory=database.sessions, report_path=tmp_path / "report.json"
    )
    assert (await service._check_database()).status == "failed"

    async with database.engine.begin() as connection:
        await connection.execute(
            text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
        )
    assert (await service._check_database()).status == "failed"

    async with database.engine.begin() as connection:
        await connection.execute(text("INSERT INTO alembic_version (version_num) VALUES ('wrong')"))
    assert (await service._check_database()).status == "failed"
    async with database.engine.begin() as connection:
        await connection.execute(text("DELETE FROM alembic_version"))

    head = ScriptDirectory.from_config(Config("alembic.ini")).get_current_head()
    async with database.engine.begin() as connection:
        await connection.execute(
            text("INSERT INTO alembic_version (version_num) VALUES (:version)"),
            {"version": head},
        )
    async with database.sessions() as session, session.begin():
        session.add(CrawlRun(connector="pncp", status=CrawlRunStatus.RUNNING, filters={}))
    assert (await service._check_database()).status == "warning"


@pytest.mark.asyncio
async def test_scheduler_check_requires_heartbeat(
    database: DatabaseContext, tmp_path: Path
) -> None:
    settings = Settings(
        app_env="test",
        document_storage_path=tmp_path / "documents",
        raw_data_storage_path=tmp_path / "raw",
        municipal_payment_sources=[],
        payment_sources=["manual"],
        scheduler_enabled=True,
    )
    service = ApplicationTestService(
        settings, session_factory=database.sessions, report_path=tmp_path / "report.json"
    )
    outcome = await service._check_scheduler()

    assert outcome.status == "warning"
    assert "heartbeat" in outcome.detail


@pytest.mark.asyncio
async def test_scheduler_check_distinguishes_disabled_paused_and_alive(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    base = dict(
        app_env="test",
        document_storage_path=tmp_path / "documents",
        raw_data_storage_path=tmp_path / "raw",
        municipal_payment_sources=[],
        payment_sources=["manual"],
        scheduler_enabled=True,
    )
    settings = Settings(**base)
    service = ApplicationTestService(
        settings, session_factory=database.sessions, report_path=tmp_path / "report.json"
    )

    set_scheduler_paused(True, settings=settings)
    assert (await service._check_scheduler()).status == "warning"
    set_scheduler_paused(False, settings=settings)
    assert (await service._check_scheduler()).status == "warning"

    write_scheduler_heartbeat(settings)
    assert (await service._check_scheduler()).status == "passed"

    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        session.add(
            JobLease(
                name="scheduler:test",
                owner_id="owner",
                acquired_at=now,
                heartbeat_at=now,
                expires_at=now + timedelta(minutes=5),
            )
        )
    assert (await service._check_scheduler()).status == "warning"

    disabled = Settings(**{**base, "scheduler_enabled": False})
    disabled_service = ApplicationTestService(
        disabled, session_factory=database.sessions, report_path=tmp_path / "report2.json"
    )
    assert (await disabled_service._check_scheduler()).status == "warning"


@pytest.mark.asyncio
async def test_security_check_warns_for_wildcard_host(
    database: DatabaseContext, tmp_path: Path
) -> None:
    settings = Settings(
        app_env="test",
        document_storage_path=tmp_path / "documents",
        raw_data_storage_path=tmp_path / "raw",
        municipal_payment_sources=[],
        payment_sources=["manual"],
        allowed_hosts=["*"],
    )
    service = ApplicationTestService(
        settings, session_factory=database.sessions, report_path=tmp_path / "report.json"
    )

    outcome = await service._check_security(asgi_app)

    assert outcome.status == "warning"
    assert "TrustedHost" in (outcome.output or "")
