"""Unit tests for the scheduler pause flag and the leased job wrapper."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config import Settings
from app.jobs.scheduler import _leased
from app.services import scheduler_control


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        app_env="test",
        raw_data_storage_path=tmp_path / "raw",
        document_storage_path=tmp_path / "documents",
        holiday_calendar_path=tmp_path / "holidays.csv",
    )


def test_pause_flag_roundtrip_survives_outside_storage(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    assert scheduler_control.scheduler_paused(settings) is False
    scheduler_control.set_scheduler_paused(True, settings=settings)
    assert scheduler_control.scheduler_paused(settings) is True
    assert (tmp_path / "scheduler.paused").is_file()
    scheduler_control.set_scheduler_paused(False, settings=settings)
    assert scheduler_control.scheduler_paused(settings) is False
    assert not (tmp_path / "scheduler.paused").exists()


@pytest.mark.asyncio
async def test_leased_skips_jobs_while_paused(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    async def callback() -> None:
        nonlocal called
        called = True

    monkeypatch.setattr("app.jobs.scheduler.scheduler_paused", lambda: True)
    await _leased("discover_new_procurements", callback)

    assert called is False


@pytest.mark.asyncio
async def test_refresh_active_uses_update_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.jobs import scheduler

    requests = []

    class FakePipeline:
        def __init__(self, settings: object) -> None:
            del settings
            self.pipeline = self

        async def run(self, request: object) -> None:
            requests.append(request)

    monkeypatch.setattr(scheduler, "_ScheduledPipeline", FakePipeline)
    monkeypatch.setattr(scheduler, "recheck_compras", AsyncMock())
    monkeypatch.setattr(
        scheduler,
        "get_settings",
        lambda: SimpleNamespace(
            default_uf="MA",
            default_lookback_days=30,
            default_modalities=["pregao_eletronico"],
            active_refresh_batch_size=25,
        ),
    )
    session = SimpleNamespace(scalars=AsyncMock(return_value=SimpleNamespace(all=lambda: [])))
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=session)
    context.__aexit__ = AsyncMock(return_value=False)
    session.begin = lambda: context
    monkeypatch.setattr(scheduler, "async_session_factory", lambda: context)

    await scheduler.refresh_active_procurements()

    assert [(item.connector, item.discovery_kind) for item in requests] == [
        ("all", "update"),
    ]
