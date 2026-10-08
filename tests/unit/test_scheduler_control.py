"""Unit tests for the scheduler pause flag and the leased job wrapper."""

from __future__ import annotations

from pathlib import Path

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
