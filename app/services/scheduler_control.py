"""Operator control for the automatic collection scheduler.

The pause flag lives next to the storage directories (not inside them), so it
survives the destructive database reset.  The scheduler process reads it on
every job tick, which makes pause/resume effective without a restart.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.models import JobLease


def pause_file(settings: Settings | None = None) -> Path:
    """Return the flag path used by the scheduler process and the UI."""

    selected = settings or get_settings()
    return selected.raw_data_storage_path.parent / "scheduler.paused"


def heartbeat_file(settings: Settings | None = None) -> Path:
    selected = settings or get_settings()
    return selected.raw_data_storage_path.parent / "scheduler.heartbeat"


def write_scheduler_heartbeat(settings: Settings | None = None) -> None:
    path = heartbeat_file(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(datetime.now(UTC).isoformat(), encoding="utf-8")


def clear_scheduler_heartbeat(settings: Settings | None = None) -> None:
    heartbeat_file(settings).unlink(missing_ok=True)


def scheduler_heartbeat_age(settings: Settings | None = None) -> float | None:
    path = heartbeat_file(settings)
    try:
        modified = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return None
    return max((datetime.now(UTC) - modified).total_seconds(), 0.0)


def scheduler_paused(settings: Settings | None = None) -> bool:
    return pause_file(settings).is_file()


def set_scheduler_paused(paused: bool, *, settings: Settings | None = None) -> None:
    path = pause_file(settings)
    if paused:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(datetime.now(UTC).isoformat(), encoding="utf-8")
    else:
        path.unlink(missing_ok=True)


async def scheduler_status(
    session: AsyncSession,
    settings: Settings | None = None,
) -> dict[str, object]:
    """Return pause, configuration, heartbeat and newest scheduler lease."""

    selected = settings or get_settings()
    last_activity = await session.scalar(
        select(func.max(JobLease.heartbeat_at)).where(JobLease.name.like("scheduler:%"))
    )
    age = scheduler_heartbeat_age(selected)
    alive = bool(selected.scheduler_enabled and age is not None and age <= 120)
    return {
        "paused": scheduler_paused(selected),
        "enabled": selected.scheduler_enabled,
        "last_activity_at": last_activity,
        "heartbeat_age_seconds": age,
        "alive": alive,
    }
