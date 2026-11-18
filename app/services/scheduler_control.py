"""Operator control for the automatic collection scheduler.

The pause flag lives next to the storage directories (not inside them), so it
survives the destructive database reset.  The scheduler process reads it on
every job tick, which makes pause/resume effective without a restart.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from contextlib import contextmanager
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
    return component_paused("collections", settings)


def control_state(settings: Settings | None = None) -> dict[str, bool]:
    path = pause_file(settings).with_name("scheduler.controls.json")
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(state, dict) or any(
            type(state.get(k)) is not bool for k in ("collections", "llm")
        ):
            raise ValueError("invalid controls")
        if pause_file(settings).is_file() and not all(state.values()):
            return {"collections": True, "llm": True}
        return {k: state[k] for k in ("collections", "llm")}
    except FileNotFoundError:
        legacy = pause_file(settings).is_file()
        return {"collections": legacy, "llm": legacy}
    except (ValueError, OSError):
        # Damaged operational state must never start paid calls or collections.
        return {"collections": True, "llm": True}


def component_paused(component: str, settings: Settings | None = None) -> bool:
    return control_state(settings)[component]


@contextmanager
def _control_lock(path: Path):
    deadline = time.monotonic() + 3
    with path.open("a+b") as stream:
        if path.stat().st_size == 0:
            stream.write(b"0")
            stream.flush()
        while True:
            try:
                stream.seek(0)
                if sys.platform == "win32":
                    import msvcrt

                    msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise RuntimeError("Controle do scheduler ocupado") from None
                time.sleep(0.02)
        try:
            yield
        finally:
            stream.seek(0)
            if sys.platform == "win32":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def set_component_paused(component: str, paused: bool, *, settings: Settings | None = None) -> None:
    if component not in {"collections", "llm", "all"}:
        raise ValueError("Controle desconhecido")
    flag = pause_file(settings)
    flag.parent.mkdir(parents=True, exist_ok=True)
    path = flag.with_name("scheduler.controls.json")
    with _control_lock(path.with_suffix(".lock")):
        state = control_state(settings)
        for key in state:
            if component in {key, "all"}:
                state[key] = paused
        descriptor, name = tempfile.mkstemp(prefix="scheduler.controls-", dir=path.parent)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(state, stream)
            os.replace(name, path)
        finally:
            Path(name).unlink(missing_ok=True)
        if all(state.values()):
            flag.write_text(datetime.now(UTC).isoformat(), encoding="utf-8")
        else:
            flag.unlink(missing_ok=True)


def set_scheduler_paused(paused: bool, *, settings: Settings | None = None) -> None:
    set_component_paused("all", paused, settings=settings)


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
        "llm_paused": component_paused("llm", selected),
        "enabled": selected.scheduler_enabled,
        "last_activity_at": last_activity,
        "heartbeat_age_seconds": age,
        "alive": alive,
    }
