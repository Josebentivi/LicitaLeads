"""Bounded, coalesced crawl telemetry; counters remain owned by committed writes."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from app.connectors.progress import ProgressEvent
from app.models import CrawlRun

STAGE_LABELS = {
    "queued": "Aguardando início",
    "discovery": "Buscando registros",
    "details": "Consultando detalhes",
    "documents": "Processando documentos do lote",
    "persisting": "Gravando registros",
    "finalizing": "Finalizando coleta",
    "completed": "Coleta concluída",
    "partial": "Concluída com pendências",
    "failed": "Coleta com falha",
    "cancelled": "Coleta cancelada",
}


def safe_label(value: str) -> str:
    value = re.sub(r"https?://\S+", "[endereço omitido]", value)
    value = re.sub(
        r"(?i)(password|token|secret|api[_-]?key|authorization|cookie|chave-api-dados)\s*[:=]\s*\S+",
        r"\1=***",
        value,
    )
    return " ".join(value.split())[:180]


class CrawlProgressRecorder:
    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self.progress: dict[str, Any] = {}
        self.parallel: dict[str, dict[str, int | float]] = {}
        self._observe_lock = asyncio.Lock()
        self.activity: list[dict[str, Any]] = []
        self.clock = clock
        self.last_saved = float("-inf")
        self.pending: asyncio.Task[None] | None = None

    def record(self, event: ProgressEvent) -> bool:
        previous = self.progress
        if event.source is not None:
            stats = {
                key: value
                for key, value in asdict(event).items()
                if value is not None
                and key.startswith(("http_", "download_", "process_", "queue_"))
            }
            self.parallel[event.source] = {**self.parallel.get(event.source, {}), **stats}
            if stats and not any(
                value is not None
                for key, value in asdict(event).items()
                if key != "source" and key not in stats
            ):
                return False
        values = {key: value for key, value in asdict(event).items() if value is not None}
        changed_stage = any(
            key in values and values[key] != previous.get(key) for key in ("stage", "source")
        )
        changed_operation = changed_stage or any(
            key in values and values[key] != previous.get(key) for key in ("operation", "subject")
        )
        current = dict(previous)
        if changed_stage:
            for key in ("processed", "total", "operation", "subject", "page", "total_pages"):
                current.pop(key, None)
        if changed_operation:
            for key in ("page", "total_pages", "attempt", "max_attempts", "retry_at"):
                current.pop(key, None)
        elif "attempt" in values and "retry_at" not in values:
            current.pop("retry_at", None)
        for key, value in values.items():
            current[key] = safe_label(value) if isinstance(value, str) else max(0, value)
        current["updated_at"] = datetime.now(UTC).isoformat()
        self.progress = current
        message = current.get("message") or STAGE_LABELS.get(
            current.get("stage", ""), "Em andamento"
        )
        if event.page is not None:
            message = f"{message} · página {event.page} recebida"
        if not self.activity or (message, current.get("source")) != (
            self.activity[-1]["message"],
            self.activity[-1].get("source"),
        ):
            self.activity.append(
                {
                    "at": current["updated_at"],
                    "message": message,
                    "source": current.get("source"),
                    "stage": current.get("stage"),
                }
            )
            self.activity = self.activity[-20:]
        return changed_stage

    async def observe(self, event: ProgressEvent, persist: Callable[[], Awaitable[None]]) -> None:
        async with self._observe_lock:
            await self._observe(event, persist)

    def enqueue(self, event: ProgressEvent, persist: Callable[[], Awaitable[None]]) -> None:
        """Never hold a network admission slot while telemetry waits for SQLite."""
        self.record(event)
        if self.pending is None:

            async def later() -> None:
                try:
                    await asyncio.sleep(max(0, 2 - (self.clock() - self.last_saved)))
                    await persist()
                except Exception:
                    logging.getLogger(__name__).warning("crawl_progress_update_unavailable")
                finally:
                    self.pending = None

            self.pending = asyncio.create_task(later(), name="crawl-resource-flush")

    async def _observe(self, event: ProgressEvent, persist: Callable[[], Awaitable[None]]) -> None:
        force = self.record(event)
        elapsed = self.clock() - self.last_saved
        if (force and elapsed >= 0.25) or elapsed >= 2:
            await self.aclose()
            await persist()
        elif self.pending is None:

            async def later() -> None:
                try:
                    await asyncio.sleep(max(0, 2 - (self.clock() - self.last_saved)))
                    await persist()
                except Exception:
                    logging.getLogger(__name__).warning("crawl_progress_update_unavailable")
                finally:
                    self.pending = None

            self.pending = asyncio.create_task(later(), name="crawl-progress-flush")

    async def aclose(self) -> None:
        task = self.pending
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await task
            self.pending = None

    def apply(self, run: CrawlRun) -> None:
        cursor = dict(run.cursor or {})
        cursor.update(
            progress=dict(self.progress),
            activity=list(self.activity),
            parallel={source: dict(stats) for source, stats in self.parallel.items()},
        )
        run.cursor = cursor
        self.last_saved = self.clock()
