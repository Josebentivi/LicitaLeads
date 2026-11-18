"""Cooperative cancellation, durable recovery and launcher shutdown coordination."""

from __future__ import annotations

import asyncio
import time
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select, update

from app.config import Settings
from app.database import async_session_factory, write_session
from app.models import CollectionBatch, CrawlRun, CrawlRunStatus
from app.models.llm_queue import LLMAttempt, LLMBatch, LLMDemand, LLMWorkItem
from app.repositories.crawls import JobLeaseRepository
from app.runtime_control import RuntimeChannel, atomic_json, database_key, read_json
from app.services.scheduler_control import set_scheduler_paused

_mutations: set[asyncio.Task[Any]] = set()


def track_mutation(task: asyncio.Task[Any], *, remove: bool = False) -> None:
    (_mutations.discard if remove else _mutations.add)(task)


class ApplicationShutdown:
    def __init__(self, settings: Settings, *, factory=None):
        self.settings = settings
        self.factory = factory or async_session_factory
        self.channel = RuntimeChannel.from_environment()
        self.lock = asyncio.Lock()
        self.task: asyncio.Task[None] | None = None

    async def request(self) -> dict[str, Any]:
        assert self.channel is not None
        async with self.lock:
            if not self.channel.requested:
                await asyncio.to_thread(set_scheduler_paused, True, settings=self.settings)
                receipt = {
                    "instance": self.channel.instance,
                    "requested_at": time.time(),
                    "database": database_key(self.settings.database_url),
                    "cleanup_complete": False,
                    "leases": self._owned_leases(),
                }
                await asyncio.to_thread(atomic_json, self.channel.receipt_path, receipt)
                await asyncio.to_thread(self.channel.write, "request", receipt)
        return self.channel.public_status()

    def _owned_leases(self) -> list[dict[str, Any]]:
        assert self.channel is not None
        return [
            lease
            for path in self.channel.directory.glob("leases-*.json")
            for lease in read_json(path).get("leases", [])
        ]

    async def _receipt(self, **values: Any) -> dict[str, Any]:
        assert self.channel is not None
        receipt = {**self.channel.receipt(), **values}
        await asyncio.to_thread(atomic_json, self.channel.receipt_path, receipt)
        return receipt

    async def cancel_persisted(self) -> dict[str, Any]:
        """Snapshot first, then cancel in a transaction; replay is idempotent."""
        assert self.channel is not None
        receipt = self.channel.receipt()
        if receipt.get("database") != database_key(self.settings.database_url):
            raise RuntimeError("O banco do encerramento pendente difere do banco efetivo")
        await asyncio.to_thread(set_scheduler_paused, True, settings=self.settings)
        async with write_session(self.factory) as session:
            run_ids = receipt.get("run_ids")
            if run_ids is None:
                run_ids = [
                    str(value)
                    for value in await session.scalars(
                        select(CrawlRun.id).where(
                            CrawlRun.status.in_(
                                [
                                    CrawlRunStatus.PENDING,
                                    CrawlRunStatus.RUNNING,
                                ]
                            )
                        )
                    )
                ]
                leases = receipt.get("leases", []) + self._owned_leases()
                receipt = await self._receipt(run_ids=run_ids, leases=leases)
            await session.execute(
                update(CrawlRun)
                .where(
                    CrawlRun.id.in_([UUID(value) for value in run_ids]),
                    CrawlRun.status.in_([CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING]),
                )
                .values(cancel_requested=True)
            )
            # All pending demands, including manual and shared batches, are cancelled.
            unfinished = (
                select(LLMDemand.batch_id)
                .join(
                    LLMWorkItem,
                    LLMWorkItem.id == LLMDemand.work_id,
                )
                .where(LLMWorkItem.status.in_(["pending", "running"]))
            )
            await session.execute(
                update(LLMBatch)
                .where(
                    LLMBatch.id.in_(unfinished),
                    LLMBatch.cancelled.is_(False),
                )
                .values(cancelled=True)
            )
            await session.execute(
                update(LLMWorkItem)
                .where(
                    LLMWorkItem.status.in_(["pending", "running"]),
                )
                .values(status="cancelled")
            )
            await session.execute(
                update(CollectionBatch)
                .where(
                    CollectionBatch.status.in_(["pending", "running", "limited"]),
                )
                .values(status="suspended")
            )
        return receipt

    async def finalize(self) -> None:
        assert self.channel is not None
        receipt = self.channel.receipt()
        moment = datetime.now(UTC)
        from app.services.ingestion.recovery import _normalize_cursor

        async with write_session(self.factory) as session:
            # Only the exact generations acquired by this runtime can be released.
            for lease in receipt.get("leases", []):
                await JobLeaseRepository(session).release(
                    name=lease["name"],
                    owner_id=lease["owner"],
                    generation=lease["generation"],
                )
            for identifier in receipt.get("run_ids", []):
                run = await session.get(CrawlRun, UUID(identifier))
                if run and run.status in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}:
                    run.cancel_requested = True
                    run.status = CrawlRunStatus.CANCELLED
                    run.started_at = run.started_at or moment
                    run.finished_at = moment
                    run.diagnostic = "Coleta cancelada pelo encerramento da aplicação"
                    run.errors = [
                        *(run.errors or []),
                        {
                            "message": run.diagnostic,
                            "shutdown_id": receipt["instance"],
                        },
                    ]
                    _normalize_cursor(run, run.diagnostic)
                    run.cursor = {**(run.cursor or {}), "shutdown_id": receipt["instance"]}
            await session.execute(
                update(LLMAttempt)
                .where(
                    LLMAttempt.finished_at.is_(None),
                    LLMAttempt.work_id.in_(
                        select(LLMWorkItem.id).where(
                            LLMWorkItem.status == "cancelled",
                        )
                    ),
                )
                .values(status="interrupted", finished_at=moment)
            )
        await self._receipt(cleanup_complete=True)

    async def recover(self) -> None:
        if self.channel is None:
            return
        receipt = self.channel.receipt()
        if receipt and not receipt.get("cleanup_complete"):
            # A new launcher owns no old process. Only recorded leases are released.
            if receipt.get("database") != database_key(self.settings.database_url):
                raise RuntimeError("O banco do encerramento pendente difere do banco efetivo")
            async with write_session(self.factory) as session:
                for lease in receipt.get("leases", []):
                    await JobLeaseRepository(session).release(
                        name=lease["name"],
                        owner_id=lease["owner"],
                        generation=lease["generation"],
                    )
            await self.cancel_persisted()
            await self.finalize()

    async def watch(self, app) -> None:
        assert self.channel is not None
        while not self.channel.requested:  # noqa: ASYNC110 - cross-process file notification
            await asyncio.sleep(0.2)
        try:
            await asyncio.to_thread(
                self.channel.write,
                "api",
                {
                    "phase": "cancelling",
                    "detail": "Cancelando coletas, IA e testes…",
                },
            )
            from app.api.routes.crawls import cancel_all_running_crawls
            from app.services.ingestion.municipal_payments import cancel_all_municipal_tasks
            from app.web import _application_tests

            tasks = [task for task in _mutations if task is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.cancel_persisted()
            await asyncio.gather(
                cancel_all_running_crawls(),
                cancel_all_municipal_tasks(),
                _application_tests.aclose(),
            )
            while not self.channel.read("launcher").get("scheduler_stopped"):  # noqa: ASYNC110
                await asyncio.sleep(0.2)
            await self.cancel_persisted()
            await self.finalize()
            await asyncio.to_thread(
                self.channel.write,
                "api",
                {
                    "phase": "drained",
                    "detail": "Trabalhos cancelados; finalizando os serviços…",
                },
            )
            while self.channel.read("launcher").get("phase") != "exit":  # noqa: ASYNC110
                await asyncio.sleep(0.2)
            callback = getattr(app.state, "request_server_exit", None)
            if callback:
                callback()
        except asyncio.CancelledError:
            raise
        except Exception:
            import logging

            logging.getLogger(__name__).exception("application shutdown cleanup failed")
            await asyncio.to_thread(
                self.channel.write,
                "api",
                {
                    "phase": "error",
                    "detail": "Limpeza incompleta; o launcher concluirá o encerramento.",
                },
            )

    async def close(self) -> None:
        if self.task and not self.task.done():
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task
