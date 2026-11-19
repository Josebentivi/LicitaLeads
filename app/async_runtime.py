"""Event loops for Windows database and subprocess entry points."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Coroutine
from contextlib import suppress
from typing import Any, Literal

from app.config import get_settings


def _uses_psycopg(database_url: str) -> bool:
    return database_url.partition(":")[0] in {
        "postgres",
        "postgresql",
        "postgresql+psycopg",
        "postgresql+psycopg_async",
    }


def run_database_async[Result](coroutine: Coroutine[Any, Any, Result]) -> Result:
    if sys.platform == "win32" and _uses_psycopg(get_settings().database_url):
        # The psycopg async driver needs add_reader(), which Proactor lacks.
        return asyncio.run(coroutine, loop_factory=asyncio.SelectorEventLoop)
    return asyncio.run(coroutine)


def configure_uvicorn_loop(database_url: str) -> Literal["auto", "none"]:
    if sys.platform == "win32" and _uses_psycopg(database_url):
        # Uvicorn's `none` uses the installed policy. This also works with
        # versions that predate Uvicorn's custom loop factory support.
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        return "none"
    return "auto"


async def run_subprocess_async[Result](coroutine: Coroutine[Any, Any, Result]) -> Result:
    if sys.platform != "win32" or isinstance(asyncio.get_running_loop(), asyncio.ProactorEventLoop):
        return await coroutine

    # Selector loops do not support Windows subprocess transports. Keep the
    # process supervisor and its cancellation handling together on a Proactor.
    caller_loop = asyncio.get_running_loop()
    started: asyncio.Future[tuple[asyncio.AbstractEventLoop, asyncio.Task[Any]]] = (
        caller_loop.create_future()
    )

    def worker() -> Result:
        async def supervise() -> Result:
            task = asyncio.current_task()
            assert task is not None
            caller_loop.call_soon_threadsafe(started.set_result, (asyncio.get_running_loop(), task))
            return await coroutine

        return asyncio.run(supervise(), loop_factory=asyncio.ProactorEventLoop)

    running = asyncio.create_task(asyncio.to_thread(worker))
    try:
        return await asyncio.shield(running)
    except asyncio.CancelledError:
        if not running.done():
            worker_loop, worker_task = await started
            # If the coroutine completed concurrently, the worker loop may
            # already be closed and there is no child left to cancel.
            with suppress(RuntimeError):
                worker_loop.call_soon_threadsafe(worker_task.cancel)
        with suppress(asyncio.CancelledError):
            await asyncio.shield(running)
        raise
