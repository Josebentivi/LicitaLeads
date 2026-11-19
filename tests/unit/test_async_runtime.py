import asyncio
import subprocess
import sys
from threading import Event
from types import SimpleNamespace

import pytest

from app import async_runtime

pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="loops específicos do Windows")


def test_windows_selects_the_compatible_loop_only_for_psycopg(monkeypatch):
    original_policy = asyncio.get_event_loop_policy()
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    async def current_loop():
        return asyncio.get_running_loop()

    sqlite_url = "sqlite:///:memory:"
    postgres_url = "postgresql://localhost/licitaleads"
    settings = SimpleNamespace(database_url=sqlite_url)
    monkeypatch.setattr(async_runtime, "get_settings", lambda: settings)
    try:
        assert async_runtime.configure_uvicorn_loop(sqlite_url) == "auto"
        assert isinstance(
            async_runtime.run_database_async(current_loop()), asyncio.ProactorEventLoop
        )
        settings.database_url = postgres_url
        assert isinstance(
            async_runtime.run_database_async(current_loop()), asyncio.SelectorEventLoop
        )
        assert isinstance(asyncio.get_event_loop_policy(), asyncio.WindowsProactorEventLoopPolicy)
        assert async_runtime.configure_uvicorn_loop(postgres_url) == "none"
        assert isinstance(asyncio.run(current_loop()), asyncio.SelectorEventLoop)
    finally:
        asyncio.set_event_loop_policy(original_policy)


def test_windows_selector_can_own_and_cancel_supervised_subprocess_operations():
    async def child():
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "print('subprocesso supervisionado')",
            stdout=asyncio.subprocess.PIPE,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        stdout, _ = await process.communicate()
        assert process.returncode == 0
        return stdout

    async def exercise():
        assert b"subprocesso supervisionado" in await async_runtime.run_subprocess_async(child())

        caller = asyncio.get_running_loop()
        started, cleaned = asyncio.Event(), Event()

        async def operation():
            try:
                caller.call_soon_threadsafe(started.set)
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)
                cleaned.set()

        task = asyncio.create_task(async_runtime.run_subprocess_async(operation()))
        await asyncio.wait_for(started.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5)
        assert cleaned.is_set()

    asyncio.run(exercise(), loop_factory=asyncio.SelectorEventLoop)
