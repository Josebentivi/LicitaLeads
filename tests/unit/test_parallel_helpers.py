"""Cancellation cannot release owned storage work while its thread still writes."""

import asyncio
import threading

import pytest

from app.services.parallel import joined_thread


@pytest.mark.parametrize("worker_fails", [False, True])
async def test_repeated_cancellation_joins_file_write_and_preserves_cancellation(
    tmp_path, worker_fails
):
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    path = tmp_path / "owned.txt"

    def write():
        started.set()
        assert release.wait(timeout=10)
        path.write_text("complete", encoding="utf-8")
        finished.set()
        if worker_fails:
            raise OSError("worker failed after cleanup")
        return path

    task = asyncio.create_task(joined_thread(write))
    try:
        assert await asyncio.to_thread(started.wait, 10)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not finished.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5)
        assert finished.is_set() and path.read_text(encoding="utf-8") == "complete"
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
