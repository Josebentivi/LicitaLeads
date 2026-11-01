"""Bounded asynchronous preparation and a single, cancellable commit consumer."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Coroutine, Iterable
from contextlib import asynccontextmanager
from typing import Any


async def joined_thread[R, **P](function: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
    """An owned file/CPU operation must finish before cancellation releases leases."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not task.cancelled():
            task.exception()
        raise


async def bounded_map[T, R](
    items: Iterable[T], prepare: Callable[[T], Coroutine[Any, Any, R]], *, capacity: int
) -> AsyncGenerator[tuple[T, R], None]:
    """Keep at most capacity prepared/in-flight results; always join children."""
    iterator = iter(items)
    tasks: dict[asyncio.Task[R], T] = {}

    def fill() -> None:
        while len(tasks) < max(1, capacity):
            try:
                item = next(iterator)
            except StopIteration:
                break
            tasks[asyncio.create_task(prepare(item))] = item

    try:
        fill()
        while tasks:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                item = tasks.pop(task)
                yield item, task.result()
                fill()
    finally:
        for task in tasks:
            if not task.cancelling():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class SerialWriter:
    """One consumer owns domain commits; producers never share ORM sessions."""

    def __init__(self) -> None:
        self.queue: asyncio.Queue[tuple[Callable[[], Awaitable[Any]], asyncio.Future[Any]]] = (
            asyncio.Queue(maxsize=2)
        )
        self.worker: asyncio.Task[None] | None = None

    async def submit[R](self, commit: Callable[[], Awaitable[R]]) -> R:
        future: asyncio.Future[R] = asyncio.get_running_loop().create_future()
        await self.queue.put((commit, future))
        return await future

    async def _consume(self) -> None:
        while True:
            commit, future = await self.queue.get()
            try:
                if not future.cancelled():
                    try:
                        result = await commit()
                    except Exception as exc:
                        if not future.done():
                            future.set_exception(exc)
                    except BaseException:
                        future.cancel()
                        raise
                    else:
                        if not future.done():
                            future.set_result(result)
            finally:
                self.queue.task_done()

    @asynccontextmanager
    async def running(self) -> AsyncIterator[SerialWriter]:
        self.worker = asyncio.create_task(self._consume(), name="crawl-domain-writer")
        try:
            yield self
        finally:
            self.worker.cancel()
            await asyncio.gather(self.worker, return_exceptions=True)
            while not self.queue.empty():
                _, future = self.queue.get_nowait()
                future.cancel()
                self.queue.task_done()
