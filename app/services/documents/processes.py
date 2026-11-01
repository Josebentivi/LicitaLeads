"""Supervise bounded extraction jobs, including all their OCR descendants."""

from __future__ import annotations

import asyncio
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from app.config import Settings
from app.services.documents.errors import ExtractionFailure
from app.services.documents.security import ArchiveLimits
from app.services.documents.types import DocumentExtraction
from app.services.documents.worker import decode_extraction
from app.services.parallel import joined_thread

WORKER_TIMEOUT_SECONDS = 1800


def _stop_tree(child: Any) -> None:
    """Reap the supervisor; its Windows job owns every interpreter/OCR child."""
    if sys.platform == "win32":
        child.kill()
    else:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    child.wait()


async def extract_in_process(
    input_path: Path,
    *,
    filename: str,
    mime_type: str | None,
    settings: Settings,
    archive_limits: ArchiveLimits,
) -> DocumentExtraction:
    # The temporary directory is owned by this job; input is an already
    # validated immutable local file. No credentials or database URL cross IPC.
    temporary = tempfile.TemporaryDirectory(prefix="licita-document-")
    operation: asyncio.Task[int] | None = None
    stop = threading.Event()
    try:
        root = Path(temporary.name)
        job_path, output_path = root / "job.json", root / "result.json"
        job: dict[str, Any] = {
            "input": str(input_path.resolve()),
            "filename": filename,
            "mime_type": mime_type,
            "archive_limits": asdict(archive_limits),
            "archive_max_depth": settings.archive_max_depth,
            **{
                key: getattr(settings, key)
                for key in (
                    "ocr_enabled",
                    "ocr_provider",
                    "ocr_language",
                    "ocr_dpi",
                    "ocr_min_confidence",
                )
            },
        }
        await joined_thread(job_path.write_text, json.dumps(job), encoding="utf-8")
        supervisor = Path(__file__).resolve().parents[1] / "subprocess_supervisor.py"
        kwargs: dict[str, Any] = (
            {"creationflags": 0x08000000}
            if sys.platform == "win32"
            else {"start_new_session": True}
        )
        kwargs["cwd"] = str(Path(__file__).resolve().parents[3])
        # A Windows venv python.exe is a redirector. Own the actual stdlib-only
        # supervisor directly so killing it closes its KILL_ON_JOB_CLOSE handle,
        # rather than leaving it alive behind a terminated redirector.
        supervisor_python = (
            getattr(sys, "_base_executable", sys.executable)
            if sys.platform == "win32"
            else sys.executable
        )
        stderr_tail = bytearray()
        operation = asyncio.create_task(
            asyncio.to_thread(
                _supervise,
                [
                    supervisor_python,
                    str(supervisor),
                    sys.executable,
                    "-m",
                    "app.services.documents.worker",
                    str(job_path),
                    str(output_path),
                ],
                kwargs,
                stop,
                stderr_tail,
            )
        )
        code = await asyncio.shield(operation)

        def load_result() -> DocumentExtraction:
            if not output_path.is_file():
                # Diagnose startup failures without retaining traceback lines,
                # paths, document contents, or arbitrary exception messages.
                match = re.search(
                    rb"\b(ModuleNotFoundError|ImportError|FileNotFoundError|PermissionError|OSError|MemoryError)\b",
                    bytes(stderr_tail),
                )
                kind = (
                    "worker_initialization"
                    if match
                    else "worker_exit"
                    if code
                    else "worker_output_missing"
                )
                raise ExtractionFailure(
                    kind, exception_type=match[1].decode("ascii") if match else None, exit_code=code
                )
            try:
                payload = json.loads(output_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict) and "error" in payload:
                    raise ExtractionFailure.from_dict(payload["error"], exit_code=code)
                if code:
                    raise ExtractionFailure("worker_exit", exit_code=code)
                return decode_extraction(payload)
            except ExtractionFailure:
                raise
            except Exception as exc:
                raise ExtractionFailure(
                    "worker_output_invalid", exception_type=type(exc).__name__, exit_code=code
                ) from None

        return await joined_thread(load_result)
    finally:
        if operation is not None and not operation.done():
            stop.set()
            await operation
        await joined_thread(temporary.cleanup)


def _supervise(
    argv: list[str],
    kwargs: dict[str, Any],
    stop: threading.Event,
    stderr_tail: bytearray | None = None,
) -> int:
    """Popen and waiting both run off the API loop, including on Windows."""
    if stop.is_set():
        return -1
    try:
        child = subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, **kwargs)  # noqa: S603
    except OSError as exc:
        raise ExtractionFailure("worker_spawn_failed", exception_type=type(exc).__name__) from None

    def drain_stderr() -> None:
        stream = getattr(child, "stderr", None)
        if stream is None:
            return
        try:
            while block := stream.read(1024):
                if stderr_tail is not None:
                    stderr_tail.extend(block)
                    del stderr_tail[:-8192]
        finally:
            stream.close()

    reader = threading.Thread(target=drain_stderr, name="extraction-stderr", daemon=True)
    reader.start()
    deadline = time.monotonic() + WORKER_TIMEOUT_SECONDS
    try:
        while child.poll() is None:
            if stop.is_set() or time.monotonic() >= deadline:
                _stop_tree(child)
                if not stop.is_set():
                    raise TimeoutError("tempo limite do processo de extração")
                return -1
            try:
                child.wait(timeout=0.1)
            except subprocess.TimeoutExpired:
                pass
        return child.returncode
    finally:
        if child.poll() is None:
            _stop_tree(child)
        if sys.platform != "win32":
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        reader.join(timeout=5)
