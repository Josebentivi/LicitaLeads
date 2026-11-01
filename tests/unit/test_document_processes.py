"""Extraction IPC stays independent of database/LLM and is cancellable off-loop."""

import asyncio
import hashlib
import json
import subprocess
import sys
import threading

import pytest

from app.config import Settings
from app.services.documents import processes
from app.services.documents.errors import ExtractionFailure
from app.services.documents.security import ArchiveLimits
from app.services.documents.types import DocumentExtractionStatus


async def test_real_extraction_worker_receives_only_local_file_and_explicit_options(
    tmp_path, monkeypatch
):
    content = b"Ata oficial: documento em texto para extracao isolada."
    path = tmp_path / "input.txt"
    path.write_bytes(content)
    # A worker must never initialize the application database or an LLM client.
    monkeypatch.setenv("DATABASE_URL", "invalid://worker-must-not-open-a-database")
    monkeypatch.setenv("LLM_ENABLED", "true")
    result = await processes.extract_in_process(
        path,
        filename="ata.txt",
        mime_type="text/plain",
        settings=Settings(_env_file=None, llm_enabled=False, ocr_enabled=False),
        archive_limits=ArchiveLimits(),
    )
    assert result.status is DocumentExtractionStatus.EXTRACTED
    assert result.sha256 == hashlib.sha256(content).hexdigest()
    assert result.text == content.decode() and result.chunks


async def test_cancellation_during_slow_spawn_waits_for_supervised_cleanup(tmp_path, monkeypatch):
    path = tmp_path / "input.txt"
    path.write_text("Ata oficial", encoding="utf-8")
    started, allow_spawn = threading.Event(), threading.Event()
    killed = threading.Event()
    calls = []

    class Child:
        pid = 123
        returncode = None

        def poll(self):
            return self.returncode

        def kill(self):
            self.returncode = -9
            killed.set()

        def wait(self, timeout=None):
            if self.returncode is None:
                raise subprocess.TimeoutExpired("worker", timeout)
            return self.returncode

    child = Child()
    monkeypatch.setattr(processes, "_stop_tree", lambda child: child.kill())
    monkeypatch.setattr(processes.os, "killpg", lambda *args: child.kill(), raising=False)

    def spawn(argv, **kwargs):
        calls.append(argv)
        started.set()
        allow_spawn.wait(timeout=10)
        return child

    monkeypatch.setattr(processes.subprocess, "Popen", spawn)
    task = asyncio.create_task(
        processes.extract_in_process(
            path,
            filename="ata.txt",
            mime_type="text/plain",
            settings=Settings(_env_file=None),
            archive_limits=ArchiveLimits(),
        )
    )
    try:
        assert await asyncio.to_thread(started.wait, 10)
        # The event loop can cancel while Popen is still blocked in its thread.
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        allow_spawn.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=10)
        assert killed.is_set()
        assert "subprocess_supervisor.py" in calls[0][1]
    finally:
        allow_spawn.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_extraction_timeout_joins_worker_before_returning(tmp_path, monkeypatch):
    path = tmp_path / "input.txt"
    path.write_text("Ata oficial", encoding="utf-8")
    monkeypatch.setattr(processes, "WORKER_TIMEOUT_SECONDS", 0)
    with pytest.raises(TimeoutError, match="tempo limite"):
        await processes.extract_in_process(
            path,
            filename="ata.txt",
            mime_type="text/plain",
            settings=Settings(_env_file=None),
            archive_limits=ArchiveLimits(),
        )


@pytest.mark.skipif(sys.platform != "win32", reason="Windows job ownership")
async def test_stopping_supervisor_ends_its_interpreter_descendants(tmp_path):
    import ctypes
    from pathlib import Path

    marker = tmp_path / "descendant.pid"
    script = tmp_path / "hold.py"
    script.write_text(
        "import subprocess,sys,time\n"
        "from pathlib import Path\n"
        "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(120)'])\n"
        "Path(sys.argv[1]).write_text(str(child.pid))\n"
        "child.wait()\n",
        encoding="utf-8",
    )
    base_python = getattr(sys, "_base_executable", sys.executable)
    supervisor = Path(processes.__file__).resolve().parents[1] / "subprocess_supervisor.py"
    stop = threading.Event()
    task = asyncio.create_task(
        asyncio.to_thread(
            processes._supervise,
            [base_python, str(supervisor), base_python, str(script), str(marker)],
            {"creationflags": subprocess.CREATE_NO_WINDOW},
            stop,
            bytearray(),
        )
    )
    handle = None
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
    kernel.OpenProcess.restype = ctypes.c_void_p
    kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    try:
        async with asyncio.timeout(20):
            while not marker.is_file():
                assert not task.done(), "supervisor exited before creating its child"
                await asyncio.sleep(0.05)
        handle = kernel.OpenProcess(0x00100000, False, int(marker.read_text()))
        assert handle
        stop.set()
        assert await asyncio.wait_for(asyncio.shield(task), 10) == -1
        assert await asyncio.to_thread(kernel.WaitForSingleObject, handle, 5000) == 0
    finally:
        stop.set()
        await task
        if handle:
            kernel.CloseHandle(handle)


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("missing", "worker_output_missing"),
        ("invalid", "worker_output_invalid"),
        ("error", "archive_depth_exceeded"),
        ("startup", "worker_initialization"),
        ("exit", "worker_exit"),
    ],
)
async def test_worker_failures_are_structured_and_do_not_echo_stderr(
    tmp_path, monkeypatch, mode, expected
):
    from pathlib import Path

    source = tmp_path / "input.txt"
    source.write_text("official source", encoding="utf-8")

    def supervise(argv, kwargs, stop, stderr_tail):
        assert Path(kwargs["cwd"]).is_absolute()
        out = Path(argv[-1])
        if mode == "invalid":
            out.write_text("{")
        if mode == "error":
            out.write_text(
                json.dumps(
                    {
                        "error": {
                            "code": "archive_depth_exceeded",
                            "member_path": "inside.zip",
                            "exception_type": "UnsafeArchiveError",
                            "message": "sk-private-secret should never be shown",
                        }
                    }
                )
            )
        if mode == "startup":
            stderr_tail.extend(b"ModuleNotFoundError: sk-private-secret source text")
        if mode == "exit":
            out.write_text("{}")
        return 0 if mode in {"missing", "invalid"} else 1

    monkeypatch.setattr(processes, "_supervise", supervise)
    with pytest.raises(ExtractionFailure) as error:
        await processes.extract_in_process(
            source,
            filename="ata.txt",
            mime_type="text/plain",
            settings=Settings(_env_file=None),
            archive_limits=ArchiveLimits(),
        )
    assert error.value.code == expected
    assert "sk-private-secret" not in str(error.value)
    assert "source text" not in json.dumps(error.value.as_dict())


async def test_real_worker_records_archive_member_and_hash(tmp_path):
    import io
    from zipfile import ZipFile

    content = io.BytesIO()
    with ZipFile(content, "w") as archive:
        archive.writestr("../unsafe.docx", b"invalid")
    source = tmp_path / "input.zip"
    source.write_bytes(content.getvalue())
    with pytest.raises(ExtractionFailure) as error:
        await processes.extract_in_process(
            source,
            filename="ata.zip",
            mime_type="application/zip",
            settings=Settings(_env_file=None),
            archive_limits=ArchiveLimits(),
        )
    assert error.value.code == "archive_unsafe_path"
    assert error.value.input_hash == hashlib.sha256(content.getvalue()).hexdigest()
    assert error.value.member_path == "../unsafe.docx"


async def test_real_worker_is_independent_of_parent_working_directory(tmp_path, monkeypatch):
    path = tmp_path / "ata.txt"
    path.write_text("Documento oficial com conteudo.", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    result = await processes.extract_in_process(
        path,
        filename="ata.txt",
        mime_type="text/plain",
        settings=Settings(_env_file=None),
        archive_limits=ArchiveLimits(),
    )
    assert result.status is DocumentExtractionStatus.EXTRACTED


def test_supervisor_spawn_failure_is_sanitized(monkeypatch):
    def spawn(*args, **kwargs):
        raise OSError("sk-private-secret document contents")

    monkeypatch.setattr(processes.subprocess, "Popen", spawn)
    with pytest.raises(ExtractionFailure) as error:
        processes._supervise(["python"], {}, threading.Event(), bytearray())
    assert error.value.code == "worker_spawn_failed" and "private" not in str(error.value)
    import io

    class Child:
        stderr = io.BytesIO(b"x" * 50000 + b"ModuleNotFoundError")
        returncode, pid = 0, 123

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(processes.subprocess, "Popen", lambda *a, **k: Child())
    monkeypatch.setattr(processes.os, "killpg", lambda *a: None, raising=False)
    tail = bytearray()
    assert processes._supervise(["python"], {}, threading.Event(), tail) == 0
    assert len(tail) == 8192 and tail.endswith(b"ModuleNotFoundError")
