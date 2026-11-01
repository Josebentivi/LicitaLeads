"""Real child-process isolation and cancellation, including descendants."""

import asyncio
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest

from app.config import Settings
from app.services.application_tests import ApplicationTestService


def service_for(tmp_path):
    return ApplicationTestService(
        Settings(
            _env_file=None,
            app_env="test",
            raw_data_storage_path=tmp_path / "raw",
            document_storage_path=tmp_path / "documents",
        ),
        report_path=tmp_path / "report.json",
    )


async def test_child_ignores_hostile_dotenv_and_preserves_sentinels(tmp_path, monkeypatch):
    sentinel = tmp_path / "operator.sqlite3"
    with closing(sqlite3.connect(sentinel)) as database, database:
        database.execute("CREATE TABLE sentinel (value TEXT)")
        database.execute("INSERT INTO sentinel VALUES ('preserve')")
    storage = tmp_path / "operator-files"
    storage.mkdir()
    marker = storage / "keep.txt"
    marker.write_text("keep", encoding="utf-8")
    (tmp_path / ".env").write_text(
        f"DATABASE_URL=sqlite:///{sentinel.as_posix()}\nRAW_DATA_STORAGE_PATH={storage}\nLLM_ENABLED=true\nRUN_LIVE_CONTRACT_TESTS=true\n",
        encoding="utf-8",
    )
    before = hashlib.sha256(sentinel.read_bytes()).hexdigest()
    service = service_for(tmp_path)
    monkeypatch.setattr(service, "_repo_root", lambda: tmp_path)
    child = tmp_path / "child"
    child.mkdir()
    environment = service._pytest_environment(child)
    script = """import asyncio
from sqlalchemy import text
from app.config import get_settings
from app.database import engine
settings=get_settings()
assert not settings.llm_enabled and not settings.run_live_contract_tests
assert not settings.contact_search_enabled and not settings.contact_domain_discovery_enabled
assert not settings.llm_api_key and not settings.transparencia_api_key
async def main():
 async with engine.begin() as connection:
  await connection.execute(text('CREATE TABLE child_only (id INTEGER)'))
 await engine.dispose()
asyncio.run(main())
"""
    code, output = await service._run_process([sys.executable, "-c", script], environment, 15)
    assert code == 0, output.decode(errors="replace")
    assert hashlib.sha256(sentinel.read_bytes()).hexdigest() == before
    assert marker.read_text(encoding="utf-8") == "keep"
    assert (child / "suite.sqlite3").is_file()


async def test_cancel_during_lease_acquisition_is_terminal(tmp_path, monkeypatch):
    service = service_for(tmp_path)
    entered = asyncio.Event()

    async def acquire(run_id):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(service, "_acquire_run_lease", acquire)
    state = service.start("quick", object())
    await entered.wait()
    await service.aclose()
    assert state.status == "cancelled"


async def test_cancel_before_first_instruction_is_terminal(tmp_path):
    service = service_for(tmp_path)
    state = service.start("quick", object())
    assert service.cancel()
    await service.aclose()
    assert state.status == "cancelled"
    assert service.last_report().status == "cancelled"


def is_alive(pid):
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        kernel = ctypes.windll.kernel32
        kernel.OpenProcess.restype = wintypes.HANDLE
        handle = kernel.OpenProcess(0x00100000, False, pid)
        if not handle:
            return False
        try:
            return kernel.WaitForSingleObject(wintypes.HANDLE(handle), 0) == 258
        finally:
            kernel.CloseHandle(wintypes.HANDLE(handle))
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@pytest.mark.parametrize("cancel", [True, False])
async def test_child_and_grandchild_end_on_cancellation_or_timeout(tmp_path, cancel):
    service = service_for(tmp_path)
    ready = tmp_path / "pids.json"
    script = (
        "import os,subprocess,sys,time,json; "
        "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']); "
        f"open({str(ready)!r},'w').write(json.dumps([os.getpid(),child.pid])); "
        "time.sleep(60)"
    )
    task = asyncio.create_task(
        service._run_process(
            [sys.executable, "-c", script], os.environ.copy(), 5 if not cancel else 60
        )
    )
    for _ in range(100):
        if ready.exists() and ready.stat().st_size:
            break
        await asyncio.sleep(0.05)
    assert ready.exists()
    pids = json.loads(ready.read_text())
    if cancel:
        task.cancel()
    with pytest.raises(asyncio.CancelledError if cancel else TimeoutError):
        await task
    assert not any(is_alive(pid) for pid in pids)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows Job Object regression")
@pytest.mark.parametrize("exit_code", [0, 7])
async def test_descendant_ends_when_its_parent_exits_first(tmp_path, exit_code):
    service = service_for(tmp_path)
    ready = tmp_path / "grandchild.json"
    script = (
        "import subprocess,sys,json; "
        "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']); "
        f"open({str(ready)!r},'w').write(json.dumps(child.pid)); "
        f"sys.exit({exit_code})"
    )
    try:
        code, _ = await service._run_process([sys.executable, "-c", script], os.environ.copy(), 15)
        assert code == exit_code
        pid = json.loads(ready.read_text())
        assert not is_alive(pid)
    finally:
        if ready.exists():
            pid = json.loads(ready.read_text())
            if is_alive(pid):
                await asyncio.to_thread(
                    subprocess.run,
                    [
                        str(Path(os.environ["SystemRoot"]) / "System32" / "taskkill.exe"),
                        "/PID",
                        str(pid),
                        "/T",
                        "/F",
                    ],
                    check=False,
                    capture_output=True,
                    timeout=10,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                )


@pytest.mark.parametrize(
    ("live_environment", "mark_expression", "expected_live"),
    [("true", "", "false"), ("false", "live", "false"), ("true", "live", "true")],
)
def test_plain_pytest_isolates_import_time_factory_and_hostile_dotenv(
    tmp_path, live_environment, mark_expression, expected_live
):
    repository = Path(__file__).resolve().parents[2]
    operator = tmp_path / "operator.sqlite3"
    with closing(sqlite3.connect(operator)) as database, database:
        database.execute("CREATE TABLE sentinel (value TEXT)")
        database.execute("INSERT INTO sentinel VALUES ('preserve')")
    marker = tmp_path / "operator-files" / "sentinel.txt"
    marker.parent.mkdir()
    marker.write_text("preserve", encoding="utf-8")
    (tmp_path / ".env").write_text(
        f"DATABASE_URL=sqlite:///{operator.as_posix()}\n"
        f"RAW_DATA_STORAGE_PATH={marker.parent}\nLLM_ENABLED=true\n"
        "RUN_LIVE_CONTRACT_TESTS=true\n",
        encoding="utf-8",
    )
    before = hashlib.sha256(operator.read_bytes()).hexdigest()
    (tmp_path / "pytest.ini").write_text(
        "[pytest]\nmarkers = live: explicit live opt-in\n", encoding="utf-8"
    )
    (tmp_path / "conftest.py").write_text(
        "import runpy\nglobals().update({k:v for k,v in runpy.run_path("
        f"{str(repository / 'tests' / 'conftest.py')!r}).items() if not k.startswith('__')}})\n",
        encoding="utf-8",
    )
    (tmp_path / "test_safety.py").write_text(
        """import asyncio, os, pytest
from app.config import Settings, get_settings
from app.services.application_tests import ApplicationTestService
from sqlalchemy import text
from app.database import async_session_factory

@pytest.mark.live
def test_import_time_factory_is_isolated():
    settings=get_settings()
    assert settings.app_env == 'test' and not settings.llm_enabled
    assert os.environ['RUN_LIVE_CONTRACT_TESTS'] == '__EXPECTED_LIVE__'
    assert 'operator.sqlite3' not in settings.database_url
    supplied=Settings(_env_file=None,app_env='test',database_url='sqlite:///:memory:')
    service=ApplicationTestService(supplied)
    async def check():
        name,owner=await service._acquire_run_lease('isolation-proof')
        assert name == 'selftest:application'
        async with async_session_factory() as session:
            assert await session.scalar(text("SELECT COUNT(*) FROM job_leases WHERE name='selftest:application'")) == 1
        await service._release_run_lease(name,owner)
    asyncio.run(check())
""".replace("__EXPECTED_LIVE__", expected_live),
        encoding="utf-8",
    )
    environment = os.environ.copy()
    for key in ("APP_ENV", "DATABASE_URL", "RAW_DATA_STORAGE_PATH", "DOCUMENT_STORAGE_PATH"):
        environment.pop(key, None)
    environment["RUN_LIVE_CONTRACT_TESTS"] = live_environment
    environment["PYTHONPATH"] = str(repository)
    environment = {
        key: value for key, value in environment.items() if not key.startswith("COV_CORE_")
    }
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "--no-cov",
        "-p",
        "no:cacheprovider",
        "test_safety.py",
    ]
    if mark_expression:
        command.extend(["-m", mark_expression])
    result = subprocess.run(  # noqa: S603 - fixed isolated pytest, no shell
        command,
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=90,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert hashlib.sha256(operator.read_bytes()).hexdigest() == before
    assert marker.read_text(encoding="utf-8") == "preserve"
