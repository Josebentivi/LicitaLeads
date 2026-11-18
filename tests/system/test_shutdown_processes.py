"""Real API/scheduler/process termination without touching the running application."""

import json
import os
import re
import sqlite3
import subprocess
import sys

import httpx
import pytest

from tests.support.processes import wait_for

pytestmark = pytest.mark.system


def test_shutdown_stops_workers_and_preserves_frozen_work(runtime):
    owner = runtime.enable_managed()
    wait_for(lambda: (runtime.root / "owned-grandchild.pid").exists())
    # This independent process must survive; it is not a child of the launcher.
    unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    try:
        (runtime.root / "hold").touch()
        with httpx.Client(base_url=runtime.url, timeout=15) as client:
            response = client.post(
                "/api/crawls/run",
                json={
                    "connector": "pncp",
                    "uf": "MA",
                    "days": 7,
                    "process_documents": False,
                },
            )
            assert response.status_code == 202, response.text
            run_id = response.json()["id"]
            wait_for(lambda: (runtime.root / "entered").exists())
            page = client.get("/application/shutdown")
            token = re.search(r'name="token" value="([^"]+)"', page.text)[1]
            stopped = client.post(
                "/api/application/shutdown", json={"token": token}, headers={"Origin": runtime.url}
            )
            assert stopped.status_code == 202, stopped.text
            assert client.post("/api/crawls/run", json={}).status_code == 503
            wait_for(lambda: owner.poll() is not None, seconds=35)
        assert unrelated.poll() is None
        assert (runtime.root / "scheduler-job-cancelled").exists()
        for pid in json.loads((runtime.root / "owned-processes.json").read_text()).values():
            assert not _alive(pid)
        assert not _alive(int((runtime.root / "owned-grandchild.pid").read_text()))
        receipt = json.loads((runtime.root / "application.shutdown.json").read_text())
        assert receipt["cleanup_complete"] and receipt["forced"] == []
        assert json.loads((runtime.root / "scheduler.controls.json").read_text()) == {
            "collections": True,
            "llm": True,
        }
        with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
            assert (
                db.execute("select status from crawl_runs where id=?", (run_id,)).fetchone()[0]
                == "cancelled"
            )
            assert db.execute("select count(*) from job_leases").fetchone()[0] == 0
            assert db.execute("select count(*) from procurements").fetchone()[0] >= 1
        (runtime.root / "hold").unlink()
        runtime.start("api")
        wait_for(lambda: _healthy(runtime.url))
        controls = json.loads((runtime.root / "scheduler.controls.json").read_text())
        assert all(controls.values())
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=10)


def _healthy(url):
    try:
        return httpx.get(url + "/health", timeout=1).status_code == 200
    except httpx.HTTPError:
        return False


def _alive(pid):
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        handle = kernel.OpenProcess(0x1000, False, pid)
        if not handle:
            return False
        try:
            code = wintypes.DWORD()
            return bool(kernel.GetExitCodeProcess(handle, ctypes.byref(code)) and code.value == 259)
        finally:
            kernel.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def test_forced_worker_shutdown_is_recovered_on_next_launch(runtime):
    (runtime.root / "force-shutdown-test").touch()
    owner = runtime.enable_managed()
    wait_for(lambda: (runtime.root / "owned-grandchild.pid").exists())
    with httpx.Client(base_url=runtime.url, timeout=15) as client:
        page = client.get("/application/shutdown")
        token = re.search(r'name="token" value="([^"]+)"', page.text)[1]
        assert (
            client.post(
                "/api/application/shutdown", json={"token": token}, headers={"Origin": runtime.url}
            ).status_code
            == 202
        )
    wait_for(lambda: owner.poll() is not None, seconds=30)
    receipt_path = runtime.root / "application.shutdown.json"
    receipt = json.loads(receipt_path.read_text())
    assert not receipt["cleanup_complete"] and "scheduler" in receipt["forced"]
    assert not _alive(int((runtime.root / "owned-grandchild.pid").read_text()))
    (runtime.root / "force-shutdown-test").unlink()
    runtime.start("managed")
    wait_for(lambda: _healthy(runtime.url))
    assert json.loads(receipt_path.read_text())["cleanup_complete"]
    assert all(json.loads((runtime.root / "scheduler.controls.json").read_text()).values())
    with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
        assert db.execute("select count(*) from job_leases").fetchone()[0] == 0
