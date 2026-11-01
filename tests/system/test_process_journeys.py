"""Actual API/scheduler processes, using only local controlled sources."""

import sqlite3
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from tests.support.processes import wait_for

pytestmark = pytest.mark.system


def test_api_cancel_restart_and_continue_saved_work(runtime):
    client = httpx.Client(base_url=runtime.url, timeout=15)
    (runtime.root / "hold").touch()
    created = client.post(
        "/api/crawls/run",
        json={"connector": "pncp", "uf": "MA", "days": 7, "process_documents": False},
    )
    assert created.status_code == 202, created.text
    identifier = created.json()["id"]
    wait_for(lambda: (runtime.root / "entered").exists())
    assert client.get("/health").status_code == 200
    assert client.post(f"/api/crawls/{identifier}/cancel").status_code == 202
    wait_for(lambda: client.get(f"/api/crawls/{identifier}").json()["status"] == "cancelled")
    (runtime.root / "hold").unlink()
    second = client.post(
        "/api/crawls/run",
        json={"connector": "pncp", "uf": "MA", "days": 7, "process_documents": False},
    )
    second_id = second.json()["id"]
    wait_for(lambda: client.get(f"/api/crawls/{second_id}").json()["status"] == "partial")
    runtime.stop(runtime.children[0])
    runtime.start("api")
    wait_for(lambda: _healthy(client))
    child = client.post(f"/api/crawls/{second_id}/continue")
    assert child.status_code == 202, child.text
    child_id = child.json()["id"]
    wait_for(lambda: client.get(f"/api/crawls/{child_id}").json()["status"] == "partial")
    with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
        assert (
            db.execute("select count(*) from procurements where external_id != 'seed'").fetchone()[
                0
            ]
            == 2
        )
        assert (
            db.execute(
                "select count(*) from collection_work_items where kind='record' and status='succeeded'"
            ).fetchone()[0]
            == 2
        )
    client.close()


def _healthy(client):
    try:
        return client.get("/health").status_code == 200
    except httpx.HTTPError:
        return False


def test_scheduler_pause_resume_exclusion_and_restart(runtime):
    pause = runtime.root / "scheduler.paused"
    pause.touch()
    first = runtime.start("scheduler")
    import time

    time.sleep(2)
    assert not (runtime.root / "ticks").exists()
    (runtime.root / "scheduler-hold").touch()
    pause.unlink()
    wait_for(lambda: (runtime.root / "scheduler-entered").exists())
    runtime.start("scheduler")
    time.sleep(2)
    assert (runtime.root / "ticks").read_text().count("tick") == 1
    runtime.stop(first)
    with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
        db.execute(
            "update job_leases set expires_at=? where name='scheduler:discover_price_registries'",
            ((datetime.now(UTC) - timedelta(seconds=1)).replace(tzinfo=None).isoformat(" "),),
        )
    (runtime.root / "scheduler-hold").unlink()
    wait_for(lambda: (runtime.root / "ticks").read_text().count("tick") >= 2)
