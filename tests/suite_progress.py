"""A fixed pytest plugin: inventory and durable progress inside the suite's temp tree."""

import json
import os
from pathlib import Path


def pytest_collection_finish(session):
    directory = os.environ.get("LICITA_TEST_PROGRESS_DIR")
    if not directory:
        return
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    inventory = [
        {
            "nodeid": item.nodeid,
            "stage": next(
                (
                    name
                    for name in ("browser", "system", "ocr_real")
                    if item.get_closest_marker(name)
                ),
                "core",
            ),
        }
        for item in session.items
    ]
    (root / "inventory.json").write_text(json.dumps(inventory), encoding="utf-8")


def pytest_runtest_logreport(report):
    directory = os.environ.get("LICITA_TEST_PROGRESS_DIR")
    if not directory:
        return
    if report.when == "call" or (
        report.when in {"setup", "teardown"} and report.outcome != "passed"
    ):
        value = {
            "nodeid": report.nodeid,
            "outcome": report.outcome,
            "when": report.when,
            "duration": report.duration,
            "detail": str(report.longrepr)[:3000] if report.longrepr else "",
        }
        with (Path(directory) / "events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(value) + "\n")
