"""Stage failures do not hide independent checks or missing prerequisites."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.config import Settings
from app.services.application_tests import ApplicationTestService, TestResult, TestRunState
from app.services.test_suite import run_suite


@pytest.mark.parametrize("dependencies", [True, False])
async def test_sequential_stages_inventory_quality_failure_and_pending_dependencies(
    tmp_path, monkeypatch, dependencies
):
    service = ApplicationTestService(
        Settings(_env_file=None, raw_data_storage_path=tmp_path / "raw")
    )
    service._state = TestRunState("controlled", "full", datetime.now(UTC))
    invoked = []

    async def prepare(environment):
        assert "suite.sqlite3" in environment["DATABASE_URL"]

    async def command(argv, environment, time_limit):
        invoked.append(tuple(argv))
        assert environment["LLM_ENABLED"] == environment["OCR_ENABLED"] == "false"
        assert environment["RUN_LIVE_CONTRACT_TESTS"] == "false"
        if "--collect-only" in argv:
            directory = Path(environment["LICITA_TEST_PROGRESS_DIR"])
            (directory / "inventory.json").write_text(
                json.dumps(
                    [
                        {"nodeid": key, "stage": key}
                        for key in ("core", "system", "ocr_real", "browser")
                    ]
                )
            )
        if "json" in argv:
            target = Path(argv[argv.index("-o") + 1])
            target.write_text(json.dumps({"totals": {"percent_covered": 90}, "files": {}}))
        return (1 if "check" in argv and "--check" not in argv else 0), b"controlled lint failure"

    async def pytest_phase(identifier, *, directory, environment, expression, time_limit):
        invoked.append((identifier, expression))
        assert "not live" in expression and time_limit > 0
        (directory / ".coverage").touch()
        return [TestResult(identifier, "Controle", identifier, "passed", "ok", 0)], {
            "total": 1,
            "passed": 1,
            "failed": 0,
            "skipped": 0,
        }

    monkeypatch.setattr(service, "_prepare_suite_database", prepare)
    monkeypatch.setattr(service, "_run_process", command)
    monkeypatch.setattr(service, "_run_pytest", pytest_phase)
    monkeypatch.setattr(
        "app.services.test_browser.browser_readiness",
        lambda: {"available": dependencies, "detail": "missing browser"},
    )
    monkeypatch.setattr(
        "app.services.test_suite.ocr_readiness",
        lambda: {"available": dependencies, "detail": "missing OCR"},
    )
    results, counts = await run_suite(service, "controlled")
    assert next(item for item in results if item.key == "ruff").status == "failed"
    assert any("mypy" in call for call in invoked)
    assert counts["discovered"] == 4
    assert counts["executed"] == (4 if dependencies else 2)
    assert counts["not_executed"] == (0 if dependencies else 2)
    assert sum(item.status == "warning" for item in results) == (0 if dependencies else 2)
    assert service.current_state.coverage["totals"]["percent_covered"] == 90
