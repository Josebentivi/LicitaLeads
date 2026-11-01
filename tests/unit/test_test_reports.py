"""Content-bound reports never turn history, skips or interrupted work green."""

import asyncio
import json

import pytest
from pydantic import ValidationError

from app.config import Settings
from app.services.application_tests import ApplicationTestService, TestResult
from app.services.test_reports import code_identity, freshness, save_report
from app.services.test_suite import event_results


def test_identity_tracks_code_tests_fixtures_and_config_excluding_runtime_and_secrets(tmp_path):
    for folder in ("app", "tests/fixtures", "scripts", "migrations", "data"):
        (tmp_path / folder).mkdir(parents=True)
    for name in (
        "app/a.py",
        "tests/test_a.py",
        "tests/fixtures/input.json",
        "pyproject.toml",
        ".env.example",
        "data/holidays.csv",
    ):
        (tmp_path / name).write_text("initial")
    original = code_identity(tmp_path)
    for name in (".env", "data/local.db", "data/test-report.json", "data/log"):
        (tmp_path / name).write_text("secret")
    assert freshness(original, code_identity(tmp_path)) == "current"
    for name in (
        "app/a.py",
        "tests/test_a.py",
        "tests/fixtures/input.json",
        "pyproject.toml",
        ".env.example",
        "data/holidays.csv",
    ):
        (tmp_path / name).write_text("changed")
        assert freshness(original, code_identity(tmp_path)) == "outdated"
        (tmp_path / name).write_text("initial")
    assert freshness(None, original) == "unknown"
    assert freshness({"sha256": original["sha256"]}, original) == "outdated"


def test_report_archives_legacy_and_separates_quick_full(tmp_path):
    path = tmp_path / "test-report.json"
    legacy = {"run_id": "legacy", "mode": "full"}
    path.write_text(json.dumps(legacy))
    save_report(path, {"run_id": "quick", "mode": "quick"})
    assert json.loads((tmp_path / "test-report-full.json").read_text()) == legacy
    save_report(path, {"run_id": "full", "mode": "full"})
    assert json.loads((tmp_path / "test-report-history/legacy.json").read_text()) == legacy
    assert json.loads((tmp_path / "test-report-quick.json").read_text())["run_id"] == "quick"
    assert json.loads((tmp_path / "test-report-full.json").read_text())["run_id"] == "full"


def test_mode_summary_reads_legacy_without_inventing_identity(tmp_path):
    from datetime import UTC, datetime

    from app.services.application_tests import TestReport

    now = datetime.now(UTC)
    report = TestReport("legacy", "full", now, now, []).to_dict()
    report.pop("identity")
    path = tmp_path / "test-report.json"
    path.write_text(json.dumps(report))
    service = ApplicationTestService(Settings(_env_file=None), report_path=path)
    legacy = service.report_for_mode("full")
    assert legacy.run_id == "legacy" and legacy.freshness == "unknown" and legacy.identity is None
    assert service.report_for_mode("quick") is None


def test_partial_event_reader_deduplicates_and_redacts_even_teardown_failure(tmp_path):
    path = tmp_path / "events.jsonl"
    events = [
        {"nodeid": "one", "outcome": "passed"},
        {"nodeid": "one", "outcome": "failed", "detail": "token=secret"},
        {"nodeid": "two", "outcome": "skipped"},
    ]
    path.write_text("\n".join(json.dumps(e) for e in events) + '\n{"incomplete":')
    results, counts = event_results(path, lambda text: text.replace("secret", "***"))
    assert counts == {"total": 2, "passed": 0, "failed": 1, "skipped": 1}
    assert "secret" not in str(results)


@pytest.mark.parametrize("value", [59, 21601, 0])
def test_suite_timeout_rejects_invalid_configuration(value):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, test_suite_timeout_seconds=value)


@pytest.mark.parametrize(
    "outcome,expected",
    [("passed", "completed"), ("warning", "completed_with_pending"), ("failed", "failed")],
)
async def test_full_run_status_separates_validation_from_termination(
    tmp_path, monkeypatch, outcome, expected
):
    service = ApplicationTestService(Settings(_env_file=None), report_path=tmp_path / "report.json")

    async def lease(identifier):
        return "fake", "fake"

    async def run(owner, identifier):
        return [TestResult("controlled", "Controle", "Controle", outcome, "ok", 0)], {
            "total": 1,
            "passed": int(outcome == "passed"),
            "failed": int(outcome == "failed"),
            "skipped": 0,
        }

    monkeypatch.setattr(service, "_acquire_run_lease", lease)
    monkeypatch.setattr("app.services.test_suite.run_suite", run)
    state = service.start("full", object())
    await service._task
    assert state.status == expected and state.report.identity["sha256"]


async def test_whole_deadline_includes_setup_and_preserves_partial_results(tmp_path, monkeypatch):
    service = ApplicationTestService(
        Settings(_env_file=None).model_copy(update={"test_suite_timeout_seconds": 0.05}),
        report_path=tmp_path / "report.json",
    )

    async def lease(identifier):
        return "fake", "fake"

    async def run(owner, identifier):
        owner._partial_results = [TestResult("done", "Controle", "Já concluído", "passed", "ok", 0)]
        owner._suite_counts = {"total": 1, "discovered": 2, "executed": 1, "not_executed": 1}
        await asyncio.Event().wait()

    monkeypatch.setattr(service, "_acquire_run_lease", lease)
    monkeypatch.setattr("app.services.test_suite.run_suite", run)
    state = service.start("full", object())
    await service._task
    assert state.status == "failed" and state.report.results[0].key == "done"
    assert state.report.test_counts["not_executed"] == 1


@pytest.mark.parametrize("restore", [False, True])
async def test_code_change_during_run_makes_report_historical(tmp_path, monkeypatch, restore):
    root = tmp_path / "repo"
    (root / "app").mkdir(parents=True)
    source = root / "app/a.py"
    source.write_text("before")
    service = ApplicationTestService(Settings(_env_file=None), report_path=tmp_path / "report.json")

    async def lease(identifier):
        return "fake", "fake"

    async def quick(*args, **kwargs):
        source.write_text("after")
        if restore:
            source.write_text("before")
        return []

    monkeypatch.setattr(service, "_repo_root", lambda: root)
    monkeypatch.setattr(service, "_acquire_run_lease", lease)
    monkeypatch.setattr(service, "_run_quick", quick)
    state = service.start("quick", object())
    await service._task
    assert state.report.freshness == "changed_during_run"
    assert state.status == "completed_with_pending"


def test_browser_readiness_is_offline_and_rejects_missing_or_changed_assets(tmp_path, monkeypatch):
    from app.services import test_browser

    monkeypatch.setattr(test_browser, "browser_cache", lambda: tmp_path)
    assert not test_browser.browser_readiness()["available"]
    binary, driver = tmp_path / "browser", tmp_path / "driver"
    binary.touch()
    driver.touch()
    (tmp_path / "prepared.json").write_text(
        json.dumps({"binary": str(binary), "driver": str(driver), "version": "1.2.3.4"})
    )
    monkeypatch.setattr(test_browser, "executable_version", lambda p: "1.2.3.5")
    assert test_browser.browser_readiness()["available"]
    assert test_browser.browser_readiness()["version"] == "1.2.3.5"
    monkeypatch.setattr(test_browser, "executable_version", lambda p: "2.0.0.0")
    assert not test_browser.browser_readiness()["available"]
