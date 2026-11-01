"""Self-test service: JUnit parsing, report persistence and safe checks."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from app.config import Settings
from app.services.application_tests import (
    ApplicationTestService,
    TestReport,
    TestResult,
    TestRunBusy,
)


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        app_env="test",
        document_storage_path=tmp_path / "documents",
        raw_data_storage_path=tmp_path / "raw",
        database_url="sqlite:///./data/test.sqlite3",
    )


def test_junit_parser_groups_tests_and_preserves_failures(tmp_path: Path) -> None:
    service = ApplicationTestService(
        _settings(tmp_path),
        report_path=tmp_path / "report.json",
    )
    report = tmp_path / "junit.xml"
    report.write_text(
        """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite tests="3" failures="1" errors="0" skipped="1">
    <testcase classname="tests.unit.test_alpha" name="passes" time="0.10" />
    <testcase classname="tests.unit.test_alpha" name="fails" time="0.20">
      <failure message="boom">traceback detail</failure>
    </testcase>
    <testcase classname="tests.integration.test_beta" name="skipped" time="0.30">
      <skipped message="not configured" />
    </testcase>
  </testsuite>
</testsuites>
""",
        encoding="utf-8",
    )

    results, counts = service._parse_junit(report)

    assert counts == {"total": 3, "passed": 1, "failed": 1, "skipped": 1}
    assert len(results) == 2
    alpha = next(item for item in results if "test_alpha" in item.key)
    beta = next(item for item in results if "test_beta" in item.key)
    assert alpha.status == "failed"
    assert alpha.title == "unit/test_alpha.py"
    assert "fails" in (alpha.output or "") and "traceback detail" in (alpha.output or "")
    assert beta.status == "skipped"
    assert beta.detail == "0 passaram, 0 falharam, 1 ignorados."


def test_report_round_trip_preserves_statuses(tmp_path: Path) -> None:
    service = ApplicationTestService(
        _settings(tmp_path),
        report_path=tmp_path / "report.json",
    )
    now = datetime.now(UTC)
    report = TestReport(
        run_id="run-1",
        mode="quick",
        started_at=now,
        finished_at=now,
        results=[
            TestResult(
                key="environment",
                group="Ambiente",
                title="Python",
                status="passed",
                detail="ok",
                duration_ms=4,
            ),
            TestResult(
                key="database",
                group="Banco",
                title="Schema",
                status="warning",
                detail="stale run",
                duration_ms=2,
                output="details",
            ),
        ],
    )

    service._write_report(report)
    loaded = service.last_report()

    assert loaded is not None
    assert loaded.run_id == "run-1"
    assert loaded.summary == {
        "total": 2,
        "passed": 1,
        "failed": 0,
        "warning": 1,
        "skipped": 0,
    }
    assert loaded.results[1].output == "details"


def test_safe_domain_and_sources_checks_pass(tmp_path: Path) -> None:
    service = ApplicationTestService(
        _settings(tmp_path),
        report_path=tmp_path / "report.json",
    )

    assert service._check_domain().status == "passed"
    assert service._check_sources().status == "skipped"


def test_redaction_removes_known_secrets(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    settings = settings.model_copy(
        update={
            "transparencia_api_key": "super-secret-token",
            "llm_api_key": "another-secret-token",
        }
    )
    service = ApplicationTestService(settings, report_path=tmp_path / "report.json")

    redacted = service._redact(
        "TRANSPARENCIA_API_KEY=super-secret-token token=another-secret-token"
    )

    assert "super-secret-token" not in redacted
    assert "another-secret-token" not in redacted
    assert "***" in redacted


@pytest.mark.asyncio
async def test_start_executes_quick_run_and_saves_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = ApplicationTestService(
        _settings(tmp_path),
        report_path=tmp_path / "report.json",
    )

    async def fake_quick(asgi_app: object, *, include_live: bool) -> list[TestResult]:
        del asgi_app, include_live
        return [
            TestResult(
                key="web",
                group="Páginas web",
                title="Dashboard",
                status="passed",
                detail="ok",
                duration_ms=1,
            )
        ]

    monkeypatch.setattr(service, "_run_quick", fake_quick)
    state = service.start("quick", object())

    assert state.status == "running"
    assert service.current_state is state
    assert service._task is not None
    await service._task

    assert state.status == "completed"
    assert state.report is not None
    assert state.report.summary["passed"] == 1
    assert (tmp_path / "report.json").exists()


@pytest.mark.asyncio
async def test_full_run_without_pytest_is_skipped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = ApplicationTestService(
        _settings(tmp_path),
        report_path=tmp_path / "report.json",
    )
    monkeypatch.setattr(
        ApplicationTestService,
        "pytest_available",
        property(lambda self: False),
    )

    state = service.start("full", object())
    assert service._task is not None
    await service._task

    assert state.report is not None
    assert state.report.results[0].status == "skipped"
    assert "iniciar.bat --dev" in state.report.results[0].detail


@pytest.mark.asyncio
async def test_pytest_runner_uses_fixed_command_and_summarizes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = ApplicationTestService(
        _settings(tmp_path),
        report_path=tmp_path / "report.json",
    )

    class FakeProcess:
        returncode = 0
        stdout = None

        async def wait(self) -> int:
            return 0

        async def kill(self) -> None:
            return None

    calls: list[dict[str, object]] = []

    async def fake_create(*args: object, **kwargs: object) -> FakeProcess:
        calls.append({"args": args, "kwargs": kwargs})
        return FakeProcess()

    def fake_parse(path: Path) -> tuple[list[TestResult], dict[str, int]]:
        path.write_text("<testsuites/>", encoding="utf-8")
        return (
            [
                TestResult(
                    key="pytest:tests.unit.test_x",
                    group="Suíte completa",
                    title="unit/test_x.py",
                    status="passed",
                    detail="1 passaram",
                    duration_ms=1,
                )
            ],
            {"total": 1, "passed": 1, "failed": 0, "skipped": 0},
        )

    async def fake_prepare(environment: dict[str, str]) -> None:
        del environment

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)
    monkeypatch.setattr(service, "_prepare_suite_database", fake_prepare)
    monkeypatch.setattr(service, "_parse_junit", fake_parse)

    results, counts = await service._run_pytest("run-1")

    assert counts == {"total": 1, "passed": 1, "failed": 0, "skipped": 0}
    assert results[0].status == "passed"
    command = calls[0]["args"]
    assert isinstance(command, tuple)
    if command[1].endswith("subprocess_supervisor.py"):
        command = command[2:]
    assert command[:3] == (command[0], "-m", "pytest")
    assert "no:cacheprovider" in command
    assert command[command.index("pytest") + 1 : command.index("pytest") + 4] == (
        "-q",
        "-m",
        "not live",
    )
    assert "-o" not in command
    child_environment = calls[0]["kwargs"]["env"]
    assert child_environment["RUN_LIVE_CONTRACT_TESTS"] == "false"
    assert child_environment["LLM_ENABLED"] == "false"
    assert child_environment["DATABASE_URL"].startswith("sqlite:///")
    assert any(str(item).startswith("--junitxml=") for item in command)


def test_pytest_environment_is_isolated_and_offline(tmp_path: Path) -> None:
    service = ApplicationTestService(
        _settings(tmp_path),
        report_path=tmp_path / "report.json",
    )
    environment = service._pytest_environment(tmp_path / "suite")

    assert environment["APP_ENV"] == "test"
    assert environment["DATABASE_URL"].startswith("sqlite:///")
    assert (tmp_path / "suite").as_posix() in environment["DATABASE_URL"]
    assert environment["RUN_LIVE_CONTRACT_TESTS"] == "false"
    assert environment["LLM_ENABLED"] == "false"
    assert environment["CONTACT_SEARCH_ENABLED"] == "false"
    assert environment["OCR_ENABLED"] == "false"
    assert environment["DOCUMENT_STORAGE_PATH"].startswith(str(tmp_path / "suite"))
    assert (tmp_path / "suite" / "documents").is_dir()
    assert (tmp_path / "suite" / "raw").is_dir()


def test_junit_failure_details_are_redacted(tmp_path: Path) -> None:
    settings = _settings(tmp_path).model_copy(
        update={
            "transparencia_api_key": "transparencia-secret",
            "llm_api_key": "llm-secret",
            "contact_search_api_key": "contact-secret",
        }
    )
    service = ApplicationTestService(settings, report_path=tmp_path / "report.json")
    report = tmp_path / "junit.xml"
    report.write_text(
        """<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite>
  <testcase classname="tests.unit.test_alpha.TestClass" name="fails" time="0.1">
    <failure message="contact-secret">llm-secret transparencia-secret</failure>
  </testcase>
</testsuite></testsuites>
""",
        encoding="utf-8",
    )

    results, counts = service._parse_junit(report)

    assert counts == {"total": 1, "passed": 0, "failed": 1, "skipped": 0}
    assert results[0].title == "unit/test_alpha.py"
    assert "contact-secret" not in (results[0].output or "")
    assert "llm-secret" not in (results[0].output or "")
    assert "transparencia-secret" not in (results[0].output or "")


def test_junit_message_only_failure_is_preserved(tmp_path: Path) -> None:
    service = ApplicationTestService(_settings(tmp_path), report_path=tmp_path / "report.json")
    report = tmp_path / "junit.xml"
    report.write_text(
        """<testsuites><testsuite><testcase classname="tests.unit.test_x" name="x">
<failure message="message-only failure" />
</testcase></testsuite></testsuites>""",
        encoding="utf-8",
    )

    results, _ = service._parse_junit(report)

    assert "message-only failure" in (results[0].output or "")


@pytest.mark.asyncio
async def test_connectivity_requires_real_endpoints(tmp_path: Path) -> None:
    settings = _settings(tmp_path).model_copy(update={"municipal_payment_sources": []})
    service = ApplicationTestService(settings, report_path=tmp_path / "report.json")
    with respx.mock(assert_all_called=False) as router:
        router.get(
            url__regex=r"https://pncp\.gov\.br/api/consulta/v1/contratacoes/publicacao.*"
        ).mock(
            return_value=httpx.Response(
                200, json={"data": [], "totalPaginas": 1, "totalRegistros": 0}
            )
        )
        router.get(url__regex=r"https://dadosabertos\.compras\.gov\.br/.*").mock(
            return_value=httpx.Response(
                200, json={"resultado": [], "totalPaginas": 1, "totalRegistros": 0}
            )
        )
        outcome = await service._check_connectivity()

    assert outcome.status == "passed", outcome
    assert "2 fonte" in outcome.detail


@pytest.mark.asyncio
async def test_connectivity_fails_when_all_sources_fail(tmp_path: Path) -> None:
    settings = _settings(tmp_path).model_copy(update={"municipal_payment_sources": []})
    service = ApplicationTestService(settings, report_path=tmp_path / "report.json")
    with respx.mock(assert_all_called=False) as router:
        router.get(url__regex=r"https://pncp\.gov\.br/.*").mock(
            return_value=httpx.Response(503, text="down")
        )
        router.get(url__regex=r"https://dadosabertos\.compras\.gov\.br/.*").mock(
            return_value=httpx.Response(503, text="down")
        )
        outcome = await service._check_connectivity()

    assert outcome.status == "failed"
    assert "Nenhuma fonte" in outcome.detail


@pytest.mark.parametrize("password", ["p%40ssword", "x"])
def test_redaction_covers_encoded_and_short_database_passwords(tmp_path, password):
    from urllib.parse import unquote

    settings = _settings(tmp_path).model_copy(
        update={"database_url": f"postgresql://operator:{password}@db/app"}
    )
    service = ApplicationTestService(settings, report_path=tmp_path / "report.json")
    for representation in {password, unquote(password)}:
        assert representation not in service._redact(f"credential [{representation}]")


@pytest.mark.parametrize("hosts", [["*.example.org"], ["testserver"], ["*"]])
async def test_diagnostic_host_is_accepted_by_configured_trusted_hosts(tmp_path, hosts):
    from starlette.middleware.trustedhost import TrustedHostMiddleware
    from starlette.responses import PlainTextResponse

    async def healthy(scope, receive, send):
        await PlainTextResponse("ok")(scope, receive, send)

    service = ApplicationTestService(
        _settings(tmp_path).model_copy(update={"allowed_hosts": hosts}),
        report_path=tmp_path / "report.json",
    )
    app = TrustedHostMiddleware(healthy, allowed_hosts=hosts)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=service._base_url()
    ) as client:
        assert (await client.get("/dashboard")).status_code == 200


async def test_finalization_remains_owned_and_blocks_another_run(tmp_path, monkeypatch):
    service = ApplicationTestService(_settings(tmp_path), report_path=tmp_path / "report.json")
    entered, release = asyncio.Event(), asyncio.Event()

    async def acquire(run_id):
        return "test-lease", "test-owner"

    async def quick(asgi_app, *, include_live):
        return []

    async def save(report):
        entered.set()
        await release.wait()
        return None

    monkeypatch.setattr(service, "_acquire_run_lease", acquire)
    monkeypatch.setattr(service, "_run_quick", quick)
    monkeypatch.setattr(service, "_save_report", save)
    state = service.start("quick", object())
    original = service._task
    await entered.wait()
    assert state.status == "running"
    with pytest.raises(TestRunBusy):
        service.start("quick", object())
    closing = asyncio.create_task(service.aclose())
    await asyncio.sleep(0)
    assert not closing.done()
    assert original is service._task
    release.set()
    await closing
    assert original.done()
    assert state.status == "completed"


async def test_quick_cancellation_preserves_completed_checks(tmp_path, monkeypatch):
    service = ApplicationTestService(_settings(tmp_path), report_path=tmp_path / "report.json")
    entered = asyncio.Event()
    calls = 0

    async def acquire(run_id):
        return "test-lease", "test-owner"

    async def timed(key, group, title, operation):
        nonlocal calls
        calls += 1
        if calls == 2:
            entered.set()
            await asyncio.Event().wait()
        return TestResult(key, group, title, "passed", "ok", 0)

    monkeypatch.setattr(service, "_acquire_run_lease", acquire)
    monkeypatch.setattr(service, "_timed", timed)
    state = service.start("quick", object())
    await entered.wait()
    await service.aclose()
    assert state.status == "cancelled"
    assert len(state.report.results) == 1
    assert state.report.results[0].key == "environment"


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        (
            "default-src 'self'; frame-ancestors 'none'; form-action 'self'; script-src 'self' 'unsafe-inline'",
            "passed",
        ),
        ("default-src *; frame-ancestors 'none'; form-action 'self'", "failed"),
        ("default-src *; default-src 'self'; frame-ancestors 'none'; form-action 'self'", "failed"),
        (
            "default-src 'self'; script-src-elem *; frame-ancestors 'none'; form-action 'self'",
            "failed",
        ),
        (
            "default-src 'self'; frame-ancestors 'none'; form-action 'self'; script-src 'self' 'unsafe-eval'",
            "failed",
        ),
        ("default-src 'self'; frame-ancestors *; form-action *", "failed"),
    ],
)
async def test_security_diagnostic_checks_csp_values(tmp_path, policy, expected):
    from starlette.datastructures import Headers
    from starlette.responses import PlainTextResponse

    async def application(scope, receive, send):
        status = 200
        if Headers(scope=scope).get("host") == "evil.invalid":
            status = 400
        if scope["method"] == "POST":
            status = 403
        await PlainTextResponse(
            "ok",
            status_code=status,
            headers={
                "x-content-type-options": "nosniff",
                "x-frame-options": "DENY",
                "content-security-policy": policy,
            },
        )(scope, receive, send)

    service = ApplicationTestService(_settings(tmp_path), report_path=tmp_path / "report.json")
    assert (await service._check_security(application)).status == expected


@pytest.mark.asyncio
async def test_pytest_timeout_and_parse_failure_have_no_test_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = ApplicationTestService(_settings(tmp_path), report_path=tmp_path / "report.json")
    monkeypatch.setattr("app.services.application_tests._PYTEST_TIMEOUT_SECONDS", 0.01)

    class SlowProcess:
        returncode = None
        stdout = None

        async def wait(self) -> int:
            await asyncio.sleep(5)
            return self.returncode or 0

        def kill(self) -> None:
            self.returncode = -9

    async def slow_create(*args: object, **kwargs: object) -> SlowProcess:
        return SlowProcess()

    async def fake_prepare(environment: dict[str, str]) -> None:
        del environment

    monkeypatch.setattr(asyncio, "create_subprocess_exec", slow_create)
    monkeypatch.setattr(service, "_prepare_suite_database", fake_prepare)
    results, counts = await service._run_pytest("timeout")

    assert counts is None
    assert results[0].key == "pytest:timeout"


@pytest.mark.asyncio
async def test_invalid_report_is_reported(tmp_path: Path) -> None:
    service = ApplicationTestService(_settings(tmp_path), report_path=tmp_path / "report.json")
    service.report_path.write_text("{invalid", encoding="utf-8")

    assert service.last_report() is None
    assert service.report_load_error is not None


def test_environment_reports_missing_required_assets(tmp_path: Path) -> None:
    settings = _settings(tmp_path).model_copy(
        update={
            "document_storage_path": tmp_path / "missing-documents",
            "raw_data_storage_path": tmp_path / "missing-raw",
        }
    )
    service = ApplicationTestService(settings, report_path=tmp_path / "report.json")
    outcome = service._check_environment()

    assert outcome.status == "failed"
    assert "documentos ausente" in (outcome.output or "")


def test_sources_check_reports_unknown_ids_and_disabled_catalog(tmp_path: Path) -> None:
    settings = _settings(tmp_path).model_copy(
        update={"municipal_payment_sources": ["imperatriz", "does_not_exist"]}
    )
    service = ApplicationTestService(settings, report_path=tmp_path / "report.json")
    assert service._check_sources().status == "failed"

    disabled = _settings(tmp_path).model_copy(update={"municipal_payment_sources": []})
    disabled_service = ApplicationTestService(disabled, report_path=tmp_path / "report.json")
    assert disabled_service._check_sources().status == "skipped"


@pytest.mark.asyncio
async def test_report_write_failure_is_surfaced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = ApplicationTestService(_settings(tmp_path), report_path=tmp_path / "report.json")

    def fail_write(report: object) -> None:
        del report
        raise OSError("disk full")

    monkeypatch.setattr(service, "_write_report", fail_write)
    assert (
        await service._save_report(
            TestReport(
                run_id="x",
                mode="quick",
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
                results=[],
            )
        )
        is not None
    )


def test_base_url_handles_wildcards_and_ports(tmp_path: Path) -> None:
    wildcard = _settings(tmp_path).model_copy(update={"allowed_hosts": ["*"]})
    service = ApplicationTestService(wildcard, report_path=tmp_path / "report.json")
    assert service._base_url() == "http://localhost:8000"

    host = _settings(tmp_path).model_copy(update={"allowed_hosts": ["localhost:8123"]})
    service = ApplicationTestService(host, report_path=tmp_path / "report.json")
    assert service._base_url() == "http://localhost:8123"


class _RunnerProcess:
    def __init__(self, returncode: int) -> None:
        self.returncode = returncode
        self.stdout = None

    async def wait(self) -> int:
        return self.returncode

    def kill(self) -> None:
        self.returncode = -9


@pytest.mark.asyncio
async def test_pytest_parse_and_empty_results_are_honest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = ApplicationTestService(_settings(tmp_path), report_path=tmp_path / "report.json")

    async def create(*args: object, **kwargs: object) -> _RunnerProcess:
        return _RunnerProcess(1)

    async def fake_prepare(environment: dict[str, str]) -> None:
        del environment

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(service, "_prepare_suite_database", fake_prepare)

    def broken(path: Path) -> tuple[list[TestResult], dict[str, int]]:
        del path
        raise ValueError("bad junit")

    monkeypatch.setattr(service, "_parse_junit", broken)
    parsed_results, parsed_counts = await service._run_pytest("parse")
    assert parsed_counts is None
    assert parsed_results[0].key == "pytest:parse"

    monkeypatch.setattr(
        service,
        "_parse_junit",
        lambda path: ([], {"total": 0, "passed": 0, "failed": 0, "skipped": 0}),
    )
    empty_results, empty_counts = await service._run_pytest("empty")
    assert empty_counts is None
    assert empty_results[0].key == "pytest:empty"


@pytest.mark.asyncio
async def test_pytest_nonzero_exit_without_failed_case_is_runner_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = ApplicationTestService(_settings(tmp_path), report_path=tmp_path / "report.json")

    async def create(*args: object, **kwargs: object) -> _RunnerProcess:
        return _RunnerProcess(2)

    async def fake_prepare(environment: dict[str, str]) -> None:
        del environment

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(service, "_prepare_suite_database", fake_prepare)
    monkeypatch.setattr(
        service,
        "_parse_junit",
        lambda path: (
            [
                TestResult(
                    key="pytest:tests.unit.test_x",
                    group="Suíte completa",
                    title="unit/test_x.py",
                    status="passed",
                    detail="ok",
                    duration_ms=1,
                )
            ],
            {"total": 1, "passed": 1, "failed": 0, "skipped": 0},
        ),
    )

    results, counts = await service._run_pytest("exit")

    assert counts == {"total": 1, "passed": 1, "failed": 0, "skipped": 0}
    assert results[-1].key == "pytest:exit"


@pytest.mark.asyncio
async def test_web_api_and_security_checks_report_failures(tmp_path: Path) -> None:
    async def broken_app(scope, receive, send) -> None:
        del receive
        await send(
            {
                "type": "http.response.start",
                "status": 500,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"error"})

    service = ApplicationTestService(
        _settings(tmp_path).model_copy(update={"allowed_hosts": ["localhost"]}),
        report_path=tmp_path / "report.json",
    )

    assert (await service._check_web(broken_app)).status == "failed"
    assert (await service._check_api(broken_app)).status == "failed"
    security = await service._check_security(broken_app)
    assert security.status == "failed"


@pytest.mark.asyncio
async def test_cancelled_run_persists_terminal_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = ApplicationTestService(_settings(tmp_path), report_path=tmp_path / "report.json")
    entered = asyncio.Event()

    async def slow_quick(asgi_app: object, *, include_live: bool) -> list[TestResult]:
        del asgi_app, include_live
        entered.set()
        await asyncio.sleep(30)
        return []

    monkeypatch.setattr(service, "_run_quick", slow_quick)
    state = service.start("quick", object())
    await entered.wait()
    assert service.cancel() is True
    assert service._task is not None
    with pytest.raises(asyncio.CancelledError):
        await service._task

    assert state.status == "cancelled"
    assert state.report is not None and state.report.status == "cancelled"
    assert (tmp_path / "report.json").is_file()


@pytest.mark.asyncio
async def test_scheduler_check_reports_database_failure(tmp_path: Path) -> None:
    class BrokenSessions:
        def __call__(self) -> BrokenSessions:
            return self

        async def __aenter__(self) -> object:
            raise RuntimeError("database unavailable")

        async def __aexit__(self, *args: object) -> None:
            return None

    service = ApplicationTestService(
        _settings(tmp_path),
        session_factory=BrokenSessions(),  # type: ignore[arg-type]
        report_path=tmp_path / "report.json",
    )

    outcome = await service._check_scheduler()

    assert outcome.status == "failed"


@pytest.mark.parametrize("write_fails", [False, True])
async def test_runner_failure_redacts_exception_and_releases_lease(
    tmp_path, monkeypatch, write_fails
):
    settings = _settings(tmp_path).model_copy(update={"llm_api_key": "synthetic-private-token"})
    service = ApplicationTestService(settings, report_path=tmp_path / "report.json")
    released = []

    async def acquire(run_id):
        return "test-lease", "test-owner"

    async def release(name, owner):
        released.append((name, owner))

    async def broken(asgi_app, *, include_live):
        raise RuntimeError("driver leaked synthetic-private-token")

    def no_write(report):
        raise PermissionError("read-only report directory")

    monkeypatch.setattr(service, "_acquire_run_lease", acquire)
    monkeypatch.setattr(service, "_release_run_lease", release)
    monkeypatch.setattr(service, "_run_quick", broken)
    if write_fails:
        monkeypatch.setattr(service, "_write_report", no_write)
    state = service.start("quick", object())
    await service._task
    assert state.status == "failed"
    assert state.report.results[0].key == "runner:error"
    assert "synthetic-private-token" not in str(state.report.to_dict())
    assert released == [("test-lease", "test-owner")]
    if write_fails:
        assert "gravar" in state.error and state.report.status == "failed"
    else:
        assert "synthetic-private-token" not in service.report_path.read_text()


async def test_quick_global_timeout_keeps_completed_diagnostics(tmp_path, monkeypatch):
    service = ApplicationTestService(_settings(tmp_path), report_path=tmp_path / "report.json")
    calls = 0

    async def timed(key, group, title, operation):
        nonlocal calls
        calls += 1
        if calls > 1:
            await asyncio.Event().wait()
        return TestResult(key, group, title, "passed", "ok", 0)

    monkeypatch.setattr(service, "_timed", timed)
    monkeypatch.setattr("app.services.application_tests._QUICK_TOTAL_TIMEOUT_SECONDS", 0.01)
    results = await service._run_quick(object(), include_live=False)
    assert [(item.key, item.status) for item in results] == [
        ("environment", "passed"),
        ("quick:timeout", "failed"),
    ]


@pytest.mark.parametrize("fails", [False, True])
async def test_individual_check_errors_and_timeout_are_honest(tmp_path, monkeypatch, fails):
    service = ApplicationTestService(_settings(tmp_path), report_path=tmp_path / "report.json")

    async def operation():
        if fails:
            raise ValueError("schema unavailable")
        await asyncio.Event().wait()

    monkeypatch.setattr("app.services.application_tests._QUICK_CHECK_TIMEOUT_SECONDS", 0.01)
    result = await service._timed("proof", "group", "title", operation)
    assert result.status == "failed"
    assert ("ValueError" in result.detail) if fails else ("Tempo limite" in result.detail)


async def test_web_and_api_diagnostics_distinguish_content_and_health_errors(tmp_path):
    from starlette.responses import JSONResponse, PlainTextResponse

    async def application(scope, receive, send):
        if scope["path"] == "/dashboard":
            raise RuntimeError("template unavailable")
        if scope["path"] == "/health":
            response = JSONResponse({"status": "degraded", "database": "error"})
        elif scope["path"].startswith("/api/"):
            response = JSONResponse(
                {}, status_code=422 if scope.get("query_string") == b"phase=invalida" else 200
            )
        else:
            response = PlainTextResponse("not HTML or JSON")
        await response(scope, receive, send)

    service = ApplicationTestService(_settings(tmp_path), report_path=tmp_path / "report.json")
    web = await service._check_web(application)
    api = await service._check_api(application)
    assert web.status == "failed" and "RuntimeError" in web.output and "HTML" in web.output
    assert api.status == "failed" and "saudáveis" in api.output and "JSON" in api.output


@pytest.mark.parametrize("content", ["[]", '{"started_at":"invalid","finished_at":"invalid"}'])
def test_invalid_report_shapes_do_not_crash_the_page(tmp_path, content):
    service = ApplicationTestService(_settings(tmp_path), report_path=tmp_path / "report.json")
    service.report_path.write_text(content, encoding="utf-8")
    assert service.last_report() is None
    assert "inválido" in service.report_load_error
