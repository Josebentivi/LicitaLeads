"""Web tests for the operator-facing self-test page."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from app.services.application_tests import (
    ApplicationTestService,
    TestReport,
    TestResult,
    TestRunBusy,
    TestRunState,
)


class FakeTestService:
    pytest_available = True

    def __init__(self) -> None:
        self.started: list[tuple[str, bool]] = []
        self.busy = False
        now = datetime.now(UTC)
        self.current_state = TestRunState(
            run_id="fake-run",
            mode="quick",
            started_at=now,
            status="completed",
            finished_at=now,
            report=TestReport(
                run_id="fake-run",
                mode="quick",
                started_at=now,
                finished_at=now,
                results=[
                    TestResult(
                        key="environment",
                        group="Ambiente",
                        title="Python e diretórios",
                        status="passed",
                        detail="ok",
                        duration_ms=1,
                    )
                ],
            ),
        )
        self.last_report_value = self.current_state.report
        self.report_path = Path("data/test-report.json")
        self.report_load_error: str | None = None
        self.cancelled = False

    def start(self, mode: str, asgi_app: object, *, include_live: bool = False) -> TestRunState:
        del asgi_app
        if self.busy:
            raise TestRunBusy
        self.started.append((mode, include_live))
        assert self.current_state is not None
        return self.current_state

    def last_report(self) -> TestReport | None:
        return self.last_report_value

    def cancel(self) -> bool:
        self.cancelled = True
        return True


@pytest.mark.asyncio
async def test_tests_page_lists_diagnostics_and_full_suite(
    api_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeTestService()
    monkeypatch.setattr("app.web._application_tests", fake)

    page = await api_client.get("/testes")

    assert page.status_code == 200
    assert "Testes da aplicação" in page.text
    assert "Executar diagnóstico" in page.text
    assert "Executar suíte completa" in page.text
    assert "Python e diretórios" in page.text
    assert 'id="test-status"' in page.text
    assert page.text.count('id="test-actions"') == 1
    assert 'class="sidebar-item active"' in page.text


@pytest.mark.asyncio
async def test_post_starts_quick_run_and_redirects(
    api_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeTestService()
    monkeypatch.setattr("app.web._application_tests", fake)

    response = await api_client.post(
        "/testes/executar",
        data={"mode": "quick", "include_live": "true"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/testes"
    assert fake.started == [("quick", True)]


@pytest.mark.asyncio
async def test_post_accepts_localhost_and_ipv4_aliases(
    api_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeTestService()
    monkeypatch.setattr("app.web._application_tests", fake)

    response = await api_client.post(
        "/testes/executar",
        data={"mode": "quick"},
        headers={
            "host": "127.0.0.1:8000",
            "origin": "http://localhost:8000",
            "sec-fetch-site": "same-origin",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/testes"


@pytest.mark.asyncio
async def test_post_reports_busy_run(
    api_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeTestService()
    fake.busy = True
    monkeypatch.setattr("app.web._application_tests", fake)

    response = await api_client.post(
        "/testes/executar",
        data={"mode": "full"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/testes?status=busy"


@pytest.mark.asyncio
async def test_status_partial_renders_last_report(
    api_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeTestService()
    monkeypatch.setattr("app.web._application_tests", fake)

    response = await api_client.get("/testes/status")

    assert response.status_code == 200
    assert 'id="test-status"' in response.text
    assert "Python e diretórios" in response.text
    assert 'hx-get="/testes/status"' not in response.text


def test_pytest_available_detection_does_not_require_tests_run(tmp_path: Path) -> None:
    service = ApplicationTestService(report_path=tmp_path / "report.json")
    assert isinstance(service.pytest_available, bool)


@pytest.mark.asyncio
async def test_status_partial_restores_actions_out_of_band(
    api_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeTestService()
    monkeypatch.setattr("app.web._application_tests", fake)

    response = await api_client.get("/testes/status")

    assert response.status_code == 200
    assert 'id="test-actions"' in response.text
    assert 'hx-swap-oob="outerHTML"' in response.text
    assert "Executar" in response.text


@pytest.mark.asyncio
async def test_cancel_route_calls_service(
    api_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeTestService()
    monkeypatch.setattr("app.web._application_tests", fake)

    response = await api_client.post("/testes/cancelar", follow_redirects=False)

    assert response.status_code == 303
    assert fake.cancelled is True
