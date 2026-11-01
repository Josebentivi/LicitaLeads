"""Read-only diagnostics and isolated, staged local platform validation.

The full run owns sequential test/quality subprocesses and an exclusive lease.
Content-bound reports preserve history and expose unavailable prerequisites.
Validation is always requested explicitly by the operator.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from collections import Counter
from collections.abc import Awaitable, Callable
from contextlib import nullcontext, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar, Literal
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import httpx
from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app import __version__
from app.config import Settings, get_settings
from app.connectors.capabilities import get_source_capabilities
from app.connectors.municipal_payments.sources import (
    list_municipal_sources,
    selected_municipal_sources,
    unknown_municipal_source_ids,
)
from app.connectors.payment_sources import list_payment_sources
from app.database import async_session_factory, write_session
from app.models import CrawlRun, JobLease
from app.models.enums import CrawlRunStatus
from app.repositories.crawls import JobLeaseRepository
from app.services.deadlines import DeadlineEngine, DeadlineMethod, DeadlineRequest
from app.services.event_detection import (
    CompanyReference,
    DocumentEvidence,
    EventDetector,
    EventType,
)
from app.services.identifiers import is_valid_cnpj, normalize_cnpj
from app.services.maintenance import active_leases
from app.services.scheduler_control import scheduler_status
from app.services.test_reports import code_identity, freshness, save_historical_report, save_report

CheckStatus = Literal["passed", "failed", "warning", "skipped"]
RunMode = Literal["quick", "full"]

_WEB_PATHS = (
    "/testes",
    "/dashboard",
    "/procurements",
    "/empresas",
    "/atas",
    "/contratos",
    "/pagamentos",
    "/leads",
    "/crawls",
    "/settings",
    "/ajuda",
    "/source-capabilities",
    "/grupos/compras",
    "/grupos/financeiro",
    "/grupos/oportunidades",
    "/grupos/operacao",
    "/grupos/sistema",
)
_API_PATHS = (
    "/health",
    "/openapi.json",
    "/api/procurements?page_size=1",
    "/api/companies?page_size=1",
    "/api/contracts?page_size=1",
    "/api/payments?page_size=1",
    "/api/price-registries?page_size=1",
    "/api/leads?page_size=1",
    "/api/crawls?page_size=1",
    "/api/payments/sources",
    "/api/payments/municipal/sources",
    "/api/source-capabilities",
)
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_SECRET_RE = re.compile(
    r"(?i)((?:api[_-]?key|token|password|secret|chave-api-dados|authorization)\s*[=:]\s*)[^\s&]+"
)
_PYTEST_TIMEOUT_SECONDS = 3600
_QUICK_CHECK_TIMEOUT_SECONDS = 30.0
_QUICK_TOTAL_TIMEOUT_SECONDS = 180.0
_MAX_OUTPUT_CHARS = 4_000
_MAX_JUNIT_BYTES = 20 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class CheckOutcome:
    status: CheckStatus
    detail: str
    output: str | None = None


@dataclass(frozen=True, slots=True)
class TestResult:
    __test__: ClassVar[bool] = False
    key: str
    group: str
    title: str
    status: CheckStatus
    detail: str
    duration_ms: int
    output: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "group": self.group,
            "title": self.title,
            "status": self.status,
            "detail": self.detail,
            "duration_ms": self.duration_ms,
            "output": self.output,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> TestResult:
        return cls(
            key=str(value.get("key", "")),
            group=str(value.get("group", "")),
            title=str(value.get("title", "")),
            status=str(value.get("status", "failed")),  # type: ignore[arg-type]
            detail=str(value.get("detail", "")),
            duration_ms=int(value.get("duration_ms", 0)),
            output=value.get("output"),
        )


@dataclass(slots=True)
class TestReport:
    __test__: ClassVar[bool] = False
    run_id: str
    mode: RunMode
    started_at: datetime
    finished_at: datetime
    results: list[TestResult]
    test_counts: dict[str, int] | None = None
    error: str | None = None
    status: str = "completed"
    identity: dict[str, Any] | None = None
    freshness: str = "unknown"
    stages: list[dict[str, Any]] = field(default_factory=list)
    coverage: dict[str, Any] | None = None
    scope: str = "local; integrações externas e provedores pagos não verificados"

    @property
    def summary(self) -> dict[str, int]:
        counts = Counter(result.status for result in self.results)
        return {
            "total": len(self.results),
            "passed": counts.get("passed", 0),
            "failed": counts.get("failed", 0),
            "warning": counts.get("warning", 0),
            "skipped": counts.get("skipped", 0),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": 2,
            "run_id": self.run_id,
            "mode": self.mode,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "results": [result.to_dict() for result in self.results],
            "test_counts": self.test_counts,
            "error": self.error,
            "status": self.status,
            "identity": self.identity,
            "freshness": self.freshness,
            "stages": self.stages,
            "coverage": self.coverage,
            "scope": self.scope,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> TestReport:
        return cls(
            run_id=str(value.get("run_id", "")),
            mode=str(value.get("mode", "quick")),  # type: ignore[arg-type]
            started_at=datetime.fromisoformat(str(value["started_at"])),
            finished_at=datetime.fromisoformat(str(value["finished_at"])),
            results=[
                TestResult.from_dict(item)
                for item in value.get("results", [])
                if isinstance(item, dict)
            ],
            test_counts=value.get("test_counts")
            if isinstance(value.get("test_counts"), dict)
            else None,
            error=str(value["error"]) if value.get("error") else None,
            status=str(value.get("status") or "completed"),
            identity=value.get("identity") if isinstance(value.get("identity"), dict) else None,
            stages=value["stages"] if isinstance(value.get("stages"), list) else [],
            coverage=value.get("coverage") if isinstance(value.get("coverage"), dict) else None,
            freshness=str(value.get("freshness", "unknown")),
            scope=str(value.get("scope", "local; escopo histórico não identificado")),
        )


@dataclass(slots=True)
class TestRunState:
    __test__: ClassVar[bool] = False
    run_id: str
    mode: RunMode
    started_at: datetime
    status: str = "running"
    finished_at: datetime | None = None
    report: TestReport | None = None
    error: str | None = None
    identity: dict[str, Any] | None = None
    stages: list[dict[str, Any]] = field(default_factory=list)
    progress: dict[str, int] = field(default_factory=dict)
    coverage: dict[str, Any] | None = None


class TestRunBusy(RuntimeError):
    __test__ = False

    """Raised when a second test run is requested before the first ends."""


class ApplicationTestService:
    """Run the quick diagnostics and, optionally, the project test suite."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        report_path: Path | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.session_factory = session_factory or async_session_factory
        self.report_path = (
            Path(report_path)
            if report_path is not None
            else self.settings.raw_data_storage_path.parent / "test-report.json"
        )
        self._state: TestRunState | None = None
        self._task: asyncio.Task[None] | None = None
        self._report_write_task: asyncio.Task[str | None] | None = None
        self._partial_results: list[TestResult] = []
        self._finalizing = False
        self._lock = asyncio.Lock()
        self.report_load_error: str | None = None
        self._current_identity: dict[str, Any] | None = None
        self._lease_generation: int | None = None
        self._lease_lost = False
        self._suite_counts: dict[str, int] | None = None

    async def refresh_identity(self) -> None:
        self._current_identity = await asyncio.to_thread(code_identity, self._repo_root())

    def _decorate_report(self, report: TestReport) -> TestReport:
        if report.freshness != "changed_during_run":
            report.freshness = (
                freshness(report.identity, self._current_identity)
                if self._current_identity
                else "unknown"
            )
        return report

    def report_for_mode(self, mode: str) -> TestReport | None:
        if mode not in {"quick", "full"}:
            return None
        path = self.report_path.with_name(self.report_path.stem + f"-{mode}.json")
        try:
            return self._decorate_report(
                TestReport.from_dict(json.loads(path.read_text(encoding="utf-8")))
            )
        except (OSError, ValueError, TypeError, KeyError):
            legacy = self.last_report()
            return legacy if legacy is not None and legacy.mode == mode else None

    @property
    def pytest_available(self) -> bool:
        root = self._repo_root()
        return importlib.util.find_spec("pytest") is not None and (root / "tests").is_dir()

    @property
    def current_state(self) -> TestRunState | None:
        return self._state

    def start(
        self,
        mode: str,
        asgi_app: Any,
        *,
        include_live: bool = False,
    ) -> TestRunState:
        if mode not in {"quick", "full"}:
            raise ValueError("modo de teste invalido")
        if self._state is not None and self._state.status == "running":
            raise TestRunBusy
        if self._task is not None and not self._task.done():
            raise TestRunBusy
        if self._report_write_task is not None and not self._report_write_task.done():
            raise TestRunBusy
        state = TestRunState(
            run_id=uuid4().hex,
            mode=mode,  # type: ignore[arg-type]
            started_at=datetime.now(UTC),
        )
        self._lease_generation = None
        self._state = state
        self._task = asyncio.create_task(
            self._execute(state, asgi_app=asgi_app, include_live=include_live)
        )
        self._task.add_done_callback(lambda task: self._after_task(task, state))
        return state

    def _after_task(self, task: asyncio.Task[None], state: TestRunState) -> None:
        # Cancellation can occur before the coroutine's first instruction or
        # while waiting for the lock, outside the execution handler.
        if task.cancelled() and state.status == "running":
            state.status = "cancelled"
            state.finished_at = datetime.now(UTC)
            state.error = "Execução cancelada pelo operador."
            state.report = TestReport(
                run_id=state.run_id,
                mode=state.mode,
                started_at=state.started_at,
                finished_at=state.finished_at,
                results=[],
                status="cancelled",
                error=state.error,
            )
            self._report_write_task = asyncio.create_task(self._archive_report(state.report))

    def cancel(self) -> bool:
        """Cancel the current run and let its execution path persist the state."""

        if self._state is None or self._state.status != "running":
            return False
        if self._task is None or self._task.done():
            return False
        if self._finalizing:
            return False
        self._task.cancel()
        return True

    async def aclose(self) -> None:
        """Cancel and drain a pending run so pytest does not outlive the API."""

        task = self._task
        if task is not None and not task.done():
            if not self._finalizing:
                task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await task
        if task is not None and task.cancelled() and self._state is not None:
            # A completed task's done callback may still be queued when the
            # shutdown coroutine runs; persist that cancellation before return.
            self._after_task(task, self._state)
        self._task = None
        if self._report_write_task is not None:
            await self._report_write_task
            self._report_write_task = None

    async def _execute(
        self,
        state: TestRunState,
        *,
        asgi_app: Any,
        include_live: bool,
    ) -> None:
        async with self._lock:
            self._partial_results = []
            self._suite_counts = None
            self._lease_lost = False
            results: list[TestResult] = []
            counts = None
            error = None
            lease_name = lease_owner = None
            heartbeat = None
            cancelled = False
            try:
                state.identity = await asyncio.to_thread(code_identity, self._repo_root())
                lease_name, lease_owner = await self._acquire_run_lease(state.run_id)
                if lease_name is None or lease_owner is None:
                    raise TestRunBusy(
                        "Outra validação ou manutenção está em andamento; nada foi iniciado."
                    )
                heartbeat = asyncio.create_task(
                    self._renew_run_lease(lease_name, lease_owner, asyncio.current_task())
                )
                if state.mode == "full":
                    if self.pytest_available:
                        from app.services.test_suite import run_suite

                        async with asyncio.timeout(self.settings.test_suite_timeout_seconds):
                            results, counts = await run_suite(self, state.run_id)
                    else:
                        results = [
                            TestResult(
                                key="pytest:unavailable",
                                group="Suíte completa",
                                title="pytest indisponível",
                                status="skipped",
                                detail="Instale as dependências de desenvolvimento com iniciar.bat --dev.",
                                duration_ms=0,
                            )
                        ]
                else:
                    results = await self._run_quick(asgi_app, include_live=include_live)
            except asyncio.CancelledError:
                cancelled = True
                error = (
                    "Lease da validação perdida; processos encerrados."
                    if self._lease_lost
                    else "Execução cancelada pelo operador."
                )
                results = results or list(self._partial_results)
                counts = counts or self._suite_counts
            except Exception as exc:
                error = (
                    "Prazo total da suíte excedido; processos encerrados."
                    if isinstance(exc, TimeoutError)
                    else self._redact(f"{type(exc).__name__}: {exc}")
                )
                results = results or list(self._partial_results)
                counts = counts or self._suite_counts
                results.append(
                    TestResult(
                        key="runner:error",
                        group="Execução",
                        title="Falha do executor",
                        status="failed",
                        detail=error,
                        duration_ms=0,
                    )
                )
            finally:
                self._finalizing = True
                if heartbeat:
                    heartbeat.cancel()
                    with suppress(asyncio.CancelledError, Exception):
                        await heartbeat
                try:
                    self._current_identity = await asyncio.to_thread(
                        code_identity, self._repo_root()
                    )
                    changed = bool(
                        state.identity
                        and (
                            freshness(state.identity, self._current_identity) != "current"
                            or state.identity.get("observation_sha256")
                            != self._current_identity.get("observation_sha256")
                        )
                    )
                    if changed:
                        results.append(
                            TestResult(
                                key="identity:changed",
                                group="Identificação",
                                title="Código alterado durante a execução",
                                status="warning",
                                detail="Os resultados foram preservados, mas não validam a versão atual.",
                                duration_ms=0,
                            )
                        )
                    pending = (
                        changed
                        or any(x.status in {"warning", "skipped"} for x in results)
                        or bool((counts or {}).get("not_executed"))
                        or bool((counts or {}).get("skipped"))
                    )
                    status = (
                        "cancelled"
                        if cancelled
                        else "failed"
                        if error or any(x.status == "failed" for x in results)
                        else "completed_with_pending"
                        if pending
                        else "completed"
                    )
                    for stage in state.stages:
                        if stage.get("status") == "running":
                            stage["status"] = (
                                "cancelled" if cancelled else "failed" if error else "pending"
                            )
                    state.finished_at = datetime.now(UTC)
                    state.error = error
                    state.report = TestReport(
                        run_id=state.run_id,
                        mode=state.mode,
                        started_at=state.started_at,
                        finished_at=state.finished_at,
                        results=results,
                        test_counts=counts,
                        error=error,
                        status=status,
                        identity=state.identity,
                        freshness="changed_during_run" if changed else "current",
                        stages=state.stages,
                        coverage=state.coverage,
                        scope="local + conectividade de fontes solicitada"
                        if include_live and state.mode == "quick"
                        else "local; integrações externas e provedores pagos não verificados",
                    )
                    # A fenced-out old worker may not overwrite a newer report.
                    if not self._lease_lost and lease_name is not None:
                        save_error = await self._save_report(state.report)
                        if save_error:
                            state.error = state.report.error = save_error
                            if self._lease_lost:
                                state.report.status = "failed"
                            elif state.report.status == "completed":
                                state.report.status = "completed_with_pending"
                    else:
                        await self._archive_report(state.report)
                    state.status = state.report.status
                finally:
                    await self._release_run_lease(lease_name, lease_owner)
                    self._finalizing = False
            if cancelled:
                raise asyncio.CancelledError

    async def _acquire_run_lease(self, run_id: str) -> tuple[str | None, str | None]:
        name, owner = "selftest:application", f"selftest:{run_id}"
        try:
            async with write_session(self.session_factory) as session:
                acquired = await JobLeaseRepository(session).acquire(
                    name=name,
                    owner_id=owner,
                    now=datetime.now(UTC),
                    expires_at=datetime.now(UTC) + timedelta(minutes=5),
                )
                if acquired:
                    self._lease_generation = await session.scalar(
                        select(JobLease.version).where(JobLease.name == name)
                    )
        except Exception:
            return None, None
        return (name, owner) if acquired else (None, None)

    async def _renew_run_lease(
        self, name: str, owner: str, worker: asyncio.Task[Any] | None
    ) -> None:
        while True:
            await asyncio.sleep(30)
            try:
                async with write_session(self.session_factory) as session:
                    renewed = await JobLeaseRepository(session).renew(
                        name=name,
                        owner_id=owner,
                        generation=self._lease_generation,
                        now=datetime.now(UTC),
                        expires_at=datetime.now(UTC) + timedelta(minutes=5),
                    )
            except Exception:
                renewed = False
            if not renewed:
                self._lease_lost = True
                if worker:
                    worker.cancel()
                return

    async def _release_run_lease(self, name: str | None, owner: str | None) -> None:
        if not name or not owner:
            return
        try:
            async with write_session(self.session_factory) as session:
                await JobLeaseRepository(session).release(
                    name=name, owner_id=owner, generation=self._lease_generation
                )
        except Exception:
            return

    async def _timed(
        self,
        key: str,
        group: str,
        title: str,
        operation: Callable[[], Awaitable[CheckOutcome]],
    ) -> TestResult:
        started = time.perf_counter()
        try:
            outcome = await asyncio.wait_for(
                operation(),
                timeout=_QUICK_CHECK_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            outcome = CheckOutcome(
                status="failed",
                detail=f"Tempo limite de {_QUICK_CHECK_TIMEOUT_SECONDS:.0f}s excedido.",
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            outcome = CheckOutcome(
                status="failed",
                detail=f"{type(exc).__name__}: {exc}",
            )
        duration_ms = int((time.perf_counter() - started) * 1000)
        return TestResult(
            key=key,
            group=group,
            title=title,
            status=outcome.status,
            detail=self._truncate(self._redact(outcome.detail)),
            duration_ms=duration_ms,
            output=self._truncate(self._redact(outcome.output)) if outcome.output else None,
        )

    async def _run_quick(self, asgi_app: Any, *, include_live: bool) -> list[TestResult]:
        checks: list[tuple[str, str, str, Callable[[], Awaitable[CheckOutcome]]]] = [
            (
                "environment",
                "Ambiente",
                "Python, diretórios e arquivos obrigatórios",
                lambda: asyncio.to_thread(self._check_environment),
            ),
            ("database", "Banco e schema", "Conexão e revisão Alembic", self._check_database),
            (
                "web",
                "Páginas web",
                "Todas as páginas operacionais",
                lambda: self._check_web(asgi_app),
            ),
            ("api", "API", "Endpoints e validação", lambda: self._check_api(asgi_app)),
            (
                "security",
                "Segurança",
                "Headers, Host e formulário cross-origin",
                lambda: self._check_security(asgi_app),
            ),
            (
                "domain",
                "Domínio",
                "CNPJ, prazos e detector de eventos",
                lambda: asyncio.to_thread(self._check_domain),
            ),
            (
                "sources",
                "Fontes",
                "Catálogo e matriz de capacidades",
                lambda: asyncio.to_thread(self._check_sources),
            ),
            (
                "scheduler",
                "Scheduler",
                "Pausa, leases e heartbeat",
                self._check_scheduler,
            ),
        ]
        if include_live:
            checks.append(
                (
                    "connectivity",
                    "Conectividade",
                    "Portais externos (somente leitura)",
                    self._check_connectivity,
                )
            )
        results: list[TestResult] = []
        self._partial_results = results

        async def run_checks() -> None:
            for key, group, title, operation in checks:
                results.append(await self._timed(key, group, title, operation))

        try:
            async with asyncio.timeout(_QUICK_TOTAL_TIMEOUT_SECONDS):
                await run_checks()
        except TimeoutError:
            results.append(
                TestResult(
                    key="quick:timeout",
                    group="Execução",
                    title="Diagnóstico excedeu o tempo total",
                    status="failed",
                    detail=(
                        f"O diagnóstico excedeu {_QUICK_TOTAL_TIMEOUT_SECONDS:.0f} segundos. "
                        "As verificações concluídas foram preservadas."
                    ),
                    duration_ms=int(_QUICK_TOTAL_TIMEOUT_SECONDS * 1000),
                )
            )
        return results

    def _check_environment(self) -> CheckOutcome:
        root = self._repo_root()
        issues: list[str] = []
        if sys.version_info[:2] != (3, 12):
            issues.append(f"Python {sys.version.split()[0]} incompatível; use Python 3.12")
        storage_paths = (
            ("documentos", self.settings.document_storage_path),
            ("dados brutos", self.settings.raw_data_storage_path),
        )
        for label, path in storage_paths:
            if not path.exists():
                issues.append(f"diretório de {label} ausente: {path}")
                continue
            probe = path / f".licitalead-test-{os.getpid()}-{uuid4().hex[:8]}"
            try:
                probe.write_text("ok", encoding="utf-8")
            except OSError as exc:
                issues.append(f"sem escrita em {label}: {exc}")
            finally:
                probe.unlink(missing_ok=True)
        for artifact in ("Dockerfile", "docker-compose.yml"):
            if (root / artifact).exists():
                issues.append(f"arquivo não permitido encontrado: {artifact}")
        for required in (
            "alembic.ini",
            "migrations/env.py",
            "app/templates/base.html",
            "app/static/app.css",
            "app/static/app.js",
        ):
            if not (root / required).exists():
                issues.append(f"arquivo obrigatório ausente: {required}")
        if issues:
            return CheckOutcome(
                "failed", "Foram encontrados problemas de ambiente.", "\n".join(issues)
            )
        return CheckOutcome(
            "passed",
            f"Python {sys.version.split()[0]}; versão {__version__}; diretórios graváveis.",
        )

    async def _check_database(self) -> CheckOutcome:
        root = self._repo_root()
        try:
            config = AlembicConfig(str(root / "alembic.ini"))
            head = ScriptDirectory.from_config(config).get_current_head()
        except Exception as exc:
            return CheckOutcome("failed", "Não foi possível ler a revisão Alembic.", str(exc))
        try:
            async with self.session_factory() as session:
                await session.execute(text("SELECT 1"))
                revision = await session.scalar(text("SELECT version_num FROM alembic_version"))
                stale = int(
                    await session.scalar(
                        select(func.count())
                        .select_from(CrawlRun)
                        .where(
                            CrawlRun.status.in_([CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING])
                        )
                    )
                    or 0
                )
        except Exception as exc:
            return CheckOutcome("failed", "Falha ao consultar o banco.", self._redact(str(exc)))
        if not revision:
            return CheckOutcome("failed", "Banco sem revisão Alembic registrada.")
        if revision != head:
            return CheckOutcome(
                "failed",
                f"Banco em {revision}, mas o código espera {head}.",
            )
        detail = f"Banco conectado; revisão {revision}."
        if stale:
            return CheckOutcome(
                "warning",
                detail + f" {stale} coleta(s) pendente(s)/em andamento.",
            )
        return CheckOutcome("passed", detail)

    async def _check_web(self, asgi_app: Any) -> CheckOutcome:
        failures: list[str] = []
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=asgi_app),
            base_url=self._base_url(),
            follow_redirects=True,
            timeout=20.0,
        ) as client:
            for path in _WEB_PATHS:
                try:
                    response = await client.get(path)
                except Exception as exc:
                    failures.append(f"{path}: {type(exc).__name__}: {exc}")
                    continue
                content_type = response.headers.get("content-type", "")
                if response.status_code != 200:
                    failures.append(f"{path}: HTTP {response.status_code}")
                elif "text/html" not in content_type or "<html" not in response.text[:500].lower():
                    failures.append(f"{path}: resposta não parece uma página HTML")
        if failures:
            return CheckOutcome(
                "failed",
                "Uma ou mais páginas não responderam corretamente.",
                "\n".join(failures),
            )
        return CheckOutcome("passed", f"{len(_WEB_PATHS)} páginas responderam HTML em 200.")

    async def _check_api(self, asgi_app: Any) -> CheckOutcome:
        failures: list[str] = []
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=asgi_app),
            base_url=self._base_url(),
            follow_redirects=True,
            timeout=20.0,
        ) as client:
            for path in _API_PATHS:
                try:
                    response = await client.get(path)
                except Exception as exc:
                    failures.append(f"{path}: {type(exc).__name__}: {exc}")
                    continue
                if response.status_code != 200:
                    failures.append(f"{path}: HTTP {response.status_code}")
                    continue
                try:
                    payload = response.json()
                except ValueError:
                    failures.append(f"{path}: resposta não é JSON")
                    continue
                if path == "/health" and (
                    not isinstance(payload, dict)
                    or payload.get("status") != "ok"
                    or payload.get("database") != "ok"
                ):
                    failures.append("/health: status ou banco não estão saudáveis")
            invalid = await client.get("/api/procurements?phase=invalida")
            if invalid.status_code != 422:
                failures.append(f"validação de enum: esperado 422, recebido {invalid.status_code}")
        if failures:
            return CheckOutcome("failed", "Um ou mais endpoints falharam.", "\n".join(failures))
        return CheckOutcome("passed", f"{len(_API_PATHS)} endpoints e validação de enum OK.")

    async def _check_security(self, asgi_app: Any) -> CheckOutcome:
        failures: list[str] = []
        warnings: list[str] = []
        host_header = urlsplit(self._base_url()).netloc
        wildcard_host = "*" in self.settings.allowed_hosts
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=asgi_app),
            base_url=self._base_url(),
            follow_redirects=False,
            timeout=20.0,
        ) as client:
            page = await client.get("/dashboard")
            expected_headers = {
                "x-content-type-options": "nosniff",
                "x-frame-options": "DENY",
                "content-security-policy": None,
            }
            for header, expected in expected_headers.items():
                value = page.headers.get(header)
                if not value or (expected is not None and value != expected):
                    failures.append(f"header ausente/incorreto: {header}")
            csp = page.headers.get("content-security-policy", "")
            policy: dict[str, set[str]] = {}
            for value in csp.split(";"):
                if parts := value.split():
                    directive = parts[0].lower()
                    if directive in policy:
                        failures.append(f"CSP com diretiva duplicada: {directive}")
                    else:
                        policy[directive] = set(parts[1:])
            defaults = policy.get("default-src", set())
            if not defaults or not defaults <= {"'self'", "'none'"}:
                failures.append("CSP default-src deve restringir a origem local")
            scripts = policy.get("script-src", defaults)
            for directive in ("script-src", "script-src-elem", "script-src-attr"):
                if any(
                    source not in {"'self'", "'none'", "'unsafe-inline'"}
                    and not source.startswith(("'nonce-", "'sha256-", "'sha384-", "'sha512-"))
                    for source in policy.get(directive, scripts)
                ):
                    failures.append(f"CSP {directive} permite scripts de origem insegura")
            if policy.get("frame-ancestors") != {"'none'"}:
                failures.append("CSP frame-ancestors deve bloquear enquadramento")
            if policy.get("form-action") != {"'self'"}:
                failures.append("CSP form-action deve restringir a origem local")
            if wildcard_host:
                warnings.append("ALLOWED_HOSTS=*: TrustedHost não restringe hosts.")
            else:
                evil_host = await client.get("/dashboard", headers={"host": "evil.invalid"})
                if evil_host.status_code != 400:
                    failures.append(f"TrustedHost: esperado 400, recebido {evil_host.status_code}")
            cross_origin = await client.post(
                "/pagamentos/importar",
                headers={
                    "host": host_header,
                    "origin": "https://evil.invalid",
                    "sec-fetch-site": "cross-site",
                },
            )
            if cross_origin.status_code != 403:
                failures.append(
                    f"FormOriginMiddleware HTML: esperado 403, recebido {cross_origin.status_code}"
                )
            api_cross_origin = await client.post(
                "/api/crawls/00000000-0000-0000-0000-000000000000/cancel",
                headers={
                    "host": host_header,
                    "origin": "https://evil.invalid",
                    "sec-fetch-site": "cross-site",
                },
            )
            if api_cross_origin.status_code != 403:
                failures.append(
                    "FormOriginMiddleware API: esperado 403, "
                    f"recebido {api_cross_origin.status_code}"
                )
        if failures:
            return CheckOutcome("failed", "Falha nas proteções HTTP.", "\n".join(failures))
        detail = "Headers, CSP, TrustedHost e bloqueio cross-origin (HTML e API) OK."
        if warnings:
            return CheckOutcome("warning", detail, "\n".join(warnings))
        return CheckOutcome("passed", detail)

    def _check_domain(self) -> CheckOutcome:
        failures: list[str] = []
        if not is_valid_cnpj("00000000000191"):
            failures.append("CNPJ de teste válido foi rejeitado")
        if normalize_cnpj("00.000.000/0001-91") != "00000000000191":
            failures.append("normalização de CNPJ divergente")
        now = datetime(2026, 9, 16, 12, tzinfo=UTC)
        deadline = DeadlineEngine().calculate(
            DeadlineRequest(
                event_type="INELIGIBLE",
                trigger_at=now,
                explicit_deadline_at=now + timedelta(days=3),
                now=now,
            )
        )
        if deadline.method is not DeadlineMethod.EXPLICIT:
            failures.append(f"prazo explícito retornou {deadline.method}")
        company = CompanyReference(
            external_id="test-company",
            legal_name="ABC Equipamentos Ltda.",
            cnpj="04252011000110",
        )
        evidence = DocumentEvidence(
            text=(
                "Item 4. A licitante ABC Equipamentos Ltda., CNPJ 04.252.011/0001-10, "
                "foi inabilitada por não apresentar a certidão exigida."
            ),
            document_type="ata_sessao",
        )
        found = EventDetector().detect(evidence, known_companies=[company])
        if not any(event.event_type is EventType.INELIGIBLE for event in found):
            failures.append("detector não encontrou inabilitação canônica")
        negated = DocumentEvidence(
            text="A empresa ABC Equipamentos Ltda. não foi inabilitada após a diligência.",
            document_type="ata_sessao",
        )
        if any(
            event.event_type is EventType.INELIGIBLE
            for event in EventDetector().detect(negated, known_companies=[company])
        ):
            failures.append("detector aceitou evento negado")
        if failures:
            return CheckOutcome("failed", "Regras de domínio divergiram.", "\n".join(failures))
        return CheckOutcome("passed", "CNPJ, prazo explícito e detector (incluindo negação) OK.")

    def _check_sources(self) -> CheckOutcome:
        try:
            capabilities = get_source_capabilities()
            municipalities = list_municipal_sources(self.settings)
            selected = selected_municipal_sources(self.settings)
            unknown = unknown_municipal_source_ids(self.settings)
            payment_sources = list_payment_sources(self.settings)
        except Exception as exc:
            return CheckOutcome("failed", "Catálogo de fontes não pôde ser carregado.", str(exc))
        issues: list[str] = []
        if len(capabilities) < 2:
            issues.append("matriz de capacidades incompleta")
        if len(municipalities) != 5:
            issues.append(f"esperadas 5 fontes municipais, encontradas {len(municipalities)}")
        for source in municipalities:
            if not source.portal_url or not source.export_url:
                issues.append(f"{source.id}: links de importação ausentes")
        if unknown:
            issues.append("fontes municipais desconhecidas: " + ", ".join(sorted(unknown)))
        if not any(source.id == "manual" for source in payment_sources):
            issues.append("fonte manual não listada")
        if issues:
            return CheckOutcome("failed", "Catálogo de fontes inconsistente.", "\n".join(issues))
        if not selected:
            return CheckOutcome(
                "skipped",
                "Nenhuma fonte municipal habilitada; catálogo e fonte manual OK.",
            )
        return CheckOutcome(
            "passed",
            f"{len(capabilities)} fontes na matriz; {len(selected)} municipais habilitadas.",
        )

    async def _check_scheduler(self) -> CheckOutcome:
        try:
            async with self.session_factory() as session:
                status = await scheduler_status(session, self.settings)
                leases = [
                    lease
                    for lease in await active_leases(session)
                    if not lease[0].startswith("selftest:")
                ]
        except Exception as exc:
            return CheckOutcome(
                "failed", "Não foi possível ler o scheduler.", self._redact(str(exc))
            )
        if not self.settings.scheduler_enabled:
            return CheckOutcome(
                "warning",
                "Scheduler desabilitado por configuração; nenhum processo é esperado.",
            )
        detail = (
            "Scheduler habilitado; nenhuma lease ativa."
            if not leases
            else f"{len(leases)} lease(s) ativa(s)."
        )
        if status.get("paused"):
            return CheckOutcome("warning", "Scheduler pausado; coletas automáticas suspensas.")
        if not status.get("alive"):
            return CheckOutcome(
                "warning",
                "Scheduler não confirmou processo ativo por heartbeat; "
                "leases isoladas não provam que ele está rodando.",
            )
        if leases:
            return CheckOutcome("warning", detail)
        return CheckOutcome("passed", detail)

    async def _check_connectivity(self) -> CheckOutcome:
        from app.connectors.base import DataAvailability
        from app.services.source_probes import probe_connectivity

        outcomes = await probe_connectivity(self.settings)
        healthy = sum(
            status in {DataAvailability.AVAILABLE, DataAvailability.EMPTY}
            for _, status, _ in outcomes
        )
        failures = [
            f"{name}: {status.value}: {detail}"
            for name, status, detail in outcomes
            if status not in {DataAvailability.AVAILABLE, DataAvailability.EMPTY}
        ]
        detail = f"{healthy} fonte(s) com endpoint e resposta reconhecidos, de {len(outcomes)} verificada(s)."
        if not healthy:
            return CheckOutcome(
                "failed",
                "Nenhuma fonte oficial respondeu com contrato reconhecido.",
                detail + "\n" + "\n".join(failures),
            )
        if failures:
            return CheckOutcome(
                "warning", "Conectividade parcial.", detail + "\n" + "\n".join(failures)
            )
        return CheckOutcome("passed", detail)

    async def _run_pytest(
        self,
        run_id: str,
        *,
        directory: Path | None = None,
        environment: dict[str, str] | None = None,
        expression: str = "not live",
        time_limit: float | None = None,
    ) -> tuple[list[TestResult], dict[str, int] | None]:
        context = (
            nullcontext(str(directory))
            if directory
            else tempfile.TemporaryDirectory(prefix=f"licitalead-suite-{run_id[:8]}-")
        )
        with context as temporary:
            temp = Path(temporary)
            junit_path = temp / "junit.xml"
            environment = environment or self._pytest_environment(temp)
            await self._prepare_suite_database(environment)
            command = [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "-m",
                expression,
                "-p",
                "no:cacheprovider",
                f"--junitxml={junit_path}",
            ]
            watcher = None
            if directory:
                from app.services.test_suite import watch_progress

                command.extend(
                    ["-p", "tests.suite_progress", "--cov-fail-under=0", "--cov-report="]
                )
                watcher = asyncio.create_task(watch_progress(self, directory))
            try:
                returncode, stdout = await self._run_process(
                    command,
                    environment,
                    time_limit
                    if time_limit is not None
                    else min(self.settings.test_suite_timeout_seconds, _PYTEST_TIMEOUT_SECONDS),
                )
            except TimeoutError:
                from app.services.test_suite import event_results

                partial, tally = await asyncio.to_thread(
                    event_results, temp / "events.jsonl", self._redact
                )
                partial.append(
                    TestResult(
                        key="pytest:timeout",
                        group="Suíte completa",
                        title="Tempo esgotado",
                        status="failed",
                        detail="Prazo total da suíte excedido; processos encerrados.",
                        duration_ms=0,
                    )
                )
                return partial, tally if tally["total"] else None
            finally:
                if watcher:
                    watcher.cancel()
                    with suppress(asyncio.CancelledError):
                        await watcher
            output = self._truncate(self._redact(self._decode(stdout)))
            try:
                if junit_path.exists() and junit_path.stat().st_size > _MAX_JUNIT_BYTES:
                    raise ValueError("JUnit maior que o limite permitido")
                results, counts = self._parse_junit(junit_path)
            except Exception as exc:
                return (
                    [
                        TestResult(
                            key="pytest:parse",
                            group="Suíte completa",
                            title="Falha ao ler resultado do pytest",
                            status="failed",
                            detail=self._redact(str(exc)),
                            duration_ms=0,
                            output=output,
                        )
                    ],
                    None,
                )
            if not results:
                return (
                    [
                        TestResult(
                            key="pytest:empty",
                            group="Suíte completa",
                            title="Nenhum teste executado",
                            status="failed",
                            detail=f"pytest terminou com código {returncode}.",
                            duration_ms=0,
                            output=output,
                        )
                    ],
                    None,
                )
            if returncode != 0 and counts.get("failed", 0) == 0:
                results.append(
                    TestResult(
                        key="pytest:exit",
                        group="Suíte completa",
                        title="pytest terminou com erro",
                        status="failed",
                        detail=f"Código de saída {returncode}.",
                        duration_ms=0,
                        output=output,
                    )
                )
            return results, counts

    def _pytest_environment(self, temp: Path) -> dict[str, str]:
        environment = os.environ.copy()
        environment.update(
            {
                "APP_ENV": "test",
                "DATABASE_URL": f"sqlite:///{(temp / 'suite.sqlite3').as_posix()}",
                "DOCUMENT_STORAGE_PATH": str(temp / "documents"),
                "DOCUMENT_DOWNLOAD_CONCURRENCY": "4",
                "DOCUMENT_PROCESS_CONCURRENCY": "2",
                "RAW_DATA_STORAGE_PATH": str(temp / "raw"),
                "HOLIDAY_CALENDAR_PATH": str(temp / "holidays.csv"),
                "RUN_LIVE_CONTRACT_TESTS": "false",
                "LLM_ENABLED": "false",
                "LLM_PROVIDER": "none",
                "LLM_API_KEY": "",
                "LLM_MODEL": "",
                "CONTACT_SEARCH_API_KEY": "",
                "TRANSPARENCIA_API_KEY": "",
                "CONTACT_SEARCH_ENABLED": "false",
                "CONTACT_SEARCH_PROVIDER": "none",
                "CONTACT_DOMAIN_DISCOVERY_ENABLED": "false",
                "OCR_ENABLED": "false",
                "OCR_PROVIDER": "none",
                "SCHEDULER_ENABLED": "true",
                "ALLOWED_HOSTS": "testserver,localhost,127.0.0.1",
                "PYTHONDONTWRITEBYTECODE": "1",
                "COVERAGE_FILE": str(temp / ".coverage"),
            }
        )
        environment.pop("PYTEST_ADDOPTS", None)
        for key in list(environment):
            if key.startswith("COV_CORE_") or key == "COVERAGE_PROCESS_START":
                environment.pop(key)
        holiday_source = self.settings.holiday_calendar_path
        if not holiday_source.is_file():
            holiday_source = self._repo_root() / "data" / "holidays.csv"
        if holiday_source.is_file():
            shutil.copy2(holiday_source, temp / "holidays.csv")
        (temp / "documents").mkdir(parents=True)
        (temp / "raw").mkdir(parents=True)
        return environment

    async def _run_process(
        self, command: list[str], environment: dict[str, str], timeout_seconds: float
    ) -> tuple[int, bytes]:
        """Own the child and its descendants through completion and cancellation."""
        environment = {
            key: value
            for key, value in environment.items()
            if not key.startswith("COV_CORE_") and key != "COVERAGE_PROCESS_START"
        }
        supervised_command = command
        if os.name == "nt":
            # The supervisor joins a kill-on-close Job Object before spawning
            # the command, so descendants remain owned even after it exits.
            supervised_command = [
                sys.executable,
                str(Path(__file__).with_name("subprocess_supervisor.py")),
                *command,
            ]
        process = await asyncio.create_subprocess_exec(
            *supervised_command,
            cwd=str(self._repo_root()),
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            creationflags=(
                getattr(subprocess, "CREATE_NO_WINDOW", 0)
                | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            ),
            start_new_session=os.name != "nt",
        )
        reader = asyncio.create_task(self._read_bounded(process.stdout))
        try:
            async with asyncio.timeout(timeout_seconds):
                await process.wait()
                return int(process.returncode or 0), await reader
        except (TimeoutError, asyncio.CancelledError):
            await self._terminate_process(process)
            raise
        finally:
            if not reader.done():
                reader.cancel()
            with suppress(asyncio.CancelledError):
                await reader

    async def _prepare_suite_database(self, environment: dict[str, str]) -> None:
        code, output = await self._run_process(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            environment,
            90,
        )
        if code != 0:
            raise RuntimeError(
                "não foi possível preparar o banco temporário da suíte: "
                + self._truncate(self._redact(self._decode(output)))
            )

    @staticmethod
    async def _read_bounded(stream: asyncio.StreamReader | None) -> bytes:
        if stream is None:
            return b""
        buffer = bytearray()
        while chunk := await stream.read(65_536):
            buffer.extend(chunk)
            if len(buffer) > 512 * 1024:
                del buffer[: len(buffer) - 256 * 1024]
        return bytes(buffer)

    @staticmethod
    async def _terminate_process(process: asyncio.subprocess.Process) -> None:
        pid = getattr(process, "pid", None)
        if pid and os.name == "nt":
            killer = await asyncio.create_subprocess_exec(
                "taskkill",
                "/PID",
                str(pid),
                "/T",
                "/F",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            try:
                await asyncio.wait_for(killer.wait(), timeout=10)
            except TimeoutError:
                killer.kill()
                await killer.wait()
        elif sys.platform != "win32" and pid:
            import signal

            with suppress(ProcessLookupError):
                os.killpg(pid, signal.SIGKILL)
        if process.returncode is None:
            with suppress(ProcessLookupError):
                process.kill()
        with suppress(ProcessLookupError):
            await asyncio.wait_for(process.wait(), timeout=10)

    def _parse_junit(self, path: Path) -> tuple[list[TestResult], dict[str, int]]:
        tree = ET.parse(path)  # noqa: S314 - local JUnit generated by pytest
        modules: dict[str, dict[str, Any]] = {}
        counts = {"total": 0, "passed": 0, "failed": 0, "skipped": 0}
        for case in tree.getroot().iter("testcase"):
            classname = str(case.attrib.get("classname") or "testes")
            name = str(case.attrib.get("name") or "teste")
            parts = classname.split(".")
            if len(parts) > 1 and parts[-1][:1].isupper():
                parts = parts[:-1]
            module_name = ".".join(parts) or classname
            duration = int(float(case.attrib.get("time") or 0) * 1000)
            module = modules.setdefault(
                module_name,
                {"passed": 0, "failed": 0, "skipped": 0, "duration_ms": 0, "failures": []},
            )
            module["duration_ms"] += duration
            counts["total"] += 1
            failure = case.find("failure")
            error = case.find("error")
            skipped_element = case.find("skipped")
            if failure is not None or error is not None:
                module["failed"] += 1
                counts["failed"] += 1
                detail = self._element_text(failure if failure is not None else error)
                module["failures"].append(self._redact(f"{name}:\n{detail}"))
            elif skipped_element is not None:
                module["skipped"] += 1
                counts["skipped"] += 1
            else:
                module["passed"] += 1
                counts["passed"] += 1
        results: list[TestResult] = []
        for module_name, data in sorted(modules.items()):
            failed = int(data["failed"])
            skipped_count = int(data["skipped"])
            passed = int(data["passed"])
            status: CheckStatus = "failed" if failed else ("skipped" if not passed else "passed")
            results.append(
                TestResult(
                    key=f"pytest:{module_name}",
                    group="Suíte completa",
                    title=module_name.removeprefix("tests.").replace(".", "/") + ".py",
                    status=status,
                    detail=f"{passed} passaram, {failed} falharam, {skipped_count} ignorados.",
                    duration_ms=int(data["duration_ms"]),
                    output=self._truncate("\n\n".join(data["failures"])) or None,
                )
            )
        return results, counts

    @staticmethod
    def _element_text(element: ET.Element | None) -> str:
        if element is None:
            return ""
        message = str(element.attrib.get("message") or "").strip()
        body = "\n".join(
            part for part in (element.text or "", *(child.text or "" for child in element))
        ).strip()
        return "\n".join(part for part in (message, body) if part)

    @staticmethod
    def _decode(value: bytes | None) -> str:
        return (value or b"").decode("utf-8", errors="replace")

    def last_report(self) -> TestReport | None:
        if self._state is not None and self._state.report is not None:
            return self._decorate_report(self._state.report)
        self.report_load_error = None
        try:
            value = json.loads(self.report_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, ValueError, KeyError, TypeError) as exc:
            self.report_load_error = self._redact(f"Relatório inválido: {type(exc).__name__}")
            return None
        if not isinstance(value, dict):
            self.report_load_error = "Relatório inválido: conteúdo não é um objeto JSON."
            return None
        try:
            return self._decorate_report(TestReport.from_dict(value))
        except (TypeError, ValueError, KeyError) as exc:
            self.report_load_error = self._redact(f"Relatório inválido: {type(exc).__name__}")
            return None

    async def _save_report(self, report: TestReport) -> str | None:
        try:
            if self._lease_generation is not None:
                async with write_session(self.session_factory) as session:
                    valid = await JobLeaseRepository(session).fence(
                        name="selftest:application",
                        owner_id=f"selftest:{report.run_id}",
                        generation=self._lease_generation,
                        now=datetime.now(UTC),
                    )
                    if not valid:
                        self._lease_lost = True
                        report.status = "failed"
                        report.error = "Lease perdida; relatório corrente preservado."
                        await asyncio.to_thread(
                            save_historical_report, self.report_path, report.to_dict()
                        )
                        return "Lease perdida; relatório corrente preservado."
                    await asyncio.to_thread(self._write_report, report)
            else:
                await asyncio.to_thread(self._write_report, report)
        except (OSError, ValueError) as exc:
            return self._redact(
                f"Não foi possível gravar o relatório em {self.report_path}: {type(exc).__name__}"
            )
        return None

    async def _archive_report(self, report: TestReport) -> str | None:
        try:
            await asyncio.to_thread(save_historical_report, self.report_path, report.to_dict())
        except OSError:
            return "Não foi possível preservar o relatório histórico."
        return None

    def _write_report(self, report: TestReport) -> None:
        save_report(self.report_path, report.to_dict())

    def _redact(self, value: str | None) -> str:
        text = _ANSI_RE.sub("", value or "")
        secrets = [
            self.settings.transparencia_api_key,
            self.settings.llm_api_key,
            self.settings.contact_search_api_key,
        ]
        database_password = urlsplit(self.settings.database_url).password
        if database_password:
            secrets.extend([database_password, unquote(database_password)])
        for secret in sorted(set(secrets), key=len, reverse=True):
            if secret:
                text = text.replace(secret, "***")
        return _SECRET_RE.sub(r"\1***", text)

    @staticmethod
    def _truncate(value: str | None, limit: int = _MAX_OUTPUT_CHARS) -> str:
        text = value or ""
        return text if len(text) <= limit else text[-limit:]

    def _base_url(self) -> str:
        hosts = [host.strip() for host in self.settings.allowed_hosts if host.strip()]
        host = next(
            (
                item
                for item in hosts
                if item != "*" and not item.startswith("*.") and not item.startswith(".")
            ),
            next(
                (f"diagnostico.{item[2:]}" for item in hosts if item.startswith("*.")), "localhost"
            ),
        )
        if host in {"0.0.0.0", "::", "[::]"}:  # noqa: S104
            host = "localhost"
        if host.startswith("[") and "]" in host:
            hostname, _, remainder = host.partition("]")
            hostname = hostname + "]"
            explicit_port = remainder[1:] if remainder.startswith(":") else ""
        elif ":" in host and host.count(":") == 1:
            hostname, explicit_port = host.rsplit(":", 1)
        else:
            hostname, explicit_port = host, ""
        port = int(explicit_port) if explicit_port.isdigit() else self.settings.app_port
        suffix = "" if port == 80 else f":{port}"
        return f"http://{hostname}{suffix}"

    @staticmethod
    def _repo_root() -> Path:
        return Path(__file__).resolve().parents[2]
