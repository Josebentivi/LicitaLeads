"""Sequential, deadline-bound local validation stages for the operator's button."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
import tempfile
import time
from dataclasses import replace
from pathlib import Path
from typing import Any


def event_results(path: Path, redact: Any) -> tuple[list[Any], dict[str, int]]:
    from app.services.application_tests import TestResult

    events: dict[str, dict[str, Any]] = {}
    if path.is_file():
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            try:
                item = json.loads(line)
                previous = events.get(item["nodeid"])
                if previous is None or previous["outcome"] != "failed":
                    events[item["nodeid"]] = item
            except (ValueError, KeyError, TypeError):
                continue  # The writer may still be completing its final line.
    counts = {"total": len(events), "passed": 0, "failed": 0, "skipped": 0}
    results = []
    for nodeid, item in events.items():
        outcome = item["outcome"]
        if outcome not in counts:
            continue
        counts[outcome] += 1
        results.append(
            TestResult(
                key="partial:" + nodeid,
                group="Resultados parciais",
                title=nodeid,
                status=outcome,
                detail=outcome,
                duration_ms=int(item.get("duration", 0) * 1000),
                output=redact(item.get("detail", "")) or None,
            )
        )
    return results, counts


async def watch_progress(service: Any, directory: Path) -> None:
    while True:
        _, counts = await asyncio.to_thread(
            event_results, directory / "events.jsonl", service._redact
        )
        if service.current_state:
            previous = service._suite_counts or {}
            service.current_state.progress = {
                **{name: previous.get(name, 0) + count for name, count in counts.items()},
                "discovered": previous.get("discovered", 0),
            }
        await asyncio.sleep(0.5)


def ocr_readiness() -> dict[str, Any]:
    import shutil
    import subprocess

    executable = shutil.which("tesseract")
    if not executable and os.name == "nt":
        candidate = (
            Path(os.environ.get("PROGRAMFILES", "C:/Program Files")) / "Tesseract-OCR/tesseract.exe"
        )
        if candidate.is_file():
            executable = str(candidate)
    if not executable or importlib.util.find_spec("pytesseract") is None:
        return {"available": False, "detail": "Tesseract/Python OCR não instalado."}
    try:
        result = subprocess.run(  # noqa: S603 - installed executable, fixed arguments
            [executable, "--list-langs"],
            capture_output=True,
            text=True,
            timeout=15,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        available = result.returncode == 0 and "por" in result.stdout.splitlines()
        return {
            "available": available,
            "executable": executable,
            "detail": "OCR português disponível." if available else "Idioma por indisponível.",
        }
    except (OSError, subprocess.TimeoutExpired):
        return {"available": False, "detail": "Tesseract não respondeu à verificação local."}


async def run_suite(service: Any, run_id: str) -> tuple[list[Any], dict[str, int]]:
    from app.services.application_tests import TestResult
    from app.services.test_browser import browser_readiness

    results: list[Any] = []
    counts = {
        "total": 0,
        "passed": 0,
        "failed": 0,
        "skipped": 0,
        "discovered": 0,
        "executed": 0,
        "not_executed": 0,
    }
    service._partial_results = results
    service._suite_counts = counts
    deadline = time.monotonic() + service.settings.test_suite_timeout_seconds
    with tempfile.TemporaryDirectory(prefix=f"licitalead-local-suite-{run_id[:8]}-") as name:
        directory = Path(name)
        environment = service._pytest_environment(directory)

        async def command(
            key: str, title: str, args: list[str], env: dict[str, str]
        ) -> tuple[int, bytes]:
            stage = {"key": key, "title": title, "status": "running", "required": True}
            if service.current_state:
                service.current_state.stages.append(stage)
            started = time.monotonic()
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError
                code, output = await service._run_process(args, env, remaining)
            except TimeoutError:
                stage["status"] = "failed"
                results.append(
                    TestResult(
                        key=key + ":timeout",
                        group="Execução",
                        title=title,
                        status="failed",
                        detail="Prazo total da suíte excedido; processos encerrados.",
                        duration_ms=int((time.monotonic() - started) * 1000),
                    )
                )
                raise
            stage["status"] = "passed" if code == 0 else "failed"
            return code, output

        try:
            await service._prepare_suite_database(environment)
            inventory_env = {**environment, "LICITA_TEST_PROGRESS_DIR": str(directory)}
            code, output = await command(
                "inventory",
                "Descoberta dos testes offline",
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "--collect-only",
                    "-q",
                    "--no-cov",
                    "-m",
                    "not live",
                    "-p",
                    "no:cacheprovider",
                    "-p",
                    "tests.suite_progress",
                ],
                inventory_env,
            )
            if code:
                results.append(
                    TestResult(
                        key="inventory:error",
                        group="Execução",
                        title="Descoberta dos testes",
                        status="failed",
                        detail="Não foi possível descobrir a suíte.",
                        duration_ms=0,
                        output=service._redact(service._decode(output))[-4000:],
                    )
                )
                return results, counts
            inventory = json.loads((directory / "inventory.json").read_text())
            counts["discovered"] = len(inventory)
            for key, title, args in (
                ("ruff", "Ruff", ["ruff", "check", "."]),
                ("format", "Formatação", ["ruff", "format", "--check", "."]),
                ("mypy", "Mypy", ["mypy", "app"]),
            ):
                if importlib.util.find_spec(args[0]) is None:
                    if service.current_state:
                        service.current_state.stages.append(
                            {"key": key, "title": title, "status": "pending", "required": True}
                        )
                    results.append(
                        TestResult(
                            key=key,
                            group="Qualidade",
                            title=title,
                            status="warning",
                            detail="Ferramenta indisponível; instale as dependências de desenvolvimento.",
                            duration_ms=0,
                        )
                    )
                    continue
                start = time.monotonic()
                code, output = await command(key, title, [sys.executable, "-m", *args], environment)
                results.append(
                    TestResult(
                        key=key,
                        group="Qualidade",
                        title=title,
                        status="passed" if code == 0 else "failed",
                        detail="Aprovado." if code == 0 else "Verificação falhou.",
                        duration_ms=int((time.monotonic() - start) * 1000),
                        output=service._redact(service._decode(output))[-4000:] if code else None,
                    )
                )
            browser = await asyncio.to_thread(browser_readiness)
            ocr = await asyncio.to_thread(ocr_readiness)
            files: list[str] = []
            for key, title, expression, ready in (
                (
                    "core",
                    "Testes Python offline",
                    "not live and not browser and not system and not ocr_real",
                    None,
                ),
                ("system", "API e scheduler em processos separados", "not live and system", None),
                ("ocr_real", "OCR português real", "not live and ocr_real", ocr),
                ("browser", "Jornadas de navegador", "not live and browser", browser),
            ):
                expected = sum(item["stage"] == key for item in inventory)
                if not expected or (ready is not None and not ready["available"]):
                    detail = (
                        ready["detail"]
                        if ready and not ready["available"]
                        else "Nenhum teste cadastrado nesta etapa."
                    )
                    results.append(
                        TestResult(
                            key=key + ":unavailable",
                            group="Etapas locais",
                            title=title,
                            status="warning",
                            detail=detail,
                            duration_ms=0,
                        )
                    )
                    if service.current_state:
                        service.current_state.stages.append(
                            {"key": key, "title": title, "status": "pending", "required": True}
                        )
                    continue
                stage_dir = directory / key
                stage_dir.mkdir()
                env = {
                    **environment,
                    "COVERAGE_FILE": str(stage_dir / ".coverage"),
                    "LICITA_TEST_PROGRESS_DIR": str(stage_dir),
                    "SE_OFFLINE": "true",
                    "SE_AVOID_STATS": "true",
                }
                if ocr.get("executable"):
                    env["PATH"] = (
                        str(Path(ocr["executable"]).parent) + os.pathsep + env.get("PATH", "")
                    )
                stage = {"key": key, "title": title, "status": "running", "required": True}
                if service.current_state:
                    service.current_state.stages.append(stage)
                try:
                    part, tally = await service._run_pytest(
                        run_id + "-" + key,
                        directory=stage_dir,
                        environment=env,
                        expression=expression,
                        time_limit=max(0.001, deadline - time.monotonic()),
                    )
                except asyncio.CancelledError:
                    partial, tally = await asyncio.to_thread(
                        event_results, stage_dir / "events.jsonl", service._redact
                    )
                    results.extend(partial)
                    for name in ("total", "passed", "failed", "skipped"):
                        counts[name] += tally[name]
                    raise
                results.extend(replace(item, group=title) for item in part)
                stage["status"] = (
                    "failed"
                    if any(item.status == "failed" for item in part)
                    else "pending"
                    if (tally or {}).get("skipped")
                    else "passed"
                )
                if tally:
                    for name in ("total", "passed", "failed", "skipped"):
                        counts[name] += tally[name]
                counts["executed"] = counts["total"]
                counts["not_executed"] = max(0, counts["discovered"] - counts["executed"])
                files.extend(str(p) for p in stage_dir.glob(".coverage*"))
                if time.monotonic() >= deadline:
                    raise TimeoutError
            if not files:
                results.append(
                    TestResult(
                        key="coverage:missing",
                        group="Cobertura",
                        title="Cobertura indisponível",
                        status="failed",
                        detail="Nenhum resultado de cobertura foi produzido.",
                        duration_ms=0,
                    )
                )
            if files:
                cov_env = {**environment, "COVERAGE_FILE": str(directory / "combined.coverage")}
                code, output = await command(
                    "combine",
                    "Consolidação de cobertura",
                    [sys.executable, "-m", "coverage", "combine", "--keep", *files],
                    cov_env,
                )
                if code == 0:
                    target = directory / "coverage.json"
                    code, output = await command(
                        "coverage",
                        "Cobertura mínima de 80%",
                        [
                            sys.executable,
                            "-m",
                            "coverage",
                            "json",
                            "--fail-under=80",
                            "-o",
                            str(target),
                        ],
                        cov_env,
                    )
                    if target.is_file() and service.current_state:
                        data = json.loads(target.read_text())
                        areas: dict[str, dict[str, int]] = {}
                        for name, entry in data["files"].items():
                            parts = name.replace("\\", "/").split("/")
                            area = "/".join(parts[:2])
                            total = areas.setdefault(area, {})
                            for metric in (
                                "covered_lines",
                                "num_statements",
                                "covered_branches",
                                "num_branches",
                            ):
                                total[metric] = total.get(metric, 0) + entry["summary"].get(
                                    metric, 0
                                )
                        service.current_state.coverage = {
                            "totals": data["totals"],
                            "areas": areas,
                            "files": {
                                name: value["summary"] for name, value in data["files"].items()
                            },
                        }
                results.append(
                    TestResult(
                        key="coverage",
                        group="Cobertura",
                        title="Cobertura Python (inclui scheduler)",
                        status="passed" if code == 0 else "failed",
                        detail="Mínimo global: 80%.",
                        duration_ms=0,
                        output=service._redact(service._decode(output))[-4000:] if code else None,
                    )
                )
        except TimeoutError:
            if not any(item.key.endswith(":timeout") for item in results):
                results.append(
                    TestResult(
                        key="suite:timeout",
                        group="Execução",
                        title="Tempo esgotado",
                        status="failed",
                        detail="Prazo total excedido; etapas restantes não executadas.",
                        duration_ms=0,
                    )
                )
        finally:
            counts["executed"] = counts["total"]
            counts["not_executed"] = max(0, counts["discovered"] - counts["executed"])
    return results, counts
