"""Content identities and backwards-compatible, archived local test reports."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any
from uuid import uuid4


def code_identity(root: Path) -> dict[str, Any]:
    paths: set[Path] = set()
    for directory in ("app", "tests", "scripts", "migrations"):
        folder = root / directory
        if folder.is_dir():
            paths.update(
                p
                for p in folder.rglob("*")
                if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc"
            )
    paths.update(
        root / name
        for name in (
            "pyproject.toml",
            "alembic.ini",
            "run.py",
            "iniciar.bat",
            ".env.example",
            "Makefile",
            "data/holidays.csv",
        )
        if (root / name).is_file()
    )
    manifest = [
        {
            "path": p.relative_to(root).as_posix(),
            "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
        }
        for p in sorted(paths)
    ]
    # Content identity remains stable across checkouts. A separate observation
    # detects edits during a run even when the original content is restored.
    observations = [(p.relative_to(root).as_posix(), p.stat().st_mtime_ns) for p in sorted(paths)]
    packages: dict[str, str | None] = {}
    for name in (
        "pytest",
        "pytest-cov",
        "sqlalchemy",
        "fastapi",
        "pydantic",
        "selenium",
        "pymupdf",
        "pytesseract",
        "ruff",
        "mypy",
    ):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = None
    from app.services.test_browser import browser_readiness, executable_version

    browser = browser_readiness()
    tesseract = shutil.which("tesseract")
    if not tesseract and os.name == "nt":
        candidate = (
            Path(os.environ.get("PROGRAMFILES", "C:/Program Files")) / "Tesseract-OCR/tesseract.exe"
        )
        tesseract = str(candidate) if candidate.is_file() else None
    try:
        ocr_version = executable_version(Path(tesseract)) if tesseract else None
    except (OSError, ValueError, subprocess.SubprocessError):
        ocr_version = None
    environment = {
        "python": platform.python_version(),
        "packages": packages,
        "platform": platform.platform(),
        "browser": {
            key: browser.get(key) for key in ("available", "browser", "version", "driver_version")
        },
        "tesseract": ocr_version,
    }
    return {
        "sha256": hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest(),
        "manifest": manifest,
        "observation_sha256": hashlib.sha256(json.dumps(observations).encode()).hexdigest(),
        "environment": environment,
        "environment_sha256": hashlib.sha256(
            json.dumps(environment, sort_keys=True).encode()
        ).hexdigest(),
    }


def freshness(saved: dict[str, Any] | None, current: dict[str, Any]) -> str:
    if not saved or not saved.get("sha256"):
        return "unknown"
    return (
        "current"
        if (
            saved.get("sha256") == current["sha256"]
            and saved.get("environment_sha256") == current["environment_sha256"]
        )
        else "outdated"
    )


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def save_report(path: Path, value: dict[str, Any]) -> None:
    history = path.parent / (path.stem + "-history")

    def archive(report: dict[str, Any]) -> None:
        identifier = str(report.get("run_id", ""))
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", identifier):
            identifier = hashlib.sha256(json.dumps(report, sort_keys=True).encode()).hexdigest()
        target = history / (identifier + ".json")
        if not target.exists():
            atomic_json(target, report)

    if path.is_file():
        try:
            old = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(old, dict):
                archive(old)
                old_mode = old.get("mode")
                if old_mode in {"quick", "full"}:
                    old_latest = path.with_name(path.stem + f"-{old_mode}.json")
                    if not old_latest.exists():
                        atomic_json(old_latest, old)
        except (ValueError, OSError):
            # Keep corrupt historical bytes too, without interpreting them.
            old_bytes = path.read_bytes()
            history.mkdir(parents=True, exist_ok=True)
            target = history / ("invalid-" + hashlib.sha256(old_bytes).hexdigest() + ".json")
            if not target.exists():
                target.write_bytes(old_bytes)
    archive(value)
    mode = value.get("mode")
    if mode in {"quick", "full"}:
        atomic_json(path.with_name(path.stem + f"-{mode}.json"), value)
    atomic_json(path, value)


def save_historical_report(path: Path, value: dict[str, Any]) -> None:
    """A fenced-out worker can archive its own results, never replace latest."""
    identifier = hashlib.sha256(str(value.get("run_id", "")).encode()).hexdigest()
    atomic_json(path.parent / (path.stem + "-history") / (identifier + ".json"), value)
