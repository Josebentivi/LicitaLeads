"""Isolate every pytest entry point before application modules are imported.

This also protects plain ``pytest``: constructor-only Settings overrides do
not replace app.database's import-time engine and session factory.
"""

from __future__ import annotations

import ast
import asyncio
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_ORIGINAL_ENVIRONMENT = os.environ.copy()
_TEST_DIRECTORY = tempfile.TemporaryDirectory(
    prefix="licitaleads-pytest-", ignore_cleanup_errors=True
)
_TEST_ROOT = Path(_TEST_DIRECTORY.name)
_ISOLATED_ENVIRONMENT = {
    "APP_ENV": "test",
    "DATABASE_URL": f"sqlite:///{(_TEST_ROOT / 'global.sqlite3').as_posix()}",
    "DOCUMENT_STORAGE_PATH": str(_TEST_ROOT / "documents"),
    "DOCUMENT_DOWNLOAD_CONCURRENCY": "4",
    "DOCUMENT_PROCESS_CONCURRENCY": "2",
    "RAW_DATA_STORAGE_PATH": str(_TEST_ROOT / "raw"),
    "HOLIDAY_CALENDAR_PATH": str(_TEST_ROOT / "holidays.csv"),
    "LLM_ENABLED": "false",
    "LLM_PROVIDER": "none",
    "LLM_API_KEY": "",
    "LLM_MODEL": "",
    "CONTACT_SEARCH_ENABLED": "false",
    "CONTACT_SEARCH_PROVIDER": "none",
    "CONTACT_SEARCH_API_KEY": "",
    "CONTACT_DOMAIN_DISCOVERY_ENABLED": "false",
    "TRANSPARENCIA_API_KEY": "",
    "OCR_ENABLED": "false",
    "OCR_PROVIDER": "none",
    "ALLOWED_HOSTS": "testserver,localhost,127.0.0.1",
    "PYTHONDONTWRITEBYTECODE": "1",
    "RUN_LIVE_CONTRACT_TESTS": "false",
}
os.environ.update(_ISOLATED_ENVIRONMENT)
sys.dont_write_bytecode = True
(_TEST_ROOT / "documents").mkdir()
(_TEST_ROOT / "raw").mkdir()
if (_REPO_ROOT / "data" / "holidays.csv").is_file():
    shutil.copy2(_REPO_ROOT / "data" / "holidays.csv", _TEST_ROOT / "holidays.csv")


def _explicit_live_marker(expression: str) -> bool:
    """Require a positive live marker in the explicit -m expression."""
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError:
        return False

    def positive(node: ast.AST, negated: bool = False) -> bool:
        if isinstance(node, ast.Name):
            return node.id == "live" and not negated
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return positive(node.operand, not negated)
        return any(positive(child, negated) for child in ast.iter_child_nodes(node))

    return positive(tree)


def pytest_configure(config: pytest.Config) -> None:
    live = _ORIGINAL_ENVIRONMENT.get(
        "RUN_LIVE_CONTRACT_TESTS", ""
    ).lower() == "true" and _explicit_live_marker(config.getoption("markexpr"))
    os.environ["RUN_LIVE_CONTRACT_TESTS"] = "true" if live else "false"
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("COV_CORE_") and key != "COVERAGE_PROCESS_START"
    }
    result = subprocess.run(  # noqa: S603 - fixed migration of the isolated global database
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=_REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    if result.returncode:
        raise pytest.UsageError("Falha ao preparar o banco temporário do pytest: " + result.stderr)


def pytest_unconfigure(config: pytest.Config) -> None:
    # Modules may hold pooled SQLite handles even when their per-test fixtures
    # used another database. Drain the global pool before removing its tree.
    database = sys.modules.get("app.database")
    if database is not None:
        asyncio.run(database.dispose_database())
    _TEST_DIRECTORY.cleanup()
    for key in _ISOLATED_ENVIRONMENT:
        if key in _ORIGINAL_ENVIRONMENT:
            os.environ[key] = _ORIGINAL_ENVIRONMENT[key]
        else:
            os.environ.pop(key, None)
