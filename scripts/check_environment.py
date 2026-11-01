"""Check whether the local runtime can execute LicitaLead Monitor."""

from __future__ import annotations

import importlib.util
import os
import platform
import shutil
import sys
from pathlib import Path

REQUIRED_MODULES = ("fastapi", "sqlalchemy", "alembic", "httpx", "pydantic")


def main() -> int:
    """Print actionable environment diagnostics and return a process code."""
    print(f"Python: {platform.python_version()} ({sys.executable})")
    if sys.version_info[:2] != (3, 12):
        print("ERROR: Python 3.12 is required for the supported MVP environment.")
        return 1
    missing = [name for name in REQUIRED_MODULES if importlib.util.find_spec(name) is None]
    if missing:
        print(f"ERROR: missing packages: {', '.join(missing)}")
        print("Run: python -m pip install -e .")
        return 1
    if Path("Dockerfile").exists() or Path("docker-compose.yml").exists():
        print("ERROR: container files are outside this project's requirements.")
        return 1
    if os.environ.get("OCR_ENABLED", "").strip().lower() in {"1", "true", "yes", "sim"}:
        if importlib.util.find_spec("pytesseract") is None:
            print(
                "WARNING: OCR_ENABLED=true but pytesseract is not installed (pip install -e '.[ocr]')."
            )
        if shutil.which("tesseract") is None:
            print("WARNING: OCR_ENABLED=true but the tesseract executable was not found in PATH.")
    print("Environment is ready.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
