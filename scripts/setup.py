"""Idempotent local setup for the LicitaLead Monitor MVP."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


def main() -> int:
    """Prepare configuration, directories and the database."""
    if sys.version_info[:2] != (3, 12):
        print(f"Python 3.12 is required; found {sys.version.split()[0]}", file=sys.stderr)
        return 1

    for directory in (Path("data/documents"), Path("data/raw")):
        directory.mkdir(parents=True, exist_ok=True)
    env_path = Path(".env")
    if not env_path.exists():
        shutil.copyfile(".env.example", env_path)
        print("Created .env from .env.example")
    else:
        print("Kept existing .env")

    try:
        import alembic  # noqa: F401, PLC0415
    except ImportError:
        print("Dependencies are missing. Run: python -m pip install -e .", file=sys.stderr)
        return 1

    result = subprocess.run(["alembic", "upgrade", "head"], check=False)
    if result.returncode:
        return result.returncode
    print("Setup complete.")
    print("Start the API with: python run.py")
    print("Run a pipeline with: python -m app.cli run-pipeline --uf MA --days 7")
    print("Start scheduled jobs separately with: python -m app.jobs.scheduler")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
