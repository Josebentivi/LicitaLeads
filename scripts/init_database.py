"""Apply all database migrations without starting the web application."""

from __future__ import annotations

import subprocess

from app.config import get_settings


def main() -> int:
    """Create runtime directories and apply the Alembic head revision."""
    get_settings().ensure_directories()
    return subprocess.run(["alembic", "upgrade", "head"], check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
