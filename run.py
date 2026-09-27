"""Convenient local launcher for LicitaLead Monitor."""

from __future__ import annotations

import argparse
import asyncio
import shutil
import subprocess
import sys
from pathlib import Path

import uvicorn
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text

from app.config import get_settings


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start LicitaLead Monitor")
    parser.add_argument("--reload", action="store_true", help="reload on source changes")
    parser.add_argument("--migrate", action="store_true", help="apply Alembic migrations first")
    return parser.parse_args()


def _run_migrations() -> None:
    alembic = shutil.which("alembic")
    if not alembic:
        raise RuntimeError("Alembic is not installed. Run: pip install -e .")
    result = subprocess.run([alembic, "upgrade", "head"], check=False)
    if result.returncode:
        raise RuntimeError("Database migration failed")


async def _check_database_schema() -> None:
    """Require the database to be reachable and at the current Alembic head."""
    from app.database import check_database, dispose_database, engine  # noqa: PLC0415

    try:
        await check_database()
        config = Config(str(Path(__file__).with_name("alembic.ini")))
        expected_revision = ScriptDirectory.from_config(config).get_current_head()
        async with engine.connect() as connection:
            current_revision = await connection.scalar(
                text("SELECT version_num FROM alembic_version")
            )
        if current_revision != expected_revision:
            raise RuntimeError(
                f"database revision is {current_revision!r}; expected {expected_revision!r}"
            )
    finally:
        await dispose_database()


def main() -> None:
    """Validate the local environment and start Uvicorn."""
    args = _parse_args()
    if sys.version_info[:2] != (3, 12):
        print(
            f"Warning: Python 3.12 is the supported version; running {sys.version.split()[0]}",
            file=sys.stderr,
        )
    env_path = Path(".env")
    if not env_path.exists():
        print("Warning: .env not found; using safe development defaults", file=sys.stderr)
    settings = get_settings()
    settings.ensure_directories()
    if args.migrate:
        _run_migrations()

    try:
        asyncio.run(_check_database_schema())
    except Exception as exc:
        raise SystemExit(
            "Database is not ready. Run `alembic upgrade head` or `python run.py --migrate`. "
            f"Details: {exc}"
        ) from exc

    uvicorn.run(
        "app.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
