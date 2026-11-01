"""Verified database backups required before legacy repairs."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from sqlalchemy.engine import URL, make_url

from app.config import Settings


def _passwordless_postgres_url(url: URL) -> URL:
    # URL.set(password=None) retains the old password; construct it explicitly.
    return URL.create(
        "postgresql",
        username=url.username,
        host=url.host,
        port=url.port,
        database=url.database,
        query={key: value for key, value in url.query.items() if key != "password"},
    )


def database_identity(url: str | URL) -> str:
    selected = make_url(url) if isinstance(url, str) else url
    if selected.get_backend_name() == "sqlite":
        value = "sqlite:" + str(Path(selected.database or "").resolve())
    else:
        value = _passwordless_postgres_url(selected).render_as_string()
    return hashlib.sha256(value.encode()).hexdigest()


def file_hash(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


@dataclass(frozen=True, slots=True)
class BackupReceipt:
    path: Path
    backend: str
    database_id: str
    revision: str
    sha256: str
    maintenance_owner: str | None = None

    def verify(self, database_url: str | URL, maintenance_owner: str | None = None) -> None:
        if (
            self.database_id != database_identity(database_url)
            or not self.path.is_file()
            or file_hash(self.path) != self.sha256
        ):
            raise ValueError("backup ausente, alterado ou pertencente a outro banco")
        if maintenance_owner is not None and self.maintenance_owner != maintenance_owner:
            raise ValueError("backup não pertence à janela de manutenção atual")
        url = make_url(database_url) if isinstance(database_url, str) else database_url
        if url.get_backend_name() == "sqlite":
            database = Path(url.database or "").resolve()
            with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as source:
                row = source.execute("SELECT version_num FROM alembic_version").fetchone()
        else:
            import psycopg

            with psycopg.connect(
                url.set(drivername="postgresql").render_as_string(hide_password=False)
            ) as connection:
                row = connection.execute("SELECT version_num FROM alembic_version").fetchone()
        if row is None or str(row[0]) != self.revision:
            raise ValueError("backup de outra revisão; crie um novo backup")

    def as_dict(self) -> dict[str, str | None]:
        return {
            "path": str(self.path),
            "backend": self.backend,
            "database_id": self.database_id,
            "revision": self.revision,
            "sha256": self.sha256,
            "maintenance_owner": self.maintenance_owner,
        }


def create_backup(
    settings: Settings,
    directory: Path | None = None,
    *,
    maintenance_owner: str | None = None,
    sqlite_timeout_seconds: float = 60,
) -> BackupReceipt:
    """Create a consistent snapshot; never copy a live SQLite file directly."""
    url = make_url(settings.database_url)
    backend = url.get_backend_name()
    folder = directory or (
        Path(url.database).resolve().parent / "backups"
        if backend == "sqlite" and url.database and url.database != ":memory:"
        else settings.raw_data_storage_path.parent / "backups"
    )
    folder.mkdir(parents=True, exist_ok=True)
    name = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex
    destination = folder / (name + (".sqlite3" if backend == "sqlite" else ".dump"))
    revision = ""
    if backend == "sqlite":
        if not url.database or url.database == ":memory:":
            raise ValueError("backup requer SQLite em arquivo")
        source_path = Path(url.database).resolve()
        started = time.monotonic()

        def progress(_status: int, _remaining: int, _total: int) -> None:
            if time.monotonic() - started > sqlite_timeout_seconds:
                raise TimeoutError(f"backup SQLite excedeu {sqlite_timeout_seconds:g} segundos")

        with closing(sqlite3.connect(source_path.as_uri() + "?mode=ro", uri=True)) as source:
            # Keep a stable WAL read snapshot across backup_step calls. Lease
            # heartbeats must not restart a multi-gigabyte backup indefinitely.
            source.execute("BEGIN")
            revision = str(source.execute("SELECT version_num FROM alembic_version").fetchone()[0])
            with closing(sqlite3.connect(destination)) as target:
                source.backup(target, pages=1000, progress=progress)
                if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise RuntimeError("backup SQLite não passou na verificação de integridade")
    elif backend == "postgresql":
        command = shutil.which("pg_dump")
        if command is None:
            raise RuntimeError("pg_dump não encontrado; nenhum reparo foi aplicado")
        import psycopg

        with psycopg.connect(
            url.set(drivername="postgresql").render_as_string(hide_password=False)
        ) as pg_source:
            version_row = pg_source.execute("SELECT version_num FROM alembic_version").fetchone()
            if version_row is None:
                raise RuntimeError("banco PostgreSQL sem revisão Alembic")
            revision = str(version_row[0])
        environment = os.environ.copy()
        # libpq accepts a connection URI in PGDATABASE. Retain SSL/service and
        # other URL options while keeping the password out of argv/manifest.
        dump_url = _passwordless_postgres_url(url)
        query_password = url.query.get("password")
        environment.update(
            {
                "PGDATABASE": dump_url.render_as_string(hide_password=False),
                "PGHOST": url.host or "",
                "PGPORT": str(url.port or 5432),
                "PGUSER": url.username or "",
                "PGPASSWORD": url.password
                or (query_password if isinstance(query_password, str) else "")
                or environment.get("PGPASSWORD", ""),
            }
        )
        result = subprocess.run(  # noqa: S603 - resolved pg_dump, no shell or credentials in argv
            [command, "--format=custom", "--no-password", "--file", str(destination)],
            env=environment,
            capture_output=True,
            timeout=120,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if result.returncode or not destination.is_file():
            raise RuntimeError("pg_dump falhou; nenhum reparo foi aplicado")
        with destination.open("rb") as source_file:
            if source_file.read(5) != b"PGDMP":
                raise RuntimeError("backup PostgreSQL inválido")
    else:
        raise ValueError("backend sem suporte a backup verificado")
    receipt = BackupReceipt(
        destination.resolve(),
        backend,
        database_identity(url),
        revision,
        file_hash(destination),
        maintenance_owner,
    )
    destination.with_suffix(destination.suffix + ".json").write_text(
        json.dumps(receipt.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return receipt
