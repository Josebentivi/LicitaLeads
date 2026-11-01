"""Consistent WAL backups and fail-closed repair prerequisites."""

import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.config import Settings
from app.services.backups import create_backup


def test_sqlite_backup_includes_committed_wal_and_verifies_identity(tmp_path):
    database = tmp_path / "live.sqlite3"
    settings = Settings(_env_file=None, database_url=f"sqlite:///{database.as_posix()}")
    with closing(sqlite3.connect(database)) as source:
        source.execute("PRAGMA journal_mode=WAL")
        source.execute("CREATE TABLE alembic_version (version_num TEXT)")
        source.execute("INSERT INTO alembic_version VALUES ('0020_source_completeness')")
        source.execute("CREATE TABLE sentinel (value TEXT)")
        source.execute("INSERT INTO sentinel VALUES ('preserved')")
        source.commit()
        receipt = create_backup(settings, tmp_path / "backups")
        receipt.verify(settings.database_url)
        with closing(sqlite3.connect(receipt.path)) as backup:
            assert backup.execute("SELECT value FROM sentinel").fetchone()[0] == "preserved"
        with pytest.raises(ValueError, match="outro banco"):
            receipt.verify("sqlite:///different.sqlite3")
        receipt.path.write_bytes(b"damaged")
        with pytest.raises(ValueError, match="alterado"):
            receipt.verify(settings.database_url)


def test_backup_is_closed_and_bound_to_revision_and_maintenance_window(tmp_path):
    database = tmp_path / "live.sqlite3"
    settings = Settings(_env_file=None, database_url=f"sqlite:///{database.as_posix()}")
    with closing(sqlite3.connect(database)) as source:
        source.execute("CREATE TABLE alembic_version (version_num TEXT)")
        source.execute("INSERT INTO alembic_version VALUES ('revision1')")
        source.commit()
    receipt = create_backup(settings, tmp_path / "backups", maintenance_owner="first-window")
    receipt.verify(settings.database_url, "first-window")
    with pytest.raises(ValueError, match="janela"):
        receipt.verify(settings.database_url, "second-window")
    with closing(sqlite3.connect(database)) as source:
        source.execute("UPDATE alembic_version SET version_num='revision2'")
        source.commit()
    with pytest.raises(ValueError, match="revisão"):
        receipt.verify(settings.database_url)
    # Windows refuses these renames if either backup connection was leaked.
    database.rename(tmp_path / "moved.sqlite3")
    receipt.path.rename(tmp_path / "moved-backup.sqlite3")


def test_postgres_apply_cannot_bypass_missing_dump_tool(tmp_path, monkeypatch):
    monkeypatch.setattr("app.services.backups.shutil.which", lambda name: None)
    settings = Settings(
        _env_file=None, database_url="postgresql://operator:secret@localhost/procurement"
    )
    with pytest.raises(RuntimeError, match="pg_dump não encontrado"):
        create_backup(settings, tmp_path)


@pytest.mark.parametrize("failure", [None, "tool", "invalid", "revision", "timeout"])
def test_postgres_backup_checks_dump_and_retains_ssl_without_credentials_in_argv(
    tmp_path, monkeypatch, failure
):
    import psycopg

    settings = Settings(
        _env_file=None,
        database_url="postgresql://operator:secret@official.test/procurement?sslmode=require",
    )
    monkeypatch.setattr("app.services.backups.shutil.which", lambda _: "pg_dump")
    calls = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, _statement):
            return SimpleNamespace(
                fetchone=lambda: None if failure == "revision" else ("revision",)
            )

    monkeypatch.setattr(psycopg, "connect", lambda _url: Connection())

    def dump(command, **kwargs):
        calls.append((command, kwargs))
        assert "secret" not in " ".join(command)
        assert "secret" not in kwargs["env"]["PGDATABASE"]
        assert "sslmode=require" in kwargs["env"]["PGDATABASE"]
        assert kwargs["env"]["PGPASSWORD"] == "secret"
        destination = Path(command[-1])
        if failure == "timeout":
            raise TimeoutError("dump timeout")
        destination.write_bytes(b"invalid" if failure == "invalid" else b"PGDMPsnapshot")
        return SimpleNamespace(returncode=1 if failure == "tool" else 0)

    monkeypatch.setattr("app.services.backups.subprocess.run", dump)
    if failure:
        with pytest.raises((RuntimeError, TimeoutError)):
            create_backup(settings, tmp_path, maintenance_owner="window")
        assert not list(tmp_path.glob("*.json"))
    else:
        receipt = create_backup(settings, tmp_path, maintenance_owner="window")
        receipt.verify(settings.database_url, "window")
        assert receipt.backend == "postgresql" and receipt.revision == "revision"
        assert "secret" not in receipt.path.with_suffix(".dump.json").read_text()
        assert calls


def test_backup_rejects_in_memory_and_unsupported_backends(tmp_path):
    with pytest.raises(ValueError, match="em arquivo"):
        create_backup(Settings(_env_file=None, database_url="sqlite:///:memory:"), tmp_path)
    with pytest.raises(ValueError, match="sem suporte"):
        create_backup(Settings(_env_file=None, database_url="mysql://localhost/example"), tmp_path)


def test_wal_heartbeat_does_not_restart_a_stepped_backup(tmp_path, monkeypatch):
    import threading
    import time

    from app.services import backups

    database = tmp_path / "heartbeat.sqlite3"
    connect = sqlite3.connect
    with closing(connect(database)) as source:
        source.execute("PRAGMA journal_mode=WAL")
        source.execute("CREATE TABLE alembic_version (version_num TEXT)")
        source.execute("INSERT INTO alembic_version VALUES ('test')")
        source.execute("CREATE TABLE heartbeat (counter INTEGER)")
        source.execute("INSERT INTO heartbeat VALUES (0)")
        source.execute("CREATE TABLE payload (body BLOB)")
        source.execute("INSERT INTO payload VALUES (randomblob(1048576))")
        source.commit()
    stop = threading.Event()
    failures = []

    def heartbeat():
        try:
            with closing(connect(database)) as writer:
                while not stop.wait(0.002):
                    writer.execute("UPDATE heartbeat SET counter=counter+1")
                    writer.commit()
        except Exception as exc:
            failures.append(exc)

    class SteppedConnection(sqlite3.Connection):
        def backup(self, target, *, pages=-1, progress=None, **kwargs):
            started = time.monotonic()

            def tick(status, remaining, total):
                if time.monotonic() - started > 3:
                    raise TimeoutError("a renovação da lease reiniciou a cópia")
                if progress:
                    progress(status, remaining, total)
                time.sleep(0.001)

            return super().backup(target, pages=10, progress=tick, **kwargs)

    def selected(*args, **kwargs):
        if "mode=ro" in str(args[0]):
            kwargs["factory"] = SteppedConnection
        return connect(*args, **kwargs)

    monkeypatch.setattr(backups.sqlite3, "connect", selected)
    worker = threading.Thread(target=heartbeat)
    worker.start()
    try:
        settings = Settings(_env_file=None, database_url=f"sqlite:///{database.as_posix()}")
        receipt = create_backup(settings, tmp_path / "backups")
        receipt.verify(settings.database_url)
        with closing(connect(receipt.path)) as copied:
            assert copied.execute("SELECT length(body) FROM payload").fetchone()[0] == 1048576
    finally:
        stop.set()
        worker.join(timeout=2)
    assert not worker.is_alive() and not failures
