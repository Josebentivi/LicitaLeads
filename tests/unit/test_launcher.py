"""Unit tests for the one-click launcher helpers."""

from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
from pathlib import Path

LAUNCHER_PATH = Path(__file__).resolve().parents[2] / "scripts" / "launcher.py"


def _load_launcher() -> object:
    spec = importlib.util.spec_from_file_location("licita_launcher", LAUNCHER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


launcher = _load_launcher()


def test_venv_python_path_matches_platform() -> None:
    path = launcher._venv_python_path(Path("repo/.venv"))
    expected_directory = "Scripts" if os.name == "nt" else "bin"
    assert path.parent.name == expected_directory
    assert path.name.startswith("python")


def test_hash_inputs_tracks_pyproject_and_python_version(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("[project]\nname = 'a'\n", encoding="utf-8")
    first = launcher._hash_inputs(pyproject, (3, 12, 0))
    assert first == launcher._hash_inputs(pyproject, (3, 12, 0))
    assert first != launcher._hash_inputs(pyproject, (3, 12, 1))
    pyproject.write_text("[project]\nname = 'b'\n", encoding="utf-8")
    assert first != launcher._hash_inputs(pyproject, (3, 12, 0))


def test_needs_install_compares_hash_and_dev_extras(tmp_path: Path) -> None:
    state = tmp_path / ".bootstrap-state.json"
    assert launcher._needs_install(state, "abc")
    state.write_text(json.dumps({"inputs_sha256": "abc"}), encoding="utf-8")
    assert not launcher._needs_install(state, "abc")
    assert launcher._needs_install(state, "other")
    assert launcher._needs_install(state, "abc", include_dev=True)
    state.write_text(
        json.dumps({"inputs_sha256": "abc", "include_dev": True}),
        encoding="utf-8",
    )
    assert not launcher._needs_install(state, "abc", include_dev=True)
    state.write_text("not-json", encoding="utf-8")
    assert launcher._needs_install(state, "abc")


def test_confirm_accepts_portuguese_answers() -> None:
    assert launcher._confirm("?", input_fn=lambda _: "")
    assert launcher._confirm("?", input_fn=lambda _: "s")
    assert launcher._confirm("?", input_fn=lambda _: "SIM")
    assert launcher._confirm("?", input_fn=lambda _: "yes")
    assert not launcher._confirm("?", input_fn=lambda _: "n")
    assert not launcher._confirm("?", input_fn=lambda _: "nao")


def test_browser_host_maps_wildcards_to_localhost() -> None:
    assert launcher._browser_host("") == "127.0.0.1"
    assert launcher._browser_host("0.0.0.0") == "127.0.0.1"  # noqa: S104
    assert launcher._browser_host("::") == "127.0.0.1"
    assert launcher._browser_host("localhost") == "localhost"


def test_sqlite_database_path_resolution(tmp_path: Path) -> None:
    assert launcher._sqlite_database_path("sqlite:///./data/app.db", tmp_path) == (
        tmp_path / "data" / "app.db"
    )
    assert (
        launcher._sqlite_database_path("sqlite+aiosqlite:///./data/app.db", tmp_path)
        == tmp_path / "data" / "app.db"
    )
    assert launcher._sqlite_database_path("postgresql+psycopg://u:p@host/db", tmp_path) is None
    assert launcher._sqlite_database_path("sqlite://", tmp_path) is None
    assert launcher._sqlite_database_path("sqlite:///:memory:", tmp_path) is None


def test_synced_location_detection() -> None:
    assert launcher._looks_like_cloud_sync_path(Path("G:/Meu Drive/projetos/LicitaLeads"))
    assert launcher._looks_like_cloud_sync_path(Path("C:/Users/x/My Drive/LicitaLeads"))
    assert launcher._looks_like_cloud_sync_path(Path("D:/Google Drive/LicitaLeads"))
    assert launcher._looks_like_cloud_sync_path(Path("C:/Users/x/OneDrive/LicitaLeads"))
    assert launcher._looks_like_cloud_sync_path(Path("C:/Users/x/OneDrive - Contoso/LicitaLeads"))
    assert launcher._looks_like_cloud_sync_path(Path("C:/Users/x/onedrive/licitaleads"))
    assert not launcher._looks_like_cloud_sync_path(Path("C:/dev/licitaleads"))
    assert not launcher._looks_like_cloud_sync_path(Path("C:/dev/onedrive-tools"))


def test_is_synced_location_detects_onedrive_environment(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "sync" / "LicitaLeads"
    root.mkdir(parents=True)
    monkeypatch.setenv("OneDrive", str(tmp_path / "sync"))
    monkeypatch.delenv("OneDriveConsumer", raising=False)
    monkeypatch.delenv("OneDriveCommercial", raising=False)
    assert launcher._is_synced_location(root)


def test_relative_sqlite_filename_detection() -> None:
    assert launcher._relative_sqlite_filename("sqlite:///./data/app.db") == "app.db"
    assert launcher._relative_sqlite_filename("sqlite+aiosqlite:///./data/app.db") == "app.db"
    assert launcher._relative_sqlite_filename("postgresql+psycopg://u:p@host/db") is None
    assert launcher._relative_sqlite_filename("sqlite:///:memory:") is None
    assert launcher._relative_sqlite_filename("sqlite://") is None
    absolute = Path("C:/app/app.db") if os.name == "nt" else Path("/opt/app/app.db")
    assert launcher._relative_sqlite_filename(f"sqlite:///{absolute.as_posix()}") is None


def test_database_override_redirects_only_relative_sqlite(monkeypatch) -> None:
    monkeypatch.setattr(launcher, "_is_synced_location", lambda root: True)
    override, notice = launcher._database_override(
        {"database_url": "sqlite:///./data/app.db"},
        migrate=False,
    )
    assert override["DATABASE_URL"].endswith("/app.db")
    assert notice and "disco local" in notice
    assert launcher._database_override(
        {"database_url": "postgresql+psycopg://u:p@host/db"},
        migrate=False,
    ) == ({}, None)
    absolute = Path("C:/app/app.db") if os.name == "nt" else Path("/opt/app/app.db")
    assert launcher._database_override(
        {"database_url": f"sqlite:///{absolute.as_posix()}"},
        migrate=False,
    ) == ({}, None)


def _make_database(path: Path, *, rows: int = 1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE sample (value TEXT)")
        connection.executemany(
            "INSERT INTO sample (value) VALUES (?)",
            [(f"dados-{index}",) for index in range(rows)],
        )
        connection.commit()
    finally:
        connection.close()


def _corrupt_database(path: Path) -> None:
    with path.open("r+b") as handle:
        handle.write(b"\x00" * 100)


def test_migrate_database_copies_without_overwriting(tmp_path: Path) -> None:
    source = tmp_path / "repo.db"
    _make_database(source)
    with sqlite3.connect(source) as connection:
        connection.execute("UPDATE sample SET value = 'dados'")
    destination = tmp_path / "local" / "local.db"
    assert launcher._migrate_database(source, destination)
    with sqlite3.connect(destination) as connection:
        assert connection.execute("SELECT value FROM sample").fetchone() == ("dados",)
    assert not launcher._migrate_database(source, destination)
    assert not launcher._migrate_database(tmp_path / "missing.db", tmp_path / "other.db")


def test_sqlite_integrity_check_detects_corruption(tmp_path: Path) -> None:
    healthy = tmp_path / "healthy.db"
    _make_database(healthy)
    assert launcher._sqlite_integrity_ok(healthy)
    corrupt = tmp_path / "corrupt.db"
    _make_database(corrupt)
    _corrupt_database(corrupt)
    assert not launcher._sqlite_integrity_ok(corrupt)
    assert not launcher._sqlite_integrity_ok(tmp_path / "missing.db")


def test_recover_sqlite_rebuilds_readable_data(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    _make_database(source, rows=3)
    destination = tmp_path / "local" / "recovered.db"
    assert launcher._recover_sqlite(source, destination)
    with sqlite3.connect(destination) as connection:
        assert connection.execute("SELECT COUNT(*) FROM sample").fetchone() == (3,)
    assert not launcher._recover_sqlite(tmp_path / "missing.db", tmp_path / "never.db")
    assert not (tmp_path / "never.db").exists()


def test_repair_local_database_quarantines_unrecoverable(tmp_path: Path) -> None:
    database = tmp_path / "data" / "app.db"
    _make_database(database)
    assert launcher._repair_local_database(database) is None
    _corrupt_database(database)
    notice = launcher._repair_local_database(database)
    assert notice is not None and "nao pode ser recuperado" in notice
    assert not database.exists()
    assert len(list(database.parent.glob("app.corrupt-*.db"))) == 1


def test_database_override_quarantines_corrupt_legacy(monkeypatch, tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    local = tmp_path / "local"
    legacy = repo / "data" / "app.db"
    _make_database(legacy)
    _corrupt_database(legacy)
    monkeypatch.setattr(launcher, "REPO_ROOT", repo)
    monkeypatch.setattr(launcher, "_is_synced_location", lambda root: True)
    monkeypatch.setattr(launcher, "_local_data_dir", lambda: local)
    override, notice = launcher._database_override(
        {"database_url": "sqlite:///./data/app.db"},
        migrate=True,
    )
    assert override["DATABASE_URL"] == f"sqlite:///{(local / 'app.db').as_posix()}"
    assert notice is not None and "corrompido" in notice
    assert not legacy.exists()
    assert len(list(legacy.parent.glob("app.corrupt-*.db"))) == 1
    assert not (local / "app.db").exists()


def test_parse_args_defaults_and_flags() -> None:
    defaults = launcher._parse_args([])
    assert not defaults.check
    assert not defaults.no_browser
    assert not defaults.no_scheduler
    assert defaults.venv is None
    flags = launcher._parse_args(["--check", "--no-scheduler", "--dev", "--venv", "C:/tmp/venv"])
    assert flags.check
    assert flags.no_scheduler
    assert flags.dev
    assert flags.venv == Path("C:/tmp/venv")
