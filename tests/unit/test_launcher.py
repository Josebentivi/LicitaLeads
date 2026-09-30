"""Unit tests for the one-click launcher helpers."""

from __future__ import annotations

import importlib.util
import json
import os
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
    assert not launcher._looks_like_cloud_sync_path(Path("C:/dev/licitaleads"))


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
