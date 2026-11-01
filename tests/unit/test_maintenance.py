"""Unit tests for the destructive maintenance helpers."""

from __future__ import annotations

from pathlib import Path

from app.config import Settings
from app.services.maintenance import (
    ClearDataResult,
    _remove_stored_files,
    _truncate_scheduler_log,
)


def test_clear_data_result_totals() -> None:
    result = ClearDataResult(counts={"procurements": 2, "leads": 1}, files_removed=3)

    assert result.total_records == 3
    assert result.files_removed == 3


def test_remove_stored_files_keeps_gitkeep_and_ignores_missing(tmp_path: Path) -> None:
    root = tmp_path / "documents"
    (root / "ab").mkdir(parents=True)
    (root / "ab" / "ata.pdf").write_bytes(b"x")
    (root / ".gitkeep").write_text("", encoding="utf-8")

    removed = _remove_stored_files([root, tmp_path / "missing"], settings=Settings(app_env="test"))

    assert removed == 1
    assert (root / ".gitkeep").exists()
    assert not (root / "ab").exists()


def test_truncate_scheduler_log_ignores_missing_and_empties_file(tmp_path: Path) -> None:
    _truncate_scheduler_log(tmp_path / "missing.log")
    log = tmp_path / "scheduler.log"
    log.write_text("linha\n", encoding="utf-8")

    _truncate_scheduler_log(log)

    assert log.read_text(encoding="utf-8") == ""


def test_remove_stored_files_refuses_repository_root(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "app").mkdir()
    (tmp_path / "pyproject.toml").write_text("", encoding="utf-8")
    sentinel = tmp_path / "sentinel.txt"
    sentinel.write_text("keep", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    removed = _remove_stored_files([tmp_path], settings=Settings(app_env="test"))

    assert removed == 0
    assert sentinel.exists()
