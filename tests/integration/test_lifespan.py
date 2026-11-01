"""Startup and shutdown use isolated resources and drain every owner."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.config import Settings
from app.main import app, lifespan


@pytest.mark.parametrize("cleanup_error", [False, True])
async def test_lifespan_cleans_all_workers_even_if_one_cleanup_fails(
    tmp_path, monkeypatch, cleanup_error
):
    settings = Settings(
        _env_file=None,
        app_env="test",
        document_storage_path=tmp_path / "docs",
        raw_data_storage_path=tmp_path / "raw",
        database_url=f"sqlite:///{(tmp_path / 'isolated.db').as_posix()}",
    )
    startup = AsyncMock(return_value=0)
    tests = AsyncMock(side_effect=RuntimeError("cleanup") if cleanup_error else None)
    crawls, municipal, dispose = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr("app.main.get_settings", lambda: settings)
    monkeypatch.setattr("app.main.reconcile_stale_crawls", startup)
    monkeypatch.setattr("app.web._application_tests", SimpleNamespace(aclose=tests))
    monkeypatch.setattr("app.api.routes.crawls.cancel_all_running_crawls", crawls)
    monkeypatch.setattr(
        "app.services.ingestion.municipal_payments.cancel_all_municipal_tasks", municipal
    )
    monkeypatch.setattr("app.database.dispose_database", dispose)

    async def exercise():
        async with lifespan(app):
            startup.assert_awaited_once()
            assert settings.raw_data_storage_path.is_dir()

    if cleanup_error:
        with pytest.raises(RuntimeError, match="cleanup"):
            await exercise()
    else:
        await exercise()
    for cleanup in (tests, crawls, municipal, dispose):
        cleanup.assert_awaited_once()
