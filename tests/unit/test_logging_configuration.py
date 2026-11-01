"""Application diagnostics stay useful without dependency payload logging."""

import io
import json
import logging

import pytest
from pydantic import ValidationError

from app.config import Settings
from app.logging import configure_logging


@pytest.mark.parametrize("level", ["DEBUG", "info", " WARNING ", "ERROR", "CRITICAL"])
def test_log_level_is_explicit_and_validated(level):
    settings = Settings(_env_file=None, log_level=level)
    assert settings.log_level == level.strip().upper()
    assert settings.public_view()["log_level"] == settings.log_level
    assert Settings(_env_file=None, app_env="development").log_level == "INFO"
    with pytest.raises(ValidationError):
        Settings(_env_file=None, log_level="SQL")


def test_verbose_dependency_payloads_are_suppressed_even_with_app_debug():
    root = logging.getLogger()
    handlers, old_level = list(root.handlers), root.level
    names = ("sqlalchemy", "sqlalchemy.engine", "aiosqlite", "httpx", "httpcore")
    levels = {name: logging.getLogger(name).level for name in names}
    stream = io.StringIO()
    try:
        configure_logging("DEBUG")
        root.handlers[0].setStream(stream)
        for name in names:
            logging.getLogger(name).debug("SELECT secret document contents")
            logging.getLogger(name).info("Bearer api-private-secret")
        from sqlalchemy import create_engine, text

        engine = create_engine("sqlite://")
        try:
            with engine.connect() as connection:
                assert (
                    connection.scalar(
                        text("SELECT :document"), {"document": "Bearer api-private-secret"}
                    )
                    == "Bearer api-private-secret"
                )
        finally:
            engine.dispose()
        logging.getLogger("app.connectors.http").info(
            "connector_http_timing", extra={"context": {"network_seconds": 0.2, "api_key": "key"}}
        )
        logging.getLogger("app").debug("safe_application_diagnostic")
        data = stream.getvalue()
        assert "secret" not in data and "document contents" not in data
        rows = [json.loads(line) for line in data.splitlines()]
        assert rows[0]["message"] == "connector_http_timing"
        assert rows[0]["api_key"] == "***" and rows[0]["network_seconds"] == 0.2
        assert rows[1]["message"] == "safe_application_diagnostic"
    finally:
        for handler in root.handlers:
            handler.close()
        root.handlers, root.level = handlers, old_level
        for name, level in levels.items():
            logging.getLogger(name).setLevel(level)
