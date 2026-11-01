"""The Alembic chain must build the schema expected by the application."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path


def test_alembic_upgrade_head_creates_integrity_columns(tmp_path: Path) -> None:
    database = tmp_path / "migration.sqlite3"
    environment = os.environ.copy()
    environment["DATABASE_URL"] = f"sqlite:///{database.as_posix()}"
    environment["APP_ENV"] = "production"

    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=Path.cwd(),
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    with closing(sqlite3.connect(database)) as connection:
        document_columns = {row[1] for row in connection.execute("PRAGMA table_info(documents)")}
        crawl_columns = {row[1] for row in connection.execute("PRAGMA table_info(crawl_runs)")}
        payment_columns = {row[1] for row in connection.execute("PRAGMA table_info(payments)")}
        revision = connection.execute("SELECT version_num FROM alembic_version").fetchone()[0]
        participant_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='participants'"
        ).fetchone()[0]
        monetary_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(participant_values)")
        }

    assert {"is_active", "deactivated_at", "resolved_url"} <= document_columns
    assert "'cancelled'" in participant_sql
    assert "heartbeat_at" in crawl_columns
    assert {"commitment_external_id", "is_current"} <= payment_columns
    assert {"amount", "scope", "lot_number", "evidence_id", "document_hash"} <= monetary_columns
    assert revision == "0026_participation_integrity"


def test_upgrade_legacy_schema_keeps_raw_sentinel_and_unknown_completeness(tmp_path):
    database = tmp_path / "legacy.sqlite3"
    environment = os.environ.copy()
    environment.update({"DATABASE_URL": f"sqlite:///{database.as_posix()}", "APP_ENV": "test"})

    def migrate(*arguments):
        result = subprocess.run(  # noqa: S603 - fixed Alembic test commands, no shell
            [sys.executable, "-m", "alembic", *arguments],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        assert result.returncode == 0, result.stderr

    migrate("upgrade", "head")
    migrate("downgrade", "0019_participant_cancelled")
    with closing(sqlite3.connect(database)) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(source_records)")}
        assert "collection_complete" not in columns
        connection.execute(
            """INSERT INTO source_records
            (id, source, endpoint, request_parameters, response_hash, raw_payload,
             availability, collected_at, fingerprint)
            VALUES (?, 'pncp', 'https://official.test', '{}', ?, ?, 'available', ?, ?)""",
            ("0" * 32, "a" * 64, '{"sentinel": true}', "2026-01-01 00:00:00", "b" * 64),
        )
        connection.execute(
            """INSERT INTO payments
            (id, source, external_id, fingerprint, created_at, updated_at,
             match_status, requires_manual_review, commitment_number)
            VALUES (?, 'municipal:test', 'legacy', ?, ?, ?, 'unlinked', 1, '123')""",
            ("1" * 32, "c" * 64, "2026-01-01 00:00:00", "2026-01-01 00:00:00"),
        )
        connection.commit()
    migrate("upgrade", "head")
    with closing(sqlite3.connect(database)) as connection:
        row = connection.execute(
            "SELECT raw_payload, collection_complete, pagination_metadata FROM source_records"
        ).fetchone()
        payment = connection.execute(
            "SELECT commitment_number, commitment_external_id, is_current FROM payments"
        ).fetchone()
        assert "resolved_url" in {r[1] for r in connection.execute("PRAGMA table_info(documents)")}
        for table in (
            "collection_sync_states",
            "verified_collection_versions",
            "source_response_validators",
            "collection_recheck_states",
            "llm_batches",
            "llm_demands",
            "llm_work_items",
            "llm_results",
            "llm_attempts",
            "llm_provider_states",
            "fact_verifications",
            "participant_values",
            "document_identities",
            "document_aliases",
            "reconciliation_runs",
        ):
            assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0  # noqa: S608 - fixed table allowlist
    assert row == ('{"sentinel": true}', None, None)
    assert payment == ("123", None, 1)
