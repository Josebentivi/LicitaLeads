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
        payment_indexes = {
            row[1]: row[2] for row in connection.execute("PRAGMA index_list(payments)")
        }
        revision = connection.execute("SELECT version_num FROM alembic_version").fetchone()[0]
        participant_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='participants'"
        ).fetchone()[0]
        monetary_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(participant_values)")
        }
        work_columns = {row[1] for row in connection.execute("PRAGMA table_info(llm_work_items)")}
        contact_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(company_contacts)")
        }
        procurement_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(procurements)")
        }
        item_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(procurement_items)")
        }
        tracking_columns = {
            row[1]: row for row in connection.execute("PRAGMA table_info(payment_tracking_targets)")
        }
        event_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='procurement_events'"
        ).fetchone()[0]
        contact_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='company_contacts'"
        ).fetchone()[0]
        review_tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }

    assert {"is_active", "deactivated_at", "resolved_url"} <= document_columns
    assert "'cancelled'" in participant_sql
    assert "heartbeat_at" in crawl_columns
    assert {"commitment_external_id", "is_current"} <= payment_columns
    assert {
        "federal_identity",
        "federal_beneficiary_role",
        "federal_intermediary",
        "federal_creditor_list",
        "federal_identity_conflict",
    } <= payment_columns
    assert payment_indexes["ux_payments_federal_identity"] == 1
    assert {"amount", "scope", "lot_number", "evidence_id", "document_hash"} <= monetary_columns
    assert {"context", "coverage"} <= work_columns
    assert "contract_control_number" in payment_columns
    assert {"llm_suggestions", "llm_event_citations"} <= review_tables
    assert "municipal_budget_entries" in review_tables
    assert "source_record_id" in contact_columns
    assert "'address'" in contact_sql
    assert "'APPEAL_WINDOW_OPENED'" in event_sql
    assert {
        "object_complement",
        "object_description_search",
        "object_complement_search",
    } <= procurement_columns
    assert {
        "summary_description",
        "description_search",
        "summary_description_search",
    } <= item_columns
    assert tracking_columns["cnpj"][5] == 1
    assert tracking_columns["active"][3] == 1
    assert revision == "0035_federal_legacy_lookup"


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
    migrate("downgrade", "0018_integrity_heartbeat")
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
        connection.execute(
            """INSERT INTO procurements
            (id, source, external_id, fingerprint, created_at, updated_at, current_phase)
            VALUES (?, 'pncp', 'legacy', ?, ?, ?, 'unknown')""",
            ("2" * 32, "d" * 64, "2026-01-01 00:00:00", "2026-01-01 00:00:00"),
        )
        connection.execute(
            """INSERT INTO procurement_items (id, procurement_id, item_number, description, fingerprint)
            VALUES (?, ?, '1', 'Descrição existente', ?)""",
            ("3" * 32, "2" * 32, "e" * 64),
        )
        connection.execute(
            """INSERT INTO companies (id, normalized_name, fingerprint, created_at, updated_at)
            VALUES (?, 'EMPRESA', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""",
            ("4" * 32, "f" * 64),
        )
        connection.execute(
            """INSERT INTO participants (id, procurement_id, company_id, fingerprint,
            participation_role, status_code, source, source_record_id, confidence, created_at)
            VALUES (?, ?, ?, ?, 'participant', 'participant', 'pncp', ?, 1, CURRENT_TIMESTAMP)""",
            ("5" * 32, "2" * 32, "4" * 32, "0" * 64, "0" * 32),
        )
        connection.commit()
    migrate("upgrade", "head")
    with closing(sqlite3.connect(database)) as connection:
        row = connection.execute(
            "SELECT raw_payload, collection_complete, pagination_metadata FROM source_records"
        ).fetchone()
        payment = connection.execute(
            "SELECT commitment_number, commitment_external_id, is_current, federal_identity, "
            "federal_beneficiary_role, federal_identity_conflict FROM payments"
        ).fetchone()
        procurement = connection.execute("SELECT object_complement FROM procurements").fetchone()
        item = connection.execute(
            "SELECT description, summary_description, description_search FROM procurement_items"
        ).fetchone()
        participant = connection.execute(
            "SELECT id, status_code, source_record_id FROM participants"
        ).fetchone()
        assert (
            connection.execute(
                "UPDATE participants SET status_code = 'cancelled' WHERE id = ?", ("5" * 32,)
            ).rowcount
            == 1
        )
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
            "llm_suggestions",
            "llm_event_citations",
            "fact_verifications",
            "participant_values",
            "document_identities",
            "document_aliases",
            "reconciliation_runs",
        ):
            assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0  # noqa: S608 - fixed table allowlist
    assert row == ('{"sentinel": true}', None, None)
    assert payment == ("123", None, 1, None, None, None)
    assert procurement == (None,)
    assert item == ("Descrição existente", None, " descricao existente ")
    assert participant == ("5" * 32, "participant", "0" * 32)
