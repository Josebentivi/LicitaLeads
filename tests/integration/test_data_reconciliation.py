"""Deterministic legacy-data repairs are constrained to raw evidence."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import select, text

from app.config import Settings
from app.models import Document, ExtractionStatus, Payment, Procurement, SourceRecord
from app.services.backups import create_backup
from app.services.data_reconciliation import reconcile_data
from app.services.maintenance import exclusive_maintenance

from .conftest import DatabaseContext


async def _backup(database: DatabaseContext, owner: str):
    async with database.sessions() as session, session.begin():
        session.info["maintenance_owner"] = owner
        await session.execute(text("CREATE TABLE IF NOT EXISTS alembic_version (version_num TEXT)"))
        await session.execute(text("INSERT INTO alembic_version VALUES ('test')"))
    return await asyncio.to_thread(
        create_backup,
        Settings(_env_file=None, database_url=str(database.engine.url)),
        maintenance_owner=owner,
    )


@pytest.mark.asyncio
async def test_reconcile_deactivates_documents_from_latest_raw_state(
    database: DatabaseContext,
) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        procurement = Procurement(
            source="pncp", external_id="00000000000191:2026:1", fingerprint="a" * 64
        )
        session.add(procurement)
        await session.flush()
        document = Document(
            procurement_id=procurement.id,
            title="Ata",
            original_url="https://pncp.test/ata.pdf",
            extraction_status=ExtractionStatus.PENDING,
            fingerprint="b" * 64,
        )
        source = SourceRecord(
            source="pncp",
            collection_complete=True,
            endpoint="https://pncp.test/orgaos/00000000000191/compras/2026/1/arquivos",
            request_parameters={},
            response_hash="c" * 64,
            raw_payload=[
                {
                    "url": "https://pncp.test/ata.pdf",
                    "statusAtivo": False,
                }
            ],
            http_status=200,
            collected_at=now,
            fingerprint="d" * 64,
        )
        session.add_all([document, source])

    async with exclusive_maintenance(database.sessions) as owner:
        backup = await _backup(database, owner)
        async with database.sessions() as session, session.begin():
            report = await reconcile_data(
                session, apply=True, backup=backup, maintenance_owner=owner
            )

    assert report.deactivated_documents == 1
    async with database.sessions() as session:
        stored = await session.scalar(select(Document))

    assert stored is not None
    assert stored.is_active is False
    assert stored.extraction_status is ExtractionStatus.UNSUPPORTED


@pytest.mark.asyncio
async def test_reconcile_repairs_legacy_federal_payment_identity(
    database: DatabaseContext,
) -> None:
    row = {
        "documento": "OB-1",
        "fase": "Pagamento",
        "valor": "10,00",
        "data": "2026-01-02",
        "codigoFavorecido": "00000000000191",
        "codigoOrgao": "11111111111111",
    }
    async with database.sessions() as session, session.begin():
        source = SourceRecord(
            source="transparencia",
            endpoint="https://transparencia.test/payments",
            request_parameters={"pagina": 1},
            response_hash="e" * 64,
            raw_payload=[row],
            http_status=200,
            collected_at=datetime.now(UTC),
            fingerprint="f" * 64,
        )
        payment = Payment(
            source="transparencia",
            external_id="OB-1:Pagamento:00000000000191:10.00",
            document_number="OB-1",
            supplier_cnpj="00000000000191",
            amount=Decimal("10.00"),
            payment_date=datetime(2026, 1, 2, tzinfo=UTC),
            fingerprint="1" * 64,
        )
        session.add_all([source, payment])
        await session.flush()
        payment.source_record_id = source.id

    async with exclusive_maintenance(database.sessions) as owner:
        backup = await _backup(database, owner)
        async with database.sessions() as session, session.begin():
            report = await reconcile_data(
                session, apply=True, backup=backup, maintenance_owner=owner
            )

    assert report.repaired_payment_ids == 1
    async with database.sessions() as session:
        stored = await session.scalar(select(Payment))

    assert stored is not None
    assert "2026-01-02" in stored.external_id
    assert "10.00" not in stored.external_id


@pytest.mark.asyncio
async def test_apply_requires_verified_backup(database):
    async with database.sessions() as session:
        with pytest.raises(ValueError, match="backup verificado"):
            await reconcile_data(session, apply=True)


@pytest.mark.asyncio
async def test_dry_run_never_changes_documents_and_unknown_completeness_is_preserved(database):
    async with database.sessions() as session, session.begin():
        procurement = Procurement(
            source="pncp", external_id="00000000000191:2026:1", fingerprint="7" * 64
        )
        session.add(procurement)
        await session.flush()
        session.add(
            Document(
                procurement_id=procurement.id,
                original_url="https://official.test/doc",
                title="Sentinela",
                fingerprint="8" * 64,
            )
        )
        source = SourceRecord(
            source="pncp",
            endpoint="https://official.test/orgaos/00000000000191/compras/2026/1/arquivos",
            fingerprint="9" * 64,
            response_hash="a" * 64,
            http_status=200,
            raw_payload=[{"url": "https://official.test/doc", "statusAtivo": False}],
        )
        session.add(source)
    async with database.sessions() as session, session.begin():
        report = await reconcile_data(session)
        assert report.deactivated_documents == 0
        source = await session.scalar(select(SourceRecord))
        source.collection_complete = True
    async with database.sessions() as session, session.begin():
        report = await reconcile_data(session)
        assert report.deactivated_documents == 1
    async with database.sessions() as session:
        assert (await session.scalar(select(Document))).is_active


@pytest.mark.asyncio
async def test_ambiguous_federal_rows_are_reported_without_repair(database):
    rows = [
        {
            "documento": "OB-1",
            "codigoFavorecido": "00000000000191",
            "fase": "Pagamento",
            "codigoUg": unit,
            "valor": "10,00",
        }
        for unit in ("100", "200")
    ]
    async with database.sessions() as session, session.begin():
        source = SourceRecord(
            source="transparencia",
            endpoint="https://official.test/payments",
            fingerprint="b" * 64,
            response_hash="c" * 64,
            raw_payload=rows,
        )
        session.add(source)
        await session.flush()
        session.add(
            Payment(
                source="transparencia",
                external_id="legacy",
                document_number="OB-1",
                supplier_cnpj="00000000000191",
                amount=Decimal("10"),
                source_record_id=source.id,
                fingerprint="d" * 64,
            )
        )
    async with exclusive_maintenance(database.sessions) as owner:
        backup = await _backup(database, owner)
        async with database.sessions() as session, session.begin():
            report = await reconcile_data(
                session, apply=True, backup=backup, maintenance_owner=owner
            )
            assert report.repaired_payment_ids == 0
            assert any("ambígua" in item for item in report.diagnostics)
    async with database.sessions() as session:
        assert (await session.scalar(select(Payment))).external_id == "legacy"
