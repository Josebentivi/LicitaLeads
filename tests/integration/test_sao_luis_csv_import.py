import asyncio
import hashlib
import os
import re
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import func, select

from app.config import get_settings
from app.connectors.municipal_payments.sao_luis import SaoLuisCsvConnector
from app.models import CrawlRun, Payment, SourceRecord
from app.repositories.crawls import JobLeaseRepository
from app.services.ingestion.payment_import import PaymentImportErrorList
from app.services.ingestion.payments import PaymentIngestionService
from app.services.ingestion.sao_luis_import import SaoLuisCsvImport, SaoLuisImportBusy

CSV = (
    "Fornecedor;CNPJ;Valor Pago;Data Pagamento;Empenho\n"
    "Alfa;04.252.011/0001-10;20,50;20/11/2026;1\n"
    '"Nome\ninválido";;;;\n'
    "Alfa;04.252.011/0001-10;20,50;20/11/2026;1\n"
    "Beta;;42,00;;2\n"
).encode()


async def test_official_csv_preview_concurrent_confirmation_and_live_identity(database, tmp_path):
    settings = get_settings().model_copy(update={"raw_data_storage_path": tmp_path / "raw"})
    service = SaoLuisCsvImport(settings, database.sessions)
    preview = await service.preview(CSV)
    assert (preview.created, preview.existing, preview.repeated_rows) == (2, 0, 1)
    assert [number for number, _ in preview.errors] == [3]
    async with database.sessions() as db:
        assert await db.scalar(select(func.count()).select_from(SourceRecord)) == 0

    results = await asyncio.gather(service.confirm(preview.token), service.confirm(preview.token))
    assert results[0][0] == results[1][0]
    assert sorted(reused for _, reused in results) == [False, True]
    async with database.sessions() as db:
        run = await db.get_one(CrawlRun, results[0][0])
        source = await db.scalar(select(SourceRecord).where(SourceRecord.crawl_run_id == run.id))
        assert run.records_created == 2 and run.status.value == "partial"
        assert source and source.response_hash == hashlib.sha256(CSV).hexdigest()
        assert source.request_parameters["method"] == "operator_csv"
        assert source.http_status is None

    connector = SaoLuisCsvConnector(service.source, settings)
    writer = PaymentIngestionService(settings, session_factory=database.sessions)
    try:
        result = connector._result_from_rows(
            connector._csv_rows(CSV),
            normalized_cnpj=None,
            year=2026,
            url=service.source.export_url,
            http_status=200,
            raw_payload={},
        )
        refreshed = await writer.persist_raw_payments(result.data)
        assert refreshed.payments_created == 0
    finally:
        await connector.aclose()
        await writer.aclose()
    next_preview = await service.preview(CSV)
    assert (next_preview.created, next_preview.existing) == (0, 2)
    async with database.sessions() as db:
        assert await db.scalar(select(func.count()).select_from(Payment)) == 2


async def test_csv_preview_expiration_and_municipal_lock(database, tmp_path):
    settings = get_settings().model_copy(update={"raw_data_storage_path": tmp_path / "raw"})
    service = SaoLuisCsvImport(settings, database.sessions)
    preview = await service.preview(CSV)
    now = datetime.now(UTC)
    async with database.sessions.begin() as db:
        assert await JobLeaseRepository(db).acquire(
            name="municipal:sao_luis",
            owner_id="automatic",
            now=now,
            expires_at=now + timedelta(minutes=5),
        )
    with pytest.raises(SaoLuisImportBusy, match="em andamento"):
        await service.confirm(preview.token)
    old = (now - timedelta(hours=2)).timestamp()
    os.utime(service._path(preview.token), (old, old))
    with pytest.raises(PaymentImportErrorList, match="expirada"):
        await service.confirm(preview.token)
    assert not service._path(preview.token).exists()


async def test_csv_preview_can_be_confirmed_from_payments_page(api_client):
    response = await api_client.post(
        "/pagamentos/sao-luis/previa", files={"file": ("export.csv", CSV)}
    )
    assert response.status_code == 200
    assert "Linha 3" in response.text
    token = UUID(re.search(r'name="token" value="([^"]+)"', response.text).group(1))
    result = await api_client.post("/pagamentos/sao-luis/confirmar", data={"token": str(token)})
    assert result.status_code == 200
    assert "2 registros novos" in result.text
