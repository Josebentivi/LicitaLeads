"""Preview and confirm a São Luís portal export under the municipal write lease."""

from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import os
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.connectors.base import RawPayment
from app.connectors.http import ConnectorContractError
from app.connectors.municipal_payments.sao_luis import SaoLuisCsvConnector
from app.connectors.municipal_payments.sources import get_municipal_source
from app.database import write_session
from app.models import CrawlRun, CrawlRunStatus, DataAvailability, Payment, SourceRecord
from app.repositories.crawls import JobLeaseRepository
from app.services.ingestion.municipal_payments import municipal_payment_lease_name
from app.services.ingestion.payment_import import PaymentImportErrorList
from app.services.ingestion.payments import PaymentIngestionService
from app.services.ingestion.pipeline import stable_fingerprint

MAX_CSV_BYTES = 10 * 1024 * 1024
_PREVIEW_TTL = timedelta(hours=1)


class SaoLuisImportBusy(ValueError):
    """The source is being collected by another worker."""


@dataclass(frozen=True, slots=True)
class SaoLuisPreview:
    token: UUID
    digest: str
    created: int
    existing: int
    repeated_rows: int
    errors: list[tuple[int, str]]


class SaoLuisCsvImport:
    def __init__(self, settings: Settings, sessions: async_sessionmaker[AsyncSession]):
        self.settings = settings
        self.sessions = sessions
        self.source = get_municipal_source("sao_luis")
        self.directory = settings.raw_data_storage_path / "sao-luis-previews"

    def _path(self, token: UUID) -> Path:
        return self.directory / f"{token.hex}.preview"

    def _purge_expired(self) -> None:
        cutoff = (datetime.now(UTC) - _PREVIEW_TTL).timestamp()
        for path in self.directory.glob("*.preview"):
            with suppress(FileNotFoundError, PermissionError):
                if path.stat().st_mtime < cutoff:
                    path.unlink()

    def _stage(self, token: UUID, digest: str, content: bytes) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        self._purge_expired()
        with tempfile.NamedTemporaryFile(
            dir=self.directory, suffix=".preview", delete=False
        ) as stream:
            path = Path(stream.name)
            try:
                stream.write(digest.encode("ascii") + b"\n")
                stream.write(content)
                stream.close()
                os.replace(path, self._path(token))
            finally:
                path.unlink(missing_ok=True)

    def _read(self, token: UUID) -> tuple[str, bytes]:
        path = self._path(token)
        try:
            with path.open("rb") as stream:
                if datetime.fromtimestamp(os.fstat(stream.fileno()).st_mtime, UTC) < (
                    datetime.now(UTC) - _PREVIEW_TTL
                ):
                    raise FileNotFoundError
                digest = stream.readline(66).strip()
                content = stream.read(MAX_CSV_BYTES + 1)
        except FileNotFoundError:
            self._purge_expired()
            raise PaymentImportErrorList(
                "Prévia expirada ou cancelada. Envie o CSV novamente."
            ) from None
        actual = hashlib.sha256(content).hexdigest()
        if len(content) > MAX_CSV_BYTES or digest != actual.encode("ascii"):
            raise PaymentImportErrorList("O arquivo da prévia mudou. Envie o CSV novamente.")
        return actual, content

    async def cancel(self, token: UUID) -> None:
        await asyncio.to_thread(self._path(token).unlink, missing_ok=True)

    async def _parse(self, content: bytes) -> tuple[list[RawPayment], list[tuple[int, str]], int]:
        if not content or len(content) > MAX_CSV_BYTES:
            raise PaymentImportErrorList("Envie um CSV com até 10 MB.")
        connector = SaoLuisCsvConnector(self.source, self.settings)
        try:
            rows, errors = await asyncio.to_thread(connector.parse_csv_export, content)
        except (ConnectorContractError, csv.Error) as exc:
            raise PaymentImportErrorList(
                "CSV de São Luís inválido ou sem cabeçalho reconhecido."
            ) from exc
        finally:
            await connector.aclose()
        unique = {row.external_id: row for row in rows}
        return list(unique.values()), errors, len(rows) - len(unique)

    async def preview(self, content: bytes) -> SaoLuisPreview:
        rows, errors, repeated = await self._parse(content)
        fingerprints = [stable_fingerprint("payment", row.source, row.external_id) for row in rows]
        existing = 0
        async with self.sessions() as db:
            for offset in range(0, len(fingerprints), 500):
                found = await db.scalars(
                    select(Payment.fingerprint).where(
                        Payment.fingerprint.in_(fingerprints[offset : offset + 500])
                    )
                )
                existing += len(found.all())
        token, digest = uuid4(), hashlib.sha256(content).hexdigest()
        if rows:
            await asyncio.to_thread(self._stage, token, digest, content)
        return SaoLuisPreview(token, digest, len(rows) - existing, existing, repeated, errors)

    async def confirm(self, token: UUID) -> tuple[UUID, bool]:
        digest, content = await asyncio.to_thread(self._read, token)
        rows, errors, repeated = await self._parse(content)
        if not rows:
            raise PaymentImportErrorList("O arquivo não contém pagamentos válidos.")
        payment_ingestion = PaymentIngestionService(self.settings, session_factory=self.sessions)
        owner = f"sao-luis-csv:{uuid4()}"
        lease = municipal_payment_lease_name(self.source.id)
        observation = stable_fingerprint("sao_luis_csv_import", digest)
        try:
            async with write_session(self.sessions) as db:
                leases = JobLeaseRepository(db)
                now = datetime.now(UTC)
                if not await leases.acquire(
                    name=lease, owner_id=owner, now=now, expires_at=now + timedelta(hours=2)
                ):
                    raise SaoLuisImportBusy(
                        "Há uma coleta de São Luís em andamento. Tente novamente."
                    )
                previous = await db.scalar(
                    select(SourceRecord.crawl_run_id).where(SourceRecord.fingerprint == observation)
                )
                if previous:
                    await leases.release(name=lease, owner_id=owner)
                    return previous, True
                run = CrawlRun(
                    id=uuid4(),
                    connector=lease,
                    status=CrawlRunStatus.RUNNING,
                    filters={
                        "mode": "municipal_csv",
                        "source_id": self.source.id,
                        "sha256": digest,
                    },
                    started_at=now,
                    counting_version=2,
                    records_changed=0,
                    records_unchanged=0,
                    records_stale_ignored=0,
                    records_conflicted=0,
                )
                db.add(run)
                await db.flush()
                source = SourceRecord(
                    id=uuid4(),
                    crawl_run_id=run.id,
                    source=lease,
                    endpoint=self.source.open_data_url or self.source.export_url,
                    request_parameters={"method": "operator_csv", "sha256": digest},
                    response_hash=digest,
                    raw_payload={
                        "content_base64": base64.b64encode(content).decode("ascii"),
                        "rejected_lines": [number for number, _ in errors],
                        "duplicate_rows": repeated,
                    },
                    collection_complete=False,
                    availability=DataAvailability.AVAILABLE,
                    collected_at=now,
                    diagnostic="Exportação de São Luís enviada pelo operador",
                    fingerprint=observation,
                )
                db.add(source)
                await db.flush()
                db.info["crawl_run_id"] = run.id
                result = await payment_ingestion.persist_raw_payments(
                    rows, source_record_id=source.id, session=db
                )
                run.records_found = len(rows)
                run.finished_at = run.heartbeat_at = datetime.now(UTC)
                run.status = CrawlRunStatus.PARTIAL if errors else CrawlRunStatus.COMPLETED
                run.diagnostic = f"{len(errors)} linhas rejeitadas na prévia" if errors else None
                run.cursor = {
                    "availability": DataAvailability.AVAILABLE.value,
                    "payments_linked": result.payments_linked,
                    "payments_unlinked": result.payments_unlinked,
                }
                await leases.release(name=lease, owner_id=owner)
                return run.id, False
        finally:
            await payment_ingestion.aclose()
