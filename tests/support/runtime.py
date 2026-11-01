"""Launch the real API/scheduler with deterministic providers and an isolated DB.

All injection lives in test support. There are no test endpoints in production.
The parent migrates the database before invoking this module.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path

from app.connectors.base import ConnectorResult, DataAvailability
from app.database import async_session_factory
from app.models import Document, Evidence, Procurement
from app.services.application_tests import ApplicationTestService, TestResult
from app.services.ingestion.pipeline import IngestionPipeline
from tests.integration.test_collection_continuation import CountingConnector, records
from tests.integration.test_price_registry_pipeline import PriceRegistryConnector, _registry

ROOT = Path(os.environ["LICITA_RUNTIME_ROOT"])


class ControlledConnector(CountingConnector):
    async def discover_procurements(self, filters):
        while (ROOT / "hold").exists():
            (ROOT / "entered").touch()
            await asyncio.sleep(0.05)
        return await super().discover_procurements(filters)


class ControlledRegistries(PriceRegistryConnector):
    def __init__(self):
        super().__init__(_registry(), [])
        self.name = "pncp"

    async def discover_price_registries(self, filters):
        result = await super().discover_price_registries(filters)
        data = [self.registry.model_copy(update={"source": "pncp"})]
        return result.model_copy(update={"data": data})

    async def fetch_price_registry(self, external_id):
        return ConnectorResult(
            data=self.registry.model_copy(update={"source": "pncp"}),
            availability=DataAvailability.AVAILABLE,
        )


def install_sources():
    def selected(self, requested):
        if (ROOT / "registries").exists():
            return [ControlledRegistries()]
        return [ControlledConnector(records())]

    IngestionPipeline._selected_connectors = selected


class SmallTestService(ApplicationTestService):
    """A controlled runner for the test-page journey, never recursive pytest."""

    async def _execute(self, state, *, asgi_app, include_live):
        from app.services.application_tests import TestReport
        from app.services.test_reports import code_identity

        state.identity = await asyncio.to_thread(code_identity, self._repo_root())
        state.stages.append({"title": "Executor controlado", "status": "running"})
        result = TestResult(
            "controlled", "Jornada", "Resultado parcial controlado", "passed", "ok", 1
        )
        self._partial_results = [result]
        state.progress = {"total": 1, "passed": 1}
        try:
            await asyncio.sleep(3)
            state.status = "completed"
        except asyncio.CancelledError:
            state.status = "cancelled"
        state.finished_at = datetime.now(UTC)
        state.report = TestReport(
            state.run_id,
            state.mode,
            state.started_at,
            state.finished_at,
            [result],
            status=state.status,
            identity=state.identity,
        )
        await self._save_report(state.report)


async def seed():
    from sqlalchemy import func, select

    async with async_session_factory() as session, session.begin():
        if await session.scalar(select(func.count()).select_from(Procurement)):
            return
        procurement = Procurement(
            source="pncp",
            external_id="seed",
            fingerprint="seed",
            agency_name="Órgão sintético",
            agency_cnpj="00000000000191",
            uasg="980921",
            uf="MA",
            title="Processo sintético de teste",
        )
        session.add(procurement)
        await session.flush()
        file = ROOT / "documents/evidence.txt"
        file.write_text("Evidência sintética oficial de teste", encoding="utf-8")
        document = Document(
            procurement_id=procurement.id,
            original_url="https://pncp.gov.br/synthetic",
            title="evidence.txt",
            mime_type="text/plain",
            local_path=str(file),
            fingerprint="synthetic-doc",
        )
        session.add(document)
        await session.flush()
        evidence = Evidence(
            procurement_id=procurement.id,
            document_id=document.id,
            source_url="https://pncp.gov.br/synthetic",
            fingerprint="synthetic-evidence",
            text_excerpt="Evidência sintética",
        )
        session.add(evidence)
        await session.flush()
        (ROOT / "evidence-id").write_text(str(evidence.id))


async def scheduler():
    from app.jobs import scheduler as module

    async def callback():
        with (ROOT / "ticks").open("a") as output:
            output.write("tick\n")
        while (ROOT / "scheduler-hold").exists():
            (ROOT / "scheduler-entered").touch()
            await asyncio.sleep(0.05)

    module.discover_price_registries = callback
    instance = module.build_scheduler()
    for job in instance.get_jobs():
        if job.id != "discover_price_registries":
            instance.remove_job(job.id)
    instance.reschedule_job("discover_price_registries", trigger="interval", seconds=0.5)
    instance.start()
    try:
        await asyncio.Event().wait()
    finally:
        instance.shutdown(wait=False)


def main():
    install_sources()
    if os.environ.get("LICITA_RUNTIME_ROLE") == "scheduler":
        asyncio.run(scheduler())
    else:
        import uvicorn

        from app import web
        from app.config import get_settings
        from app.main import app
        from app.services.ingestion.documents import DownloadedDocument
        from app.services.ingestion.processor import DocumentProcessingService

        class Downloader:
            async def download(self, url):
                return DownloadedDocument(
                    url=url,
                    content=b"Documento sintetico autorizado para teste.",
                    filename="evidence.txt",
                    declared_mime="text/plain",
                )

            async def aclose(self):
                pass

        original = DocumentProcessingService.__init__

        def initialize(self, *args, **kwargs):
            kwargs.setdefault("downloader", Downloader())
            original(self, *args, **kwargs)

        DocumentProcessingService.__init__ = initialize

        def controlled_settings():
            settings = get_settings()
            if (ROOT / "enable-ai-controls").exists():
                return settings.model_copy(
                    update={
                        "llm_enabled": True,
                        "llm_provider": "openai",
                        "llm_model": "controlled",
                        "llm_api_key": "synthetic",
                        "llm_min_chunk_characters": 1,
                    }
                )
            return settings

        web.get_settings = controlled_settings
        from app.api.routes import crawls

        crawls.get_settings = controlled_settings

        asyncio.run(seed())
        web._application_tests = SmallTestService(report_path=ROOT / "test-report.json")
        uvicorn.run(
            app, host="127.0.0.1", port=int(os.environ["LICITA_RUNTIME_PORT"]), log_level="warning"
        )


if __name__ == "__main__":
    main()
