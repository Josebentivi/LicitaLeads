from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from bs4 import BeautifulSoup

from app.models import (
    CrawlRun,
    CrawlRunStatus,
    DataAvailability,
    Document,
    Procurement,
    SourceRecord,
)


async def test_old_crawl_focus_keeps_one_row_on_htmx_refresh_and_reports_missing(
    api_client, database
):
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        old = CrawlRun(
            connector="pncp", status=CrawlRunStatus.COMPLETED, created_at=now - timedelta(days=20)
        )
        session.add(old)
        session.add_all(
            CrawlRun(connector="pncp", status=CrawlRunStatus.COMPLETED, created_at=now)
            for _ in range(201)
        )
        await session.flush()
        identifier = old.id
    recent = await api_client.get("/crawls/table")
    assert f"execucao-{identifier}" not in recent.text
    for path in ("/crawls", "/crawls/table"):
        response = await api_client.get(path, params={"run": str(identifier)})
        table = BeautifulSoup(response.text, "html.parser").select_one("#crawl-table")
        assert table["hx-get"] == f"/crawls/table?run={identifier}"
        assert len(table.select("tr[id^=execucao-]")) == 1
        assert table.find(id=f"execucao-{identifier}")
    assert (await api_client.get("/crawls/table", params={"run": str(uuid4())})).status_code == 404


@pytest.mark.parametrize(
    ("availability", "complete", "expected"),
    [
        (DataAvailability.EMPTY, True, "Consulta concluída sem registros"),
        (DataAvailability.NOT_PUBLISHED, False, "Sem publicação indicada pela fonte"),
        (DataAvailability.TEMPORARY_ERROR, False, "Erro na consulta"),
        (DataAvailability.ACCESS_RESTRICTED, False, "Acesso restrito"),
        (DataAvailability.AVAILABLE, False, "Resposta parcial"),
        (DataAvailability.AVAILABLE, None, "completude desconhecida"),
    ],
)
async def test_exact_document_catalog_response(
    api_client, database, availability, complete, expected
):
    async with database.sessions() as session, session.begin():
        process = Procurement(source="pncp", external_id="00000000000191:2026:3", fingerprint="p")
        run = CrawlRun(connector="pncp", status=CrawlRunStatus.PARTIAL)
        session.add_all([process, run])
        await session.flush()
        record = SourceRecord(
            source="pncp",
            endpoint="https://pncp.gov.br/api/pncp/v1/orgaos/00000000000191/compras/2026/3/arquivos?pagina=1",
            response_hash="a",
            fingerprint="a",
            availability=availability,
            http_status=200,
            collection_complete=complete,
            crawl_run_id=run.id,
        )
        session.add(record)
        await session.flush()
        identifier, source_id = process.id, record.id
    response = await api_client.get(f"/procurements/{identifier}")
    soup = BeautifulSoup(response.text, "html.parser")
    docs = soup.find("h2", string="Documentos").parent
    assert expected in docs.text
    assert str(source_id) in docs.text
    assert "Sem consulta registrada" in soup.find("h2", string="Itens").find_parent("section").text


async def test_coverage_without_response_and_stored_excerpt(api_client, database):
    async with database.sessions() as session, session.begin():
        process = Procurement(source="pncp", external_id="00000000000191:2026:3", fingerprint="p")
        other = Procurement(source="compras_gov", external_id="9876", fingerprint="q")
        run = CrawlRun(
            connector="all",
            status=CrawlRunStatus.PARTIAL,
            started_at=datetime.now(UTC),
            diagnostic="pncp/00000000000191:2026:3: documents: temporary network failure",
        )
        session.add_all([process, other, run])
        await session.flush()
        doc = Document(
            procurement_id=process.id,
            original_url="javascript:alert(0)",
            extracted_text="Trecho oficial",
            fingerprint="doc",
        )
        session.add(doc)
        await session.flush()
        identifier, unsupported, document_id = process.id, other.id, doc.id
    page = await api_client.get(f"/procurements/{identifier}")
    assert 'href="javascript:' not in page.text
    assert f'href="/api/documents/{document_id}/view"' in page.text
    assert "Falha registrada na consulta" in page.text
    assert (
        "não oferece consulta pública"
        in (await api_client.get(f"/procurements/{unsupported}")).text
    )


async def test_dashboard_last_procurement_run_counts_only_its_own_recorded_stage(
    api_client, database
):
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        run = CrawlRun(
            connector="pncp",
            status=CrawlRunStatus.PARTIAL,
            filters={"mode": "procurements"},
            created_at=now - timedelta(hours=1),
            records_found=17,
            records_created=4,
            cursor={
                "sources": {
                    "documents": {
                        "status": "partial",
                        "documents_total": 7,
                        "documents_processed": 3,
                        "documents_failed": 2,
                        "leads_created": 0,
                    }
                }
            },
        )
        session.add_all(
            [
                run,
                CrawlRun(
                    connector="pncp",
                    status=CrawlRunStatus.COMPLETED,
                    filters={"mode": "contracts"},
                    created_at=now,
                ),
            ]
        )
        await session.flush()
        identifier = run.id
    soup = BeautifulSoup((await api_client.get("/dashboard")).text, "html.parser")
    panel = soup.select_one("#ultimo-lote")
    assert panel.find("dt", string="Falhas de processamento").find_next_sibling("dd").text == "2"
    assert panel.find("dt", string="Oportunidades criadas").find_next_sibling("dd").text == "0"
    assert "Eventos criados" not in panel.text
    assert panel.select_one(f'a[href="/crawls/{identifier}/documents"]')
    assert panel.select_one(f'a[href="/procurements?run={identifier}"]')
