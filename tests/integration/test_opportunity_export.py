import asyncio
import csv
from datetime import UTC, datetime, timedelta
from io import StringIO

from bs4 import BeautifulSoup
from typer.testing import CliRunner

from app.models import Deadline, Document, Lead, LeadStatus

from .test_api_leads import _full_graph


async def test_cli_default_and_explicit_history_have_the_same_scope_as_web(
    database, tmp_path, monkeypatch
):
    from app.cli import main as cli

    ids = await _full_graph(database)
    async with database.sessions() as session, session.begin():
        (await session.get_one(Document, ids.document)).is_active = False
    monkeypatch.setattr(cli, "async_session_factory", database.sessions)
    destination = tmp_path / "oportunidades.csv"
    for args, count in [([], 0), (["--incluir-historico", "--busca", "Empresa"], 1)]:
        result = await asyncio.to_thread(
            CliRunner().invoke, cli.app, ["export-leads", str(destination), *args]
        )
        assert result.exit_code == 0, result.output
        with destination.open(encoding="utf-8-sig", newline="") as stream:
            rows = list(csv.DictReader(stream))
        assert len(rows) == count
        if rows:
            assert rows[0]["validade"] == "Histórica ou invalidada"
            assert rows[0]["evidencia_id"] == str(ids.evidence)


def csv_rows(response):
    assert response.status_code == 200
    assert response.headers["content-disposition"].startswith("attachment")
    return list(csv.DictReader(StringIO(response.content.decode("utf-8-sig"))))


async def test_export_matches_filters_traces_and_current_deadlines(api_client, database):
    ids = await _full_graph(database)
    async with database.sessions() as session, session.begin():
        lead = await session.get_one(Lead, ids.lead)
        lead.reason_summary = '=texto, com "aspas"\ne quebra'
        (await session.get_one(Deadline, lead.deadline_id)).explicit_deadline_at = datetime.now(
            UTC
        ) - timedelta(hours=1)
    soup = BeautifulSoup((await api_client.get("/leads?q=Empresa&page=8")).text, "html.parser")
    link = soup.find("a", string="Exportar esta busca em CSV")["href"]
    assert "page=" not in link and "q=Empresa" in link
    [row] = csv_rows(await api_client.get(link))
    assert row["lead_id"] == str(ids.lead) and row["validade"] == "Atual"
    assert row["pagina"] == "7" and row["evidencia_local"] == f"/evidencias/{ids.evidence}"
    assert row["status_prazo"] == "Data-limite ultrapassada"
    assert row["status_prazo_gravado"] == "OPEN"
    assert row["motivo"].startswith("'=") and "\n" in row["motivo"]
    assert datetime.fromisoformat(row["emissao_utc"]).tzinfo
    assert not csv_rows(await api_client.get("/leads/exportar.csv?q=sem-correspondencia"))
    async with database.sessions() as session, session.begin():
        (await session.get_one(Document, ids.document)).is_active = False
    assert not csv_rows(await api_client.get("/leads/exportar.csv"))
    [historical] = csv_rows(await api_client.get("/leads/exportar.csv?include_history=true"))
    assert (
        historical["validade"] == "Histórica ou invalidada"
        and historical["status_prazo"] == "Data histórica"
    )


async def test_work_queue_stops_before_decided_cases_and_does_not_wrap(api_client, database):
    ids = await _full_graph(database)
    async with database.sessions() as session, session.begin():
        lead = await session.get_one(Lead, ids.lead)
        decided = Lead(
            procurement_id=lead.procurement_id,
            company_id=lead.company_id,
            triggering_event_id=lead.triggering_event_id,
            lead_status=LeadStatus.REJECTED,
            score=100,
            urgency_score=10,
            evidence_score=10,
            legal_relevance_score=10,
            contact_score=0,
            reason_summary="Caso decidido",
            fingerprint="queue-decided",
        )
        session.add(decided)
        await session.flush()
        decided_id = decided.id
    agenda = BeautifulSoup((await api_client.get("/dashboard")).text, "html.parser").select_one(
        ".agenda-results"
    )
    assert f"/leads/{ids.lead}" in str(agenda) and f"/leads/{decided_id}" not in str(agenda)
    page = await api_client.get(f"/leads/{ids.lead}")
    assert "Registrar e analisar próxima" not in page.text
    assert "Próxima oportunidade →" not in page.text
    response = await api_client.post(
        f"/leads/{ids.lead}/review",
        data={"decision": "approved", "reviewer": "Ana", "continue_review": "true"},
    )
    assert f"/leads/{ids.lead}?" in response.headers["location"]
    assert "Nenhuma oportunidade aguarda" in (await api_client.get("/dashboard")).text
