from datetime import UTC, datetime, timedelta
from decimal import Decimal

from bs4 import BeautifulSoup

from app.models import FieldChange, Procurement, ProcurementItem, SourceRecord


async def test_item_search_filters_and_opens_the_precise_row(api_client, database):
    async with database.sessions.begin() as db:
        procurements = [
            Procurement(source="pncp", external_id=str(i), agency_name=agency)
            for i, agency in enumerate(("Secretaria de Cultura", "Secretaria de Educação"))
        ]
        db.add_all(procurements)
        await db.flush()
        for i, (procurement, unit) in enumerate(
            (
                (procurements[0], "kg"),
                (procurements[0], "L"),
                (procurements[1], "kg"),
            )
        ):
            db.add(
                ProcurementItem(
                    procurement_id=procurement.id,
                    item_number=str(i),
                    description="Tinta azul",
                    quantity=2,
                    unit=unit,
                    estimated_unit_value=Decimal("0.0025"),
                )
            )
    url = "/itens?q=tinta&agency=Cultura&unit=kg"
    listing = await api_client.get(url)
    soup = BeautifulSoup(listing.text, "html.parser")
    assert listing.status_code == 200 and "1 item(ns)" in soup.get_text()
    assert "0,0025" in soup.get_text()
    anchor = soup.select_one("tbody a")["href"]
    detail = await api_client.get(anchor)
    target = BeautifulSoup(detail.text, "html.parser")
    assert detail.status_code == 200 and target.find(id=anchor.split("#")[1])
    assert target.find("a", href=url)


async def test_changes_ignore_initial_load_and_remember_explicit_review(api_client, database):
    yesterday = datetime.now(UTC) - timedelta(days=1)
    async with database.sessions.begin() as db:
        process = Procurement(source="pncp", external_id="changes", title="Consulta")
        source = SourceRecord(
            source="pncp", endpoint="https://pncp.gov.br/api?token=private", response_hash="1" * 64
        )
        db.add_all([process, source])
        await db.flush()
        for decision, applied, value in [
            ("created", True, "PRIMEIRO"),
            ("stale_ignored", False, "ANTIGO"),
            ("updated", True, "RETIFICADO"),
        ]:
            db.add(
                FieldChange(
                    entity_id=process.id,
                    entity_type="procurement",
                    parent_id=process.id,
                    parent_type="procurement",
                    field_name="status",
                    source="pncp",
                    source_record_id=source.id,
                    previous_value="Aberto",
                    received_value=value,
                    observed_at=yesterday,
                    recorded_at=yesterday,
                    decision=decision,
                    applied=applied,
                )
            )
    listing = await api_client.get("/alteracoes")
    assert listing.status_code == 200
    soup = BeautifulSoup(listing.text, "html.parser")
    assert "RETIFICADO" in listing.text and "Aberto" in listing.text
    assert all(text not in listing.text for text in ("PRIMEIRO", "ANTIGO", "token=private"))
    assert soup.find("a", href="https://pncp.gov.br/api")
    marker = soup.select_one('form[action="/alteracoes/revisar"] input')["value"]
    marked = await api_client.post("/alteracoes/revisar", data={"ate": marker})
    assert marked.status_code == 303 and "licita_changes_review" in marked.cookies
    assert "RETIFICADO" not in (await api_client.get("/alteracoes")).text
    assert "RETIFICADO" in (await api_client.get("/alteracoes?todas=true")).text
