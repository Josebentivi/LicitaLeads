"""Company origin filters and dossier navigation use the same current, exact links."""

from bs4 import BeautifulSoup

from app.models import (
    Company,
    Contract,
    Participant,
    Payment,
    PriceRegistry,
    PriceRegistryItem,
    Procurement,
    ProcurementEvent,
)
from app.models.participation import FactVerification


async def test_activity_location_is_distinct_from_headquarters_and_stale_facts(
    api_client, database
):
    async with database.sessions() as session, session.begin():
        company = Company(
            cnpj="00000000000191",
            legal_name="Empresa do teste",
            normalized_name="EMPRESA",
            uf="SP",
            municipality="Campinas",
        )
        other = Company(cnpj="11222333000181", normalized_name="OUTRA", uf="MA")
        process = Procurement(
            source="pncp",
            external_id="public",
            pncp_control_number="PUB-1",
            municipality="Bacabal",
            uf="MA",
            agency_cnpj="12345678000199",
        )
        wrong_place = Procurement(
            source="pncp",
            external_id="other-source",
            municipality="Timon",
            agency_cnpj="00000000000192",
        )
        session.add_all([company, other, process, wrong_place])
        await session.flush()
        registry = PriceRegistry(
            source="pncp", external_id="registry", linked_pncp_control_number="PUB-1"
        )
        current_participation = Participant(
            company_id=company.id, procurement_id=process.id, source="document"
        )
        invalid = Participant(
            company_id=company.id, procurement_id=wrong_place.id, source="document"
        )
        session.add_all([registry, current_participation, invalid])
        await session.flush()
        session.add_all(
            [
                FactVerification(
                    entity_type="participant",
                    entity_id=invalid.id,
                    status="invalidated",
                    reason="Atribuição retirada",
                    rule_version="test",
                ),
                Contract(
                    source="pncp",
                    external_id="exact",
                    supplier_cnpj=company.cnpj,
                    procurement_control_number="PUB-1",
                    agency_cnpj=process.agency_cnpj,
                ),
                Contract(
                    source="pncp",
                    external_id="agency-conflict",
                    supplier_cnpj=company.cnpj,
                    procurement_id=wrong_place.id,
                    agency_cnpj=process.agency_cnpj,
                ),
                Contract(
                    source="pncp",
                    external_id="conflict",
                    company_id=other.id,
                    supplier_cnpj=company.cnpj,
                    agency_name="Órgão divergente",
                ),
                PriceRegistryItem(
                    price_registry_id=registry.id, item_number="1", supplier_cnpj=company.cnpj
                ),
                PriceRegistryItem(
                    price_registry_id=registry.id, item_number="2", supplier_cnpj=company.cnpj
                ),
                Payment(
                    source="municipal:imperatriz",
                    external_id="current",
                    supplier_cnpj=company.cnpj,
                    phase="pago_agregado",
                    is_current=True,
                ),
                Payment(
                    source="municipal:caxias",
                    external_id="old",
                    supplier_cnpj=company.cnpj,
                    phase="pago_agregado",
                    is_current=False,
                ),
            ]
        )
        cnpj = company.cnpj

    async def companies(**filters):
        response = await api_client.get("/api/companies", params=filters)
        assert response.status_code == 200
        return response.json()["items"]

    rows = await companies(origin="payments", activity_location="Imperatriz", uf="SP")
    assert [row["company"]["cnpj"] for row in rows] == [cnpj]
    activity = rows[0]["activity"]
    assert activity["counts"] == {
        "contracts": 2,
        "participations": 1,
        "price_registries": 1,
        "payments": 1,
    }
    assert set(activity["locations"]) == {"Bacabal/MA", "Imperatriz/MA"}
    assert activity["aggregates"] == 1
    assert await companies(origin="contracts", activity_location="Bacabal")
    assert await companies(origin="price_registries", activity_location="Bacabal")
    for filters in [
        {"activity_location": "Campinas"},
        {"activity_location": "Timon"},
        {"activity_location": "divergente"},
        {"activity_location": "Caxias"},
        {"origin": "contracts", "activity_location": "Imperatriz"},
        {"origin": "payments", "activity_location": "Imperatriz", "uf": "MA"},
    ]:
        assert await companies(**filters) == []

    listing = BeautifulSoup((await api_client.get("/empresas?origin=payments")).text, "html.parser")
    link = listing.find("a", string="Empresa do teste")
    assert link and "section=payments" in link["href"]
    page = BeautifulSoup((await api_client.get(link["href"])).text, "html.parser")
    assert page.select_one("details[open]#company-payments .company-record")
    direct = BeautifulSoup((await api_client.get(f"/empresas/{cnpj}")).text, "html.parser")
    assert direct.select_one("details[open]#company-leads")
    filtered = await api_client.get(f"/empresas/{cnpj}?section=procurements&status=documented")
    selection = BeautifulSoup(filtered.text, "html.parser")
    assert selection.select_one("#company-procurements option[value=documented][selected]")
    assert len(selection.select("#company-procurements .company-record")) == 1


async def test_company_losses_count_distinct_verified_processes_in_each_dimension(
    api_client, database
):
    async with database.sessions() as session, session.begin():
        company = Company(cnpj="00000000000191", normalized_name="EMPRESA")
        current = Procurement(source="pncp", external_id="atual")
        historical = Procurement(source="pncp", external_id="retirada")
        session.add_all([company, current, historical])
        await session.flush()
        events = [
            ProcurementEvent(
                company_id=company.id,
                procurement_id=process.id,
                source="document",
                event_type=event,
                reason_category=reason,
                raw_description="Trecho",
                confidence=1,
            )
            for process, event, reason in [
                (current, "DISQUALIFIED", "OTHER"),
                (current, "DISQUALIFIED", "FAILURE_TO_RESPOND"),
                (current, "INELIGIBLE", "OTHER"),
                (historical, "DISQUALIFIED", "OTHER"),
            ]
        ]
        session.add_all(events)
        await session.flush()
        session.add(
            FactVerification(
                entity_type="procurement_event",
                entity_id=events[-1].id,
                status="invalidated",
                reason="Atribuição retirada",
                rule_version="teste",
            )
        )
    response = await api_client.get("/api/companies/00000000000191/overview")
    assert response.status_code == 200
    assert response.json()["failure_breakdown"] == {
        "documented_losses": 1,
        "by_event": {"DISQUALIFIED": 1, "INELIGIBLE": 1},
        "by_reason": {"OTHER": 1, "FAILURE_TO_RESPOND": 1},
    }
