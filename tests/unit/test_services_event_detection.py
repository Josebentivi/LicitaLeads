# ruff: noqa: E501

from app.services.event_detection import (
    CompanyReference,
    DocumentEvidence,
    EventDetector,
    EventType,
    ParticipantDetector,
    ParticipationRole,
    ReasonCategory,
    associate_company,
)

COMPANY = CompanyReference("company-1", "ABC Equipamentos Ltda.", "04.252.011/0001-10")


def test_detects_ineligibility_with_exact_company_and_evidence() -> None:
    evidence = DocumentEvidence(
        text=(
            "Item 4. A licitante ABC Equipamentos Ltda., CNPJ 04.252.011/0001-10, "
            "foi inabilitada por não apresentar a certidão exigida no item 8.3."
        ),
        page_number=12,
        source_url="https://example.gov.br/ata.pdf",
        document_type="ata_sessao",
    )
    events = EventDetector().detect(evidence, known_companies=[COMPANY])
    event = next(item for item in events if item.event_type is EventType.INELIGIBLE)
    assert event.company_external_id == "company-1"
    assert event.reason_category is ReasonCategory.MISSING_DOCUMENT
    assert event.evidence.page_number == 12
    assert event.confidence == 1.0
    assert not event.requires_manual_review


def test_ignores_edital_rules_and_negated_events() -> None:
    edital = DocumentEvidence(
        text="A licitante será inabilitada se não apresentar certidão.", document_type="edital"
    )
    assert EventDetector().detect(edital, known_companies=[COMPANY]) == []
    negated = DocumentEvidence(
        text="A empresa ABC Equipamentos Ltda. não foi inabilitada após a diligência.",
        document_type="ata",
    )
    assert not any(
        event.event_type is EventType.INELIGIBLE
        for event in EventDetector().detect(negated, known_companies=[COMPANY])
    )


def test_ambiguous_name_is_not_automatically_associated() -> None:
    companies = [
        CompanyReference("1", "Alpha Serviços Ltda.", None),
        CompanyReference("2", "Alpha Serviços S.A.", None),
    ]
    company, method, review = associate_company("Alpha Serviços foi inabilitada", companies)
    assert company is None
    assert method == "ambiguous_name"
    assert review


def test_participant_detection_and_structured_winner_do_not_invent_losers() -> None:
    evidence = DocumentEvidence(
        text="Item 3 - Licitante ABC Equipamentos Ltda., CNPJ 04.252.011/0001-10, vencedora.",
        document_type="termo_julgamento",
    )
    participants = ParticipantDetector().detect(evidence, known_companies=[COMPANY])
    assert len(participants) == 1
    assert participants[0].role is ParticipationRole.WINNER
    winner = ParticipantDetector.from_structured_winner(COMPANY, evidence, item_number="3")
    assert winner.role is ParticipationRole.WINNER
    assert winner.company_external_id == "company-1"


def test_participant_name_extraction_removes_document_labels() -> None:
    evidence = DocumentEvidence(
        text="Item 3 - Licitante ABC Equipamentos Ltda., CNPJ 04.252.011/0001-10, inabilitada.",
        document_type="ata",
    )
    participant = ParticipantDetector().detect(evidence)[0]
    assert participant.company_name == "ABC Equipamentos Ltda."
