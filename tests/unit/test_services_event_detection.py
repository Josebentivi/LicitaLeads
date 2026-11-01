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


def test_detects_session_diligence_and_best_offer_events() -> None:
    evidence = DocumentEvidence(
        text=(
            "A empresa ABC Equipamentos Ltda., CNPJ 04.252.011/0001-10, "
            "foi convocada para diligencia e devera apresentar documentos no prazo de 2 horas. "
            "O detentor da melhor oferta e ABC Equipamentos Ltda. "
            "A sessao publica foi aberta."
        ),
        document_type="ata_de_sessao",
    )
    events = EventDetector().detect(evidence, known_companies=[COMPANY])
    types = {event.event_type for event in events}
    assert EventType.DILIGENCE_REQUESTED in types
    assert EventType.DOCUMENTATION_REQUESTED in types
    assert EventType.BEST_OFFER_DECLARED in types
    assert EventType.SESSION_OPENED in types
    diligence = next(event for event in events if event.event_type is EventType.DILIGENCE_REQUESTED)
    assert diligence.company_external_id == "company-1"


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


def test_document_type_confidence_folds_accents() -> None:
    from app.services.event_detection.detector import _document_base_confidence

    assert _document_base_confidence("Ata de Sess\u00e3o") == 0.96
    assert _document_base_confidence("Relat\u00f3rio de Sess\u00e3o") == 0.95
    assert _document_base_confidence("Decis\u00e3o de Recurso") == 0.92
    assert _document_base_confidence("Termo de Refer\u00eancia") == 0.0


def test_participant_detector_does_not_trust_negated_or_conditional_roles() -> None:
    evidence = DocumentEvidence(
        text=(
            "A empresa ABC Equipamentos Ltda., CNPJ 04.252.011/0001-10, "
            "nao foi declarada vencedora e podera ser inabilitada apos a analise."
        ),
        page_number=1,
        source_url="https://example.gov.br/ata.pdf",
        document_type="ata_sessao",
    )
    participants = ParticipantDetector().detect(evidence, known_companies=[COMPANY])

    assert len(participants) == 1
    assert participants[0].role is ParticipationRole.PARTICIPANT
    assert participants[0].status is None
    assert participants[0].requires_manual_review is True
