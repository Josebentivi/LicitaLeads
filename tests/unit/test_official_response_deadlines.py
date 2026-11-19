"""Source wording and party scope govern official response windows."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from app.models import ProcurementEventType, ProcurementPhase
from app.services.deadlines.response import parse_official_response_deadline, response_scope
from app.services.event_detection import (
    CompanyReference,
    DocumentEvidence,
    EventDetector,
    EventType,
)
from app.services.event_detection.detector import describe_event_scope, event_act_excerpt
from app.services.procurement_phase import derive_procurement_phase


def test_declared_winner_does_not_borrow_another_partys_reason_or_imply_adjudication():
    winner = CompanyReference("company", "ABC Equipamentos Ltda.", "04.252.011/0001-10")
    text = (
        "Lote 2. A empresa ABC Equipamentos Ltda., CNPJ 04.252.011/0001-10, "
        "foi declarada vencedora na etapa de julgamento. "
        "Quanto a outra licitante, não apresentou a certidão exigida no item 8."
    )
    detected = EventDetector().detect(
        DocumentEvidence(text=text, document_type="ata_sessao"), known_companies=[winner]
    )
    event = next(row for row in detected if row.event_type is EventType.WINNER_DECLARED)
    assert event.company_external_id == winner.external_id
    assert event.normalized_reason is None
    assert event.item_number is None
    saved = SimpleNamespace(event_type=ProcurementEventType.WINNER_DECLARED, raw_description=text)
    display = describe_event_scope(saved, text, company=winner)
    assert display.lot_number == "2"
    assert display.stage is ProcurementPhase.JUDGMENT
    assert "certidão" not in display.reason
    assert (
        derive_procurement_phase(
            SimpleNamespace(status="Em julgamento", session_start_at=None, proposal_end_at=None),
            last_event=saved,
        )
        is ProcurementPhase.JUDGMENT
    )


def test_appeal_opening_and_filing_are_different_acts():
    company = CompanyReference("company", "ABC Equipamentos Ltda.", "04.252.011/0001-10")
    opening = (
        "Fica aberto o prazo para apresentação de recurso da ABC Equipamentos Ltda., "
        "CNPJ 04.252.011/0001-10, até 18/09/2026 às 17h30."
    )
    acts = EventDetector().detect(
        DocumentEvidence(text=opening, document_type="ata_sessao"), known_companies=[company]
    )
    assert {act.event_type for act in acts} == {EventType.APPEAL_WINDOW_OPENED}
    deadline = parse_official_response_deadline(
        EventType.APPEAL_WINDOW_OPENED,
        opening,
        published_at=datetime(2026, 9, 17, 15, tzinfo=UTC),
        timezone="America/Sao_Paulo",
    )
    assert deadline is not None
    assert deadline.kind == "appeal_submission"
    assert deadline.ends_at == datetime(2026, 9, 18, 20, 30, tzinfo=UTC)
    assert (
        parse_official_response_deadline(
            EventType.APPEAL_SUBMITTED,
            opening,
            published_at=datetime(2026, 9, 17, 15, tzinfo=UTC),
            timezone="America/Sao_Paulo",
        )
        is None
    )


@pytest.mark.parametrize(
    "act",
    [
        "ABC foi convocada para diligência até 18/09/2026.",
        "ABC foi convocada para diligência no prazo de 2 horas.",
        "ABC será convocada para diligência até 18/09/2026 às 17h30.",
        "ABC foi convocada para diligência até 18/09/2026 às 17h30 ou até 18/09/2026 às 18h00.",
    ],
)
def test_uncertain_or_conflicting_windows_are_left_for_review(act):
    assert (
        parse_official_response_deadline(
            EventType.DILIGENCE_REQUESTED,
            act,
            published_at=datetime(2026, 9, 17, 15, tzinfo=UTC),
            timezone="America/Sao_Paulo",
        )
        is None
    )


def test_elapsed_window_requires_timestamp_of_the_act():
    deadline = parse_official_response_deadline(
        EventType.DILIGENCE_REQUESTED,
        "17/09/2026 14:00 — ABC foi convocada para diligência no prazo de 2 horas.",
        published_at=None,
        timezone="America/Sao_Paulo",
    )
    assert deadline is not None
    assert deadline.starts_at == datetime(2026, 9, 17, 17, tzinfo=UTC)
    assert deadline.ends_at == datetime(2026, 9, 17, 19, tzinfo=UTC)


def test_official_clock_uses_complete_durations_and_explicit_zones():
    act = (
        "[18/11/2026 09:00 UTC] ABC foi convocada para diligência no prazo de 1 hora e 30 minutos."
    )
    options = {
        "published_at": datetime(2026, 11, 18, 9, tzinfo=UTC),
        "timezone": "America/Sao_Paulo",
    }
    window = parse_official_response_deadline(EventType.DILIGENCE_REQUESTED, act, **options)
    assert window is not None
    assert window.starts_at == datetime(2026, 11, 18, 9, tzinfo=UTC)
    assert window.ends_at == datetime(2026, 11, 18, 10, 30, tzinfo=UTC)
    assert (
        parse_official_response_deadline(
            EventType.DILIGENCE_REQUESTED, act.replace("minutos.", "minutos úteis."), **options
        )
        is None
    )
    until = "Foi aberto o prazo para recurso até 20/11/2026 às 11:30 UTC."
    deadline = parse_official_response_deadline(EventType.APPEAL_WINDOW_OPENED, until, **options)
    assert deadline is not None and deadline.ends_at == datetime(2026, 11, 20, 11, 30, tzinfo=UTC)
    assert (
        parse_official_response_deadline(
            EventType.APPEAL_WINDOW_OPENED, until.replace("11:30", "11:30:5"), **options
        )
        is None
    )


def test_response_scope_does_not_extrapolate_restricted_acts():
    limited = response_scope("Foi suspensa a sessão do certame apenas para os lotes 2 e 3.")
    assert limited["scope"] == "lot_list" and limited["lots"] == "|2|3|"
    exception = response_scope("Foi suspensa toda a sessão do certame, exceto o lote 2.")
    assert exception["scope"] == "procurement_except" and exception["excluded_lots"] == "|2|"
    limited_until = response_scope("Suspenso o lote 2 até 20/11/2026.")
    assert limited_until["scope"] == "lot" and limited_until["lote"] == "2"
    assert response_scope("Foi suspensa a sessão do certame.")["scope"] == "unknown"


def test_incomplete_named_start_cannot_fall_back_to_publication_or_the_log_timestamp():
    publication = datetime(2026, 11, 18, 15, tzinfo=UTC)
    options = {"published_at": publication, "timezone": "America/Sao_Paulo"}
    act = "Fica aberto o prazo para recurso a partir de 20/11/2026 até 23/11/2026 às 18:00."
    assert parse_official_response_deadline(EventType.APPEAL_WINDOW_OPENED, act, **options) is None
    exact = act.replace("20/11/2026 até", "20/11/2026 às 09:00 até")
    window = parse_official_response_deadline(EventType.APPEAL_WINDOW_OPENED, exact, **options)
    assert window is not None
    assert window.starts_at == datetime(2026, 11, 20, 12, tzinfo=UTC)
    relative = (
        "18/11/2026 14:00 — ABC foi convocada para diligência no prazo de 2 horas "
        "a partir do recebimento da notificação."
    )
    assert (
        parse_official_response_deadline(EventType.DILIGENCE_REQUESTED, relative, **options) is None
    )


def test_counterarguments_distinguish_recipient_from_appellant():
    appellant = CompanyReference("abc", "ABC Equipamentos Ltda.", "04.252.011/0001-10")
    recipient = CompanyReference("beta", "Beta Serviços Ltda.", "10.258.066/0001-30")
    beginning = "Foi aberto o prazo para apresentação de contrarrazões"
    reference = "ao recurso interposto pela empresa ABC Equipamentos Ltda., CNPJ 04.252.011/0001-10, até 23/11/2026 às 18:00."
    detector = EventDetector()
    event = detector.detect(
        DocumentEvidence(text=f"{beginning} {reference}", document_type="ata_sessao"),
        known_companies=[appellant],
    )[0]
    assert event.event_type is EventType.COUNTERARGUMENT_OPENED
    assert event.company_external_id is None and event.requires_manual_review
    addressed = (
        f"{beginning} para a empresa Beta Serviços Ltda., CNPJ 10.258.066/0001-30, {reference}"
    )
    event = detector.detect(
        DocumentEvidence(text=addressed, document_type="ata_sessao"),
        known_companies=[appellant, recipient],
    )[0]
    assert event.company_external_id == recipient.external_id

    passive = (
        "Em face do recurso interposto por ABC Equipamentos Ltda., CNPJ 04.252.011/0001-10, "
        "o arrematante foi intimado e foi aberto prazo para contrarrazões até 20/11/2026 às 18:00."
    )
    event = detector.detect(
        DocumentEvidence(text=passive, document_type="ata_sessao"), known_companies=[appellant]
    )[0]
    assert event.company_external_id is None and event.requires_manual_review
    similar = f"{beginning} para a empresa ABC Equipamentos e Serviços Ltda., {reference}"
    event = detector.detect(
        DocumentEvidence(text=similar, document_type="ata_sessao"), known_companies=[appellant]
    )[0]
    assert event.company_external_id is None and event.requires_manual_review
    explicitly_named = f"{beginning} para a empresa Beta Serviços Ltda. {reference}"
    event = detector.detect(
        DocumentEvidence(text=explicitly_named, document_type="ata_sessao"),
        known_companies=[appellant, recipient],
    )[0]
    assert event.company_external_id == recipient.external_id


def test_two_deadline_openings_keep_each_literal_act_and_lot():
    company = CompanyReference("abc", "ABC Equipamentos Ltda.", "04.252.011/0001-10")
    acts = [
        "Lote 2: Foi aberto prazo para recurso para a empresa ABC Equipamentos Ltda. "
        "até 20/11/2026 às 18:00.",
        "Lote 3: Foi aberto prazo para recurso para a empresa ABC Equipamentos Ltda. "
        "até 22/11/2026 às 18:00.",
    ]
    events = EventDetector().detect(
        DocumentEvidence(text=" ".join(acts), document_type="ata_sessao"), known_companies=[company]
    )
    assert [event.evidence.text for event in events] == acts
    for event, lot in zip(events, ("2", "3"), strict=True):
        assert event.company_external_id == "abc"
        literal = event_act_excerpt(event.event_type, event.evidence.text, company=company)
        assert literal and response_scope(literal)["lote"] == lot


def test_wrapped_text_retains_identity_and_deadline_without_crossing_vendor_rows():
    company = CompanyReference("abc", "ABC Equipamentos Ltda.", "04.252.011/0001-10")
    other = CompanyReference("beta", "Beta Serviços Ltda.", "10.258.066/0001-30")
    text = (
        "A licitante ABC Equipamentos Ltda., CNPJ 04.252.011/0001-10,\n"
        "foi inabilitada por falta de certidão\n"
        "A empresa Beta Serviços Ltda., CNPJ 10.258.066/0001-30, foi habilitada."
    )
    acts = EventDetector().detect(
        DocumentEvidence(text=text, document_type="ata_sessao"), known_companies=[company, other]
    )
    assert {x.event_type: x.company_external_id for x in acts} == {
        EventType.INELIGIBLE: "abc",
        EventType.QUALIFIED: "beta",
    }
    opening = (
        "Fica aberto o prazo para apresentação de recurso à empresa ABC Equipamentos Ltda., CNPJ 04.252.011/0001-10,\n"
        "até 23/11/2026 às 18:00."
    )
    act = event_act_excerpt(
        EventType.APPEAL_WINDOW_OPENED, opening, company=company, include_deadline=True
    )
    assert act and "\naté 23/11/2026" in act
    assert (
        parse_official_response_deadline(
            EventType.APPEAL_WINDOW_OPENED,
            act,
            published_at=datetime(2026, 11, 18, 15, tzinfo=UTC),
            timezone="America/Sao_Paulo",
        )
        is not None
    )


def test_table_row_ending_in_company_abbreviation_remains_a_hard_boundary():
    company = CompanyReference("abc", "ABC Equipamentos Ltda.", "04.252.011/0001-10")
    other = CompanyReference("beta", "Beta Serviços Ltda.", "10.258.066/0001-30")
    text = (
        "04.252.011/0001-10 | não apresentou a certidão exigida | ABC Equipamentos Ltda.\n"
        "10.258.066/0001-30 | Beta Serviços Ltda. foi declarada vencedora."
    )
    acts = EventDetector().detect(
        DocumentEvidence(text=text, document_type="ata_sessao", locator="sheet:Resultados"),
        known_companies=[company, other],
    )
    winner = next(act for act in acts if act.event_type is EventType.WINNER_DECLARED)
    assert winner.company_external_id == "beta" and winner.normalized_reason is None
