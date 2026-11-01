"""Only evidenced bidder totals may become monetary participation facts."""

from decimal import Decimal

import pytest

from app.services.event_detection.participation_values import extract_values, reference_status
from app.services.event_detection.types import DocumentEvidence

SAO_JORGE = "10258066000130"
SUBJECT = "Licitante: SÃO JORGE DISTRIBUIDORA HOSPITALAR LTDA, CNPJ 10.258.066/0001-30.\n"


def test_explicit_proposals_in_multiple_lots_are_not_budgets_or_summed():
    text = SUBJECT + (
        "Os valores são: Lote 04, estimado em R$ 697.628,88 e proposto R$ 480.823,20; "
        "Lote 05, estimado em R$ 1.782.456,00 e proposto R$ 1.024.545,60."
    )
    result = extract_values([DocumentEvidence(text, page_number=1)])
    assert [(r.company_cnpj, r.lot_number, r.amount, r.scope) for r in result.values] == [
        (SAO_JORGE, "4", Decimal("480823.20"), "lot"),
        (SAO_JORGE, "5", Decimal("1024545.60"), "lot"),
    ]
    assert all(r.kind == "proposal" for r in result.values)


def test_subject_and_single_lot_can_be_in_previous_page():
    pages = [
        DocumentEvidence(SUBJECT + "Objeto: Lote 08.", page_number=1),
        DocumentEvidence(
            "A licitante apresentou proposta readequada, assinada em 27/10/2026, "
            "no valor de R$ 1.066.361,27. O valor estimado do lote é de R$ 1.516.045,36.",
            page_number=2,
        ),
    ]
    result = extract_values(pages)
    assert len(result.values) == 1
    value = result.values[0]
    assert (value.amount, value.kind, value.lot_number) == (
        Decimal("1066361.27"),
        "revised_proposal",
        "8",
    )
    assert value.subject_evidence.page_number == 1 and value.evidence.page_number == 2


def test_cost_quotation_does_not_establish_bidder_or_monetary_fact():
    text = (
        "Item 8 (enalapril 20 mg, custo R$ 0,0876). O documento de custo é a Proposta nº 127224, "
        "emitida pela DPROSMED Distribuidora de Produtos Médico-Hospitalares Ltda "
        "(CNPJ 11.449.180/0001-00), com identificação do fornecedor e do cliente, "
        "preço unitário R$ 43,7948 por caixa de 500 comprimidos."
    )
    assert reference_status(text, "11449180000100") == "invalidated"
    assert not extract_values([DocumentEvidence(SUBJECT + text)]).values


@pytest.mark.parametrize(
    "phrase",
    [
        "O valor estimado é de R$ 100.000,00.",
        "O custo da proposta é R$ 100.000,00.",
        "Proposta com preço unitário R$ 100.000,00 por caixa.",
        "Seguro garantia da proposta com limite de R$ 100.000,00.",
        "A proposta é de R$ -100.000,00.",
        "Por exemplo, proposta R$ 100.000,00.",
    ],
)
def test_non_totals_and_negative_values_are_not_accepted(phrase):
    assert not extract_values([DocumentEvidence(SUBJECT + phrase)]).values


def test_official_final_and_initial_zero_are_distinct_and_valid():
    result = extract_values(
        [
            DocumentEvidence(
                SUBJECT
                + "Proposta inicial total R$ 0,00. Resultado final homologado no valor total de R$ 20.000,00."
            )
        ]
    )
    assert [(r.kind, r.amount) for r in result.values] == [
        ("initial_proposal", Decimal("0.00")),
        ("final_result", Decimal("20000.00")),
    ]


def test_multiple_bidders_require_local_attribution():
    result = extract_values(
        [
            DocumentEvidence(
                SUBJECT
                + "Licitante: GRANFARMA LTDA, CNPJ 36.863.682/0001-03.\n"
                + "\n" * 320
                + "Proposta R$ 90.000,00."
            )
        ]
    )
    assert not result.values and result.diagnostics


def test_ordinary_issuer_is_not_proven_to_be_cost_supplier():
    text = "Proposta inicial emitida pela São Jorge, CNPJ 10.258.066/0001-30."
    assert reference_status(text, SAO_JORGE) == "review"


def test_page_excerpt_uses_global_stored_offsets():
    text = SUBJECT + "Proposta readequada do Lote 02 R$ 495.896,96."
    result = extract_values([DocumentEvidence(text, page_number=3, start_offset=900)])
    finding = result.values[0]
    assert finding.evidence.start_offset >= 900
    start, end = finding.evidence.start_offset - 900, finding.evidence.end_offset - 900
    assert text[start:end] == finding.evidence.text


def test_aggregate_plural_lots_is_reviewed_without_a_process_total():
    result = extract_values([DocumentEvidence(SUBJECT + "Proposta para lotes 4 e 5 R$ 50.000,00.")])
    assert not result.values and result.diagnostics


def test_item_within_lot_is_never_promoted_to_the_lot_total():
    result = extract_values(
        [DocumentEvidence(SUBJECT + "Lote 05, Item 08: proposta total R$ 50.000,00.")]
    )
    assert [(v.scope, v.lot_number, v.local_item_number) for v in result.values] == [
        ("lot_item", "5", "8")
    ]


def test_adjacent_numeric_cells_are_not_concatenated_into_money():
    result = extract_values([DocumentEvidence(SUBJECT + "Proposta total R$ 100 1.234,00.")])
    assert not result.values


def test_repeated_cnpj_context_checks_are_bounded_per_page(monkeypatch):
    from app.services.event_detection import ParticipantDetector
    from app.services.event_detection import participation_values as parser

    original = parser.reference_status
    calls = 0

    def counted(*args):
        nonlocal calls
        calls += 1
        return original(*args)

    monkeypatch.setattr(parser, "reference_status", counted)
    page = DocumentEvidence((SUBJECT + "Proposta total R$ 10,00.\n") * 100)
    assert extract_values([page]).values
    assert calls == 1
    calls = 0
    assert ParticipantDetector().detect(page)
    assert calls == 1


def test_agency_before_bidder_header_is_not_itself_a_bidder():
    text = "Prefeitura, CNPJ 01.612.325/0001-98.\n" + SUBJECT + "Proposta total R$ 10,00."
    assert reference_status(text, "01612325000198") == "review"
    assert reference_status(text, SAO_JORGE) == "confirmed"
    assert {v.company_cnpj for v in extract_values([DocumentEvidence(text)]).values} == {SAO_JORGE}


def test_flattened_award_columns_do_not_become_company_totals():
    text = (
        "Fornecedor vencedor: São Jorge, CNPJ 10.258.066/0001-30.\n"
        "Unitário\nTotal\nUnitário\nItem\nQuant.\nAdjudicado Adjudicado Orçado Total Orçado\n"
        "1 25,00 Diária\nR$ 3.012,62\nR$ 75.315,50\nR$ 6.025,23\nR$ 150.630,75\n"
    )
    result = extract_values([DocumentEvidence(text)])
    assert not result.values and result.diagnostics


def test_explicit_company_total_after_table_is_distinct_from_row_totals():
    text = SUBJECT + "Unitário\nItem\nProposta total R$ 10,00.\nValor global da proposta R$ 20,00."
    result = extract_values([DocumentEvidence(text)])
    assert [v.amount for v in result.values] == [Decimal("20.00")]
