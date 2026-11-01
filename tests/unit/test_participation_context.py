"""Published session rows and explicit decisions are stronger than generic mentions."""

from decimal import Decimal

import pytest

from app.services.event_detection.participation_values import extract_values, reference_status
from app.services.event_detection.types import DocumentEvidence


@pytest.mark.parametrize(
    "status", ["Classificado", "Desclassificado", "Inabilitado", "Arrematante"]
)
def test_session_status_immediately_after_cnpj_proves_participation(status):
    from app.services.event_detection import ParticipantDetector

    text = f"Classif.\nFornecedor\nCPF/CNPJ\nSituação\nValor Global\n2º\nALPHA COMPANY\n43.132.670/0001-74\n{status}\n20.975,16"
    assert reference_status(text, "43132670000174") == "confirmed"
    assert any(
        p.company_cnpj == "43132670000174"
        for p in ParticipantDetector().detect(DocumentEvidence(text))
    )


def test_explicit_award_decision_has_evidenced_final_lot_total():
    text = "HOMOLOGO, nos termos do art. 71, inciso IV, da Lei Federal nº 14.133/2021, o resultado do procedimento licitatório em favor da empresa CONSTRUMIL EMPREENDIMENTOS E SERVICOS LTDA, CNPJ nº 29.497.557/0001-61, lote 01, no valor global de R$ 720.669,56."
    assert reference_status(text, "29497557000161") == "confirmed"
    result = extract_values([DocumentEvidence(text, page_number=1)])
    assert [(v.company_cnpj, v.kind, v.lot_number, v.amount) for v in result.values] == [
        ("29497557000161", "final_result", "1", Decimal("720669.56"))
    ]


def test_parties_to_appeals_and_counterarguments_are_explicit_bidders():
    text = "Trata-se dos recursos administrativos interpostos, com fundamento no art. 165, pelas empresas R SOUSA COMERCIO LTDA (CNPJ 27.517.764/0001-05) e A. G. DA CRUZ LTDA (CNPJ 69.386.324/0001-06), bem como das contrarrazões apresentadas pela empresa CLAREAR SERVIÇOS LTDA (CNPJ 41.539.585/0001-08), declarada habilitada e vencedora do certame."
    for cnpj in ["27517764000105", "69386324000106", "41539585000108"]:
        assert reference_status(text, cnpj) == "confirmed"


def test_session_chat_attributes_the_act_to_its_own_cnpj():
    text = (
        "Sistema: O fornecedor 07.045.994/0001-01 - Rosilene Tonatto Spazzini foi inabilitado. "
        "Motivo: CONFORME CHAT. O fornecedor 58.059.488/0001-02 - BGS SOLUCOES LTDA foi inabilitado."
    )
    assert reference_status(text, "07045994000101") == "confirmed"
    assert reference_status(text, "58059488000102") == "confirmed"
    assert (
        reference_status(
            "PREFEITURA CNPJ: 06.424.618/0001-65\nACOLHER o pedido e DESCLASSIFICAR a empresa",
            "06424618000165",
        )
        == "review"
    )


def test_third_person_award_identifies_the_named_supplier():
    text = "A Autoridade Competente Adjudicou o(s) lote(s) em favor de:\nFornecedor: VERAS ENGENHARIA LTDA - CPF/CNPJ: 29.533.590/0001-08"
    assert reference_status(text, "29533590000108") == "confirmed"


def test_award_on_another_unpaginated_chunk_does_not_price_an_unattributed_total():
    pages = [
        DocumentEvidence(
            "HOMOLOGO o resultado em favor da empresa VERAS ENGENHARIA, CNPJ 29.533.590/0001-08.",
            locator="p:1",
        ),
        DocumentEvidence("Valor global R$ 50.000,00.", locator="p:2"),
    ]
    assert not extract_values(pages).values


def test_published_total_for_multiple_items_is_not_a_procurement_wide_amount():
    text = "Licitante: Empresa CNPJ 10.258.066/0001-30. O resultado foi homologado com adjudicação dos itens 1 a 7 e 9 à recorrente, pelo valor total de R$ 1.016.833,00."
    result = extract_values([DocumentEvidence(text, page_number=1)])
    assert not result.values and result.diagnostics


@pytest.mark.parametrize(
    "header",
    [
        "Validade das Propostas\nFornecedor\nCPF/CNPJ\nValidade (conforme edital)",
        "Intenções de Recurso\nCNPJ\nData de Envio\nIntenção\nJulgamento",
    ],
)
def test_explicit_session_table_header_identifies_row_owner(header):
    text = header + "\nC-MAX SOLUCOES\n55.828.488/0001-31\n180 dias"
    assert reference_status(text, "55828488000131") == "confirmed"


def test_generic_supplier_and_comparison_contract_are_not_current_participation():
    assert (
        reference_status(
            "Órgão licitante: Prefeitura Municipal CNPJ 01.612.325/0001-98", "01612325000198"
        )
        == "review"
    )
    for text in [
        "Fornecedor\nNi Fornecedor\nVANESSA\n09.421.172/0001-03\nMarca/modelo",
        "CONTRATOS\nÓrgão gerenciador\nEmpresa contratada\nSOS Tecnologia CNPJ 04.744.134/0001-78",
    ]:
        cnpj = "09421172000103" if "VANESSA" in text else "04744134000178"
        assert reference_status(text, cnpj) == "review"


def test_notice_rules_do_not_confirm_participant_even_with_row_like_status():
    assert (
        reference_status(
            "Proponente: Alpha CNPJ 43.132.670/0001-74\nDesclassificado", "43132670000174", "edital"
        )
        == "review"
    )
