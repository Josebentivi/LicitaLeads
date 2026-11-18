from datetime import UTC, datetime

import pytest

from app.connectors.pncp import PNCPConnector
from app.services.event_detection import DocumentEvidence, EventDetector
from app.services.event_detection.authority import assertion_diagnostic, nonaward_supplier
from app.services.llm.analyzer import EvidenceBoundLLMEventAnalyzer, LLMEvidenceError
from app.services.llm.providers import response_schema
from app.services.scheduler_control import control_state, pause_file, set_component_paused


@pytest.mark.parametrize(
    "text,title,reason",
    [
        ("A proposta foi adjudicada.", "MINUTA-ATA-REGISTRO-PRECOS.docx", "template_document"),
        ("A proposta foi adjudicada.", "Minuta de Ata.pdf", "template_document"),
        ("A licitação foi homologada.", "Minuta de Contrato.pdf", "template_document"),
        ("A empresa foi declarada vencedora.", "ANEXO_MINUTA-de Ata.pdf", "template_document"),
        (
            "Arrematante; Desclassificado; Inabilitado, Classificado e Rejeitado.",
            "Ata.pdf",
            "status_legend",
        ),
        ("A empresa será inabilitada se descumprir o edital.", "Anexo.pdf", "future_rule"),
        (
            "A recorrente alega a desclassificação da proposta.",
            "Recurso.pdf",
            "unconfirmed_allegation",
        ),
    ],
)
def test_nonacts_do_not_create_events(text, title, reason):
    assert assertion_diagnostic(text, title) == reason
    assert EventDetector().detect(DocumentEvidence(text=text, document_title=title)) == []


def test_genuine_decision_can_discuss_a_minuta():
    text = "Após avaliar a minuta, a empresa ALFA foi inabilitada por falta de certidão."
    assert assertion_diagnostic(text, "Decisão.pdf") is None
    assert EventDetector().detect(DocumentEvidence(text=text, document_title="Decisão.pdf"))


def test_nonaward_is_not_a_supplier_but_zero_award_is_valid():
    assert nonaward_supplier("NÃO HOMOLOGADO")
    assert not PNCPConnector._has_result_identity(
        {"numeroItem": 1, "nomeRazaoSocialFornecedor": "NÃO HOMOLOGADO"}
    )
    assert PNCPConnector._has_result_identity(
        {
            "numeroItem": 1,
            "niFornecedor": "04252011000110",
            "nomeRazaoSocialFornecedor": "ALFA",
            "valorTotalHomologado": 0,
        }
    )


def test_party_in_another_act_cannot_be_assigned():
    quote = "A licitante ALFA, CNPJ 04.252.011/0001-10, foi inabilitada por falta de certidão."
    text = quote + "\nA licitante BETA, CNPJ 10.258.066/0001-30, foi habilitada."
    with pytest.raises(LLMEvidenceError, match="contradicts"):
        EvidenceBoundLLMEventAnalyzer.validate(
            [
                {
                    "event_type": "INELIGIBLE",
                    "company_cnpj": "10258066000130",
                    "company_name": "BETA",
                    "evidence_quotes": [{"text": quote}],
                    "confidence": 0.9,
                    "requires_manual_review": True,
                }
            ],
            text,
        )


def test_date_without_time_does_not_invent_an_instant():
    quote = "Em 02/11/2026, a licitante ALFA foi inabilitada."
    analysis = EvidenceBoundLLMEventAnalyzer.validate(
        [
            {
                "event_type": "INELIGIBLE",
                "event_date": datetime(2026, 11, 2, 13, tzinfo=UTC),
                "evidence_quotes": [{"text": quote}],
                "confidence": 0.9,
                "requires_manual_review": True,
            }
        ],
        quote,
    )[0]
    assert analysis.event_date is None


def test_strict_schema_requires_all_properties():
    spec = response_schema()
    assert spec["json_schema"]["strict"] is True
    root = spec["json_schema"]["schema"]
    item = root["properties"]["analyses"]["items"]
    assert root["additionalProperties"] is False
    assert set(item["required"]) == set(item["properties"])
    assert item["additionalProperties"] is False


def test_pncp_previous_version_proofs_are_not_reused():
    from types import SimpleNamespace

    from app.services.ingestion.verified_versions import adapter_key

    old = SimpleNamespace(name="pncp", adapter_version=1)
    current = SimpleNamespace(name="pncp", adapter_version=PNCPConnector.adapter_version)
    assert adapter_key(old) != adapter_key(current)


def test_legacy_pause_and_independent_resume(tmp_path):
    from app.config import Settings

    settings = Settings(_env_file=None, raw_data_storage_path=tmp_path / "raw")
    pause_file(settings).touch()
    assert control_state(settings) == {"collections": True, "llm": True}
    set_component_paused("llm", False, settings=settings)
    assert control_state(settings) == {"collections": True, "llm": False}
    set_component_paused("collections", False, settings=settings)
    assert control_state(settings) == {"collections": False, "llm": False}


def test_damaged_control_state_stays_paused(tmp_path):
    from app.config import Settings

    settings = Settings(_env_file=None, raw_data_storage_path=tmp_path / "raw")
    (tmp_path / "scheduler.controls.json").write_text("broken")
    assert all(control_state(settings).values())
