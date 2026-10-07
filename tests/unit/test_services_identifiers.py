# ruff: noqa: E501

from app.models.enums import ParticipantStatus
from app.services.identifiers import (
    PROCUREMENT_TYPE_CONTRATACAO_DIRETA,
    PROCUREMENT_TYPE_LICITACAO,
    PROCUREMENT_TYPE_PROCEDIMENTO_AUXILIAR,
    canonical_modality,
    format_cnpj,
    is_valid_cnpj,
    modality_category,
    normalize_cnpj,
    normalize_company_name,
    participant_status_code,
)


def test_numeric_cnpj_normalization_and_validation() -> None:
    assert normalize_cnpj("04.252.011/0001-10") == "04252011000110"
    assert is_valid_cnpj("04.252.011/0001-10")
    assert format_cnpj("04252011000110") == "04.252.011/0001-10"
    assert not is_valid_cnpj("04.252.011/0001-11")
    assert not is_valid_cnpj("00.000.000/0000-00")


def test_alphanumeric_cnpj_uses_official_ascii_mapping() -> None:
    assert is_valid_cnpj("12.ABC.345/01DE-35")
    assert normalize_cnpj("12.ABC.345/01DE-35") == "12ABC34501DE35"
    assert not is_valid_cnpj("12ABC34501DE34")
    assert not is_valid_cnpj("12ABC34501DEAA")


def test_company_name_normalization_is_conservative() -> None:
    assert (
        normalize_company_name("  Açúcar & Café Sociedade Limitada Unipessoal ") == "ACUCAR E CAFE"
    )
    assert normalize_company_name("Alpha S.A.") == "ALPHA"
    assert normalize_company_name("Alpha Comercial") == "ALPHA COMERCIAL"
    assert normalize_company_name(None) is None


def test_canonical_modality_unifies_source_labels_and_filter_keys() -> None:
    assert canonical_modality("Pregão - Eletrônico") == "pregao_eletronico"
    assert canonical_modality("pregao_eletronico") == "pregao_eletronico"
    assert canonical_modality("PREGÃO ELETRÔNICO") == "pregao_eletronico"
    assert canonical_modality("Concorrência Eletrônica") == "concorrencia_eletronica"
    assert canonical_modality("Dispensa") == "dispensa"
    assert canonical_modality("") is None
    assert canonical_modality(None) is None


def test_modality_category_follows_lei_14133_routes() -> None:
    assert modality_category("Pregão - Eletrônico") == PROCUREMENT_TYPE_LICITACAO
    assert modality_category("Concorrência Presencial") == PROCUREMENT_TYPE_LICITACAO
    assert modality_category("Diálogo Competitivo") == PROCUREMENT_TYPE_LICITACAO
    assert modality_category("Dispensa de Licitação") == PROCUREMENT_TYPE_CONTRATACAO_DIRETA
    assert modality_category("Inexigibilidade") == PROCUREMENT_TYPE_CONTRATACAO_DIRETA
    assert modality_category("Credenciamento") == PROCUREMENT_TYPE_PROCEDIMENTO_AUXILIAR
    assert modality_category("Pré-qualificação") == PROCUREMENT_TYPE_PROCEDIMENTO_AUXILIAR
    assert modality_category("Manifestação de Interesse") == PROCUREMENT_TYPE_PROCEDIMENTO_AUXILIAR
    assert modality_category("Modalidade desconhecida") is None
    assert modality_category(None) is None


def test_participant_status_code_normalizes_source_and_document_texts() -> None:
    assert (
        participant_status_code("participant", "Empresa desclassificada")
        is ParticipantStatus.DISQUALIFIED
    )
    assert participant_status_code("participant", "inabilitada") is ParticipantStatus.INELIGIBLE
    assert participant_status_code("awarded", "homologado") is ParticipantStatus.AWARDED
    assert participant_status_code("winner", None) is ParticipantStatus.WINNER
    assert participant_status_code(None, "sem informação") is ParticipantStatus.UNKNOWN
