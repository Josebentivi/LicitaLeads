"""Catalogue of Maranhão municipal payment portals.

The entries are intentionally explicit.  A source is never marked available
just because the municipality publishes a transparency page: the adapter mode
and the note explain what was verified.
"""

from __future__ import annotations

from app.config import Settings, get_settings
from app.connectors.municipal_payments.base import MunicipalSource

MUNICIPAL_SOURCES: tuple[MunicipalSource, ...] = (
    MunicipalSource(
        id="sao_luis",
        name="São Luís",
        municipality="São Luís",
        uf="MA",
        adapter="sao_luis_csv",
        base_url="https://www.saoluis.ma.gov.br/dados-abertos",
        mode="csv",
        note=(
            "Dados Abertos em CSV por mês. O portal usa Cloudflare e pode "
            "bloquear acesso automatizado (HTTP 403); nesse caso use a "
            "exportação manual. O endereço direto pode ser configurado em "
            "SAO_LUIS_PAYMENTS_URL."
        ),
        portal_url="https://www.saoluis.ma.gov.br/portal/transparencia",
        export_url="https://www.saoluis.ma.gov.br/dados-abertos",
        open_data_url="https://www.saoluis.ma.gov.br/dados-abertos",
        import_steps=(
            "Abra Dados Abertos e escolha Despesas.",
            "Selecione o mês e o ano desejados e baixe o CSV.",
            "Se o acesso automático retornar HTTP 403, use o navegador normal e repita a exportação manual.",
        ),
    ),
    MunicipalSource(
        id="imperatriz",
        name="Imperatriz",
        municipality="Imperatriz",
        uf="MA",
        adapter="fiorilli_json",
        base_url="http://scpi3.adtrcloud.com.br:8079/transparencia",
        mode="json",
        note=(
            "API JSON Fiorilli/SCPI da Prefeitura (Empresa=2). Coleta despesas "
            "por empenho; o detalhe de pagamentos é opcional e limitado por "
            "configuração."
        ),
        entity_code="2",
        supports_details=True,
        portal_url="https://transparencia.imperatriz.ma.gov.br/portal-transparencia",
        export_url=(
            "http://scpi3.adtrcloud.com.br:8079/transparencia/"
            "?AcessoIndividual=lnkDespesasPor_NotaEmpenho"
        ),
        open_data_url="https://transparencia.imperatriz.ma.gov.br/dados-abertos",
        import_steps=(
            "Selecione Exercício {year} e Entidade PREFEITURA MUNICIPAL DE IMPERATRIZ.",
            "Abra Despesas Gerais e use a exportação em CSV.",
            "Para a Câmara, repita a importação com o Portal de origem 'Imperatriz - Câmara'.",
        ),
    ),
    MunicipalSource(
        id="timon",
        name="Timon",
        municipality="Timon",
        uf="MA",
        adapter="fiorilli_json",
        base_url="http://177.10.118.5:8079/Transparencia_Despesa",
        mode="json",
        note=(
            "Endpoint Fiorilli do portal. Empresa=18 evita o HTTP 500 do "
            "Empresa=1, mas o relatório retorna despesas de várias entidades "
            "do município. A coleta usa o agregado por empenho, sem detalhe "
            "por pagamento, e não inventa dados em caso de falha."
        ),
        entity_code="18",
        filter_client_entity=True,
        supports_details=False,
        portal_url="https://transparencia.timon.ma.gov.br/",
        export_url=(
            "http://177.10.118.5:8079/Transparencia_Despesa/"
            "?AcessoIndividual=lnkDespesasPor_NotaEmpenho"
        ),
        open_data_url="https://transparencia.timon.ma.gov.br/dados-abertos",
        import_steps=(
            "Selecione Exercício {year} e Entidade PREFEITURA MUNICIPAL DE TIMON (18).",
            "Abra Despesas Gerais e exporte somente a Prefeitura; o portal pode listar outras entidades.",
        ),
    ),
    MunicipalSource(
        id="caxias",
        name="Caxias",
        municipality="Caxias",
        uf="MA",
        adapter="fiorilli_json",
        base_url="http://177.136.250.170:5656/SCPIWEB_PMDECAXIAS",
        mode="json",
        note=(
            "API JSON Fiorilli/SCPI. Publica despesas por empenho; o detalhe "
            "de pagamentos por empenho é opcional e limitado por configuração."
        ),
        supports_details=True,
        portal_url="https://www.transparencia.caxias.ma.gov.br/transparencia?filtro=despesas",
        export_url="http://177.136.250.170:5656/SCPIWEB_PMDECAXIAS/",
        open_data_url="https://www.transparencia.caxias.ma.gov.br/transparencia/dados-abertos",
        import_steps=(
            "Selecione Exercício {year} e Entidade PREFEITURA MUNICIPAL DE CAXIAS.",
            "Abra Despesas Gerais e use a exportação em CSV.",
        ),
    ),
    MunicipalSource(
        id="bacabal",
        name="Bacabal",
        municipality="Bacabal",
        uf="MA",
        adapter="fiorilli_json",
        base_url="https://transparenciapublica.cloud/bacabal",
        mode="json",
        note=(
            "API JSON Fiorilli/SCPI da Prefeitura (Empresa=2). Publica "
            "despesas por empenho; o detalhe de pagamentos é opcional e "
            "limitado por configuração."
        ),
        entity_code="2",
        supports_details=True,
        portal_url="https://www.bacabal.ma.gov.br/despesas",
        export_url=(
            "https://transparenciapublica.cloud/bacabal/"
            "Default.aspx?AcessoIndividual=lnkDespesasPor_NotaEmpenho"
        ),
        open_data_url="https://www.bacabal.ma.gov.br/dados-abertos",
        import_steps=(
            "Selecione Exercício {year} e Entidade PREFEITURA MUNICIPAL DE BACABAL.",
            "Abra Despesas Gerais e use a exportação em CSV.",
        ),
    ),
)

_BY_ID = {source.id: source for source in MUNICIPAL_SOURCES}


def get_municipal_source(source_id: str) -> MunicipalSource:
    """Return one source or raise a clear error for an unknown id."""

    normalized = source_id.strip().lower().replace("-", "_")
    source = _BY_ID.get(normalized)
    if source is None:
        raise ValueError(f"unknown municipal payment source: {source_id}")
    return source


def list_municipal_sources(settings: Settings | None = None) -> list[MunicipalSource]:
    """Return the full catalogue; use ``selected_municipal_sources`` for enablement."""

    del settings
    return list(MUNICIPAL_SOURCES)


def selected_municipal_sources(settings: Settings | None = None) -> list[MunicipalSource]:
    """Return the enabled sources in configuration order, ignoring duplicates."""

    selected = settings or get_settings()
    result: list[MunicipalSource] = []
    seen: set[str] = set()
    for raw in selected.municipal_payment_sources:
        source_id = raw.strip().lower().replace("-", "_")
        if not source_id or source_id in seen:
            continue
        source = _BY_ID.get(source_id)
        if source is not None:
            result.append(source)
            seen.add(source_id)
    return result


def unknown_municipal_source_ids(settings: Settings | None = None) -> list[str]:
    """Return configured ids that do not exist in the catalogue."""

    selected = settings or get_settings()
    result: list[str] = []
    for raw in selected.municipal_payment_sources:
        source_id = raw.strip().lower().replace("-", "_")
        if source_id and source_id not in _BY_ID and source_id not in result:
            result.append(source_id)
    return result
