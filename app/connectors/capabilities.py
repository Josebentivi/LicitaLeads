"""Single machine-readable registry of audited public-source capabilities."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class CapabilityAvailability(StrEnum):
    AVAILABLE = "available"
    PARTIAL = "partial"
    DOCUMENT_ONLY = "document_only"
    NOT_SUPPORTED = "not_supported"


class EndpointDescription(BaseModel):
    model_config = ConfigDict(extra="forbid")

    method: str = "GET"
    path: str
    parameters: list[str] = Field(default_factory=list)
    page_size_limit: int | None = None
    authentication_required: bool = False


class SourceCapabilityItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    availability: CapabilityAvailability
    notes: str
    endpoints: list[EndpointDescription] = Field(default_factory=list)


class SourceCapability(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    documentation_url: str
    audited_at: str
    capabilities: list[SourceCapabilityItem]


_DISCOVERY_PARAMS = [
    "dataInicial",
    "dataFinal",
    "codigoModalidadeContratacao",
    "uf",
    "codigoMunicipioIbge",
    "cnpj",
    "codigoUnidadeAdministrativa",
    "pagina",
    "tamanhoPagina",
]

SOURCE_CAPABILITIES: tuple[SourceCapability, ...] = (
    SourceCapability(
        id="pncp",
        name="PNCP",
        documentation_url="https://pncp.gov.br/manual/pt-br/latest/",
        audited_at="2026-09-16",
        capabilities=[
            SourceCapabilityItem(
                name="Contratações por publicação e atualização",
                availability=CapabilityAvailability.AVAILABLE,
                notes="Consulta pública sem credencial; modalidades são consultadas separadamente.",
                endpoints=[
                    EndpointDescription(
                        path="/api/consulta/v1/contratacoes/publicacao",
                        parameters=_DISCOVERY_PARAMS,
                        page_size_limit=50,
                    ),
                    EndpointDescription(
                        path="/api/consulta/v1/contratacoes/atualizacao",
                        parameters=_DISCOVERY_PARAMS,
                        page_size_limit=50,
                    ),
                ],
            ),
            SourceCapabilityItem(
                name="Contratações com propostas abertas",
                availability=CapabilityAvailability.PARTIAL,
                notes="Lista processos que recebem propostas, não propostas ou proponentes.",
                endpoints=[
                    EndpointDescription(
                        path="/api/consulta/v1/contratacoes/proposta",
                        parameters=[
                            "dataFinal",
                            "codigoModalidadeContratacao",
                            "uf",
                            "codigoMunicipioIbge",
                            "pagina",
                            "tamanhoPagina",
                        ],
                        page_size_limit=50,
                    )
                ],
            ),
            SourceCapabilityItem(
                name="Detalhe, itens e documentos",
                availability=CapabilityAvailability.AVAILABLE,
                notes="Detalhe usa Consulta; itens e arquivos usam os GET públicos de Integração.",
                endpoints=[
                    EndpointDescription(
                        path="/api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}"
                    ),
                    EndpointDescription(
                        path="/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens"
                    ),
                    EndpointDescription(
                        path="/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/arquivos"
                    ),
                ],
            ),
            SourceCapabilityItem(
                name="Resultados homologados",
                availability=CapabilityAvailability.AVAILABLE,
                notes="Identifica fornecedor adjudicado/homologado; não é o rol de participantes.",
                endpoints=[
                    EndpointDescription(
                        path=(
                            "/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}"
                            "/itens/{numeroItem}/resultados"
                        )
                    )
                ],
            ),
            SourceCapabilityItem(
                name="Participantes, inabilitação, desclassificação e recursos",
                availability=CapabilityAvailability.DOCUMENT_ONLY,
                notes=(
                    "Somente documentos oficiais com trecho e localizador podem comprovar "
                    "esses fatos."
                ),
            ),
            SourceCapabilityItem(
                name="Eventos da sessão",
                availability=CapabilityAvailability.NOT_SUPPORTED,
                notes=(
                    "O endpoint historico registra manutenção técnica, não eventos jurídicos "
                    "da sessão."
                ),
            ),
        ],
    ),
    SourceCapability(
        id="compras_gov",
        name="Compras.gov.br — Dados Abertos",
        documentation_url="https://dadosabertos.compras.gov.br/swagger-ui/index.html",
        audited_at="2026-09-16",
        capabilities=[
            SourceCapabilityItem(
                name="Contratações Lei 14.133/2021",
                availability=CapabilityAvailability.AVAILABLE,
                notes=(
                    "Consulta pública sem credencial; códigos de modalidade são próprios "
                    "desta fonte."
                ),
                endpoints=[
                    EndpointDescription(
                        path="/modulo-contratacoes/1_consultarContratacoes_PNCP_14133",
                        parameters=[
                            "dataPublicacaoPncpInicial",
                            "dataPublicacaoPncpFinal",
                            "codigoModalidade",
                            "unidadeOrgaoUfSigla",
                            "orgaoEntidadeCnpj",
                            "pagina",
                            "tamanhoPagina",
                        ],
                        page_size_limit=500,
                    ),
                    EndpointDescription(
                        path="/modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id",
                        parameters=["tipo", "codigo"],
                    ),
                ],
            ),
            SourceCapabilityItem(
                name="Itens e resultados homologados",
                availability=CapabilityAvailability.AVAILABLE,
                notes=(
                    "Fornecedor de resultado é adjudicado/homologado, não participante presumido."
                ),
                endpoints=[
                    EndpointDescription(
                        path="/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id",
                        parameters=["tipo", "codigo"],
                    ),
                    EndpointDescription(
                        path=(
                            "/modulo-contratacoes/"
                            "3.1_consultarResultadoItensContratacoes_PNCP_14133_Id"
                        ),
                        parameters=["tipo", "codigo"],
                    ),
                ],
            ),
            SourceCapabilityItem(
                name="Documentos",
                availability=CapabilityAvailability.NOT_SUPPORTED,
                notes="Nenhum endpoint de documentos foi encontrado no OpenAPI auditado.",
            ),
            SourceCapabilityItem(
                name="Participantes, propostas, habilitação e recursos",
                availability=CapabilityAvailability.NOT_SUPPORTED,
                notes=(
                    "Não há feed estruturado público confirmado; usar documentos oficiais do PNCP."
                ),
            ),
            SourceCapabilityItem(
                name="UASG, fornecedores, ARP, contratos e OCDS",
                availability=CapabilityAvailability.AVAILABLE,
                notes=(
                    "Módulos oficiais existem, mas não substituem a proveniência do recurso "
                    "original."
                ),
            ),
        ],
    ),
)


def get_source_capabilities() -> list[SourceCapability]:
    """Return copies so callers cannot mutate the process-wide registry."""

    return [item.model_copy(deep=True) for item in SOURCE_CAPABILITIES]


def render_capability_matrix_markdown(
    capabilities: list[SourceCapability] | tuple[SourceCapability, ...] | None = None,
) -> str:
    """Render the API capability registry as a deterministic Markdown matrix.

    Documentation tooling can write this value directly to
    ``docs/matriz_de_fontes.md``.  Accepting an explicit registry
    also makes previews and tests possible without mutating the canonical data.
    """

    sources = list(capabilities) if capabilities is not None else get_source_capabilities()
    audit_dates = sorted({source.audited_at for source in sources})
    date_label = ", ".join(audit_dates) if audit_dates else "não informada"
    lines = [
        "# Matriz de capacidades das fontes oficiais",
        "",
        f"Auditoria técnica registrada em **{date_label}**.",
        "Esta matriz é gerada pelo mesmo registro usado por `GET /api/source-capabilities`.",
        "",
    ]
    for source in sources:
        lines.extend(
            [
                f"## {source.name}",
                "",
                f"Documentação oficial: <{source.documentation_url}>",
                "",
                "| Capacidade | Situação | Endpoints oficiais | Observações |",
                "|---|---|---|---|",
            ]
        )
        for capability in source.capabilities:
            endpoints = "<br>".join(
                f"`{endpoint.method} {endpoint.path}`" for endpoint in capability.endpoints
            )
            lines.append(
                "| "
                + " | ".join(
                    (
                        _markdown_cell(capability.name),
                        f"`{capability.availability.value}`",
                        endpoints or "—",
                        _markdown_cell(capability.notes),
                    )
                )
                + " |"
            )
        endpoint_rows = [
            endpoint for capability in source.capabilities for endpoint in capability.endpoints
        ]
        if endpoint_rows:
            lines.extend(["", "### Contratos auditados", ""])
            for endpoint in endpoint_rows:
                parameters = ", ".join(f"`{item}`" for item in endpoint.parameters) or "nenhum"
                pagination = (
                    f"; máximo por página: {endpoint.page_size_limit}"
                    if endpoint.page_size_limit
                    else ""
                )
                authentication = "sim" if endpoint.authentication_required else "não"
                lines.append(
                    f"- `{endpoint.method} {endpoint.path}` — parâmetros: {parameters}"
                    f"{pagination}; autenticação de leitura: {authentication}."
                )
        lines.append("")
    lines.extend(
        [
            "## Regras transversais",
            "",
            "- `numeroControlePNCP` é a chave preferencial de deduplicação; sem ela, somente "
            "CNPJ do órgão, unidade/UASG confirmada, número, ano e modalidade normalizada.",
            "- Similaridade textual nunca unifica processos ou empresas automaticamente.",
            "- Os códigos de modalidade são específicos de cada fonte: pregão eletrônico é "
            "PNCP `6` e Compras.gov.br `5`.",
            "- CNPJ numérico e alfanumérico de 14 posições são validados com os dígitos "
            "verificadores oficiais; CPF não é usado para enriquecimento.",
            "- `empty`, `not_supported`, `not_published`, `access_restricted` e "
            "`temporary_error` têm semânticas distintas no conector.",
            "- Limites globais de requisição não foram publicados; o cliente trata 429, "
            "`Retry-After` e falhas temporárias com backoff conservador e ritmo mínimo "
            "configurável entre requisições.",
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _markdown_cell(value: str) -> str:
    return " ".join(value.replace("|", r"\|").split())
