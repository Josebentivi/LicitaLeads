# Matriz de capacidades das fontes oficiais

Auditoria técnica registrada em **2026-09-16**.
Esta matriz é gerada pelo mesmo registro usado por `GET /api/source-capabilities`.

## PNCP

Documentação oficial: <https://pncp.gov.br/manual/pt-br/latest/>

| Capacidade | Situação | Endpoints oficiais | Observações |
|---|---|---|---|
| Contratações por publicação e atualização | `available` | `GET /api/consulta/v1/contratacoes/publicacao`<br>`GET /api/consulta/v1/contratacoes/atualizacao` | Consulta pública sem credencial; modalidades são consultadas separadamente. |
| Contratações com propostas abertas | `partial` | `GET /api/consulta/v1/contratacoes/proposta` | Lista processos que recebem propostas, não propostas ou proponentes. |
| Detalhe, itens e documentos | `available` | `GET /api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}`<br>`GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens`<br>`GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/arquivos` | Detalhe usa Consulta; itens e arquivos usam os GET públicos de Integração. |
| Resultados homologados | `available` | `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{numeroItem}/resultados` | Identifica fornecedor adjudicado/homologado; não é o rol de participantes. |
| Participantes, inabilitação, desclassificação e recursos | `document_only` | — | Somente documentos oficiais com trecho e localizador podem comprovar esses fatos. |
| Eventos da sessão | `not_supported` | — | O endpoint histórico registra manutenção técnica, não eventos jurídicos da sessão. |

### Contratos auditados

- `GET /api/consulta/v1/contratacoes/publicacao` — parâmetros: `dataInicial`, `dataFinal`, `codigoModalidadeContratacao`, `uf`, `codigoMunicipioIbge`, `cnpj`, `codigoUnidadeAdministrativa`, `pagina`, `tamanhoPagina`; máximo por página: 50; autenticação de leitura: não.
- `GET /api/consulta/v1/contratacoes/atualizacao` — parâmetros: `dataInicial`, `dataFinal`, `codigoModalidadeContratacao`, `uf`, `codigoMunicipioIbge`, `cnpj`, `codigoUnidadeAdministrativa`, `pagina`, `tamanhoPagina`; máximo por página: 50; autenticação de leitura: não.
- `GET /api/consulta/v1/contratacoes/proposta` — parâmetros: `dataFinal`, `codigoModalidadeContratacao`, `uf`, `codigoMunicipioIbge`, `pagina`, `tamanhoPagina`; máximo por página: 50; autenticação de leitura: não.
- `GET /api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}` — parâmetros: nenhum; autenticação de leitura: não.
- `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens` — parâmetros: nenhum; autenticação de leitura: não.
- `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/arquivos` — parâmetros: nenhum; autenticação de leitura: não.
- `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{numeroItem}/resultados` — parâmetros: nenhum; autenticação de leitura: não.

Limitações confirmadas: “propostas abertas” lista processos, e o resultado homologado identifica vencedor/adjudicatário. Participantes perdedores, habilitação, inabilitação, desclassificação, recursos, contrarrazões e estado completo da sessão não possuem feed estruturado público confirmado e exigem documento oficial como evidência. Documentos PNCP também podem ser publicados em ZIP.

## Compras.gov.br — Dados Abertos

Documentação oficial: <https://dadosabertos.compras.gov.br/swagger-ui/index.html>

| Capacidade | Situação | Endpoints oficiais | Observações |
|---|---|---|---|
| Contratações Lei 14.133/2021 | `available` | `GET /modulo-contratacoes/1_consultarContratacoes_PNCP_14133`<br>`GET /modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id` | Consulta pública sem credencial; códigos de modalidade são próprios desta fonte. |
| Itens e resultados homologados | `available` | `GET /modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id`<br>`GET /modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id` | Fornecedor de resultado é adjudicado/homologado, não participante presumido. |
| Documentos | `not_supported` | — | Nenhum endpoint de documentos foi encontrado no OpenAPI auditado. |
| Participantes, propostas, habilitação e recursos | `not_supported` | — | Não há feed estruturado público confirmado; usar documentos oficiais do PNCP. |
| UASG, fornecedores, ARP, contratos e OCDS | `available` | — | Módulos oficiais existem, mas não substituem a proveniência do recurso original. |

### Contratos auditados

- `GET /modulo-contratacoes/1_consultarContratacoes_PNCP_14133` — parâmetros: `dataPublicacaoPncpInicial`, `dataPublicacaoPncpFinal`, `codigoModalidade`, `unidadeOrgaoUfSigla`, `orgaoEntidadeCnpj`, `pagina`, `tamanhoPagina`; máximo por página: 500; autenticação de leitura: não.
- `GET /modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id` — parâmetros: `tipo`, `codigo`; autenticação de leitura: não.
- `GET /modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id` — parâmetros: `tipo`, `codigo`; autenticação de leitura: não.
- `GET /modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id` — parâmetros: `tipo`, `codigo`; autenticação de leitura: não.

O OpenAPI também expõe módulos `legado`, `uasg`, `fornecedor`, `arp`, `contratos` e `ocds`. Não há endpoint confirmado para documentos, propostas individuais, participantes completos, habilitação, recursos ou contrarrazões.

## Regras transversais

- `numeroControlePNCP` é a chave preferencial de deduplicação; sem ela, somente CNPJ do órgão, unidade/UASG confirmada, número, ano e modalidade normalizada.
- Similaridade textual nunca unifica processos ou empresas automaticamente.
- Os códigos de modalidade são específicos de cada fonte: pregão eletrônico é PNCP `6` e Compras.gov.br `5`.
- CNPJ numérico e alfanumérico de 14 posições são validados com os dígitos verificadores oficiais; CPF não é usado para enriquecimento.
- `empty`, `not_supported`, `not_published`, `access_restricted` e `temporary_error` têm semânticas distintas no conector.
- Limites globais de requisição não foram publicados; o cliente trata 429, `Retry-After` e falhas temporárias com backoff conservador e intervalo mínimo configurável entre requisições (`HTTP_MIN_REQUEST_INTERVAL_SECONDS`).

O comando `python -m app.cli audit-sources` compara os OpenAPI vivos com os contratos conhecidos e salva um relatório datado, sem alterar esta matriz automaticamente.
