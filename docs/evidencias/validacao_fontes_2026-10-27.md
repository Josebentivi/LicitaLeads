# Validação limitada das fontes — 2026-10-27

> **Registro histórico:** descreve o estado e os resultados da data indicada. Consulte a [documentação atual](../README.md).

Consultas somente leitura, sem persistência de dados de domínio. Falhas externas não são ignoradas.

| Operação | Resultado | Evidência |
|---|---|---|
| pncp descoberta | validado | 20 registros; 2 páginas; amostra limitada a duas páginas; urls=https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao?dataFinal=20261027&codigoModalidadeContratacao=6&uf=MA&pagina=1&tamanhoPagina=10&dataInicial=20260928, https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao?dataFinal=20261027&codigoModalidadeContratacao=6&uf=MA&pagina=2&tamanhoPagina=10&dataInicial=20260928 |
| pncp detalhe | validado | available; respostas=1; parcial=False; diagnóstico=nenhum; urls=https://pncp.gov.br/api/consulta/v1/orgaos/00508903000188/compras/2026/1886 |
| pncp itens | validado | available; respostas=1; parcial=False; diagnóstico=nenhum; urls=https://pncp.gov.br/api/pncp/v1/orgaos/00508903000188/compras/2026/1886/itens?pagina=1&tamanhoPagina=50 |
| pncp documentos | validado | available; respostas=1; parcial=False; diagnóstico=nenhum; urls=https://pncp.gov.br/api/pncp/v1/orgaos/00508903000188/compras/2026/1886/arquivos?pagina=1&tamanhoPagina=50 |
| pncp resultados | não verificado | amostra sem item com resultado publicado |
| compras_gov descoberta | validado | 10 registros; 1 páginas; amostra limitada a duas páginas; urls=https://dadosabertos.compras.gov.br/modulo-contratacoes/1_consultarContratacoes_PNCP_14133?pagina=1&tamanhoPagina=10&dataPublicacaoPncpInicial=2026-09-28&dataPublicacaoPncpFinal=2026-10-27&codigoModalidade=5&unidadeOrgaoUfSigla=MA |
| compras_gov detalhe | validado | available; respostas=1; parcial=False; diagnóstico=nenhum; urls=https://dadosabertos.compras.gov.br/modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id?tipo=idCompra&codigo=09000405000402026 |
| compras_gov itens | validado | available; respostas=1; parcial=False; diagnóstico=nenhum; urls=https://dadosabertos.compras.gov.br/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id?tipo=idCompra&codigo=09000405000402026&pagina=1&tamanhoPagina=500 |
| compras_gov documentos | não suportado | fonte não expõe esta operação |
| compras_gov resultados | validado | not_published; respostas=1; parcial=False |
| São Luís CSVs | falha | HTTPStatusError HTTP 403 |
| Imperatriz HTTPS | falha | ConnectError; catálogo HTTP preservado com opt-in obrigatório |
| Timon HTTPS | falha | ConnectError; catálogo HTTP preservado com opt-in obrigatório |
| Caxias HTTPS | falha | ConnectError; catálogo HTTP preservado com opt-in obrigatório |
| Transparência | não verificado | chave ou CNPJ de consulta não configurado |

Conferência complementar do OpenAPI Compras confirmou que adesões publicam unidade não participante, data e quantidade, sem número de item; mapper e contrato foram alinhados a essa estrutura. O catálogo de parâmetros de contratos foi alinhado aos parâmetros efetivamente consumidos (codigoOrgao e intervalo de vigência).

Consultas por item de ARP foram alinhadas ao parâmetro obrigatório numeroItem, derivado exclusivamente dos itens oficiais; a consulta e a proveniência preservam esse vínculo. Leitura HTTPS complementar do OpenAPI Compras com backend público fixando o IP validado confirmou TLS e URL original sem perda de validação.

# Auditoria de contratos das fontes — 2026-10-27

Relatório gerado sob demanda. Ele não altera automaticamente a matriz versionada; divergências exigem revisão humana.

## PNCP

- OpenAPI consultado: https://pncp.gov.br/api/consulta/v3/api-docs, https://pncp.gov.br/pncp-api/v3/api-docs
- Rotas no contrato vivo: 135
- Resultado: **rotas encontradas; validação por operação abaixo**

### Rotas conhecidas ausentes

- Nenhuma rota conhecida ausente.

### Métodos, parâmetros e respostas

- `GET /api/consulta/v1/contratacoes/publicacao`: contrato mínimo validado
- `GET /api/consulta/v1/contratacoes/atualizacao`: contrato mínimo validado
- `GET /api/consulta/v1/contratacoes/proposta`: contrato mínimo validado
- `GET /api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}`: operação não verificada; método não documentado
- `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens`: contrato mínimo validado
- `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/arquivos`: contrato mínimo validado
- `GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{numeroItem}/resultados`: contrato mínimo validado
- `GET /api/consulta/v1/atas`: contrato mínimo validado
- `GET /api/consulta/v1/atas/atualizacao`: contrato mínimo validado

## Compras.gov.br — Dados Abertos

- OpenAPI consultado: https://dadosabertos.compras.gov.br/v3/api-docs
- Rotas no contrato vivo: 77
- Resultado: **rotas encontradas; validação por operação abaixo**

### Rotas conhecidas ausentes

- Nenhuma rota conhecida ausente.

### Métodos, parâmetros e respostas

- `GET /modulo-contratacoes/1_consultarContratacoes_PNCP_14133`: contrato mínimo validado
- `GET /modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id`: contrato mínimo validado
- `GET /modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id`: contrato mínimo validado
- `GET /modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id`: contrato mínimo validado
- `GET /modulo-arp/1_consultarARP`: contrato mínimo validado
- `GET /modulo-arp/1.1_consultarARP_Id`: contrato mínimo validado
- `GET /modulo-arp/2_consultarARPItem`: contrato mínimo validado
- `GET /modulo-arp/2.1_consultarARPItem_Id`: contrato mínimo validado
- `GET /modulo-arp/3_consultarUnidadesItem`: contrato mínimo validado
- `GET /modulo-arp/4_consultarEmpenhosSaldoItem`: contrato mínimo validado
- `GET /modulo-arp/5_consultarAdesoesItem`: contrato mínimo validado
- `GET /modulo-contratos/1_consultarContratos`: contrato mínimo validado
- `GET /modulo-contratos/2.1_consultarContratosItem_Id`: contrato mínimo validado

## PNCP - Contratos e empenhos

- OpenAPI consultado: https://pncp.gov.br/api/consulta/v3/api-docs, https://pncp.gov.br/pncp-api/v3/api-docs
- Rotas no contrato vivo: 135
- Resultado: **rotas encontradas; validação por operação abaixo**

### Rotas conhecidas ausentes

- Nenhuma rota conhecida ausente.

### Métodos, parâmetros e respostas

- `GET /api/consulta/v1/contratos`: contrato mínimo validado
- `GET /api/pncp/v1/orgaos/{cnpj}/contratos/{ano}/{sequencial}/termos`: contrato mínimo validado

## Portal da Transparencia - Pagamentos federais

- OpenAPI consultado: https://api.portaldatransparencia.gov.br/v3/api-docs
- Rotas no contrato vivo: 109
- Resultado: **rotas encontradas; validação por operação abaixo**

### Rotas conhecidas ausentes

- Nenhuma rota conhecida ausente.

### Métodos, parâmetros e respostas

- `GET /api-de-dados/despesas/documentos-por-favorecido`: contrato mínimo validado
