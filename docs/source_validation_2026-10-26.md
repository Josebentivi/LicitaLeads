# Validação limitada das fontes — 2026-10-26

> **Registro histórico:** descreve o estado e os resultados da data indicada. Consulte a [documentação atual](README.md).

Consultas somente leitura, sem persistência de dados de domínio. Falhas externas não são ignoradas.

| Operação | Resultado | Evidência |
|---|---|---|
| pncp descoberta | validado | 20 registros; 2 páginas; amostra limitada a duas páginas |
| pncp detalhe | validado | available; respostas=1; parcial=False; diagnóstico=nenhum |
| pncp itens | validado | available; respostas=1; parcial=False; diagnóstico=nenhum |
| pncp documentos | validado | available; respostas=1; parcial=False; diagnóstico=nenhum |
| pncp resultados | não verificado | amostra sem item com resultado publicado |
| compras_gov descoberta | validado | 10 registros; 1 páginas; amostra limitada a duas páginas |
| compras_gov detalhe | validado | available; respostas=1; parcial=False; diagnóstico=nenhum |
| compras_gov itens | validado | available; respostas=1; parcial=False; diagnóstico=nenhum |
| compras_gov documentos | não suportado | fonte não expõe esta operação |
| compras_gov resultados | validado | not_published; respostas=1; parcial=False |
| São Luís CSVs | falha | HTTPStatusError HTTP 403 |
| Imperatriz HTTPS | falha | ConnectError; catálogo HTTP preservado com opt-in obrigatório |
| Timon HTTPS | falha | ConnectError; catálogo HTTP preservado com opt-in obrigatório |
| Caxias HTTPS | falha | ConnectError; catálogo HTTP preservado com opt-in obrigatório |
| Transparência | não verificado | chave ou CNPJ de consulta não configurado |

# Auditoria de contratos das fontes — 2026-10-26

Relatório gerado sob demanda. Ele não altera automaticamente a matriz versionada; divergências exigem revisão humana.

## PNCP

- OpenAPI consultado: https://pncp.gov.br/api/consulta/v3/api-docs
- Rotas no contrato vivo: 12
- Resultado: **divergência requer revisão**

### Rotas conhecidas ausentes

- `/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/arquivos`
- `/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens`
- `/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{numeroItem}/resultados`

### Métodos, parâmetros e respostas

- `GET /api/consulta/v1/contratacoes/publicacao`: contrato mínimo validado
- `GET /api/consulta/v1/contratacoes/atualizacao`: contrato mínimo validado
- `GET /api/consulta/v1/contratacoes/proposta`: contrato mínimo validado
- `GET /api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}`: contrato mínimo validado
- `GET /api/consulta/v1/atas`: operação não verificada; schema não declara campos mínimos usados pelo conector
- `GET /api/consulta/v1/atas/atualizacao`: operação não verificada; schema não declara campos mínimos usados pelo conector

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
- `GET /modulo-contratos/1_consultarContratos`: operação não verificada; parâmetro consumido ausente: dataPublicacaoPncpInicial; parâmetro consumido ausente: dataPublicacaoPncpFinal; parâmetro consumido ausente: orgaoEntidadeCnpj; parâmetro consumido ausente: unidadeOrgaoUfSigla
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
