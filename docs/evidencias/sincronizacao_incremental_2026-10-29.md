# Sincronização incremental e documentos PNCP — aplicação em 29/10/2026

> **Registro histórico:** descreve o estado e os resultados da data indicada. Consulte a [documentação atual](../README.md).

## Comportamento entregue

Licitações, atas e contratos mantêm estado por fonte, fluxo, filtros e endereço
da API. A carga inicial conserva a janela configurada. As seguintes usam dois
dias de sobreposição e uma revisão semanal; o marco acompanha o fim confirmado
do período, inclusive em consultas vazias. Limites preservam o lote congelado;
falhas e cancelamentos dependem de continuação explícita. Coletas manuais não
avançam o marco automático. Pagamentos e importações conservam o comportamento
anterior.

As comprovações de versão são prospectivas e transacionais. Somente o relógio
global de licitações PNCP pode dispensar o conjunto completo de detalhes. Uma
consulta final verifica alterações durante a leitura. Atas, contratos e
Compras.gov.br conservam a consulta dos filhos, salvo confirmação HTTP 304 de
respostas completas. Fonte, adaptador, evidências e relógio são conferidos;
uma versão comprovadamente anterior não ganha validade por ter sido recebida
novamente. Cada reaproveitamento aponta para a comprovação ou resposta original.

O Compras.gov.br revisita até 100 identidades conhecidas por rodada, respeitando
o orçamento geral. A rotação inclui processos encerrados e conserva a origem
verificada quando outra fonte atualiza o cabeçalho canônico. Não usa semelhança
de nomes, valores ou datas para correlacionar identidades.

API/serviço oferecem `force_refresh`; CLI, `--force-refresh`; formulários,
**Reconsultar detalhes já verificados**. `/crawls` apresenta estratégia,
período efetivo e reaproveitamentos. Continuações preservam a opção original.

## Segurança dos documentos

O resolvedor só reconstrói recursos conhecidos do host oficial quando a
resposta armazenada identifica exatamente órgão, compra e arquivo. O destino
usa HTTPS na porta 443. Identidades divergentes, caminhos ambíguos, credenciais,
DNS/IP privado, redirecionamentos circulares e limites de tamanho continuam
protegidos. Endereços codificados em redirecionamentos do PNCP também são
recusados quando impedem confirmar a identidade.

`original_url` e o payload recebido permanecem preservados. `resolved_url`,
conteúdo, extração e derivados são confirmados juntos, após verificar novamente
a atividade e a versão documental. Novas evidências usam o endereço efetivo.

Referências dos contratos oficiais consultados:
[PNCP](https://pncp.gov.br/api/consulta/swagger-ui/index.html),
[manual PNCP](https://pncp.gov.br/manual/pt-br/latest/singlehtml/) e
[OpenAPI Compras.gov.br](https://dadosabertos.compras.gov.br/v3/api-docs).
Os parâmetros das consultas de atas e contratos por identidade do Compras.gov.br
foram conferidos no OpenAPI em 29/10/2026.

## Aplicação e execução seletiva

- Banco efetivo confirmado pelo launcher:
  `C:\Users\jjben\AppData\Local\LicitaLeads\data\licita_lead.db`.
- Migração aditiva: `0022_incremental_audit` → `0023_collection_sync`.
  As quatro novas tabelas começaram vazias; nenhuma comprovação foi inferida
  de dados antigos. As contagens das tabelas de negócio foram preservadas.
- Foram acrescentadas ao `.env` as configurações de sobreposição (2), revisão
  (7) e rotação Compras (100), já documentadas em `.env.example`.
- Duas execuções antigas, bloqueadas pelo SQLite, foram encerradas após
  confirmação de ausência de progresso e backup verificado. Registros
  confirmados e pendências dos lotes foram preservados, com continuação
  explícita. Nenhum reparo de dados de negócio legados foi aplicado.
- Backup anterior à migração:
  `C:\Users\jjben\AppData\Local\LicitaLeads\data\backups\20261029T164514Z-ba4dc63cb55a40dbb54a4a311bbe3c69.sqlite3`.
  SHA-256: `5ac1fa746d0e37072a54eba68c405a2f49fb616d3d47dcbd7ae8efada5a99983`.
- Nova execução documental: `6b874f4a-fdae-4e9b-b984-c8eb1e9564f6`, vinculada à
  coleta `b42f11a8-3aa2-4119-8129-674f9ae9a22d`.
  Seleção: os 25 documentos ativos com erro por porta interna, cada um com uma
  tentativa anterior, sem alterar sua elegibilidade para viabilizar o retry.
- Resultado: **25 extraídos, 25 endereços públicos confirmados, nenhuma falha**.
  Os 25 endereços originais foram preservados. Os demais 25 documentos do lote
  anterior foram comparados com o backup e permaneceram iguais, incluindo a
  falha de subprocesso excluída do reparo.
- Aplicação reiniciada pelo `iniciar.bat`; `/health`, `/atas`, `/contratos` e
  `/crawls` responderam 200. O estado anterior do agendador foi restaurado:
  **ativo, sem pausa**. A coleta interrompida permanece encerrada.

## Validação

A suíte offline final passou com **853 testes**, cobertura de **85,94%**, banco
e armazenamento temporários e migrações aplicadas; os dois testes live foram
excluídos. Foram cobertos redirecionamentos ambíguos, versões anteriores
repetidas e preservação da origem verificada entre fontes. Ruff, formatação e
mypy passaram no Python 3.12 do launcher. Os relatórios JUnit e o resultado
da execução documental foram preservados na pasta temporária de manutenção
`LicitaLeads-sync-20261029`.

A primeira carga automática ainda precisa estabelecer sua cobertura. O ganho
incremental começa após a confirmação desse lote; os estados novos não assumem
que coletas antigas tenham comprovado versões ou cobertura.
