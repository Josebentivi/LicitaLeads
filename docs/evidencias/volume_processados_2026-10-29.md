# Volume de registros processados — 29/10/2026

> **Registro histórico:** descreve o estado e os resultados da data indicada. Consulte a [documentação atual](../README.md).

## Mudança aplicada

Nas licitações PNCP descobertas no feed com `dataAtualizacaoGlobal`, o detalhe
era consultado antes dos filhos e novamente ao final para conferir a versão.
Agora uma única consulta atual, após concluir itens, resultados e catálogo
documental, fornece o detalhe e confirma a versão global. Respostas parciais,
falhas e divergências continuam impedindo a comprovação de uma versão completa.

A mesma resposta serve às duas verificações e é persistida uma única vez por
esse caminho. Não é criada uma segunda observação HTTP fictícia. A consulta
final continua ignorando o replay local do lote; validadores HTTP somente
reutilizam o corpo após confirmação da fonte. Consultas por ID ainda precisam
obter o cabeçalho para a descoberta antes de ler os filhos.

O teto HTTP, o espaçamento configurado, `Retry-After`, períodos, filtros,
identidades, transações, revisão humana e processamento documental foram
preservados. A alteração não exige migração de esquema ou novas configurações.

## Aplicação e continuidade

- Suíte offline: **855 testes aprovados**, dois testes live excluídos, cobertura
  de **85,90%**, banco e armazenamento temporários com migrações aplicadas.
- Ruff, formatação e mypy aprovados no Python 3.12 do launcher. As regressões
  verificam consulta única após os filhos, referência a uma observação real,
  mudança de versão durante a leitura e cancelamento antes da confirmação final.
- Coleta anterior: `3c826d3d-fb0f-4f07-8823-968897605ca8`.
  Cancelamento cooperativo pela API, com término confirmado e leases liberadas.
  Os **297 registros confirmados** permaneceram no lote.
- Backup verificado antes do reinício:
  `C:\Users\jjben\AppData\Local\LicitaLeads\data\backups\20261029T180508Z-0a45b76ad8054ed8bd57d72900d9df26.sqlite3`.
- Aplicação reiniciada pelo `iniciar.bat`. Continuação pela API no mesmo lote
  `318977f9-7d1a-4c39-9810-6a475fc5ad5b`, execução
  `16734125-71e4-42d1-bf02-70a20cd9446e`, com os **134 pendentes**.
- Agendador restaurado ao estado anterior, ativo. Nenhum reparo automático de
  dados legados foi aplicado.

## Medição observada

Comparação de duas janelas de cinco minutos, usando as comprovações de versão
gravadas na mesma transação dos dados e da confirmação. A janela posterior
começa após a recuperação do ritmo de envio: o reinício inicialmente provocou
novos HTTP 429 e uma fase de adaptação, que não foi incluída nessa janela.

| Janela UTC | Registros confirmados | Registros/minuto |
|---|---:|---:|
| Antes: 17:59:16–18:04:16 | 37 | 7,4 |
| Depois: 18:16:11–18:21:11 | 71 | 14,2 |

O ritmo estabilizado observado aumentou **91,9%**. São janelas reais com
registros diferentes e condições externas variáveis, não uma garantia de vazão
fixa. Os limites da fonte e os HTTP 429 continuam sendo respeitados.

A etapa de APIs confirmou os **431 registros do lote**: 297 da execução anterior
e os 134 retomados, estes com zero falhas. A coleta avançou para os documentos,
mantendo o limite originalmente escolhido. O resultado da medição e os
relatórios JUnit estão na pasta temporária `LicitaLeads-throughput-20261029`.
