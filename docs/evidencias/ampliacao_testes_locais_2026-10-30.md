# Validação ampliada local — 30/10/2026

> **Registro histórico:** descreve o estado e os resultados da data indicada. Consulte a [documentação atual](../README.md).

Execução `34c2f50fb5be4da4be1e03d5049b561d` concluída pelo botão real da interface, identificada
como atual para o código `888f0762e6cf58e39208d65fc813509f34c016321dd91a868d3b940da7214f91`.

- 1001 testes descobertos, 1001 executados e 1001 aprovados.
- 0 falhas, 0 ignorados e 0 não executados.
- Ruff, formatação e mypy aprovados, além das etapas Python, API/scheduler em
  processos separados, OCR português e navegador.
- Cobertura global: 85.75%; linhas:
  88.92%; decisões: 74.80%.
  O scheduler participa da medição; o mínimo global continua em 80%.

O cancelamento real preservou 144 resultados
parciais e encerrou a árvore de processos. A validação inicial identificou uma
corrida na jornada de retentativa de IA: o teste agora aguarda a confirmação no
banco antes de verificar os efeitos. Relatórios reprovados/cancelados continuam
preservados e não foram usados como comprovação da versão atual.

O launcher confirmou Python 3.12, preparou o driver local e reiniciou API e
scheduler. Backup SQLite verificado antes da aplicação:
`C:\Users\jjben\AppData\Local\LicitaLeads\data\backups\20261030T192952Z-7ebbabccb1ad480ab40aea4f968760eb.sqlite3`. O estado anterior do scheduler foi restaurado.
Os históricos das coletas, as análises canceladas e o bloqueio
`archive_members_limit` (duas tentativas) permaneceram preservados. Não foi
iniciada coleta corretiva real nem houve migração nova.

[Relatório imutável desta execução](../../data/test-report-history/34c2f50fb5be4da4be1e03d5049b561d.json).
Os dados completos da validação estão no JSON ao lado deste documento.

As integrações oficiais e os provedores pagos não foram verificados. A
[matriz de jornadas](../testes_locais.md) documenta o alcance e suas limitações;
os resultados não afirmam cobertura integral da plataforma.
