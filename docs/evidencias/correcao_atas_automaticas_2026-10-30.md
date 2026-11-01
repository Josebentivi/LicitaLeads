# Correção das atas automáticas — 30/10/2026

> **Registro histórico:** descreve o estado e os resultados da data indicada. Consulte a [documentação atual](../README.md).

Aplicação concluída em 2026-10-30T13:05:22.483272-05:00 (America/Chicago).

## Comportamento aplicado

- Atas automáticas herdam DEFAULT_LOOKBACK_DAYS (30 dias); configuração explícita de atas continua disponível.
- Somente unidades conhecidas e fontes elegíveis da UF: 364 para PNCP, 36 para Compras.gov.br no estado atual. Ausência de unidades não dispara busca nacional.
- Capacidade de descoberta reservada antes de enviar páginas, compartilhada entre unidades: para 2.000 cabeçalhos e páginas completas, 40 novas páginas PNCP ou quatro Compras.gov.br. Sobras continuam duráveis.
- Consultas salvas são consumidas antes de buscar novas páginas. Falhas reais exigem continuação explícita; fontes ocupadas adiam jobs sem criar falsas falhas ou avançar rechecagem.
- Código de catálogo numérico normalizado, operações dependentes de itens não publicados distinguem ausência de falha e adesões repetidas preservam identidade e transições auditáveis sem somar valores.
- Versão do adaptador alterada somente para Compras.gov.br; comprovações PNCP elegíveis permanecem válidas.

## Validação

956 testes offline executados em quatro processos, cada um com banco/armazenamento temporários e schema Alembic aplicado; dois testes live desmarcados. Resultado inicial: 955 passaram e um teste existente de extração excedeu seu timeout de 20 segundos. O arquivo inteiro foi repetido com cobertura: 10/10 passaram, incluindo esse teste. Nenhuma asserção ou timeout foi alterado. Não restaram casos falhos após a repetição.

Cobertura consolidada: 86.15%. Ruff, formatação (309 arquivos) e mypy (133 arquivos da aplicação) passaram. Código validado permaneceu inalterado durante a suíte final.

Respostas oficiais já armazenadas da coleta Compras.gov.br 17ddcd11-8ff1-4f1b-b759-f2a9c807fe51 foram verificadas somente em leitura: 10.807 itens, 1.202 linhas de unidades e 372 adesões converteram sem erros de validação; nenhuma requisição externa nem gravação de domínio foi feita nessa conferência.

## Aplicação e preservação

Banco efetivo confirmado pelo launcher: C:/Users/jjben/AppData/Local/LicitaLeads/data/licita_lead.db. Não havia API, scheduler ou coleta ativa antes da manutenção. Agendamentos foram pausados; backup consistente passou por integrity_check e verificação de hash, com revisão 0024_llm_queue.

Backup: C:\Users\jjben\AppData\Local\LicitaLeads\data\backups\20261030T172956Z-2aa904a7828b469a97fac6bd30dd9256.sqlite3

SHA-256 do backup: 8f3a3bc03659f38da3723f9c869dc51fc83911d340d7060e1531802ca30d18e8

Aplicação iniciada por iniciar.bat --no-browser, com ciclo inicial do scheduler pausado. Oito endpoints retornaram HTTP 200, incluindo /atas, /crawls, painel, tabela, API, configurações, saúde e CSS. A interface exibiu a janela efetiva de 30 dias. Scheduler restaurado como ativo; sua interface e heartbeat foram verificados.

As 78 execuções anteriores permanecem idênticas, inclusive lotes nacionais anuais, erros e filtros congelados. Hash do histórico: d45deb361f3699ba05a6e489fa1eb1eca07d61346e827c3fe11596fec63663e7. Análises de IA canceladas e o documento d075633d-f10c-4eba-ac99-df4c1cd2bc37 com archive_members_limit permaneceram preservados. Nenhuma nova coleta ou retentativa corretiva foi iniciada. A próxima coleta fica a cargo do usuário ou do agendamento regular.

Relatórios detalhados e tempos dos endpoints estão no JSON adjacente. Logs completos da validação: C:\Users\jjben\AppData\Local\Temp\LicitaLeads-arp-validation-pvo7rt_j.
