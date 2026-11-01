# Auditoria dos testes da plataforma — 30/10/2026

> **Registro histórico:** descreve o estado e os resultados da data indicada. Consulte a [documentação atual](../README.md).

Verificação em 2026-10-30T13:13:10.573332-05:00 (America/Chicago). Código não alterado. Auditoria de comandos do executor, inventário fresco de testes, cobertura da validação recente e GET de /testes e /testes/status. Não foi necessário repetir a suíte inteira, consultar fontes oficiais ou chamar provedores pagos.

## Conclusão

A suíte possui testes relevantes de negócio, persistência, auditoria, segurança de downloads, concorrência, cancelamento, fila de IA e migrações SQLite. A expressão “suíte completa” significa todos os testes offline cadastrados; não comprova a experiência completa no navegador nem todas as integrações reais.

A descoberta atual confirmou 956 testes offline e dois live excluídos: 107 arquivos, sendo 48 unitários, 57 de integração e dois de contrato. Esses números contam parametrizações; o painel agrupa seus resultados por módulo/arquivo.

## Medições da validação recente

Cobertura combinada de linhas e decisões: 86,15%. Linhas executadas: 89,27%; decisões cobertas: 75,40%. São 1.801 linhas e 1.196 ramos não cobertos dentro do conjunto Python medido. Cobertura não mede diretamente quantidade de funcionalidades nem qualidade das asserções.

| Área Python | Cobertura combinada |
|---|---:|
| Conectores | 88,64% |
| Ingestão | 87,09% |
| Documentos | 86,40% |
| Fila e análise de IA | 88,65% |
| API REST | 65,95% |
| Camada web Python | 69,91% |

O denominador inclui somente app/. JavaScript, templates, scripts do launcher e migrações não entram nesse percentual. O scheduler é excluído explicitamente em pyproject.toml:71; isso não significa ausência de testes, pois existem testes diretos dos jobs e do controle do scheduler. Há testes de migrações e do launcher, embora esses arquivos não façam parte da métrica.

## Lacunas confirmadas

1. **Relatório da tela desatualizado:** /testes apresenta o relatório de 27/10/2026, com 654 testes e 78 módulos aprovados. A validação recente contém 956 casos. O modelo do relatório não identifica o código executado e last_report() não verifica sua correspondência com o código atual. A data aparece na página, mas não há aviso de desatualização. Referências: app/services/application_tests.py:153, :174 e :1217; app/templates/_test_status.html:17.
2. **Diagnóstico rápido limitado:** _check_web faz GET de 17 páginas e aceita HTTP 200 com HTML; _check_api lê 12 endpoints e verifica uma entrada inválida. Isso detecta indisponibilidade básica, mas não confirma conteúdo correto, cliques, seletores dependentes, persistência após ações ou comportamento JavaScript. Referência: app/services/application_tests.py:644.
3. **Fluxos de navegador ausentes:** os testes inspecionam HTML ou enviam requisições HTTP diretamente; não há harness de navegador configurado para executar app/static/app.js e HTMX. Faltam exercícios reais de seleção UF/unidade/fonte, polling, preservação de formulários, cancelar/continuar, navegação e mensagens de falha.
4. **Ações de escrita sem cobertura de execução recente:** existem oito handlers cujos corpos não foram executados na validação recente, listados abaixo. Isso não significa que os serviços subjacentes estejam sem teste; as APIs de continuação e retentativa, por exemplo, têm casos próprios.
5. **Integrações reais incompletas:** os dois contratos live fazem somente descoberta de licitações PNCP/Compras.gov.br, com até duas páginas; não verificam atas, contratos, detalhes, resultados e downloads de ponta a ponta. O checkbox de conectividade acrescenta sondas de descoberta e pagamentos configurados, não uma coleta completa. Referências: tests/contract/test_live_contracts.py:24; app/services/source_probes.py:22.
6. **Serviços opcionais e banco:** LLM, busca de contatos e OCR ficam desabilitados no ambiente geral da suíte; testes específicos usam substitutos para exercitar seus contratos e regras. O teste do Tesseract substitui pytesseract, sem verificar reconhecimento real ou idiomas instalados. As integrações usam SQLite; compilar SQL no dialeto PostgreSQL não substitui executar migrações e concorrência em PostgreSQL. Referências: tests/conftest.py:29; tests/unit/test_ocr_provider.py:43; tests/integration/conftest.py:27.
7. **Executor e tempo:** o botão executa um único pytest offline, com limite fixo de 600 segundos. A validação recente por quatro partes teve durações de 379, 560, 697 e 518 segundos. Isso demonstra risco de timeout para a suíte atual; não foi medido nesta auditoria o tempo da execução completa pelo botão. A proteção encerra processos corretamente, mas uma suíte interrompida não comprova todos os casos. O botão também não executa Ruff, formatação ou mypy. Referência: app/services/application_tests.py:102 e :929.

### Handlers não exercitados no relatório recente

- `G:\Meu Drive\Ubuntu\programas\LicitaLeads\app\api\routes\payments.py`:66 — `import_payments` (post /import).
- `G:\Meu Drive\Ubuntu\programas\LicitaLeads\app\api\routes\payments.py`:101 — `crawl_payments` (post /crawl).
- `G:\Meu Drive\Ubuntu\programas\LicitaLeads\app\web.py`:1239 — `link_event_company` (post /procurements/{procurement_id}/events/{event_id}/link-company).
- `G:\Meu Drive\Ubuntu\programas\LicitaLeads\app\web.py`:2463 — `retry_llm_from_web` (post /crawls/{run_id}/retry-llm).
- `G:\Meu Drive\Ubuntu\programas\LicitaLeads\app\web.py`:2471 — `cancel_llm_batch_from_web` (post /crawls/llm/{batch_id}/cancel).
- `G:\Meu Drive\Ubuntu\programas\LicitaLeads\app\web.py`:2486 — `retry_llm_batch_from_web` (post /crawls/llm/{batch_id}/retry).
- `G:\Meu Drive\Ubuntu\programas\LicitaLeads\app\web.py`:2549 — `retry_documents_from_web` (post /crawls/{run_id}/retry-documents).
- `G:\Meu Drive\Ubuntu\programas\LicitaLeads\app\web.py`:2904 — `continue_crawl_from_web` (post /crawls/{run_id}/continue).

## Prioridades recomendadas

- Identificar o código e o inventário executados em cada relatório; mostrar relatório antigo como desatualizado, com resultados ignorados e integrações não verificadas visíveis.
- Adicionar testes dos oito handlers e jornadas de navegador em ambiente isolado, incluindo falhas, polling e persistência dos filtros.
- Tornar o executor capaz de concluir a suíte atual, com limite adequado/configurável, progresso, resultados parciais e cancelamento supervisionado; manter a proteção do banco real.
- Ampliar contratos por fonte/operação e fixtures derivadas de respostas oficiais armazenadas, incluindo variações de tipos e respostas parciais que causaram falhas recentes.
- Acrescentar OCR real com amostra controlada e um fluxo API + scheduler em processos separados com dados temporários. PostgreSQL real deve ser uma suíte adicional quando esse backend for suportado na operação.
- Manter integrações externas e testes que geram custo em trilhas explícitas, separadas dos testes offline. Aplicar metas por áreas críticas e decisões, além do limite global, sem tratar porcentagem como prova de cobertura funcional integral.

A execução recente teve um timeout de 20 segundos em teste de extração; o arquivo inteiro passou na repetição com cobertura (10/10), como registrado em correcao_atas_automaticas_2026-10-30.md. Isso também indica que os tempos precisam ser acompanhados para distinguir regressão funcional de lentidão do ambiente.

Arquivos temporários desta auditoria: C:\Users\jjben\AppData\Local\Temp\LicitaLeads-platform-test-audit-atzd6oy5.
