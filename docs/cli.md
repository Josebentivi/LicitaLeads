# CLI e scheduler

A CLI fica em `app/cli` e também é exposta como `licita-lead` após
`pip install -e .`.

```bash
python -m app.cli --help
licita-lead --help
```

## Comandos

| Comando | O que faz |
|---|---|
| `audit-sources` | Compara os OpenAPI vivos com os contratos conhecidos e grava `docs/source_audit_AAAA-MM-DD.md` |
| `crawl <pncp\|compras-gov>` | Coleta registros estruturados sem baixar documentos |
| `crawl-atas [pncp\|compras-gov\|all]` | Coleta atas de registro de preços (ARP) e itens publicados |
| `crawl-contracts [pncp\|compras-gov]` | Coleta contratos e, por padrão, itens e termos |
| `import-payments` / `import-payments-csv` | Consulta pagamentos federais ou importa CSV/JSON de portal |
| `continue-crawl ID` | Continua o lote e período fixos de uma coleta limitada, falha ou cancelada, usando a fila persistida |
| `enqueue-llm --all-extracted` | Enfileira análises de documentos elegíveis já extraídos |
| `prepare-test-browser` | Prepara o driver local de Edge ou Chrome antes da suíte offline |
| `reconcile-data` | Relatório de reparos legados; `--apply` exige backup verificado |
| `crawl-payments-municipal` | Coleta pagamentos dos portais de Sao Luis, Imperatriz, Timon, Caxias e Bacabal |
| `backfill-fields` | Recupera SRP/amparo legal/rota de contratação de payloads brutos já coletados |
| `process-documents` | Baixa e extrai documentos pendentes, detecta eventos e cria leads |
| `detect-events` | Reprocessa a detecção determinística sobre textos já extraídos |
| `calculate-deadlines` | Recalcula status de prazos e o efeito no scoring/leads |
| `enrich-contacts` | Enriquece contatos corporativos com domínio já evidenciado |
| `create-leads` | Cria/atualiza leads idempotentemente a partir de eventos confirmados |
| `run-pipeline` | Coleta → documentos → eventos → prazos → leads em um comando |
| `export-leads <arquivo.csv>` | Exporta o resumo auditável dos leads em CSV UTF-8 |

### Opções principais

- `crawl`: `--uf` (padrão `MA`), `--days` (1–365, padrão 7), `--modality`
  (repetível), `--max-pages`.
- `crawl-atas`: `--days` (1–3650, padrão 365), `--max-pages`; a janela filtra
  a vigência inicial das atas no Compras.gov.br e o período de vigência no PNCP.
  Esse padrão da CLI é independente do formulário, que inicia com até 7 dias
  (ou a janela automática, se menor), e da janela automática herdada de
  `DEFAULT_LOOKBACK_DAYS` (30 no exemplo).
- `crawl-contracts`: `--uf`, `--days`, `--agency-cnpj`, `--start-date`,
  `--end-date`, `--skip-details`, `--max-pages` e `--force-refresh`.
- `crawl-payments-municipal`: `--source` (`all` ou uma fonte), `--year`,
  `--details` (pagamentos individuais por empenho) e `--cnpj` (filtro).
- `backfill-fields`: `--limit` (máximo de respostas brutas examinadas);
  processa em lotes de 200 respostas, sem carregar todo o histórico em memória.
- `process-documents` e `detect-events`: `--limit`.
- `enrich-contacts`: `--force` (executa mesmo com
  `CONTACT_SEARCH_ENABLED=false`).
- `run-pipeline`: `--uf`, `--days`, `--connector` (`pncp`, `compras-gov`,
  `all`), `--modality`, `--max-pages`, `--document-batch-size`,
  `--skip-documents`.

### Exemplos

```bash
python -m app.cli crawl pncp --uf MA --days 7
python -m app.cli crawl compras-gov --uf MA --days 7
python -m app.cli crawl-atas all --days 30
python -m app.cli crawl-contracts pncp --days 30 --agency-cnpj 00000000000191
python -m app.cli import-payments --cnpj 00000000000191 --year 2026
python -m app.cli import-payments-csv pagamentos.csv --fonte "Portal Municipal"
python -m app.cli crawl-payments-municipal --source all --year 2026
python -m app.cli crawl-payments-municipal --source imperatriz --year 2026 --details
python -m app.cli backfill-fields
python -m app.cli run-pipeline --uf MA --days 7
python -m app.cli process-documents --limit 30
python -m app.cli export-leads leads.csv
python -m app.cli audit-sources
```

O CSV exportado contém: `lead_id`, `empresa`, `cnpj`, `orgao`, `processo`,
`evento`, `motivo`, `prazo_explicito`, `prazo_estimado`, `status_prazo`,
`score`, `status_lead` e `fonte`.

## Scheduler

O scheduler é deliberadamente um processo separado; a API nunca o importa nem
o inicia.

```bash
python -m app.jobs.scheduler
```

Jobs (intervalos configuráveis em `SCHEDULER_*`):

| Job | Intervalo padrão | Ação |
|---|---|---|
| `discover_new_procurements` | 6 h | Coleta novas contratações (`all`) |
| `discover_price_registries` | 24 h e ciclo inicial | Atas para unidades conhecidas da UF; respeita a pausa |
| `refresh_active_procurements` | 60 min | Atualiza contratações recentes |
| `process_pending_documents` | 2 h | Baixa/extrai documentos pendentes |
| `recheck_session_documents` | 15 min quando habilitado | Reconsulta documentos de sessão |
| `detect_new_events` | 2 h | Detecção determinística |
| `process_llm_queue` | 5 s | Consome a fila de IA quando habilitada e elegível |
| `recalculate_deadlines` | 30 min | Recalcula prazos |
| `enrich_pending_companies` | 24 h | Contatos corporativos |
| `reconcile_stale_crawls` | 15 min | Marca coletas travadas como `failed` para permitir retry |
| `crawl_configured_contracts` / `poll_configured_payments` | 24 h quando configurados | Contratos por órgão e pagamentos por fornecedor |
| `poll_municipal_payments` | 24 h quando configurado | Fontes municipais selecionadas |

Cada job roda sob um lease no banco (`JobLease`): se outra instância já detém o
lease, o job é pulado em vez de executado em duplicidade.
`SCHEDULER_ENABLED=false` desliga o processo.

### Pausar sem desligar

A página Configurações mostra o status do scheduler (Ativo/Pausado, última
atividade) e permite pausar/retomar as coletas automáticas. O estado fica no
arquivo `data/scheduler.paused` (fora do banco), que os jobs checam a cada
execução — a pausa impede novos ciclos e sobrevive ao reset de dados, mas não
encerra um trabalho já iniciado. O launcher
também grava `data/scheduler.pid` e encerra um scheduler órfão de sessão
anterior antes de iniciar outro. Após `clear-data`, as coletas automáticas são
pausadas automaticamente.
## Reparo de dados legados

```bash
python -m app.cli reconcile-data
python -m app.cli reconcile-data --apply
```

O modo padrão de `reconcile-data` produz um relatório de impacto e não altera
registros, mesmo quando a sessão é posteriormente commitada. `--apply` cria um
backup verificado antes dos reparos e grava um relatório `.impact.json` junto
a ele. A opção `--backup-dir CAMINHO` escolhe a pasta; por padrão, SQLite usa
`backups/` ao lado do banco efetivamente selecionado. O backup SQLite inclui
WAL; PostgreSQL exige `pg_dump` disponível, e uma falha impede a aplicação.
O manifesto registra backend, revisão e SHA-256, sem credenciais.
Backup e aplicação pertencem à mesma janela exclusiva de manutenção. A janela
recusa coletas/jobs ativos e bloqueia novas gravações da aplicação até o fim;
recibos de outra janela ou revisão não autorizam reparos.

Use o `DATABASE_URL` efetivo indicado por `python scripts/launcher.py --check`.
A CLI não herda automaticamente o redirecionamento do launcher em pastas
sincronizadas. Registros legados sem completude comprovada e correspondências
ambíguas permanecem para revisão humana; processos não são separados por
similaridade. Não execute a aplicação dos reparos enquanto houver coleta ativa.

### Validação limitada das fontes oficiais

```bash
python scripts/validate_sources.py --output docs/source_validation.md
```

Esse comando usa banco e armazenamento temporários e não persiste dados de
domínio. Consulta até duas páginas de descoberta, detalhes de um processo por
fonte, até dois CSVs de São Luís e alternativas HTTPS. O relatório distingue
falhas, amostras parciais, operações não suportadas e verificações sem amostra.
Falhas externas geram código de saída diferente de zero. A auditoria OpenAPI
analisa métodos, parâmetros e schemas declarados; encontrar uma rota não basta
para declarar um contrato validado.

## Reconsulta de versões verificadas

`crawl`, `crawl-atas`, `crawl-contracts` e `run-pipeline` aceitam
`--force-refresh` para consultar novamente os detalhes no período escolhido.
Sem essa opção, versões completas e comprovadas podem ser reaproveitadas.
As opções atuais de processamento de documentos continuam valendo. A flag
é preservada por `continue-crawl ID`.


### Reparar participantes, valores e aliases documentais

Confirme o banco efetivo pelo launcher antes de executar comandos diretamente. O diagnóstico de todo o acervo usa somente material oficial já armazenado:

```powershell
python -m app.cli reconcile-data --participation-integrity
python -m app.cli reconcile-data --participation-integrity --apply
python -m app.cli reconcile-data --participation-integrity --apply --resume ID
```

A aplicação exige manutenção exclusiva e cria backup verificado, relatório de impacto e relatório de resultado. Há confirmação por processo e ponto de retomada persistente. Repetir o reparo não duplica fatos ou histórico; retomar uma execução concluída apenas retorna seu resultado.

O comando não inicia coleta, download, OCR, contatos ou IA. Arquivos ausentes, hashes divergentes e atribuições ambíguas são diagnosticados. IDs, payloads, evidências, revisões e notas históricas são preservados. Sem `--participation-integrity`, permanecem os reparos legados já existentes do comando.
