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
  a vigência inicial das atas no Compras.gov.br e a publicação no PNCP.
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
python -m app.cli crawl-atas all --days 365
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
| `refresh_active_procurements` | 60 min | Atualiza contratações recentes |
| `download_new_documents` / `process_pending_documents` | 2 h | Baixa/extrai documentos pendentes |
| `detect_new_events` | 2 h | Detecção determinística |
| `recalculate_deadlines` | 30 min | Recalcula prazos |
| `create_or_update_leads` | 30 min | Atualiza leads |
| `enrich_pending_companies` | 24 h | Contatos corporativos |
| `reconcile_stale_crawls` | 15 min | Marca coletas travadas como `failed` para permitir retry |

Cada job roda sob um lease no banco (`JobLease`): se outra instância já detém o
lease, o job é pulado em vez de executado em duplicidade.
`SCHEDULER_ENABLED=false` desliga o processo.
