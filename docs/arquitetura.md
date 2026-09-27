# Arquitetura

## Visão em camadas

```text
PNCP / Compras.gov.br
        |
        v
app/connectors          descoberta e coleta (DTOs + payload bruto)
        |
        v
app/services/ingestion  persistencia bruta, normalizacao, deduplicacao
        |
        v
app/services/documents  download seguro e extracao de texto
        |
        v
app/services/event_detection  eventos e participantes com evidencia
        |
        v
app/services/deadlines  calculo explicavel de prazos
        |
        v
app/services/contacts   contatos corporativos evidenciados
        |
        v
app/services/lead_scoring  score 0-100 com breakdown
        |
        v
app/services/outreach   rascunhos (nunca envio)
        |
        v
app/api / app/web / app/cli / app/jobs  apresentacao e operacao
```

## Fluxo de uma coleta

1. `IngestionPipeline.create_run` grava um `CrawlRun` `pending` com os filtros.
2. `run()` adquire um lease no banco (`JobLease`) para evitar coletas
   equivalentes concorrentes.
3. Cada conector executa `discover_procurements` e, por contratação,
   `fetch_procurement`, `fetch_items`, `fetch_documents` e `fetch_results`.
4. Toda resposta é persistida como `SourceRecord` (payload bruto, hash,
   parâmetros, disponibilidade) antes de qualquer normalização.
5. O `ProcurementRepository` resolve o registro canônico (dedup) e aplica os
   campos; `FieldObservation` registra valor, origem e status por campo.
6. Documentos ficam `pending` para o serviço de processamento.
7. Com `process_documents`, o lote de documentos pendentes é baixado, extraído,
   dividido em `DocumentChunk` e submetido à detecção.
8. Eventos e participantes detectados viram `Evidence` + `ProcurementEvent` +
   `Participant`; o motor de prazos gera `Deadline`; o scoring cria/atualiza
   `Lead`.
9. Contadores e diagnósticos por fonte são persistidos no `CrawlRun`; uma fonte
   que falha não descarta os registros válidos das demais.

## Componentes

| Caminho | Papel |
|---|---|
| `app/connectors/` | Protocolo `ProcurementSourceConnector`, DTOs e implementações PNCP/Compras.gov.br |
| `app/connectors/capabilities.py` | Registro auditado de capacidades usado por `GET /api/source-capabilities` |
| `app/services/ingestion/` | Pipeline, persistência, processamento de documentos, contatos, auditoria e recuperação de coletas |
| `app/services/documents/` | Download seguro, extração e limites de arquivo/ZIP |
| `app/services/event_detection/` | Regras determinísticas de eventos, motivos e associação de empresa |
| `app/services/deadlines/` | Calendário de dias úteis e motor de prazos explicável |
| `app/services/contacts/` | Provedores de contato (importação manual e site público) |
| `app/services/lead_scoring/` | Score ponderado com penalidades |
| `app/services/outreach/` | Templates determinísticos de rascunho |
| `app/services/llm/` | Analisador opcional preso à evidência |
| `app/models/`, `app/repositories/` | ORM e consultas |
| `app/api/`, `app/web.py`, `app/cli/`, `app/jobs/` | REST, UI Jinja2/HTMX, CLI Typer e scheduler |

## Contrato dos conectores

Um conector implementa `ProcurementSourceConnector` e devolve
`ConnectorResult`, que combina:

- `data` — DTOs específicos da fonte (`RawProcurement`, `RawProcurementItem`,
  `RawDocument`, `RawResult`, `RawParticipant`, `RawEvent`);
- `availability` — `available`, `empty`, `not_supported`, `not_published`,
  `access_restricted` ou `temporary_error`;
- `raw_records` — cada resposta HTTP com hash e parâmetros;
- `pagination`, `source_urls`, `collected_at` e `diagnostic`.

`EMPTY` significa "fonte suportada consultada com sucesso, zero registros" e
nunca é usado para capacidade inexistente (`NOT_SUPPORTED`). Uma lista vazia não
é o mesmo que recurso não suportado.

## Processos

| Processo | Comando | Observação |
|---|---|---|
| API/UI | `python run.py` | Uvicorn em `APP_HOST:APP_PORT`; exige banco na revisão Alembic head |
| CLI | `python -m app.cli ...` | Execuções pontuais e operação |
| Scheduler | `python -m app.jobs.scheduler` | Processo separado; a API nunca o inicia |

Jobs do scheduler são protegidos por leases no banco (`JobLeaseRepository`):
duas instâncias não executam o mesmo job ao mesmo tempo; a segunda pula.

## Persistência e tempo

- SQLAlchemy assíncrono (aiosqlite por padrão; PostgreSQL via `DATABASE_URL`).
- SQLite usa `foreign_keys=ON`, `busy_timeout=30000`, `synchronous=NORMAL` e
  `journal_mode=WAL`.
- Timestamps são armazenados em UTC e exibidos em `America/Sao_Paulo`.
- O banco é único por instância: nunca rode duas instâncias contra o mesmo
  arquivo SQLite (especialmente em pastas sincronizadas).
