# Configuração

As configurações são carregadas de variáveis de ambiente e do arquivo `.env`
(`app/config.py`, pydantic-settings). `get_settings()` é cacheada por processo;
alterações exigem reiniciar o processo.

- Nomes de variáveis não diferenciam maiúsculas/minúsculas; variáveis
  desconhecidas são ignoradas.
- Segredos (`LLM_API_KEY`, `CONTACT_SEARCH_API_KEY`) nunca aparecem em logs nem
  na página `/settings` (`public_view()`).
- `DATABASE_URL` síncrona é convertida automaticamente para a variante async
  (`sqlite:` → `sqlite+aiosqlite:`, `postgresql:` → `postgresql+psycopg:`).
- `python scripts/setup.py` copia `.env.example` para `.env` se ainda não
  existir.

## Aplicação e banco

| Variável | Padrão | Descrição |
|---|---|---|
| `APP_ENV` | `development` | `development`, `test` ou `production` |
| `APP_HOST` | `127.0.0.1` | Host do servidor (não exponha publicamente sem autenticação) |
| `APP_PORT` | `8000` | Porta |
| `DATABASE_URL` | `sqlite:///./data/licita_lead.db` | SQLite por padrão; PostgreSQL opcional |

## Domínio e fontes

| Variável | Padrão | Descrição |
|---|---|---|
| `TIMEZONE` | `America/Sao_Paulo` | Fuso de exibição |
| `DEFAULT_UF` | `MA` | UF padrão das coletas |
| `DEFAULT_LOOKBACK_DAYS` | `30` | Janela padrão de busca (1–365) |
| `DEFAULT_MODALITIES` | `pregao_eletronico` | Lista separada por vírgulas |
| `PNCP_BASE_URL` | `https://pncp.gov.br/api/consulta/v1` | Consulta PNCP |
| `PNCP_INTEGRATION_BASE_URL` | `https://pncp.gov.br/api/pncp/v1` | Integração PNCP |
| `COMPRAS_GOV_BASE_URL` | `https://dadosabertos.compras.gov.br` | Dados Abertos |

## HTTP

| Variável | Padrão | Descrição |
|---|---|---|
| `HTTP_TIMEOUT_SECONDS` | `30` | Timeout por requisição (até 120) |
| `HTTP_MAX_RETRIES` | `4` | Tentativas adicionais com backoff (0–10) |
| `HTTP_MAX_CONCURRENCY` | `4` | Requisições simultâneas (1–20) |
| `HTTP_USER_AGENT` | `LicitaLeadMonitor/0.1` | User-Agent |

## Documentos e arquivos

| Variável | Padrão | Descrição |
|---|---|---|
| `DOCUMENT_STORAGE_PATH` | `./data/documents` | Diretório dos arquivos baixados (confinamento da API) |
| `RAW_DATA_STORAGE_PATH` | `./data/raw` | Diretório de dados brutos |
| `DOCUMENT_MAX_BYTES` | `52428800` (50 MB) | Tamanho máximo por documento |
| `ARCHIVE_MAX_BYTES` | `209715200` (200 MB) | Tamanho máximo descompactado |
| `ARCHIVE_MAX_MEMBERS` | `100` | Membros máximos por ZIP (1–10.000) |
| `HOLIDAY_CALENDAR_PATH` | `./data/holidays.csv` | Feriados estaduais/municipais |

## LLM (opcional, desligado por padrão)

| Variável | Padrão | Descrição |
|---|---|---|
| `LLM_ENABLED` | `false` | Liga a análise por LLM |
| `LLM_PROVIDER` | `none` | `openai` |
| `LLM_API_KEY` | — | Obrigatória com `LLM_ENABLED=true` |
| `LLM_MODEL` | — | Obrigatório com `LLM_ENABLED=true` |
| `LLM_BASE_URL` | `https://api.openai.com/v1` | Endpoint compatível |
| `LLM_TIMEOUT_SECONDS` | `60` | Timeout |
| `LLM_MAX_RETRIES` | `2` | Tentativas |
| `LLM_MAX_OUTPUT_TOKENS` | `1200` | Limite de saída |
| `LLM_MAX_CHUNKS_PER_DOCUMENT` | `40` | Chunks analisados por documento |
| `LLM_MIN_CHUNK_CHARACTERS` | `200` | Tamanho mínimo do chunk |

Com `LLM_ENABLED=true`, `LLM_PROVIDER`, `LLM_API_KEY` e `LLM_MODEL` passam a ser
obrigatórios (validação na inicialização). Achados do LLM são rejeitados se
qualquer citação, CNPJ ou nome não existir no documento.

## Contatos e outreach

| Variável | Padrão | Descrição |
|---|---|---|
| `CONTACT_SEARCH_ENABLED` | `false` | Liga a busca de contatos em sites corporativos |
| `CONTACT_SEARCH_PROVIDER` | `none` | Provedor de contatos |
| `CONTACT_SEARCH_API_KEY` | — | Chave do provedor (nunca exibida) |
| `OUTREACH_MODE` | `draft_only` | Único valor suportado |
| `OUTREACH_SENDER_NAME` | `Equipe jurídica` | Assinatura do rascunho |
| `OUTREACH_LAW_FIRM` | `LicitaLead Monitor` | Nome do escritório |
| `OUTREACH_SENDER_CONTACT` | `contato a configurar` | Contato exibido |

## Scheduler

| Variável | Padrão | Descrição |
|---|---|---|
| `SCHEDULER_ENABLED` | `true` | Liga o processo separado do scheduler |
| `SCHEDULER_DISCOVERY_HOURS` | `6` | Descoberta de novas contratações |
| `SCHEDULER_ACTIVE_REFRESH_MINUTES` | `60` | Atualização de contratações ativas |
| `SCHEDULER_DOCUMENTS_HOURS` | `2` | Download/extração/detecção |
| `SCHEDULER_DEADLINES_MINUTES` | `30` | Prazos e leads |
| `SCHEDULER_CONTACTS_HOURS` | `24` | Contatos |

## Limites de coleta e scoring

| Variável | Padrão | Descrição |
|---|---|---|
| `DOCUMENT_BATCH_SIZE` | `30` | Documentos por lote (1–500) |
| `CRAWL_MAX_RECORDS_PER_SOURCE` | `0` | Limite de registros por fonte (0 = sem limite) |
| `CRAWL_MAX_PAGES` | `0` | Limite de páginas por coleta (0 = sem limite) |
| `CRAWL_STALE_AFTER_MINUTES` | `180` | Janela para marcar coleta travada como falha |
| `MIN_LEAD_SCORE` | `50` | Score mínimo para lead `new` |
| `MANUAL_REVIEW_CONFIDENCE_THRESHOLD` | `0.75` | Confiança mínima sem revisão |
| `RUN_LIVE_CONTRACT_TESTS` | `false` | Habilita testes live (também exige marker `live`) |
