# API e interface web

Aplicação FastAPI. Com `python run.py`, a UI fica em
<http://localhost:8000>, o Swagger em <http://localhost:8000/docs> e a
verificação em <http://localhost:8000/health>.

- Todas as rotas REST de domínio usam o prefixo `/api`.
- Listagens usam o envelope `{"items": [...], "page": 1, "page_size": 50, "total": N}`.
- Erros retornam `{"detail": "..."}` com o status correspondente.
- Datas são serializadas em ISO 8601, em UTC.

## Health

| Método e rota | Descrição |
|---|---|
| `GET /health` | Estado do processo e do banco, sem contatar APIs externas |

Resposta: `status` (`ok`/`degraded`), `version`, `database`, `scheduler`
(`separate_process`) e `environment`.

## Contratações

| Método e rota | Descrição |
|---|---|
| `GET /api/procurements` | Lista paginada com filtros |
| `GET /api/procurements/{id}` | Contratação + vínculos de fonte |
| `GET /api/procurements/{id}/items` | Itens |
| `GET /api/procurements/{id}/documents` | Documentos |
| `GET /api/procurements/{id}/participants` | Participantes/vencedores |
| `GET /api/procurements/{id}/events` | Eventos detectados |

Filtros de `GET /api/procurements`: `page`, `page_size` (1–200), `uf`,
`municipality`, `agency`, `modality`, `published_from`, `published_to` e
`status` (alias de `procurement_status`).

## Leads

| Método e rota | Descrição |
|---|---|
| `GET /api/leads` | Lista paginada, incluindo abaixo do limiar e vencidos |
| `GET /api/leads/{id}` | Lead com contratação, empresa, evento, prazo, revisões e rascunhos |
| `POST /api/leads/{id}/review` | Registra decisão humana (`approved`, `rejected`, `needs_changes`) |
| `PATCH /api/leads/{id}` | Altera campos permitidos (`lead_status`, `assigned_to`, `reason_summary`, `recommended_action`) |
| `POST /api/leads/{id}/generate-outreach` | Gera rascunho determinístico; **não envia** |

Filtros de `GET /api/leads`: `page`, `page_size`, `status` (lead_status),
`minimum_score`, `event_type`, `deadline_status`, `has_contact`,
`pending_review`, `created_from`, `created_to`.

`generate-outreach` aceita `channel` (`email`, `whatsapp`, `phone`) e
`force_regenerate`; a geração é idempotente por `facts_hash` + `template_hash`.
Sem URL pública rastreável, responde `409`.

## Coletas

| Método e rota | Descrição |
|---|---|
| `POST /api/crawls/run` | Cria e agenda uma coleta; responde `202` |
| `POST /api/crawls/{id}/cancel` | Solicita o encerramento de uma coleta pendente/em andamento; responde `202`, `404` se não existir ou `409` se já terminou |
| `GET /api/crawls` | Lista execuções (`connector`, `status`) |
| `GET /api/crawls/{id}` | Detalhe de uma execução |

Corpo de `POST /api/crawls/run`: `connector` (`pncp`, `compras_gov`, `all`),
`uf`, `days` (ou `start_date`+`end_date`), `modalities`, `municipality`,
`agency`, `keyword`, `document_batch_size`.

## Contatos

| Método e rota | Descrição |
|---|---|
| `POST /api/contacts/import` | Importa CSV UTF-8 (multipart) de contatos corporativos |

Colunas obrigatórias: `cnpj`, `contact_type`, `contact_value`, `source_url`,
`is_corporate`. Limite de 2 MB; apenas contatos explicitamente corporativos são
aceitos; `source_url` deve ser HTTP(S). Resposta:
`{created, updated, skipped, errors[]}`.

## Evidências e documentos

| Método e rota | Descrição |
|---|---|
| `GET /api/evidence/{id}` | Metadados da evidência + link de visualização |
| `GET /api/documents/{id}/view` | Serve o arquivo local (dentro do diretório configurado) ou o texto extraído |

O acesso a documentos é confinado a `DOCUMENT_STORAGE_PATH`; caminhos fora do
diretório retornam `403`.

## Eventos

| Método e rota | Descrição |
|---|---|
| `POST /api/events/{id}/link-company` | Atribui manualmente um evento a uma empresa **já participante** e recalcula o lead |

Corpo: `company_id`, `reviewer`, `note`. A empresa precisa participar da
contratação; a operação registra uma decisão humana e nunca inventa vínculo.

## Fontes

| Método e rota | Descrição |
|---|---|
| `GET /api/source-capabilities` | Capacidades auditadas por fonte (mesmo registro da matriz) |

## Manutenção

| Método e rota | Descrição |
|---|---|
| `POST /api/maintenance/clear-data` | Apaga todos os dados coletados e arquivos baixados (ação destrutiva e irreversível) |

Corpo: `confirm` (`true` obrigatório). Responde `200` com `counts` (linhas por
tabela) e `files_removed`; `422` sem confirmação; `409` quando há coleta ou job
do scheduler em andamento. Configuração (`.env`) e migrations são preservadas.

## Páginas web (Jinja2/HTMX)

As páginas não aparecem no Swagger (`include_in_schema=False`).

| Rota | Descrição |
|---|---|
| `/` | Redireciona para `/dashboard` |
| `/dashboard` | Métricas operacionais e últimos leads |
| `/procurements` | Lista com filtros de UF, município e órgão |
| `/procurements/{id}` | Detalhe: itens, documentos, participantes, eventos e vínculo manual de empresa |
| `/leads` | Lista com filtros de score, tipo de evento e revisão pendente |
| `/leads/{id}` | Detalhe do lead, evidências e último rascunho |
| `/crawls` e `/crawls/table` | Execuções por fonte, com fragmento HTMX repollado e retry por fonte |
| `/crawls/{id}/cancel` | Encerra uma coleta ativa (form; redireciona de volta) |
| `/settings` | Visão pública das configurações e zona de risco (reset) |
| `/settings/clear-data` | Apaga todos os dados após confirmação explícita (form) |
| `/source-capabilities` | Matriz de capacidades das fontes |
