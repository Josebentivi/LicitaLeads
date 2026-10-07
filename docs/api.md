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
`municipality`, `agency`, `modality` (repetível; chave canônica ou rótulo da
fonte), `published_from`, `published_to`, `status` (alias de
`procurement_status`), `status_category` (`aberta`, `encerrada`, `cancelada`,
`suspensa`, `desconhecida`), `procurement_type` (`licitacao`,
`contratacao_direta`, `procedimento_auxiliar`), `is_srp` (`true`/`false`) e
`value_min`/`value_max` (valor estimado).

Cada contratação expõe `procurement_type` derivado da modalidade, `is_srp` e
`legal_basis` (amparo legal publicado pela fonte) — quando a fonte não publica
o campo, ele permanece nulo, nunca inferido. O comando
`python -m app.cli backfill-fields` recupera esses campos de respostas brutas
já armazenadas, sem novo crawl.

## Empresas

| Método e rota | Descrição |
|---|---|
| `GET /api/companies` | Lista empresas com contadores auditáveis (`search`, `uf`) |
| `GET /api/companies/{cnpj}` | Empresa + contadores de participação |
| `GET /api/companies/{cnpj}/participations` | Histórico de participações com contexto da contratação |
| `GET /api/companies/{cnpj}/events` | Eventos documentais atribuídos à empresa |

Filtros de `participations`: `status` (repetível; `winner`, `awarded`,
`participant`, `disqualified`, `ineligible`, `unknown`), `uf`, `agency`,
`modality`, `procurement_type`, `is_srp`, `value_min`/`value_max`,
`date_from`/`date_to` e `active_only` (processos com proposta aberta).

Filtros de `events`: `event_type`, `reason_category`, `requires_manual_review`,
`date_from`/`date_to`.

Os contadores refletem apenas fatos comprovados: resultados homologados das
APIs e eventos/participações extraídos de documentos oficiais com evidência.
Cobertura é parcial e a UI sinaliza revisão pendente; ausência de evento nunca
é interpretada como fato negativo.

## Atas de registro de preços (ARP)

| Método e rota | Descrição |
|---|---|
| `GET /api/price-registries` | Lista atas com vigência, órgão e fornecedor |
| `GET /api/price-registries/{id}` | Ata + itens registrados, fornecedores e contratação vinculada |
| `GET /api/price-registries/{id}/items` | Itens registrados da ata |

Filtros de `GET /api/price-registries`: `page`, `page_size`, `search`
(objeto/número/órgão), `agency`, `agency_cnpj`, `registry_number`, `status`,
`supplier_cnpj` e `valid_on` (atas vigentes na data).

O cabeçalho da ata vem do PNCP (`/v1/atas`); itens e fornecedores vêm do
módulo ARP do Compras.gov.br quando publicados. O PNCP não publica itens de
ata: nesse caso o campo permanece ausente, nunca inferido.

Para coletar, use `mode: "price_registries"` em `POST /api/crawls/run` ou
`python -m app.cli crawl-atas`. O `valid_on`/vigência usam o período
informado (`days`/`start_date`+`end_date`).

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
`mode` (`procurements` ou `price_registries`), `uf`, `days` (ou
`start_date`+`end_date`), `modalities`, `municipality`, `agency`, `keyword`,
`document_batch_size`.

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
| `/procurements` | Lista com filtros de UF, município, órgão, modalidade, rota de contratação, situação, SRP e faixa de valor |
| `/procurements/{id}` | Detalhe: itens, documentos, participantes, eventos e vínculo manual de empresa |
| `/empresas` | Lista de empresas com contadores de participação, adjudicação, desclassificação e inabilitação |
| `/empresas/{cnpj}` | Histórico de participações e eventos com selo de origem, confiança e revisão |
| `/atas` | Lista de atas de registro de preços com filtros de órgão, número, fornecedor e vigência |
| `/atas/{id}` | Detalhe da ata com itens, fornecedores, valores e vínculo com a contratação |
| `/leads` | Lista com filtros de score, tipo de evento e revisão pendente |
| `/leads/{id}` | Detalhe do lead, evidências e último rascunho |
| `/crawls` e `/crawls/table` | Execuções por fonte, com fragmento HTMX repollado e retry por fonte |
| `/crawls/{id}/cancel` | Encerra uma coleta ativa (form; redireciona de volta) |
| `/settings` | Visão pública das configurações e zona de risco (reset) |
| `/settings/clear-data` | Apaga todos os dados após confirmação explícita (form) |
| `/source-capabilities` | Matriz de capacidades das fontes |
