# API e interface web

## Agenda e navegação de análise

`GET /dashboard` mostra a agenda do analista e o estado das atualizações.
Na interface, `GET /leads` aceita `q` (empresa, CNPJ ou processo), `lead_status`,
`due` (`open`, `today`, `24h`, `72h`), `event_type`, `min_score`, `requires_review`,
`created_from`, `created_to`, `include_history`, `run` (UUID da coleta) e `page`.
`/leads` abre a fila de prazos oficiais vigentes (`due=open`). Use `due=` para
consultar todas as oportunidades atuais. A exportação usa os mesmos filtros.
O detalhe `GET /api/leads/{id}` sinaliza `deadline.revalidation_pending=true`
quando um prazo oficial armazenado ainda exige revalidação da evidência.

`GET /procurements` também oferece `q`, `published_from`, `published_to` e `run`.
O recorte por coleta usa vínculos de origem e documentos do lote; pode incluir
oportunidades anteriores dessas contratações, não apenas as criadas na execução.
Filtros inválidos não são silenciosamente removidos para ampliar a busca.

`GET /evidencias/{id}` é a visão HTML da evidência, com trecho escapado,
localizador e links de origem. `GET /api/evidence/{id}` continua retornando
metadados JSON. Datas-base de cálculo nunca substituem a data-limite ausente.

O formulário `POST /leads/{id}/review` aceita `decision`, `reviewer`, `notes`,
`return_to` e `continue_review`. Destinos de retorno são restritos a `/leads`
com seus filtros locais; registros históricos/invalidados não aceitam revisão
nova por esse formulário. Erros mantêm os campos e o sucesso é confirmado na tela.
Os formulários de coleta também reapresentam campos não secretos em erros 422.
Veja [experiência do analista](experiencia_usuario.md).

## Encerramento da aplicação

O rodapé da sidebar abre `GET /application/shutdown`, com confirmação e alternativa
sem JavaScript. O encerramento completo exige inicialização pelo `iniciar.bat` e
conexão local; iniciar somente a API não permite controlar o scheduler externo.

- `GET /api/application/shutdown`: disponibilidade, identificador e progresso do
  encerramento. Não retorna o token privado de controle.
- `POST /api/application/shutdown`: corpo `{"token": "<confirmação da página>"}`,
  cabeçalho `Origin` da própria aplicação; retorna `202`. Chamadas repetidas
  retornam o mesmo identificador. Origem, token ou conexão inválidos recebem `403`;
  supervisão indisponível recebe `409`.
- `POST /application/shutdown`: formulário com o mesmo token e comportamento.

Durante o encerramento, novas ações de alteração recebem `503`. Coletas e IA
ficam pausadas na próxima abertura; trabalhos pendentes e ativos são cancelados,
sem remover dados concluídos. O launcher reserva 60 segundos para limpeza
cooperativa e registra processos que precisem de finalização forçada. Perda de
comunicação não comprova encerramento completo. Nenhum PID ou comando é aceito
nessas interfaces.

## Central de IA

`GET /ia` reúne Resumo, Documentos e análises, Sugestões para revisão, Consumo e
Configuração. HTML e API usam os mesmos serviços; listagens têm 20 registros por
página. Filtros: `run`, `procurement` (UUIDs), `company` (CNPJ exato), `source` e
`status`. O filtro de empresa nos documentos representa contexto das contratações;
nas sugestões, exige a identidade candidata exata.

- `GET /api/llm/overview`: fila, controles, consumo e configuração sem segredos.
- `GET /api/llm/documents` e `/documents/{id}`: documentos e análises paginadas.
- `GET /api/llm/jobs/{id}`: resultado, cobertura, evidências e tentativas recentes.
- `GET /api/llm/suggestions` e `/suggestions/{id}`: candidatos separados dos fatos.
- `POST /api/llm/controls/{collections|llm}`: corpo `{"paused": true}`.
- `POST /api/llm/documents/{id}/enqueue`: enfileiramento local, resposta `202`.
- `POST /api/llm/batches/{id}/{cancel|retry}`: ações elegíveis, resposta `202`.
- `POST /api/llm/suggestions/{id}/{confirm|reject}`: `company_id` na confirmação,
  `reviewer` e `note` opcionais. Retorna `409` para revisão anterior, evidência
  obsoleta, identidade incompatível ou conflito; `404` para recurso inexistente.

Essas ações não executam chamadas ao LLM na requisição web. A manutenção mantém
os bloqueios existentes. Endpoints antigos de IA sob `/api/crawls` são preservados.

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
`municipality`, `agency`, `agency_cnpj`, `company_cnpj`, `modality` (repetível;
chave canônica ou rótulo da fonte), `published_from`, `published_to`, `status`
(alias de `procurement_status`), `status_category` (`aberta`, `encerrada`,
`cancelada`, `suspensa`, `desconhecida`, `prazo_resposta_aberto`), `procurement_type` (`licitacao`,
`contratacao_direta`, `procedimento_auxiliar`), `is_srp` (`true`/`false`) e
`value_min`/`value_max` (valor estimado).

`contracting_form` aceita `licitacao`, `dispensa` ou `inexigibilidade`.
`licitation_modality` agrupa as variantes das modalidades em `pregao`,
`concorrencia`, `concurso`, `leilao` e `dialogo_competitivo`. Use
`auxiliary_procedure` para procedimentos como `credenciamento` e `is_srp` para
registro de preços. `government_sphere` usa `F`, `E`, `M` ou `D`.
`object_query`, `object_preset` (repetível) e `object_group` buscam no texto do
objeto e dos itens. `company_participation_only=true` restringe o recorte por
CNPJ a participações comprovadas.

`agency_cnpj` casa o CNPJ do órgão que publica a contratação (aceita pontuação;
exige 14 posições alfanuméricas). `company_cnpj` retorna contratações
vinculadas à empresa por **participações comprovadas** (resultados homologados
das APIs e fatos documentais) ou por **fornecedores registrados em atas**
(ARP), casando tanto o vínculo resolvido quanto o `numeroControlePNCP`
publicado; exige CNPJ válido (dígitos verificadores). CNPJ malformado responde
`422`.

Cada contratação expõe `procurement_type` derivado da modalidade, `is_srp` e
`legal_basis` (amparo legal publicado pela fonte) — quando a fonte não publica
o campo, ele permanece nulo, nunca inferido. O comando
`python -m app.cli backfill-fields` recupera esses campos de respostas brutas
já armazenadas, sem novo crawl.

## Empresas

| Método e rota | Descrição |
|---|---|
| `GET /api/companies` | Lista empresas com contadores auditáveis (`search`, `uf`, `published_from`, `published_to`, `has_lead`, `active_only`) |
| `GET /api/companies/{cnpj}` | Empresa + contadores de participação |
| `GET /api/companies/{cnpj}/participations` | Histórico de participações com contexto da contratação |
| `GET /api/companies/{cnpj}/events` | Eventos documentais atribuídos à empresa |

Filtros de `GET /api/companies`: `published_from`/`published_to` recortam pela
data de publicação das contratações por trás dos contadores; `has_lead` mostra
apenas empresas que já geraram lead; `active_only` mostra apenas empresas com
participação comprovada em processo que ainda pode receber propostas (a
cobertura depende de documento — processo sem participação comprovada não
aparece).

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

Empresas em processo ativo: quando um documento oficial comprova a participação
de uma empresa em contratação que ainda pode receber propostas, o sistema cria
o evento `PARTICIPATION_DETECTED` (uma vez por empresa/processo, com evidência)
e o lead correspondente pelo fluxo normal. Processos encerrados não disparam
esse gatilho.

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

As atas são coletadas automaticamente ao iniciar o scheduler e a cada 24 horas,
com janela inicial e de revisão herdada de `DEFAULT_LOOKBACK_DAYS` (30 dias),
salvo override explícito de `PRICE_REGISTRY_LOOKBACK_DAYS`. Somente os órgãos/unidades
conhecidos da UF e suas fontes elegíveis são consultados, respeitando a pausa geral. Na
interface, `/atas` oferece um formulário de coleta manual com UF, órgão/unidade,
fonte e período De/Até (inicialmente o menor entre 7 e a janela automática
configurada).
A lista reúne unidades já identificadas em
contratações coletadas; ela pode estar incompleta e não representa cobertura
integral da UF. A seleção envia CNPJ e unidade à fonte, sem iniciar uma consulta
nacional silenciosamente. As fontes disponíveis são as já identificadas para
a unidade. “Todos os órgãos/unidades conhecidos” consulta essas unidades da UF,
e “Todas as fontes disponíveis” consulta as fontes identificadas para cada uma.
A lista exata de unidades e fontes fica congelada nos filtros do lote, inclusive
nas repetições e continuações. Uma unidade com falha não interrompe a consulta
das demais; falhas exigem continuação explícita. `/crawls` acompanha a execução.
O formulário não altera o agendamento. HTTP 204 e listas vazias bem-sucedidas
são registrados como consulta concluída sem resultados.

`GET /atas/collection-scopes?uf=MA` fornece as opções do formulário.
`POST /atas/collect` recebe `uf`, `registry_scope`, `connector`, `start_date` e
`end_date`; `days` continua aceito como fallback quando as datas não são
informadas. `registry_scope=all` seleciona todas as unidades conhecidas e
`connector=all` seleciona suas fontes disponíveis.
seleções ausentes, desconhecidas ou incompatíveis retornam 422.
Na API, `POST /api/crawls/run` também aceita `unit_code` opcional (até 30 caracteres).
Unidade, órgão e período permanecem nos filtros de repetição e continuação.

Para iniciar pela API ou CLI, use `mode: "price_registries"` em `POST /api/crawls/run` ou
`python -m app.cli crawl-atas`. O `valid_on`/vigência usam o período
informado (`days`/`start_date`+`end_date`).
O formulário web usa a seleção de unidades conhecidas; a chamada direta da API
não a aplica automaticamente. No feed de atas do PNCP, a UF não é enviada como
filtro direto: sem CNPJ do órgão e unidade, a consulta pode abranger resultados
nacionais. A CLI `crawl-atas` também pode consultar nacionalmente.

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
| `POST /api/crawls/{id}/continue` | Agenda outra execução do mesmo lote; `202`, `404` ou `409` se ocupado/sem pendências |
| `POST /api/crawls/{id}/cancel` | Solicita o encerramento de uma coleta pendente/em andamento; responde `202`, `404` se não existir ou `409` se já terminou |
| `GET /api/crawls` | Lista execuções (`connector`, `status`) |
| `GET /api/crawls/{id}` | Detalhe de uma execução |
| `POST /api/crawls/{id}/retry-documents` | Agenda somente documentos com falha elegíveis do lote; responde `202`, `404` ou `409` |

O reprocessamento não recebe URLs ou IDs arbitrários do cliente: o servidor
seleciona os arquivos ativos do lote original que continuam com falha, dentro
do limite de tentativas e com prazo vencido. A resposta contém `run_id`,
`parent_run_id`, `status` e `documents_selected`. A execução filha usa
`filters.mode="documents"`, registra `retry_of_run_id` e os IDs selecionados,
sem repetir descoberta ou coleta de processos. A pausa do scheduler é preservada.

Corpo de `POST /api/crawls/run`: `connector` (`pncp`, `compras_gov`, `all`),
`mode` (`procurements`, `price_registries` ou `contracts`), `uf`, `days` (ou
`start_date`+`end_date`), `modalities`, `municipality`, `agency`, `agency_cnpj`,
`unit_code`, `keyword`, `object_query`, `object_preset` (lista), `object_group`,
`government_sphere`, `document_batch_size`, `discovery_kind` (`publication`,
`update` ou `open_proposals`) e `force_refresh` (padrão `false`). As datas
explícitas devem ser fornecidas em conjunto e em ordem e, quando presentes,
têm prioridade sobre `days`. Novas consultas de contratações por publicação
aceitam datas dentro dos últimos dois anos até hoje, no fuso da aplicação.
Os formulários web enviam `start_date`/`end_date`
pelos campos De/Até. O corpo aceito não
contém um campo `process_documents`; a opção documental da interface pertence
ao formulário web. Campos extras são ignorados pelo modelo atual: enviar
`process_documents=false` não desliga o processamento. A solicitação de
licitações pela API usa o padrão do serviço, com processamento documental;
atas e contratos não acionam essa etapa. Em licitações, atas e
contratos, `force_refresh=true` reconsulta detalhes já verificados e ignora
validadores HTTP locais; a opção permanece nas continuações. A coleta manual
não avança a cobertura automática. Veja [coletas incrementais](coletas_incrementais.md).

`records_updated` conta registros revisitados, por compatibilidade. Os novos
contadores de alterações efetivas, sem alteração, versões antigas ignoradas e
conflitos são distintos e podem estar indisponíveis em execuções antigas. A
consulta à fila de IA é separada: `GET /api/crawls` e `GET /api/crawls/{id}`
incluem `llm` quando houver dados, e `POST /api/crawls/{id}/retry-llm` responde
`202`, `404` ou `409`; veja [fila de IA](fila_ia.md).

## Contatos

| Método e rota | Descrição |
|---|---|
| `POST /api/contacts/import` | Importa CSV UTF-8 (multipart) de contatos corporativos |

Colunas obrigatórias: `cnpj`, `contact_type`, `contact_value`, `source_url`,
`is_corporate`. O limite é de 2 MB, o contato deve ser explicitamente
corporativo e `source_url` deve ser público e usar HTTP(S). Valores dos tipos
`email`, `phone` e `whatsapp` passam por validação de formato. A resposta é
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
tabela), `files_removed` e `scheduler_paused` (as coletas automáticas são
pausadas para o banco não ser repovoado); `422` sem confirmação; `409` quando há
coleta ou job do scheduler em andamento. Configuração (`.env`) e migrations são
preservadas.

## Páginas web (Jinja2/HTMX)

O histórico paginado está disponível em `GET /api/history/{entity_type}/{id}`
(`procurement`, `price_registry`, `contract`, `payment`). Aceita `page` e
`page_size` (1–200), inclui alterações dos itens e conserva observações
rejeitadas. Consulte [coletas incrementais](coletas_incrementais.md) para as
regras de precedência e os contadores adicionais, sem mudar a semântica dos
campos legados `records_updated`/`updated`.

As páginas não aparecem no Swagger (`include_in_schema=False`).

| Rota | Descrição |
|---|---|
| `/` | Redireciona para `/dashboard` |
| `/dashboard` | Métricas operacionais e últimos leads |
| `/procurements` | Lista com filtros de UF, município, órgão, CNPJ do órgão, CNPJ da empresa licitante/fornecedora, modalidade, rota de contratação, situação, SRP e faixa de valor |
| `/procurements/{id}` | Detalhe: itens, documentos, participantes, eventos e vínculo manual de empresa |
| `/empresas` | Lista de empresas com filtros de período de publicação, "somente com lead" e "em processo ativo"; contadores de participação, adjudicação, desclassificação e inabilitação |
| `/empresas/{cnpj}` | Visão completa com seções expansíveis, fontes, históricos, leads, revisão e rascunhos |
| `/atas` | Lista de atas de registro de preços com filtros de órgão, número, fornecedor e vigência |
| `/atas/collect` | Inicia uma coleta de atas nas duas fontes (POST de formulário; acompanha em `/crawls`) |
| `/atas/{id}` | Detalhe da ata com itens, fornecedores, valores e vínculo com a contratação |
| `/contratos`, `/contratos/{id}` | Contratos, coleta manual e detalhe financeiro |
| `/pagamentos`, `/pagamentos/portais` | Pagamentos, fontes municipais e importação de arquivos |
| `/leads` | Lista com filtros de score, tipo de evento e revisão pendente |
| `/leads/{id}` | Detalhe do lead, evidências e último rascunho |
| `/crawls` e `/crawls/table` | Execuções por fonte, com fragmento HTMX repollado e retry por fonte |
| `/crawls/{id}/cancel` | Encerra uma coleta ativa (form; redireciona de volta) |
| `/crawls/{id}/retry-all` | Recria uma coleta terminal (falhou/parcial/cancelada) com os filtros originais; `409` se já houver coleta ativa para a fonte |
| `/settings` | Visão pública das configurações, status do scheduler e zona de risco (reset) |
| `/settings/clear-data` | Apaga todos os dados após confirmação explícita (form) |
| `/settings/pause-scheduler` | Pausa as coletas automáticas (form) |
| `/settings/resume-scheduler` | Retoma as coletas automáticas (form) |
| `/settings/release-stale-leases` | Remove travas expiradas e travas de coleta sem coleta ativa (form) |
| `/source-capabilities` | Matriz de capacidades das fontes |
| `/crawls/{id}/documents` | Arquivos do lote, falhas, tentativas e elegibilidade atual |
| `/crawls/{id}/retry-documents` | Inicia reprocessamento seletivo de falhas (form) |
| `/testes` | Diagnóstico da aplicação, última execução e botões para a suíte completa |
| `/testes/executar` | Inicia diagnóstico rápido ou suíte local por etapas em background (form) |
| `/testes/cancelar` | Cancela a execução de testes ativa e preserva resultados parciais (form) |
| `/testes/status` | Partial HTMX com o andamento/resultado da execução atual |
| `/ajuda` | Manual de uso em linguagem simples, voltado ao advogado, com seção curta de operação das coletas |

O formulário `/crawls/{id}/continue` continua o lote; `/crawls/{id}/retry`
repete uma fonte elegível e `/crawls/{id}/retry-all` cria outra execução.
Os formulários de repetição aceitam `force_refresh` opcional; o padrão é
`false`, enquanto uma continuação mantém a opção congelada. A tela também
oferece as ações de retentativa e cancelamento dos lotes de IA.
## Contratos e pagamentos

| Metodo e rota | Descricao |
|---|---|
| `GET /api/contracts` | Lista contratos com filtros de orgao, fornecedor, numero e vigencia |
| `GET /api/contracts/{id}` | Detalhe com itens, termos, pagamentos e resumo de conciliacao |
| `GET /api/payments` | Lista fatos de pagamento; `unlinked` nao significa inadimplencia |
| `POST /api/payments/crawl` | Consulta o Portal da Transparencia federal por CNPJ/ano |
| `POST /api/payments/import` | Importa CSV/JSON de um portal estadual/municipal |
| `GET /api/payments/sources` | Lista fontes federais/manuais/municipais configuradas |
| `GET /api/payments/municipal/sources` | Lista as cinco fontes municipais, configuracao e ultima coleta |
| `POST /api/payments/municipal/run` | Coleta uma fonte municipal: `source`, `year`, `include_details`, `company_cnpj` |

Em `GET /api/contracts`, o filtro `expiring_within_days` aceita de 1 a 365 dias.
A janela inclui hoje e o último dia em horário de Brasília. Ordena os contratos pela
primeira data publicada no intervalo. `valid_until` conserva a data do cabeçalho
e `term_expirations` apresenta as datas dos termos com situação e origem para conferência.

No detalhe do contrato, `terms_availability` informa a última observação
conclusiva do PNCP, com data, status HTTP e identificador do registro de origem.
Quando a consulta ao detalhe confirma a exclusão com HTTP 410, a disponibilidade
dos termos é registrada como `not_published`. Os cabeçalhos e termos coletados
anteriormente fazem parte do histórico. Uma consulta posterior bem-sucedida
atualiza a disponibilidade com sua própria evidência.

No detalhe do contrato, `payment_summary.payment_count` conta os registros vigentes
da fase de pagamento vinculados ao contrato. `paid_value` e `balance` ficam nulos
quando a contagem é zero ou algum desses registros está sem valor. A situação
`pago` exige pagamentos de valor positivo que atinjam o valor contratual de
referência. Essa referência usa o primeiro valor publicado entre global, acumulado
e inicial, preservando zero. A ausência de registros descreve o acervo local.

As fontes municipais de Sao Luis, Imperatriz, Timon, Caxias e Bacabal sao
independentes. Uma falha fica registrada como `CrawlRun` com status `failed` e
diagnostico da fonte; nao e convertida em `empty`.


## Dossiê da empresa e pontuação 2.0

- `GET /api/companies/{cnpj}/overview`: identificação, contadores, estatísticas
  de participação, leads prioritários, prazos futuros e contatos.
- `GET /api/companies/{cnpj}/sections/{section}`: `page` (a partir de 1),
  `page_size` (20 por padrão, máximo 100), `historical=false`, `status` e
  `active_only=false`. Retorna `items`, `total`, `page` e `page_size`.
- `GET /api/companies/{cnpj}/sections/{section}/{id}`: detalhe, grupos de
  registros relacionados e fontes, com `page` para grupos de 20 registros.
- `POST /api/companies/{cnpj}/rescore`: corpo JSON com `request_id` (UUID) e
  `lead_ids` (1 a 50 UUIDs da empresa). Retorna notas anteriores/novas e
  diagnósticos dos leads inelegíveis. Repetir o mesmo identificador e seleção
  reaproveita o resultado, inclusive quando houver registros inelegíveis.

Seções permitidas: `leads`, `procurements`, `price_registries`, `contracts`,
`payments`, `events`, `deadlines`, `documents`, `contacts`. Pertencimento é
verificado antes de devolver detalhes. Identificadores conflitantes não são
corrigidos pela consulta. Recurso alheio/inexistente: 404; seleção ou seção
inválida: 422; recálculo concorrente ou identificador reaproveitado para outra
seleção: 409. Ações seguem as proteções de origem e manutenção existentes.

A nota 2.0 tem máximos de 30/30/25/5/10 para urgência, evidência, relevância,
contato e valor. A migração não recalcula notas antigas. Revisões e recibos de
idempotência são transacionais; o histórico não é reconstruído retroativamente.
Os endpoints existentes de empresas, leads e evidências continuam disponíveis.


### Verificação documental de participantes

`GET /api/procurements/{id}/participants` conserva os campos existentes e acrescenta `verification_status`, `verification_reason`, `scope` e `monetary_values`. Cada valor inclui natureza (`proposal`, `initial_proposal`, `revised_proposal`, `final_result`), escopo, lote, item local, eventual vínculo oficial, valor decimal e referências de evidência. Vários lotes não são somados nos campos legados `proposal_value` e `final_value`.

As listagens de participantes, documentos e eventos da contratação aceitam `include_history=true`. O padrão exclui atribuições invalidadas e aliases documentais das consultas operacionais. A listagem de leads também aceita `include_history=true`; seus detalhes e os rascunhos existentes continuam acessíveis. Gerar outro rascunho de um lead sem vigência retorna `409`.

Os detalhes da seção de contratações da empresa apresentam os mesmos valores e verificações. O histórico da contratação inclui alterações monetárias, verificação de atribuições e diagnósticos para revisão.
