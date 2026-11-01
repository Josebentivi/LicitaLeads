# Configuração

As configurações são carregadas de variáveis de ambiente e do arquivo `.env`
(`app/config.py`, pydantic-settings). `get_settings()` é cacheada por processo;
alterações exigem reiniciar o processo.

As tabelas indicam os valores de `.env.example` para uma instalação pelo
launcher. Os padrões internos em `Settings` podem ser diferentes quando não
há variável nem arquivo `.env`. O `.env` de cada máquina e variáveis de
ambiente podem substituir o exemplo; consulte `/settings` e o diagnóstico do
launcher para a configuração efetiva, sem divulgar segredos.

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
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` ou `CRITICAL`; independente de `APP_ENV`, exige reinício |
| `APP_HOST` | `127.0.0.1` | Host do servidor (não exponha publicamente sem autenticação) |
| `APP_PORT` | `8000` | Porta |
| `DATABASE_URL` | `sqlite:///./data/licita_lead.db` | SQLite por padrão; PostgreSQL opcional |

O nível aceita maiúsculas ou minúsculas. O SQL não usa `echo`; SQLAlchemy,
aiosqlite, HTTPX e HTTPCore ficam limitados a avisos e erros, inclusive quando
os diagnósticos da aplicação usam `DEBUG`. As métricas sanitizadas de consultas
continuam disponíveis em `INFO`. A alteração não apaga os logs anteriores nem
os registros de auditoria no banco.

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
| `HTTP_TIMEOUT_SECONDS` | `60` | Timeout por requisição (até 120); padrão interno sem `.env`: `30` |
| `HTTP_MAX_RETRIES` | `5` | Tentativas adicionais com backoff (0–10); padrão interno: `4` |
| `HTTP_MAX_CONCURRENCY` | `4` | Requisições simultâneas por fonte (1–20), compartilhadas entre páginas, detalhes e unidades |
| `HTTP_MIN_REQUEST_INTERVAL_SECONDS` | `0.5` | Intervalo mínimo entre requisições, em segundos (0–10; `0` desliga) |
| `HTTP_USER_AGENT` | `LicitaLeadMonitor/0.1` | User-Agent |

Licitações, atas e contratos consultam PNCP e Compras.gov.br em paralelo.
Cada fonte compartilha seu teto entre páginas, detalhes, modalidades e unidades;
com `HTTP_MAX_CONCURRENCY=20`, são até 20 por fonte e 40 somando as duas.
A fila repõe as vagas conforme cada requisição termina. HTTP 429
aplica pausa por origem e redução temporária de ritmo, com recuperação gradual;
`HTTP_MAX_CONCURRENCY` continua sendo o teto, e não uma garantia de requisições
sempre ativas durante uma limitação da fonte.

A admissão usa uma fila FIFO por origem e calcula somente o próximo início;
requisições pendentes não reservam horários distantes. Uma mudança de ritmo ou
cancelamento recalcula a espera sem deixar vagas vazias. Após HTTP 429,
`Retry-After` continua obrigatório para toda a origem. A consulta de detalhes de
licitações do PNCP tem seu próprio intervalo adaptativo: após a pausa global,
itens e arquivos continuam sujeitos ao espaçamento mínimo compartilhado, sem
herdar o intervalo maior do detalhe. Todas as operações compartilham o mesmo
teto HTTP, e as filas prontas são atendidas pela ordem de chegada; uma operação
em espera não impede o envio de outra já liberada.

O intervalo recua pela metade após pelo menos dois sucessos da própria operação
limitada, sob a política atual, distribuídos por uma janela saudável de 15
segundos. Respostas de itens/arquivos não comprovam recuperação do detalhe, nem
respostas JSON inválidas contam como recuperação. Se uma aceleração receber
outro 429, o controlador guarda o intervalo estável anterior como piso durante
a vida desse cliente HTTP, evitando repetir a mesma aceleração rejeitada.
Uma nova piora da fonte ainda pode aumentar o intervalo. As demais operações
sem escopo específico mantêm a política por origem.
Os limites de requisições simultâneas e de frequência permanecem independentes.

Em `/crawls`, a espera para envio inclui a fila da fonte/frequência, a capacidade
HTTP, o checkpoint de validação da coleta no banco e as novas tentativas por
falhas transitórias. Após 429, a tentativa retorna diretamente à fila comum,
que já respeita a pausa da fonte. O intervalo exibido é o maior intervalo
adaptativo das operações, não um intervalo imposto a todos os envios.
A fila de resultados prontos
para gravação é exibida separadamente. Logs `connector_http_timing` também
separam as durações de admissão, sem registrar credenciais. Os sinais HTTP de
progresso são agrupados e salvos em segundo plano. Durante pausas longas,
somente a primeira tarefa da fila verifica cancelamento/lease a cada cinco
segundos, sem ocupar capacidade de rede.

## Pagamentos e contratos

| Vari?vel | Padr?o | Descri??o |
|---|---|---|
| `TRANSPARENCIA_BASE_URL` | `https://api.portaldatransparencia.gov.br` | Portal da Transparencia (federal) |
| `TRANSPARENCIA_API_KEY` | vazio | Token `chave-api-dados`; nunca aparece em logs |
| `TRANSPARENCIA_TIMEOUT_SECONDS` | `30` | Timeout por requisicao ao Portal |
| `TRANSPARENCIA_MAX_RETRIES` | `1` | Tentativas adicionais em falhas temporarias |
| `PAYMENT_SOURCES` | `transparencia,manual,municipal` | Fontes no exemplo; padrão interno `transparencia,manual` |
| `MUNICIPAL_PAYMENT_SOURCES` | `sao_luis,imperatriz,timon,caxias,bacabal` | Fontes no exemplo; padrão interno vazio. Acesso real depende da fonte e da configuração HTTP |
| `MUNICIPAL_PAYMENT_INCLUDE_DETAILS` | `false` | Busca pagamentos individuais por empenho (Fiorilli/SCPI) |
| `MUNICIPAL_PAYMENT_DETAIL_LIMIT` | `500` | Maximo de empenhos detalhados por coleta |
| `MUNICIPAL_PAYMENT_MAX_RECORDS` | `10000` | Limite de pagamentos municipais por coleta |
| `ALLOW_INSECURE_HTTP_SOURCES` | `false` | Opt-in explícito para fontes municipais que só publicam HTTP; HTTPS continua preferido |
| `SAO_LUIS_PAYMENTS_URL` | Dados Abertos | URL do CSV/pagina de Sao Luis; util quando o Cloudflare bloqueia |
| `PAYMENT_GRACE_DAYS` | `60` | Janela minima antes de exibir "sem pagamento localizado" |
| `PAYMENT_POLL_CNPJS` | vazio | CNPJs consultados pelo scheduler |
| `CONTRACT_AGENCY_CNPJS` | vazio | Orgaos consultados pelo crawl diario de contratos |
| `SCHEDULER_CONTRACT_HOURS` | `24` | Intervalo do crawl de contratos |
| `SCHEDULER_PAYMENT_HOURS` | `24` | Intervalo do poll de pagamentos |
| `SCHEDULER_MUNICIPAL_PAYMENT_HOURS` | `24` | Intervalo do poll municipal |

## OCR opcional

| Vari?vel | Padr?o | Descri??o |
|---|---|---|
| `OCR_ENABLED` | `false` | Ativa OCR para documentos `OCR_REQUIRED` |
| `OCR_PROVIDER` | `tesseract` | Provider local; requer extra `[ocr]` e binario Tesseract |
| `OCR_LANGUAGE` | `por` | Idioma do Tesseract |
| `OCR_DPI` | `200` | Renderizacao de paginas PDF |
| `OCR_MIN_CONFIDENCE` | `0.5` | Confianca minima para aceitar o texto |

## Documentos e arquivos

| Variável | Padrão | Descrição |
|---|---|---|
| `DOCUMENT_STORAGE_PATH` | `./data/documents` | Diretório dos arquivos baixados (confinamento da API) |
| `RAW_DATA_STORAGE_PATH` | `./data/raw` | Diretório de dados brutos |
| `DOCUMENT_DOWNLOAD_CONCURRENCY` | `4` | Downloads de documentos simultâneos (inteiro maior ou igual a 1) |
| `DOCUMENT_PROCESS_CONCURRENCY` | `2` | Processos isolados de extração/OCR simultâneos (inteiro maior ou igual a 1) |
| `DOCUMENT_MAX_BYTES` | `52428800` (50 MB) | Tamanho máximo por documento |
| `ARCHIVE_MAX_BYTES` | `209715200` (200 MB) | Orçamento descompactado cumulativo de toda a árvore |
| `ARCHIVE_MAX_MEMBERS` | `100` | Componentes máximos em toda a árvore, incluindo contêineres Office (1–10.000) |
| `ARCHIVE_MAX_DEPTH` | `2` | Níveis de ZIP permitidos: `1` (externo) ou `2` (externo e um interno) |
| `HOLIDAY_CALENDAR_PATH` | `./data/holidays.csv` | Feriados estaduais/municipais |

Os documentos são processados depois das consultas às APIs. Os dois limites
são independentes do tamanho do lote e exigem reinício pelo launcher após
alterações. Downloads também respeitam o teto HTTP da fonte. A extração/OCR
usa processos supervisionados; cancelar a execução encerra os descendentes.
OCR e LLM continuam sujeitos às suas opções de habilitação.

Os limites de quantidade e tamanho são compartilhados entre todos os ZIPs internos
e os componentes de DOCX/XLSX. Auxiliares AppleDouble (nome iniciado por `._` e
assinatura binária correspondente) passam pelas mesmas verificações antes de
serem ignorados; a quantidade fica registrada nas observações da extração.
Documentos reais em `__MACOSX` são examinados normalmente. Cada documento interno
mantém seu pai, localização e evidências. Somente os documentos finais são
analisados pelas regras e pela IA. Uma enumeração bloqueada ou incompleta não
remove membros e fatos anteriormente registrados.

Falhas do processo isolado trazem categoria e motivo sanitizado em
`Document.extraction_error` e no acompanhamento da execução. Retentativas comuns
continuam respeitando a espera e o limite de falhas. A correção documental
autorizada de 29/10/2026 registra um lote específico, os contadores anteriores
e o consumo único da dispensa de espera; não há opção pública para ignorá-la.

## LLM (opcional, desligado por padrão)

| Variável | Padrão | Descrição |
|---|---|---|
| `LLM_ENABLED` | `false` | Liga a análise por LLM |
| `LLM_PROVIDER` | `openai` | Padrão interno `none`; o exemplo mantém `LLM_ENABLED=false` |
| `LLM_API_KEY` | — | Obrigatória com `LLM_ENABLED=true` |
| `LLM_MODEL` | `gpt-5.6-luna` | Exemplo; padrão interno vazio. Confirme a disponibilidade do modelo antes de ativar |
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
| `SCHEDULER_PRICE_REGISTRY_HOURS` | `24` | Coleta de atas nas duas fontes, também executada ao iniciar o scheduler; respeita a pausa geral |
| `PRICE_REGISTRY_LOOKBACK_DAYS` | herda `DEFAULT_LOOKBACK_DAYS` | Override opcional da janela inicial e da revisão semanal de atas (1–3650). Sem override, são 30 dias no padrão geral. A automática consulta somente órgãos/unidades conhecidos da UF e das respectivas fontes; sem unidades não há consulta nacional nem cobertura confirmada. PNCP usa vigência na carga inicial e atualização nas incrementais; Compras.gov.br usa início da vigência. O formulário manual permite escolher os dias apenas daquela execução e começa com o menor valor entre 7 e a janela automática. |
| `SCHEDULER_ACTIVE_REFRESH_MINUTES` | `60` | Atualização de contratações ativas |
| `SCHEDULER_DOCUMENTS_HOURS` | `2` | Download/extração/detecção |
| `SCHEDULER_DEADLINES_MINUTES` | `30` | Prazos e leads |
| `SCHEDULER_CONTACTS_HOURS` | `24` | Contatos |

## Limites de coleta e scoring

| Variável | Padrão | Descrição |
|---|---|---|
| `DOCUMENT_BATCH_SIZE` | `30` | Documentos por lote (1–500) |
| `CRAWL_MAX_RECORDS_PER_SOURCE` | `50` | Limite de registros por fonte; padrão interno `0` (sem limite) |
| `CRAWL_MAX_PAGES` | `20` | Limite de páginas por coleta; padrão interno `0` (sem limite) |
| `ACTIVE_REFRESH_BATCH_SIZE` | `25` | Processos ativos por fonte atualizados diretamente por ID, priorizando a observação mais antiga (1–500) |
| `COMPRAS_RECHECK_BATCH_SIZE` | `100` | Identidades conhecidas do Compras.gov.br revisitadas por rodada de cada fluxo (1–500) |
| `COLLECTION_INCREMENTAL_OVERLAP_DAYS` | `2` | Dias de sobreposição nas janelas automáticas (1–30) |
| `COLLECTION_RECONCILIATION_DAYS` | `7` | Intervalo em dias da revisão de toda a janela configurada (1–90) |
| `CRAWL_STALE_AFTER_MINUTES` | `180` | Janela para marcar coleta travada como falha |
| `MIN_LEAD_SCORE` | `50` | Score mínimo para lead `new` |
| `MANUAL_REVIEW_CONFIDENCE_THRESHOLD` | `0.75` | Confiança mínima sem revisão |
| `RUN_LIVE_CONTRACT_TESTS` | `false` | Habilita testes live (também exige marker `live`) |
| `TEST_SUITE_TIMEOUT_SECONDS` | `3600` | Prazo total da suíte local, incluindo preparação e qualidade (60–21600); reinicie pelo launcher após alterar |

### Configurações adicionais

| Variável | Valor do exemplo | Uso |
|---|---|---|
| `ALLOWED_HOSTS` | `127.0.0.1,localhost,testserver` | Hosts aceitos pela aplicação local |
| `CONTACT_DOMAIN_DISCOVERY_ENABLED` | `true` | Permite descoberta de domínio corporativo; o enriquecimento continua condicionado às suas configurações |
| `DOCUMENT_MAX_ATTEMPTS` / `DOCUMENT_RETRY_BASE_MINUTES` | `5` / `15` | Limite e espera-base para falhas documentais |
| `LLM_MAX_REQUESTS_PER_HOUR` | `500` | Limite global persistente, incluindo tentativas |
| `LLM_INPUT_PRICE_PER_MILLION_USD` / `LLM_OUTPUT_PRICE_PER_MILLION_USD` | `0.1` / `0.5` | Preços configurados para cálculo de custo histórico, não uma cotação automática |
| `SESSION_POLL_ENABLED` / `SESSION_POLL_MINUTES` / `SESSION_POLL_DOCUMENT_LIMIT` | `true` / `15` / `20` | Rechecagem periódica de documentos de sessão |
| `PAYMENT_INGESTION_BATCH_SIZE` | `250` | Quantidade por transação de importação de pagamentos |

O exemplo define `OCR_PROVIDER=tesseract` com `OCR_ENABLED=false`; o padrão
interno do provedor é `none`. Em todos os casos a ativação também depende das
demais configurações e dependências. Valores de preço de IA devem ser conferidos
pelo operador; resultados e custos existentes não são recalculados.
