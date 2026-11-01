# Coletas incrementais e histórico

## Janelas automáticas e versões verificadas

Licitações, atas e contratos conservam uma cobertura separada por fonte,
modalidade de coleta, filtros normalizados e endereços das APIs. A primeira
carga usa a janela configurada; as próximas rodadas partem do dia anterior ao
último dia confirmado (dois dias de sobreposição). A cada sete dias, uma
revisão percorre novamente a janela configurada. Pagamentos e importações
mantêm o comportamento anterior.

O marco só avança pelo fim do período consultado depois de confirmar todas
as páginas e registros, inclusive quando a resposta completa é vazia. Um
limite mantém o lote e seu período congelados. Falhas e cancelamentos precisam
de continuação explícita. Coletas manuais não avançam o marco automático.
Antes de confirmar a carga inicial ou a revisão do PNCP por publicação ou
vigência, o coletor também busca as atualizações ocorridas no período da carga.

O PNCP usa os feeds de atualização de licitações, atas e contratos. No
Compras.gov.br, a descoberta conserva os filtros oficiais de publicação ou
vigência: essas datas não comprovam atualização. Além da descoberta, cada
rodada revisita até `COMPRAS_RECHECK_BATCH_SIZE=100` identidades conhecidas,
priorizando as menos recentemente consultadas. A rotação dos documentos de
sessão permanece independente.

Uma comprovação de versão só nasce junto da gravação confirmada. Ela registra
fonte, identidade oficial, operação, data oficial e origem, hash do cabeçalho,
versão do adaptador e referências às respostas brutas. Em licitações PNCP,
`dataAtualizacaoGlobal` permite reutilizar detalhes, itens, resultados e catálogo
documental completos. Uma única consulta de detalhe, após ler os filhos,
completa os campos e confirma que a versão não mudou durante a leitura. Ela
continua exigindo uma resposta atual da fonte, mesmo em uma continuação;
não há uma consulta inicial de detalhe duplicada para cada registro descoberto
no feed com versão global. Versão ausente, anterior, divergente, evidência removida ou
adaptador alterado exige nova consulta.

Datas apenas de cabeçalho de atas, contratos e Compras.gov.br não dispensam
itens, termos, saldos, empenhos ou adesões. Quando a fonte fornece validadores
HTTP, uma resposta `304` pode confirmar o reaproveitamento da resposta completa
original; suas datas históricas são preservadas. `/crawls` mostra a estratégia,
o período efetivo e os contadores de registros e operações reaproveitados.
Execuções antigas deixam esses contadores indisponíveis.

API e serviço aceitam `force_refresh=true`; a CLI oferece `--force-refresh`
e os formulários, **Reconsultar detalhes já verificados**. Essa opção força as
consultas do período e permanece nas continuações, sem alterar as opções de
download ou extração. Os estados começam vazios após a migração: nenhuma
comprovação é inferida retroativamente.

**Repetir coleta** e **Tentar fonte novamente** criam uma nova execução com
reaproveitamento habilitado por padrão, mesmo quando a anterior usou consulta
completa. Os formulários oferecem a opção desmarcada **Reconsultar detalhes já
verificados** para forçar novamente. Os demais filtros e opções documentais são
preservados. **Continuar coleta** retoma o lote congelado com sua escolha
original de `force_refresh`; ela não é alterada na continuação.

## Endereços de documentos PNCP

Uma URL interna oficial como `https://pncp.gov.br:48707/pncp-api/v1/...`
só pode ser reconstruída como `https://pncp.gov.br/api/pncp/v1/...` quando
a resposta oficial armazenada identifica exatamente o mesmo órgão, compra e
arquivo. O resolvedor não remove portas de outros hosts nem interpreta caminhos
ambíguos. O download e cada redirecionamento continuam sujeitos a HTTPS,
DNS/IP público, host permitido, tamanho, assinatura e proteção de arquivos
compactados.

`Document.original_url` e o payload recebido permanecem intactos.
`Document.resolved_url` só é gravado após download bem-sucedido e confirmação
de que a versão e a atividade do documento continuam válidas. Novas evidências
usam esse endereço efetivo. Falhas históricas não são apagadas para abrir novas
tentativas; a retomada seletiva respeita o limite e o prazo existentes.

Repetir uma coleta conserva os cadastros reconhecidos por identidade oficial.
Os novos contadores distinguem **novos**, **alterados** e **sem alteração**.
Alterações somente na data da consulta ou na referência de origem não contam
como mudança de negócio. Uma alteração em item ou outro detalhe conta no
registro principal processado. Os contadores são por registro de cada fonte,
não uma contagem de empresas ou processos exclusivos entre fontes.

`records_updated` e `updated` continuam significando registros existentes
reprocessados, para compatibilidade. As respostas acrescentam `records_changed`,
`records_unchanged`, `records_stale_ignored` e `records_conflicted` nas coletas;
os resumos de pagamentos usam `changed`, `unchanged`, `stale_ignored` e
`conflicted`. Nas execuções legadas, os novos contadores ficam indisponíveis.

## Atualização de campos

Cada campo conserva sua origem e, quando publicada, a data oficial de
atualização. Datas de assinatura, publicação, vigência, empenho e pagamento
não são usadas como datas de atualização. Uma versão comprovadamente anterior
é guardada como observação rejeitada, sem substituir o valor aceito.

Quando datas não permitem ordenar versões, prevalece a última consulta e a
divergência fica destacada para revisão. Uma consulta iniciada/recebida antes
não ganha prioridade só porque sua transação terminou depois.

Campos ausentes, nulos ou vazios não apagam informação conhecida. Zero e
“não” continuam sendo valores válidos. Remoção exige indicação explícita do
adaptador e resposta completa. Desativação de documentos e cancelamento de
resultados continuam exigindo confirmação oficial completa.

O histórico aparece nos detalhes de contratações, atas e contratos, e pode ser
expandido na lista de pagamentos. Ele mostra antes/depois, decisão, fonte,
data da consulta, data oficial de atualização e data de processamento.
`GET /api/history/{entity_type}/{id}` aceita `procurement`, `price_registry`,
`contract` ou `payment`, com `page` e `page_size` (máximo 200).

O diário é transacional e preserva A → B → A. Seu início não reconstrói um
histórico anterior: os dados legados e seus registros brutos são preservados;
eventual reparo continua restrito ao `reconcile-data`, com backup.

## Continuar uma coleta

Os limites de páginas/registros passam a ser orçamentos de uma execução.
As respostas já recebidas e os registros ainda pendentes são guardados no
banco. Uma continuação mantém o período e os filtros originais, mesmo quando
ocorre em outro dia. Alterar filtros ou ampliar o período inicia outro lote.

Na tela Coletas, **Continuar coleta** cria uma nova execução do mesmo lote.
Também existem `POST /api/crawls/{id}/continue` (202, 404 ou 409) e
`python -m app.cli continue-crawl ID`. Uma segunda continuação concorrente é
recusada. O ponto de confirmação avança na transação que grava o registro.

Os fluxos agendados retomam lotes limitados no próximo ciclo normal, respeitando
a pausa geral. Falhas e cancelamentos exigem retomada explícita; uma falha de
registro não faz os outros registros válidos serem descartados. Importações
manuais são continuadas pelo botão ou CLI, sem reenviar o arquivo.

Nas fontes paginadas, o conector reconstrói os resultados usando respostas
salvas localmente. A página de fronteira é consultada novamente antes de buscar
páginas novas. Se os totais publicados mudarem, a partição é invalidada e deve
ser reconsultada; a cobertura não é apresentada como completa. As URLs são
sempre construídas pelos adaptadores, nunca recebidas de um cursor externo.

A descoberta de atas no PNCP e Compras.gov.br usa uma fila contínua de páginas,
respeitando `HTTP_MAX_CONCURRENCY` (padrão 4) e o intervalo mínimo entre
requisições. Cada resposta libera uma vaga para a próxima página, sem esperar
o grupo inteiro. O buffer de respostas prontas é limitado a quatro vezes a
concorrência para evitar crescimento ilimitado enquanto uma página anterior
está pendente. A primeira página determina o total; as seguintes são processadas
na ordem numérica, mesmo quando chegam fora de ordem. A página de fronteira
de uma continuação é validada antes de buscar páginas posteriores. O orçamento
conta apenas páginas novas, e respostas já recebidas são preservadas inclusive
quando outra página falha. Use concorrência 1 para leitura sequencial.

HTTP 429 aplica uma pausa compartilhada pelo mesmo endereço de origem,
respeitando `Retry-After` e com espera mínima de 0,5 segundo. Requisições ainda
na fila revalidam a pausa antes de sair. O intervalo entre novas requisições
aumenta temporariamente; respostas 429 do mesmo período de pausa não multiplicam
essa redução repetidamente. A cada oito respostas bem-sucedidas posteriores à
última limitação, o intervalo volta gradualmente ao mínimo configurado. O teto
de concorrência e a configuração do operador permanecem os mesmos.

Respostas 429 de chamadas iniciadas antes de uma redução não aumentam novamente
o intervalo, mesmo se chegarem depois da pausa. Quando o ritmo se recupera, os
horários pendentes são cancelados e reagendados, respeitando o intervalo mínimo.
Isso evita manter reservas antigas de dezenas de segundos após a recuperação.

Os logs `connector_http_timing` registram espera de admissão e tempo de rede por
tentativa. `crawl_snapshot_timing` mede a gravação transacional da resposta bruta;
`connector_rate_reduced` e `connector_rate_recovered` mostram o intervalo vigente.
Esses sinais registram apenas fonte, host, página, execução, tempos e status,
sem cabeçalhos, parâmetros completos ou conteúdo das respostas. Tempos de várias
requisições simultâneas podem se sobrepor e não devem ser somados como duração
real da coleta.

Nos pagamentos municipais, um grupo de detalhes cortado pelo orçamento fica
armazenado sem substituir o agregado do empenho. Ao receber a última parte,
o sistema finaliza o grupo em uma transação, revisitando também as linhas já
preparadas. Empenhos diferentes continuam separados pela identidade oficial.
Um agregado continua sem data de pagamento, e não é somado aos seus detalhes.

## Identidade nos arquivos de pagamentos

CSV/JSON aceita `external_id`, `id_pagamento` ou `identificador_oficial`.
O identificador é associado ao portal informado em **Portal de origem**.
Reimportar o mesmo identificador nesse portal atualiza o pagamento; em outro
portal ele representa outra identidade. `source_updated_at`,
`data_atualizacao` ou `ultima_atualizacao` podem informar uma data oficial de
atualização, quando ela existir na exportação.

Sem identificador oficial, permanece a identidade pelo conteúdo integral da
linha e um aviso explica que uma linha alterada pode gerar outro cadastro.
Não há união automática por nome, valor ou semelhança. Linhas sem CNPJ válido
continuam sem conciliação e exigem revisão; sem CNPJ nem fornecedor, são rejeitadas.


## Atas automáticas: período, escopo e orçamento

Sem `PRICE_REGISTRY_LOOKBACK_DAYS`, a janela inicial e a revisão semanal seguem
`DEFAULT_LOOKBACK_DAYS` (30 dias por padrão). O período manual não altera essa configuração.
As unidades conhecidas da UF e as fontes elegíveis são congeladas no lote. Sem unidades,
o scheduler registra o motivo e não consulta nacionalmente. Uma mudança no período ou no
conjunto de unidades cria outra cobertura; lotes nacionais antigos permanecem no histórico
para continuação explícita, com seus filtros intactos.

O limite de registros por fonte também reserva capacidade antes da descoberta de novas
páginas de atas, compartilhada entre unidades. Uma página final é sempre preservada inteira;
suas sobras ficam na fila. A continuação consome primeiro os registros salvos, com consulta
SQL limitada, antes de buscar mais páginas. Zero mantém a ausência desse limite. O total
informado pela fonte, cabeçalhos recebidos e registros selecionados são mostrados separadamente.
Falhas reais exigem continuação explícita mesmo quando também existem sobras por limite.

Jobs automáticos disputam os mesmos leases por fonte/UF. Uma fonte ocupada adia o job sem
criar uma coleta falha nem avançar a rotação de rechecagem; fontes diferentes continuam
independentes. Histórico e fatos anteriores não são reparados automaticamente.
