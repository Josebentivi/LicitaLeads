# Análise de IA em fila persistente

A correção de resultados PNCP atualiza a versão do adaptador: comprovações
anteriores não dispensam a próxima conferência das operações afetadas. Depois
de confirmadas novamente, versões elegíveis voltam a ser reaproveitadas.
Continuações com DTOs antigos também rejeitam marcadores sem homologação,
preservando a resposta armazenada.

A coleta confirma o conteúdo, os trechos extraídos e as regras determinísticas
sem esperar pela IA. Quando `LLM_ENABLED=true`, uma solicitação de análise é
gravada na mesma transação. O scheduler separado consome a fila a cada ciclo,
com uma requisição por vez e lease exclusivo entre API, CLI e scheduler.
A **Central de IA**, em `/ia` (Operação de dados), concentra a fila, documentos,
sugestões, consumo e configuração efetiva. Coletas automáticas e IA têm pausas
independentes. A pausa geral em Configurações atua sobre ambas; manutenção
exclusiva impede novos trabalhos dos dois tipos. A chamada já iniciada pode
terminar após uma pausa. Cancelamento continua interrompendo trabalho exclusivo
do lote, preservando demandas compartilhadas e documentos já extraídos.

Uma chave configurada não comprova conexão. Sem tentativas registradas, a central
mostra **não verificada**. A página não faz chamadas pagas de teste. Configurações
são somente para consulta; alterações no `.env` exigem reinício pelo launcher.

```dotenv
LLM_MAX_REQUESTS_PER_HOUR=500
LLM_INPUT_PRICE_PER_MILLION_USD=0.1
LLM_OUTPUT_PRICE_PER_MILLION_USD=0.5
```

O limite é global, em janela móvel de uma hora, com espaçamento de
`3600 / LLM_MAX_REQUESTS_PER_HOUR` segundos. Inclui falhas, novas tentativas e
reservas interrompidas; reiniciar não libera o orçamento. Os preços são USD por
milhão de tokens. Os valores precisam ser não negativos e o limite positivo.
Reinicie pelo launcher depois de alterar o `.env`.

Cada tentativa registra modelo, datas, diagnóstico sanitizado e os tokens
informados pela API. O custo calculado é
`(input_tokens × input_price + output_tokens × output_price) / 1.000.000`.
Tokens de raciocínio já integram a saída. Uma resposta rejeitada ainda pode ter
custo conhecido; ausência de uso informado significa custo desconhecido.
Preços históricos são preservados. Reutilização não gera novo custo.

Resultados completos e validados, inclusive listas vazias, podem ser
reutilizados. Conteúdo, localização, modelo, endpoint, parâmetros, prompt e
validador participam da identidade. A atividade, versão e cadeia de arquivos
ancestrais são conferidas antes da requisição e antes de gravar evidências.
Respostas inválidas ou truncadas não são consideradas completas.

Erros de autenticação, configuração, modelo ou quota bloqueiam novos envios
daquela configuração. Falhas transitórias respeitam `Retry-After` e
`LLM_MAX_RETRIES`; a fila controla as tentativas, sem multiplicá-las no HTTP.
A central `/ia` apresenta bloqueios, próximas liberações e custos. `/crawls`
conserva o resumo por execução e o acesso à central. “Tentar IA novamente”
libera uma nova tentativa explícita; mudar a configuração exige reenfileirar
com a configuração atual. Cancelar remove a solicitação daquela execução;
outras solicitações do mesmo trabalho continuam válidas.

Para analisar todos os documentos ativos já extraídos, sem downloads ou OCR:

```powershell
python -m app.cli enqueue-llm --all-extracted
```

O comando retorna o identificador do lote, documentos e análises solicitadas.
Reutiliza trechos armazenados, considera membros de arquivos compactados e
preserva os limites por documento. Na pasta sincronizada, use o Python 3.12 do
launcher e seu `DATABASE_URL` efetivo; o `.env` relativo pode apontar outro banco.
O scheduler precisa estar ativo para consumir a fila.

`GET /api/crawls/{id}` e a listagem acrescentam `llm`, nulo para execuções sem
solicitações registradas. `POST /api/crawls/{id}/retry-llm` retorna `202` com
`analyses_selected`, `404` se a coleta não existe, ou `409` sem trabalho elegível
ou durante processamento incompatível. Os formulários do painel oferecem ações
por lote, incluindo a carga dos documentos já extraídos.

Os resumos globais, por coleta e por lote são calculados em conjunto por
agregações SQL, sem carregar os trabalhos individuais para contagem. Trabalhos
compartilhados são contados uma vez por escopo; reaproveitamentos mantêm a
contagem por solicitação. Custos são agrupados e somados em `Decimal`, inclusive
no SQLite, preservando os valores históricos e custos desconhecidos. Não há
cache entre requisições: cancelamentos aparecem na próxima atualização.

As tabelas `llm_batches`, `llm_demands`, `llm_work_items`, `llm_results`,
`llm_attempts` e `llm_provider_states` são prospectivas (migração
`0024_llm_queue`). Não se inferem análises, custos ou versões anteriores.
Falhas da IA não alteram tentativas de download/OCR nem a revisão humana.
## Evidência, contexto e revisão

Os trechos são priorizados por relevância, com contexto adjacente limitado ao
mesmo documento e localização indicada no envio. O limite configurado de trechos
é preservado. A cobertura informa selecionados e omitidos: uma análise validada
de trecho não significa que todo o documento foi analisado.

O provedor usa Structured Outputs com esquema estrito. O validador local continua
verificando citações, identidade e relação entre empresa e ato. Minutas, legendas,
regras futuras e alegações não comprovam atos praticados. Todas as citações usadas
em fatos validados ficam vinculadas às evidências. Conteúdo, localização, contexto
e versões do seletor, prompt e validador participam do reaproveitamento.

Uma identidade presente em outro trecho não autoriza atribuir o ato à empresa.
Casos ambíguos aparecem em **Sugestões para revisão**, sem gerar novos fatos,
leads ou prazos. A hipótese de único vencedor conhecido também exige confirmação.
A revisão permite escolher uma participante comprovada da mesma contratação,
registrando revisor, observação e evidências. Documentos alterados ou inativos
impedem confirmação. Decisões anteriores não são sobrescritas.

Documentos já extraídos podem ser enfileirados pela central, sem coleta, download
ou OCR. Trabalhos antigos com configuração de validação diferente exigem tentativa
explícita; a atualização não libera chamadas automaticamente.
