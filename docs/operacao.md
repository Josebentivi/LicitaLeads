# Operação

## Requisitos

- Python 3.12 (exigido por `scripts/setup.py` e
  `scripts/check_environment.py`)
- `pip` e `venv`
- SQLite (embutido no Python)
- Sem Docker, Node.js ou banco externo obrigatório

## Instalação

```powershell
py -3.12 -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -e ".[dev,ocr]"
copy .env.example .env
python scripts/setup.py
python run.py
```

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[dev,ocr]'
cp .env.example .env
python scripts/setup.py
python run.py
```

`scripts/setup.py` cria `.env` a partir de `.env.example` (se ausente), cria
`data/documents` e `data/raw` e aplica `alembic upgrade head`.

## Launcher Windows (`iniciar.bat`)

Alternativa de um clique para usuários Windows (requer Python 3.12; se ausente,
o próprio launcher oferece a instalação via `winget`):

1. Clone o repositório e, se quiser, ajuste o `.env`.
2. Duplo clique em `iniciar.bat`.

O launcher (`scripts/launcher.py`, apenas biblioteca padrão):

- cria/reutiliza a `.venv` (recriando se estiver quebrada) e instala as
  dependências de runtime, desenvolvimento e OCR (`.[dev,ocr]`) quando o
  `pyproject.toml` mudar;
- em pastas sincronizadas (Google Drive, OneDrive), cria a `.venv` em
  `%LOCALAPPDATA%\LicitaLeads\venv`, porque carregar o `cacert.pem` do pip a
  partir dessas unidades virtuais trava; use `--venv CAMINHO` para escolher
  outro local;
- também em pastas sincronizadas, move o banco SQLite para
  `%LOCALAPPDATA%\LicitaLeads\data\` (o SQLite trava nessas unidades virtuais).
  Só o `DATABASE_URL` padrão relativo é redirecionado; caminhos absolutos e
  PostgreSQL configurados no `.env` são respeitados. Se existir um banco em
  `data/licita_lead.db`, ele é migrado uma única vez, com a API de backup do
  SQLite;
- cria o `.env` a partir de `.env.example` se estiver ausente e valida a
  configuração antes de subir o servidor;
- aplica as migrations e sobe a API/UI (`run.py --migrate`);
- no fluxo com dependências de desenvolvimento, tenta preparar o driver de
  navegador para a suíte local; `--check` não instala nem baixa componentes;
- sobe o scheduler em segundo plano (log em `data/scheduler.log`) e o encerra
  junto com a aplicação;
- abre o navegador em <http://localhost:8000> quando `/health` responde;
- se a aplicação já estiver em execução, apenas abre o navegador.

Flags: `--check` (diagnóstico sem alterações), `--venv CAMINHO`, `--repair`
(recria a `.venv`), `--reinstall`, `--upgrade-pip`, `--no-browser`,
`--no-scheduler`, `--no-dev` e `--no-ocr` (instalação mínima). O extra OCR
instala `Pillow`/`pytesseract`; o binário Tesseract é detectado no PATH e, se
ausente no Windows, o launcher oferece a instalação via `winget`
(`UB-Mannheim.TesseractOCR`).

### Troubleshooting do launcher

- **Python 3.12 ausente:** aceite a instalação via `winget` ou instale
  manualmente em <https://www.python.org/downloads/windows/> e rode de novo.
- **Porta ocupada:** ajuste `APP_PORT` no `.env` ou encerre o processo que usa a
  porta; a mensagem do launcher informa o número.
- **`.env` inválido:** o launcher mostra o erro de validação e interrompe sem
  subir o servidor.
- **Primeira execução demorada:** a instalação das dependências (PyMuPDF etc.)
  pode levar alguns minutos; as próximas execuções são rápidas.
- **Coleta demorada:** os limites padrão são 50 registros por fonte, 20 páginas
  e 0,5s entre requisições em `.env.example`; os padrões internos sem `.env`
  usam `0` para registros/páginas, significando sem esses limites. O PNCP pode
  responder HTTP 429 e o cliente aplica backoff. `/crawls` atualiza o painel a
  cada 2 segundos durante atividade e a cada 15 segundos quando inativo.
- **Interface congelada durante a coleta:** verifique se o `.env` aponta o
  `DATABASE_URL` para uma pasta sincronizada; o launcher move o banco
  automaticamente quando iniciado por `iniciar.bat`.
- **`.env` é por máquina:** não vai para o Git; um clone novo usa os padrões do
  `.env.example`. Confira os valores efetivos em <http://localhost:8000/settings>.

## Encerrar uma coleta

A página `/crawls` mostra o botão **Encerrar coleta** para execuções pendentes ou
em andamento (com confirmação). O encerramento é cooperativo:

- a requisição grava `cancel_requested` no banco e cancela imediatamente a task
  quando ela roda no processo da API;
- se a API foi reiniciada e não há task local nem lease ativo pertencente à
  coleta, a execução órfã é marcada como `Cancelada` na hora, em vez de ficar
  presa em “Em andamento”;
- coletas do scheduler verificam o cancelamento nos pontos de checagem; tarefas
  de consulta e subprocessos de extração são encerrados e aguardados pelo fluxo
  responsável. O tempo até a confirmação pode variar;
- a execução vira `Cancelada`, libera o lease `crawl:{conector}:{uf}` e permite
  iniciar outra coleta na sequência;
- a API também expõe `POST /api/crawls/{id}/cancel` (`202`/`409`).

## Coleta de pagamentos municipais

Em `/pagamentos#portais`, cada portal inicia a coleta em background e retorna
imediatamente para a página. Enquanto houver coleta ativa, os cards mostram:

- o estágio (`Consultando o portal`, `Gravando pagamentos` ou `Finalizando`);
- o tempo decorrido;
- a contagem `X de Y` e uma barra de progresso durante a gravação;
- o botão daquele portal desabilitado para evitar coletas duplicadas.

O painel consulta `/pagamentos/portais` a cada 2 segundos e interrompe o polling
quando não há mais coleta ativa. O último resumo e o diagnóstico continuam no
card após a conclusão. Se a API reiniciar, o estágio persistido no `CrawlRun`
continua visível, mas a contagem em memória pode ser perdida.

## Apagar todos os dados (reset)

A página `/settings` tem uma **Zona de risco** com as contagens atuais e o botão
**Apagar todos os dados**. A ação é **destrutiva e irreversível**:

- apaga todas as tabelas de domínio (contratações, documentos, trechos,
  evidências, eventos, prazos, leads, revisões, rascunhos, contatos, empresas,
  participantes, registros de fonte, observações de campo, coletas e leases);
- remove os arquivos baixados em `DOCUMENT_STORAGE_PATH` e
  `RAW_DATA_STORAGE_PATH` (preservando `.gitkeep`) e trunca o
  `data/scheduler.log` (best-effort);
- preserva o `.env`, o schema/migrations Alembic e o calendário de feriados;
- exige a marcação da caixa de confirmação; sem ela, nada é apagado;
- se houver coleta ativa ou job do scheduler em andamento, a página **bloqueia**
  e pede para encerrar antes (use o botão em `/crawls`);
- a API também expõe `POST /api/maintenance/clear-data` com `{"confirm": true}`
  (`422` sem confirmação, `409` bloqueado).

## Subir a aplicação

```bash
python run.py            # sobe a API/UI; exige banco na revisão Alembic head
python run.py --migrate  # aplica migrations antes de subir
python run.py --reload   # recarrega ao alterar o código (desenvolvimento)
```

A interface fica em <http://localhost:8000>, o Swagger em
<http://localhost:8000/docs> e a verificação em
<http://localhost:8000/health>.

## Banco e migrations

- Padrão: `sqlite:///./data/licita_lead.db`.
- Em pastas sincronizadas iniciadas pelo `iniciar.bat`, o banco efetivo fica em
  `%LOCALAPPDATA%\LicitaLeads\data\licita_lead.db` (ver seção do launcher).
- SQLite usa WAL, `busy_timeout=30000`, `foreign_keys=ON` e
  `synchronous=NORMAL` por conexão.
- PostgreSQL opcional:

```env
DATABASE_URL=postgresql+psycopg://user:password@localhost/licita_lead
```

- Migrations:

Antes de chamar Alembic ou a CLI diretamente em uma pasta sincronizada,
confira o banco efetivo com `python scripts/launcher.py --check` e forneça o
mesmo `DATABASE_URL` ao processo. A configuração relativa do `.env` pode
apontar para outro arquivo. Para uma migração operacional, interrompa os
workers e faça backup verificado do banco efetivo antes de aplicá-la.

```bash
alembic upgrade head
alembic revision --autogenerate -m "descricao"
```

Se o projeto estiver em pasta sincronizada (Google Drive), **nunca** rode duas
instâncias contra o mesmo SQLite. Para cargas maiores, mova o banco para disco
local ou use PostgreSQL.

## Scheduler

```bash
python -m app.jobs.scheduler
```

Processo separado da API; desligue com `SCHEDULER_ENABLED=false`. Jobs travados
são recuperados por `reconcile_stale_crawls` (a cada 15 min e na subida da API).

## Reparo de dados legados

Faça backup do banco efetivo antes de qualquer reparo. O comando abaixo audita e,
com `--apply`, corrige somente fatos deriváveis sem ambiguidade das respostas
brutas já armazenadas:

```bash
python -m app.cli reconcile-data
python -m app.cli reconcile-data --apply
```

Ele pode desativar documentos marcados como inativos na fonte e corrigir a
identidade antiga de pagamentos federais. Processos com chave forte incompleta
são apenas reportados; nunca são fundidos ou separados automaticamente.

## Auditoria de fontes

```bash
python -m app.cli audit-sources
```

Compara os OpenAPI vivos com os contratos conhecidos e grava
`docs/source_audit_AAAA-MM-DD.md`. O relatório não altera a matriz
automaticamente; divergências exigem revisão humana.

## Feriados

Feriados nacionais são automáticos. Estaduais/municipais vão em
`data/holidays.csv`:

```csv
date,name,scope,uf,municipality_ibge
2026-07-28,Adesão do Maranhão à Independência do Brasil,state,MA,
```

## Testes e qualidade

```bash
pytest                      # suíte completa com cobertura mínima de 80% em app/
pytest tests/unit --no-cov  # execução focada, sem cobertura
ruff check .
ruff format --check .
mypy app
```

- `pytest` (puro) aplica `--cov-fail-under=80`, modo branch, incluindo o
  scheduler. As fixtures preparam banco temporário; não use o banco real.
- Testes são assíncronos (`asyncio_mode=auto`); integração usa SQLite temporário.
- Testes live são opt-in duplo:

```bash
RUN_LIVE_CONTRACT_TESTS=true pytest tests/contract -m live
```

A página **Sistema > Testes** (`/testes`) oferece um diagnóstico rápido sem
alterar os dados reais (ambiente, banco/migração, páginas, API, segurança,
domínio, fontes e scheduler) e executa a suíte local por etapas sequenciais:
pytest offline, processos API/scheduler, OCR real, navegador, Ruff, formatação
e mypy. O relatório identifica o código, separa os modos e indica pendências,
falhas e resultados parciais. Veja [testes locais](testes_locais.md). O
launcher instala `.[dev,ocr]` por padrão; se `pytest` não
estiver disponível, a página mostra a instrução e mantém o diagnóstico rápido
disponível.

Aliases do Makefile: `make install|migrate|run|test|lint|format|crawl|pipeline`.

## Troubleshooting

- **Banco desatualizado:** `alembic upgrade head` ou `python run.py --migrate`.
- **SQLite bloqueado:** encerre outra instância e verifique a sincronização da
  pasta.
- **Fonte temporariamente indisponível:** consulte `/crawls`; a tela separa o
  resultado por fonte, preserva os registros válidos e permite repetir somente o
  conector que falhou.
- **Concluída com pendências:** confira o motivo por fonte. Limites conservam a
  fila para continuação; falhas e cancelamentos exigem ação explícita. Repetir
  inicia outro lote; continuar preserva período, filtros e opção de reconsulta.
- **OCR ou navegador ausente:** a suíte exibe a etapa pendente. Instale o
  Tesseract com o idioma `por` ou prepare o driver com
  `python -m app.cli prepare-test-browser`; veja [testes locais](testes_locais.md).
- **IA bloqueada:** confira o diagnóstico sanitizado e a configuração, inclusive
  limite e preços. Uma nova tentativa elegível é explícita; não zere contadores
  nem altere fatos antigos. Consulte [fila de IA](fila_ia.md).
- **Sem participantes:** verifique documentos e o estado de disponibilidade; o
  sistema não cria participantes perdedores a partir do vencedor.
- **Sem prazo:** confirme se há marco inicial ou prazo explícito; casos ambíguos
  ficam `requires_manual_review`.
- **Coleta presa em "Em andamento":** `reconcile_stale_crawls` marca como falha
  após `CRAWL_STALE_AFTER_MINUTES` (padrão 180) para permitir retry.


## Garantias de integridade e suíte isolada

A revisão `0020_source_completeness` acrescenta a `SourceRecord` os campos
`collection_complete` e `pagination_metadata`; o payload bruto é preservado.
Dados legados mantêm completude desconhecida. Cancelamentos e documentos
inativos só retraem fatos quando a coleção está completa e sem truncamento.
A referência de cada linha identifica a resposta oficial que a contém;
referências ausentes ou ambíguas são diagnosticadas.

A suíte iniciada em `/testes` sobrepõe as configurações de banco, armazenamento,
credenciais e provedores necessários ao isolamento com valores temporários;
outras configurações podem vir do ambiente ou do `.env`. LLM, contatos,
descoberta de domínios, OCR geral e contratos vivos ficam desligados; cenários
específicos habilitam provedores controlados ou o OCR local. Alembic e pytest
são encerrados, com seus descendentes, em cancelamento, timeout ou perda de lease.

Leases são verificadas por proprietário, geração e validade na transação de
gravação. A renovação mantém a geração; uma nova aquisição a modifica.
Chamadas concorrentes têm contexto próprio. Heartbeats mantêm o progresso do
run e não autorizam um worker antigo a finalizar uma coleta já encerrada.

No SQLite, as transações de escrita da coleta, do progresso e dos heartbeats
aguardam uma fila por banco antes de abrir conexão. Leitores continuam usando
WAL. A admissão usa `BEGIN IMMEDIATE` e repete apenas a aquisição do bloqueio
quando há outro processo escrevendo; o corpo da transação nunca é reexecutado
automaticamente. A verificação de manutenção, proprietário, geração e cancelamento
permanece dentro da transação.

Respostas com versões repetidas do mesmo item oficial reutilizam o registro
canônico. Cada versão passa pelo serviço de atualização e pelo diário antes da
próxima, preservando datas, divergências e transições A → B → A. Um bloqueio do
banco aparece como falha de gravação local; o nome da tabela `job_leases` não é
interpretado como prova de outra coleta simultânea. Essas mudanças valem para
novas gravações; os diagnósticos das execuções anteriores permanecem no histórico.

`PAYMENT_INGESTION_BATCH_SIZE` controla o tamanho das transações de pagamentos
federais/manuais independentes (padrão 250). Lotes já concluídos permanecem se
um lote posterior falhar; o lote atual é revertido. A substituição municipal de
agregados por detalhes continua numa única transação, com rollback integral.


## Paralelismo das coletas e dos documentos

Licitações, atas e contratos distribuem páginas, detalhes, modalidades e as
unidades selecionadas entre tarefas limitadas. PNCP e Compras.gov.br executam
simultaneamente, com um orçamento HTTP independente por fonte. A fila mantém
até duas vezes esse orçamento em resultados preparados; um consumidor confirma
os dados de negócio. Esperas por ritmo/HTTP 429 liberam vagas de rede.

Continuação, falhas e decisões de atualização pertencem a cada registro e
operação. Respostas recebidas são preservadas antes da normalização. Mudanças
nos totais invalidam apenas a partição correspondente. Conclusões de cobertura
e retrações continuam exigindo respostas completas. A ordem de conclusão das
tarefas não substitui as datas oficiais ou da observação no histórico.

Depois das APIs, documentos usam `DOCUMENT_DOWNLOAD_CONCURRENCY=4` e
`DOCUMENT_PROCESS_CONCURRENCY=2`, ajustáveis no `.env`. Cada extração/OCR
acontece em processo supervisionado, lendo um arquivo local validado e imutável.
A criação e supervisão do processo não bloqueiam o loop da API. Resultados do
LLM, quando habilitado, são preparados fora da transação, com uma chamada por
vez. A gravação revalida a atividade e a versão do documento. Arquivos iguais
são publicados uma única vez para não disputar leitura e substituição no Windows.

`/crawls` mostra requisições ativas, teto e pico por fonte, espera pela fonte,
resultados aguardando gravação e uso das vagas de download/extração. São dados
operacionais; os contadores de alterações continuam baseados em transações
confirmadas. Execuções antigas permanecem sem essas métricas.
