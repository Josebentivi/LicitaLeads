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
pip install -e ".[dev]"
copy .env.example .env
python scripts/setup.py
python run.py
```

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[dev]'
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
  dependências quando o `pyproject.toml` mudar (sem `[dev]`; use `--dev` para
  incluí-las);
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
- sobe o scheduler em segundo plano (log em `data/scheduler.log`) e o encerra
  junto com a aplicação;
- abre o navegador em <http://localhost:8000> quando `/health` responde;
- se a aplicação já estiver em execução, apenas abre o navegador.

Flags: `--check` (diagnóstico sem alterações), `--venv CAMINHO`, `--repair`
(recria a `.venv`), `--reinstall`, `--dev`, `--upgrade-pip`, `--no-browser`,
`--no-scheduler`.

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
  e 0,5s entre requisições; o PNCP pode responder HTTP 429 e o cliente aplica
  backoff. A tela `/crawls` mostra o progresso `x/y` a cada 5s.
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
- coletas do scheduler são interrompidas no próximo ponto de checagem — antes do
  próximo registro da fonte ou do próximo documento do lote (segundos a ~1 min;
  requisições HTTP e extrações já em andamento terminam antes);
- a execução vira `Cancelada`, libera o lease `crawl:{conector}:{uf}` e permite
  iniciar outra coleta na sequência;
- a API também expõe `POST /api/crawls/{id}/cancel` (`202`/`409`).

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
pytest tests/unit --no-cov  # execução rápida, sem banco
ruff check .
ruff format --check .
mypy app
```

- `pytest` (puro) aplica `--cov-fail-under=80`, modo branch, ignorando
  `app/jobs/scheduler.py`.
- Testes são assíncronos (`asyncio_mode=auto`); integração usa SQLite temporário.
- Testes live são opt-in duplo:

```bash
RUN_LIVE_CONTRACT_TESTS=true pytest tests/contract -m live
```

Aliases do Makefile: `make install|migrate|run|test|lint|format|crawl|pipeline`.

## Troubleshooting

- **Banco desatualizado:** `alembic upgrade head` ou `python run.py --migrate`.
- **SQLite bloqueado:** encerre outra instância e verifique a sincronização da
  pasta.
- **Fonte temporariamente indisponível:** consulte `/crawls`; a tela separa o
  resultado por fonte, preserva os registros válidos e permite repetir somente o
  conector que falhou.
- **Sem participantes:** verifique documentos e o estado de disponibilidade; o
  sistema não cria participantes perdedores a partir do vencedor.
- **Sem prazo:** confirme se há marco inicial ou prazo explícito; casos ambíguos
  ficam `requires_manual_review`.
- **Coleta presa em "Em andamento":** `reconcile_stale_crawls` marca como falha
  após `CRAWL_STALE_AFTER_MINUTES` (padrão 180) para permitir retry.
