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
