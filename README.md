# LicitaLead Monitor

Aplicação local e auditável para monitorar contratações públicas, processar
documentos oficiais e produzir oportunidades de atendimento jurídico em modo
**rascunho**. O sistema usa PNCP e Dados Abertos do Compras.gov.br, preserva a
origem de cada conclusão e nunca inventa participantes, eventos ou prazos.

> MVP para apoio à análise. Uma estimativa de prazo não substitui a conferência
> do edital, da ata, da plataforma oficial e da legislação aplicável.

## Documentação

A documentação completa está em [`docs/`](docs/README.md):

- [Visão geral](docs/visao_geral.md) — escopo, princípios e limitações.
- [Arquitetura](docs/arquitetura.md) — componentes, fluxo e processos.
- [Modelo de dados](docs/modelo_de_dados.md) — entidades e proveniência.
- [API](docs/api.md) — endpoints REST, páginas web e erros.
- [CLI](docs/cli.md) — comandos e scheduler.
- [Guia de uso](docs/guia_de_uso.md) — fluxo operacional e revisão de leads.
- [Configuração](docs/configuracao.md) — variáveis de ambiente.
- [Segurança](docs/seguranca.md) — downloads, arquivos e privacidade.
- [Operação](docs/operacao.md) — banco, migrations, testes e troubleshooting.
- [Matriz de fontes](docs/matriz_de_fontes.md) e
  [plano de implementação](docs/plano_de_implementacao.md).

## Requisitos

- Python 3.12
- `pip` e `venv`
- SQLite, incluído no Python
- Sem Docker, Node.js ou banco externo obrigatório

## Instalação no Windows

### Início rápido com um clique

1. Clone o repositório e entre na pasta.
2. (Opcional) Ajuste o `.env`; o launcher cria um a partir do `.env.example` se
   ele ainda não existir.
3. Dê duplo clique em `iniciar.bat`.

Na primeira execução o launcher cria a `.venv`, instala as dependências, aplica
as migrations, sobe a API/UI e o scheduler em segundo plano e abre o navegador
em <http://localhost:8000>. Nas execuções seguintes ele apenas verifica o
ambiente e sobe a plataforma. Feche a janela do console (ou pressione `Ctrl+C`)
para encerrar tudo; o scheduler é encerrado junto. Se a aplicação já estiver em
execução, o launcher apenas abre o navegador.

Se o Python 3.12 não estiver disponível, o launcher oferece a instalação via
`winget`. Se o repositório estiver em uma pasta sincronizada (Google Drive,
OneDrive), o launcher move a `.venv` e o banco SQLite para o disco local
(`%LOCALAPPDATA%\LicitaLeads\`), porque o pip e o SQLite travam nessas unidades
virtuais; na primeira vez, um banco existente na pasta do repositório é
migrado automaticamente. Se o banco estiver corrompido (por exemplo, por ter
sido sincronizado com a aplicação aberta), o launcher tenta recuperá-lo pelas
páginas legíveis; quando não é possível, preserva o arquivo como
`*.corrupt-<data>` e cria um banco novo. O `.env` é por máquina (não vai para o
Git): rode o diagnóstico para ver os valores efetivos e o caminho do banco:

```powershell
python scripts\launcher.py --check
```

`scripts\launcher.py` também aceita `--venv CAMINHO`, `--repair` (recria a
`.venv`), `--reinstall`, `--dev`, `--upgrade-pip`, `--no-browser` e
`--no-scheduler`.

### Instalação manual

```powershell
py -3.12 -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -e ".[dev]"
copy .env.example .env
python scripts/setup.py
python run.py
```

## Instalação no Linux/macOS

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[dev]'
cp .env.example .env
python scripts/setup.py
python run.py
```

A interface estará em <http://localhost:8000>, o Swagger em
<http://localhost:8000/docs> e a verificação em
<http://localhost:8000/health>.

## Arquitetura

O fluxo é dividido em conectores, normalização/persistência, documentos,
eventos, prazos, contatos, leads e apresentação. `SourceRecord`, vínculos de
fonte e evidências permitem responder de onde veio cada campo. SQLite é usado
por padrão; PostgreSQL é habilitado somente ao configurar `DATABASE_URL`.

Consulte a [matriz de fontes](docs/matriz_de_fontes.md) e o
[plano de implementação](docs/plano_de_implementacao.md).

## Comandos principais

```bash
python -m app.cli audit-sources
python -m app.cli crawl pncp --uf MA --days 7
python -m app.cli crawl compras-gov --uf MA --days 7
python -m app.cli crawl-atas all --days 365
python -m app.cli backfill-fields
python -m app.cli process-documents
python -m app.cli detect-events
python -m app.cli calculate-deadlines
python -m app.cli enrich-contacts
python -m app.cli create-leads
python -m app.cli run-pipeline --uf MA --days 7
python -m app.cli export-leads leads.csv
```

O scheduler é deliberadamente separado da API:

```bash
python -m app.jobs.scheduler
```

## Banco e migrations

O padrão é `sqlite:///./data/licita_lead.db`.

```bash
alembic upgrade head
alembic revision --autogenerate -m "descricao"
```

Para PostgreSQL:

```env
DATABASE_URL=postgresql+psycopg://user:password@localhost/licita_lead
```

Se o projeto estiver em uma pasta sincronizada, como Google Drive, evite duas
instâncias simultâneas usando o mesmo SQLite. Para cargas maiores, mova o banco
para disco local ou use PostgreSQL.

## Fontes e limitações

- PNCP fornece contratações, itens, documentos, resultados publicados e atas de
  registro de preços (cabeçalho).
- Compras.gov.br fornece contratações, itens, resultados, fornecedores, atas de registro de preços
  (com itens e fornecedores) e contratos, mas não documentos da sessão.
- Filtros de contratações usam a rota da Lei 14.133/2021 (licitação, contratação
  direta e procedimentos auxiliares), SRP, amparo legal e faixa de valor;
  `python -m app.cli backfill-fields` recupera esses campos de coletas antigas.
- A página `/empresas` consolida participações e eventos por CNPJ; desclassificação,
  inabilitação e recursos dependem de documentos oficiais e podem exigir revisão.
- Resultado homologado não equivale à lista de participantes.
- Inabilitação, desclassificação e recursos normalmente dependem de documentos.
- Valores negativos publicados por erro pela fonte são tratados como não
  disponíveis (`NULL`), preservando o payload bruto para auditoria; uma falha
  por registro não derruba a coleta inteira (fica registrada e o run termina
  `partial`).
- PDF escaneado é marcado `ocr_required` quando não há OCR configurado.
- LLM e busca de contatos ficam desligados por padrão.
- CNPJ numérico e alfanumérico são aceitos; CPF não é usado para prospecção.

## Prazos

O motor prioriza prazo explícito e evidenciado. A regra configurável de três
dias úteis só é usada para eventos compatíveis e com marco inicial confiável.
Estimativas registram método, base, calendário, confiança e necessidade de
revisão. Feriados estaduais/municipais podem ser adicionados em
`data/holidays.csv` com as colunas `date,name,scope,uf,municipality_ibge`.

## Testes e qualidade

```bash
pytest
ruff check .
ruff format --check .
mypy app
```

Testes com APIs reais são opt-in:

```bash
RUN_LIVE_CONTRACT_TESTS=true pytest tests/contract -m live
```

## Criação de conectores

Um conector implementa `ProcurementSourceConnector` e retorna
`ConnectorResult`, incluindo `DataAvailability`, URL e instante de coleta. Uma
lista vazia não deve ser confundida com capacidade não suportada. Novas fontes
devem preservar payload bruto, identificador externo e observações de campo.

## Segurança e privacidade

Downloads aceitam apenas HTTP(S) público, validam DNS/IP, tamanho, redirects e
assinatura do arquivo. Arquivos ZIP são extraídos com limites contra zip-slip e
zip-bomb. A interface não renderiza HTML bruto. Segredos não aparecem em logs ou
na página de configurações. O servidor vincula-se a `127.0.0.1` por padrão e o
MVP não deve ser exposto publicamente sem uma camada de autenticação.

## Troubleshooting

- **Banco desatualizado:** execute `alembic upgrade head`.
- **SQLite bloqueado:** encerre outra instância e verifique sincronização da pasta.
- **Fonte temporariamente indisponível:** consulte `/crawls`; a tela separa o resultado por
  fonte, preserva os registros válidos e permite repetir somente o conector que falhou.
- **Sem participantes:** verifique documentos e o estado de disponibilidade; o
  sistema não cria participantes perdedores a partir do vencedor.
- **Sem prazo:** confirme se há marco inicial ou prazo explícito; casos ambíguos
  ficam `requires_manual_review`.
