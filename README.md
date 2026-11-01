# LicitaLead Monitor

Aplicação local e auditável para monitorar contratações públicas, processar
documentos oficiais e produzir oportunidades de atendimento jurídico em modo
**rascunho**. O sistema usa PNCP e Dados Abertos do Compras.gov.br, preserva a
origem de cada conclusão e nunca inventa participantes, eventos ou prazos.

> MVP para apoio à análise. Uma estimativa de prazo não substitui a conferência
> do edital, da ata, da plataforma oficial e da legislação aplicável.

## Documentação

- [Coletas incrementais, continuação e histórico de alterações](docs/coletas_incrementais.md)
- [Fila de IA](docs/fila_ia.md) e [testes locais](docs/testes_locais.md)

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

Relatórios datados em `docs/evidencias/` e auditorias são registros históricos;
consulte os guias operacionais para o comportamento atual.

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

Na primeira execução o launcher cria a `.venv`, instala as dependências de
runtime, desenvolvimento e OCR, aplica as migrations, sobe a API/UI e o
scheduler em segundo plano e abre o navegador em <http://localhost:8000>. Nas
execuções seguintes ele apenas verifica o ambiente e sobe a plataforma. Se o
Tesseract não estiver instalado no Windows, o launcher oferece a instalação via
`winget`. Feche a janela do console (ou pressione `Ctrl+C`) para encerrar tudo;
o scheduler é encerrado junto. Se a aplicação já estiver em execução, o launcher
apenas abre o navegador.

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
`.venv`), `--reinstall`, `--upgrade-pip`, `--no-browser` e `--no-scheduler`.
As dependências de desenvolvimento e OCR são instaladas por padrão; use
`--no-dev` e/ou `--no-ocr` para uma instalação mínima.

### Instalação manual

```powershell
py -3.12 -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -e ".[dev,ocr]"
# O OCR também exige o binário Tesseract no PATH.
copy .env.example .env
python scripts/setup.py
python run.py
```

## Instalação no Linux/macOS

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[dev,ocr]'
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
python -m app.cli crawl-atas all --days 30
python -m app.cli crawl-contracts pncp --days 30 --agency-cnpj 00000000000191
python -m app.cli import-payments --cnpj 00000000000191 --year 2026
python -m app.cli import-payments-csv pagamentos.csv --fonte "Portal Municipal"
python -m app.cli crawl-payments-municipal --source all --year 2026
python -m app.cli backfill-fields
python -m app.cli process-documents
python -m app.cli detect-events
python -m app.cli calculate-deadlines
python -m app.cli enrich-contacts
python -m app.cli create-leads
python -m app.cli run-pipeline --uf MA --days 7
python -m app.cli continue-crawl ID
python -m app.cli prepare-test-browser
python -m app.cli export-leads leads.csv
```

O exemplo de atas escolhe 30 dias explicitamente. Sem `--days`, a CLI usa 365;
o formulário manual começa com até 7 dias (ou a janela automática, se menor),
e a coleta automática usa a janela geral de 30 dias no exemplo, salvo configuração específica. As unidades da interface
são as já conhecidas e podem não cobrir toda a UF.

O scheduler é deliberadamente separado da API:

```bash
python -m app.jobs.scheduler
```

Ele coleta automaticamente por padrão (descoberta a cada 6 h, atualização a
cada 60 min, documentos a cada 2 h). Acompanhe as execuções em `/crawls`.
As atas de registro de preços são coletadas por
padrão ao iniciar o scheduler e a cada 24 h, para órgãos/unidades conhecidos da UF.
A janela inicial e de revisão segue `DEFAULT_LOOKBACK_DAYS` (30 dias), salvo override
explícito de `PRICE_REGISTRY_LOOKBACK_DAYS`; o intervalo usa `SCHEDULER_PRICE_REGISTRY_HOURS`. A página
`/atas` também oferece **Coletar atas agora**. As coletas automáticas podem ser
pausadas/retomadas em Configurações (as rotinas continuam rodando, mas são
puladas) e o reset de dados as pausa para não repovoar o banco. Use
`iniciar.bat --no-scheduler` para subir sem scheduler.

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
- Empresas podem ser recortadas pelo período de publicação da contratação, por
  "somente com lead" e por participação comprovada em processo ativo; contatos
  corporativos aparecem nas páginas de empresa e de lead.
- Participação comprovada por documento em processo que ainda recebe propostas
  gera o evento `PARTICIPATION_DETECTED` e um lead (cobertura documental).
- Os rótulos e filtros das telas têm ícones "i" com explicações objetivas.
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

A interface também oferece **Sistema > Testes** (`/testes`). O diagnóstico
rápido valida ambiente, banco/migrações, páginas, API, segurança, domínio,
fontes, conectividade e scheduler sem alterar os dados coletados. A suíte
completa executa o pytest em subprocesso, com banco/diretórios temporários,
contratos live desabilitados, cobertura mínima e encerramento garantido em
timeout/cancelamento; o launcher instala `.[dev,ocr]` por padrão.
O botão também executa Ruff, formatação, mypy, jornadas em Edge/Chrome,
OCR português e cenários de API/scheduler em processos separados. Relatórios
identificam o código testado e distinguem falhas de verificações pendentes.
Veja [preparação, isolamento e matriz de alcance](docs/testes_locais.md).

Antes de reparar dados legados, faça backup do banco efetivo e use
`python -m app.cli reconcile-data` (sem `--apply` é somente relatório).
Fontes municipais publicadas apenas em HTTP exigem
`ALLOW_INSECURE_HTTP_SOURCES=true`; HTTPS verificado é sempre preferido.

A [revisão de 27/10/2026](docs/auditoria_correcoes_2026-10-27.md) registra as
correções, regressões e limitações das consultas oficiais. A revisão Alembic
`0021_integrity_guards` faz parte desta atualização; o banco de uso não é
migrado automaticamente por testes ou pela reconciliação em simulação.

O painel de `/crawls` mostra etapa atual, contadores confirmados, duração,
heartbeat e atividades recentes, com atualização automática. Veja o
[acompanhamento de coletas](docs/acompanhamento_coletas_2026-10-27.md) para os
significados dos indicadores e as evidências da validação.

Em **Arquivos, falhas e próximas tentativas**, é possível reprocessar somente
os documentos com falha elegíveis do lote. A ação respeita o prazo e o limite
de tentativas, preserva arquivos já analisados e não repete a coleta de processos.
Falhas específicas de carregamento do PNCP recebem até três tentativas curtas;
erros HTTP 422 de outra natureza não são repetidos indiscriminadamente.

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

- Contratos e pagamentos s?o fatos separados: um contrato n?o ? um pagamento e
  `unlinked` n?o prova inadimpl?ncia.
- `crawl-payments-municipal` tenta S?o Lu?s, Imperatriz, Timon, Caxias e Bacabal;
  S?o Lu?s pode retornar `access_restricted` (Cloudflare) e Timon respondeu HTTP 500
  no endpoint identificado. Falhas ficam registradas como `CrawlRun` com diagn?stico.
- Na p?gina `/pagamentos`, as coletas municipais rodam em background e mostram
  est?gio, tempo decorrido e progresso dos pagamentos; o painel ? atualizado por
  HTMX enquanto houver uma coleta ativa.

## Navegacao

A interface usa uma sidebar em grupos: Compras publicas, Execucao financeira, Empresas e oportunidades, Operacao de dados e Sistema. Cada grupo abre uma pagina-painel com indicadores reais, alertas e acoes rapidas. O menu pode ser recolhido no desktop e vira um drawer no celular.
