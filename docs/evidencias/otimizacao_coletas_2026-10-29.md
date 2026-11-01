# Reaproveitamento e custo local das coletas — 29/10/2026

> **Registro histórico:** descreve o estado e os resultados da data indicada. Consulte a [documentação atual](../README.md).

## Alterações aplicadas

- Repetir uma coleta ou tentar uma fonte novamente usa `force_refresh=false`
  por padrão. A reconsulta completa depende de escolha explícita no formulário.
  Os demais filtros, inclusive identidades selecionadas, são preservados.
  Continuar conserva a opção original do lote congelado.
- Os formulários explicam o reaproveitamento condicionado à comprovação oficial
  e preservam a escolha da caixa durante atualizações HTMX da tabela.
- `LOG_LEVEL=INFO` controla API e scheduler, independentemente de `APP_ENV`.
  SQL sem `echo`, parâmetros ocultos nas exceções e bibliotecas de transporte e
  banco limitadas a avisos/erros. Os logs existentes foram preservados.
- Resumos globais, por coleta e lote usam agregações SQL em conjunto, com oito
  consultas para um ou vinte lotes. Trabalhos compartilhados não são duplicados;
  reaproveitamentos continuam contados por demanda. Os custos agrupados são
  somados em `Decimal`, sem soma flutuante do SQLite ou recálculo de preços.
- O painel conserva atualização a cada dois segundos enquanto há atividade,
  sem cache entre requisições. API e CLI mantêm seus contratos. Sem migração.

## Validação

- Suíte offline completa: **939 testes aprovados**, dois testes live excluídos,
  bancos e armazenamento temporários, migrações aplicadas e serviços externos
  desabilitados.
- **29 regressões finais aprovadas**, incluindo as últimas mudanças de proteção
  de parâmetros SQL e os testes de consulta em conjunto dos endpoints.
- **12 cenários de repetição aprovados** após preservar os formulários HTMX;
  abrangem licitações, atas e contratos, com e sem reconsulta explícita.
- Cobertura consolidada: **86,01%**. Ruff, formatação e mypy aprovados
  (132 arquivos de aplicação). Há sobreposição entre os grupos de regressão.
- Massa de 12 mil trabalhos e vinte lotes: oito consultas, sem carregar trabalhos
  individualmente; os comandos também foram compilados para PostgreSQL.
- No mesmo snapshot real, os resumos anterior e novo de 13.636 trabalhos
  coincidiram em todos os campos; a consulta caiu de 0,765 s para 0,407 s.

Artefatos da suíte em
`%TEMP%\LicitaLeads-throughput-validation-5wr6erzr` e evidências operacionais em
`%TEMP%\LicitaLeads-throughput-optimization-wwv734si`.

## Medição depois do reinício

Cinco leituras consecutivas, sem iniciar coleta, com medianas:

| Medida | Antes | Depois |
|---|---:|---:|
| `/crawls/progress` | 1,266 s | 0,937 s |
| `/api/crawls?page_size=20` | 0,735 s | 0,390 s |
| Logs acrescentados por cinco atualizações do painel | 322.545 bytes | 340 bytes |

A amostra demonstra redução do custo local; não estima aumento de capacidade das
fontes oficiais. Concorrência, frequência HTTP, `Retry-After` e proteções dos
downloads e arquivos permaneceram iguais. Todos os endpoints verificados
responderam HTTP 200; 52 formulários de repetição apresentaram a opção desmarcada.

## Aplicação e preservação

Novos trabalhos foram pausados. Um ciclo automático já havia reservado mais de
três mil documentos e não consultava a pausa durante o ciclo. Conforme a
interrupção autorizada, o lease `documents:processing` foi revogado com conferência
do proprietário e geração; aguardou-se a liberação dos workers e dos leases antes
de substituir os módulos. As gravações confirmadas e as pendências foram mantidas.

Banco efetivo: `%LOCALAPPDATA%\LicitaLeads\data\licita_lead.db`, revisão
`0024_llm_queue`. Backup consistente com integridade e SHA-256 verificados:

`%LOCALAPPDATA%\LicitaLeads\data\backups\20261030T035146Z-deb84af810cc459d9850a822ca3b9339.sqlite3`

SHA-256: `a2ec18148740ee4fe19a4a272bb808054ca6f023fa9b6db24ebbb5abe0917633`.

A aplicação foi reiniciada por `iniciar.bat --no-browser`. A comparação integral
do histórico confirmou as mesmas **72 execuções**, sem alteração e sem nova
coleta. Os quatro lotes de IA cancelados continuam cancelados. A falha documental
`archive_members_limit` conserva erro, duas falhas e prazo de nova tentativa;
nenhuma retentativa foi forçada e nenhum limite foi ampliado.

O scheduler foi restaurado ao estado anterior, ativo, com heartbeat recente.
A próxima coleta manual permanece a cargo do usuário. Os horários UTC nos nomes
dos arquivos correspondem à noite de 29/10 em America/Chicago.
