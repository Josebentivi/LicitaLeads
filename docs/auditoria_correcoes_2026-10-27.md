# Correções da revisão profunda — 2026-10-27

> **Registro histórico:** descreve o estado e os resultados da data indicada. Consulte a [documentação atual](README.md).

As alterações existentes no workspace foram preservadas. Nenhuma migração,
reconciliação ou coleta foi executada no banco de uso. Testes e provas de
isolamento usam bancos e diretórios temporários; as consultas oficiais são
somente leitura.

## Correções entregues

| Área | Resultado |
|---|---|
| Página de testes | A execução continua ocupada até salvar o relatório e liberar a lease. Cancelamento preserva resultados parciais. Settings explícitos escolhem seu próprio engine. Hosts com wildcard e testserver são aceitos corretamente. |
| Subprocessos e isolamento | Supervisor Windows entra em Job Object antes de criar o comando, encerrando descendentes mesmo quando o processo pai termina primeiro. Pytest comum também isola engine global, credenciais e armazenamento antes dos imports. Contratos vivos continuam exigindo as duas travas explícitas. |
| Segredos e diagnósticos | Senhas curtas e percent-encoded são redigidas. CSP é avaliada pelos valores das diretivas. Probes usam tamanho mínimo válido e validadores dos conectores. Launcher verifica revisão e conteúdo JSON da saúde em modo somente leitura. |
| Transporte | Downloads JSON/CSV/OpenAPI usam streaming limitado e fixam o IP público no momento da conexão, preservando TLS/SNI. Redirects externos ou downgrade removem credenciais inclusive dos defaults do cliente. Bytes originais do CSV e envelopes JSON de São Luís são preservados. |
| Contratos dos conectores | Envelopes desconhecidos, contagens contraditórias e linhas malformadas não viram EMPTY. Paginação de detalhes e termos tem limites, detecção de repetição e preservação de respostas parciais. Identidades ausentes não viram a string None. |
| Municipais | PKEMP, entidade, ano e fonte distinguem empenhos com o mesmo número. Somente grupos completos substituem agregados. Mudanças de representação preservam histórico; consultas operacionais usam Payment.is_current. |
| Fatos e evidências | Cancelamento encontra itens já persistidos. Documentos inativos deixam de orientar fase, leads, prazos e participações atuais. Downloads revalidam atividade antes de gravar. Reconciliação exige a mesma fonte, operação, contratação e URL. |
| Pagamentos e atualização | Contratos candidatos são filtrados por vigência antes do limite. Importações independentes usam lease, heartbeat e fencing. Scheduler atualiza diretamente IDs ativos antigos, com cursor de tentativas independente do sucesso para não bloquear os demais IDs. |
| Backup e manutenção | Conexões SQLite são fechadas explicitamente. Backup é vinculado à revisão e à janela exclusiva de manutenção. O gate serializa aquisições e impede gravações ORM e DML da aplicação durante backup/reparo; aplicação verifica propriedade da lease. Cancelamento aguarda o término da thread de backup/limpeza antes de liberar a janela. pg_dump preserva parâmetros SSL/socket sem colocar senha nos argumentos ou manifesto. |
| Migração | A revisão 0021_integrity_guards adiciona commitment_external_id nullable, is_current e last_refresh_attempt_at. Não infere identidades legadas nem separa processos automaticamente. |

## Evidências e limites externos

O [relatório de fontes oficiais](evidencias/validacao_fontes_2026-10-27.md)
contém URLs, amostras limitadas, validação por operação e falhas externas.
PNCP e Compras.gov.br forneceram descoberta, detalhes e itens válidos; documentos
PNCP também foram reconhecidos. A amostra PNCP não continha resultado publicado.
São Luís respondeu 403, impedindo confirmar seu catálogo mensal. HTTPS de
Imperatriz, Timon e Caxias não pôde ser confirmado; HTTP continua exigindo
configuração explícita. Transparência não foi consultada por falta de
credenciais/configuração de amostra. Não foi usado um PostgreSQL de produção.

O banco de uso permanece na revisão anterior. A migração 0021 deve ser aplicada
no procedimento de atualização antes de iniciar esta versão; os testes verificam
upgrade do zero e de schema legado com sentinelas preservadas.

## Validação

- **562 testes aprovados; dois contratos vivos excluídos explicitamente.**
- **Cobertura de 84,46%**, com branches e o limite de 80% preservados.
- `ruff check . --no-cache`: aprovado.
- `ruff format --check .`: 231 arquivos conformes.
- `mypy app`: aprovado, 108 arquivos analisados.
- `git diff --check`: aprovado; Git apenas informou normalização LF/CRLF.
- Migrações do zero e de schema legado com dados sentinelas preservados;
  campos de identidade legada continuam desconhecidos.
- Cancelamento durante criação/aquisição, perda de lease, fencing de worker
  antigo, fechamento com exceção, retenção de resultados parciais, rollback e
  continuidade entre fontes cobertos por regressões offline.
- Banco/arquivos sentinelas preservados, cancelamento real de processos no
  Windows e lifecycle de startup/shutdown verificados.
- Backup PostgreSQL e falhas de pg_dump exercitados com substitutos controlados;
  não foi feita restauração em um servidor PostgreSQL real.

A [evidência da página de testes](evidencias/validacao_pagina_testes_2026-10-27.md)
registra a transição running→completed observada no Chrome, sem reload e sem
duplicação de ações, em servidor/banco temporários. A execução visual era
simulada; o resultado de 562 testes acima pertence à suíte real offline.

As [evidências dos checks](evidencias/validacao_offline_2026-10-27.md) registram
os comandos, o resultado agregado e os limites da validação.
