# Validação offline final — 2026-10-27

> **Registro histórico:** descreve o estado e os resultados da data indicada. Consulte a [documentação atual](../README.md).

Ambiente: Windows, Python 3.12.10. Banco, armazenamento, calendário e cobertura
em diretórios temporários. Alembic aplicado antes do pytest; contratos vivos,
LLM, OCR e busca de contatos desabilitados. O conftest raiz também isola o
engine global antes de importar a aplicação. O banco de uso não participou.

| Check | Resultado |
|---|---|
| `ruff check . --no-cache` | All checks passed |
| `ruff format --check .` | 231 files already formatted |
| `mypy app` | Success: no issues found in 108 source files |
| `git diff --check` | exit 0; avisos de normalização LF/CRLF |
| Suíte completa offline | 562 passed, 2 deselected; 233,03 segundos |
| Cobertura de app, com branches | 84,46%; mínimo obrigatório de 80% preservado |

O pytest final recebeu o diretório `tests`, `-m "not live"`, cache desabilitado
e `--basetemp` temporário. Não recebeu `--no-cov`, remoção de addopts ou redução
do limite de cobertura. As execuções focadas anteriores usaram `--no-cov`;
seus arquivos de cobertura não foram mesclados ao resultado final.

Resumo literal do fim da execução final:

```text
TOTAL                                           12535   1514   3574    679    84%
Required test coverage of 80% reached. Total coverage: 84.46%
562 passed, 2 deselected in 233.03s (0:03:53)
```

Os testes incluem Alembic do zero e upgrade legado com sentinelas, backup WAL,
bloqueio de backup ausente/falho/antigo, reconciliação ambígua, cancelamento de
subprocessos e de admissão, perda de lease e finalização antiga, transações
municipais, proveniência por resposta e impedimento de retrações parciais.
Os cenários PostgreSQL de pg_dump usam substitutos controlados; não constituem
prova de restauração num servidor real.

A [validação visual](validacao_pagina_testes_2026-10-27.md) e as
[consultas oficiais limitadas](validacao_fontes_2026-10-27.md) têm evidências e
limitações próprias. Os dois testes live excluídos não foram contabilizados
como aprovados.
