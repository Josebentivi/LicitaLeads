# Documentação — LicitaLead Monitor

Esta pasta reúne a documentação da aplicação. O conteúdo é derivado do código,
do README e das regras de auditabilidade do projeto; nada aqui descreve
comportamento que o sistema não implemente.

## Comece por aqui

| Documento | Conteúdo |
|---|---|
| [Visão geral](visao_geral.md) | O que a aplicação faz, princípios e limitações |
| [Arquitetura](arquitetura.md) | Componentes, fluxo de dados e processos |
| [Modelo de dados](modelo_de_dados.md) | Entidades, enums e cadeia de proveniência |
| [API](api.md) | Endpoints REST, páginas web e formato de erros |
| [CLI](cli.md) | Comandos operacionais e scheduler |
| [Guia de uso](guia_de_uso.md) | Instalação, fluxo de trabalho e revisão de leads |
| [Configuração](configuracao.md) | Variáveis de ambiente e valores padrão |
| [Segurança](seguranca.md) | Downloads, arquivos ZIP, segredos e privacidade |
| [Operação](operacao.md) | Banco, migrations, testes, lint e troubleshooting |

## Documentos de referência

| Documento | Conteúdo |
|---|---|
| [Matriz de fontes](matriz_de_fontes.md) | Capacidades auditadas do PNCP e do Compras.gov.br |
| [Plano de implementação](plano_de_implementacao.md) | Objetivo, decisões fixadas e ordem de entrega do MVP |
| [Auditoria de contratos — 2026-09-17](source_audit_2026-09-17.md) | Relatório datado gerado por `python -m app.cli audit-sources` |

Relatórios de auditoria de contratos são gravados como
`source_audit_AAAA-MM-DD.md` pelo comando `audit-sources`; divergências exigem
revisão humana e não alteram a matriz automaticamente.

## Regras que valem para toda a documentação

- Toda conclusão do sistema aponta para `SourceRecord` ou `Evidence`.
- `DataAvailability.EMPTY` (consulta bem-sucedida sem registros) nunca é
  confundida com `NOT_SUPPORTED` (a fonte não expõe a capacidade).
- Resultado homologado é o vencedor/adjudicatário, não a lista de participantes.
- Outreach é `draft_only`: o sistema gera rascunhos e não envia mensagens.
- LLM e busca de contatos ficam desligados por padrão.
