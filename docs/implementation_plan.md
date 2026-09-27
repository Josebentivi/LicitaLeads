# LicitaLead Monitor — plano de implementação

Data de referência: **2026-09-16**.

## Objetivo

Entregar um MVP local, auditável e executável com Python 3.12 que descubra
contratações públicas no PNCP e no Compras.gov.br, preserve os dados de origem,
processe documentos oficiais, identifique empresas e eventos somente quando
houver evidência, estime prazos de modo conservador e produza oportunidades e
rascunhos de contato. O sistema não envia mensagens.

## Decisões fixadas

- Projeto na raiz deste diretório e sem containerização.
- FastAPI, Jinja2 e HTMX; SQLite por padrão e PostgreSQL opcional.
- Scheduler em processo separado da API.
- Timestamps internos em UTC e exibição em `America/Sao_Paulo`.
- CNPJ numérico e alfanumérico aceitos.
- Ausência de dado representada por estado tipado e `NULL`, nunca por um fato
  inventado.
- OCR e LLM são integrações opcionais; a operação padrão é determinística.
- Feriados nacionais são carregados automaticamente; datas estaduais e
  municipais podem ser fornecidas por CSV.

## Ordem de entrega

1. Fundação, configuração, banco, Alembic e modelos de proveniência.
2. Conectores oficiais, persistência bruta e deduplicação entre fontes.
3. Download e extração segura de documentos, incluindo arquivos ZIP.
4. Identificação conservadora de participantes e eventos com evidências.
5. Motor de prazos, contatos públicos, scoring, leads e rascunhos.
6. API, interface web, CLI e scheduler separado.
7. Testes unitários, integração, contratos offline e documentação final.

## Critérios de engenharia

- Toda conclusão deve apontar para `SourceRecord` ou `Evidence`.
- `numeroControlePNCP` é a chave preferencial de deduplicação. A chave composta
  de órgão, unidade/UASG, número, ano e modalidade só é usada quando completa.
- Nomes semelhantes não unem empresas ou processos automaticamente.
- Resultado homologado representa vencedor/resultado, não todos os licitantes.
- Testes live são exclusivamente opt-in com `RUN_LIVE_CONTRACT_TESTS=true`.
- A suíte padrão não depende de rede e exige cobertura mínima de 80% em `app/`.

