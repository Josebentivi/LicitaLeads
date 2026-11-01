# Modelo de dados

Implementação: `app/models/entities.py` (ORM SQLAlchemy) e `app/models/enums.py`.
Migrações em `migrations/`.

## Convenções

- Chaves primárias UUID (`UUIDPrimaryKeyMixin`); `JobLease` usa `name` como PK.
- `TimestampMixin` adiciona `created_at`/`updated_at` em UTC.
- `FingerprintMixin` adiciona `fingerprint` (SHA-256 determinístico) para
  idempotência de upserts.
- Enums são gravados como `VARCHAR` com `CHECK` (`database_enum`), não como
  tipos nativos do banco.
- Restrições de checagem protegem invariantes (faixas de score, confiança entre
  0 e 1, valores não negativos, `sent_requires_approval`).

## Cadeia de proveniência

```text
CrawlRun --> SourceRecord (payload bruto, hash, endpoint, disponibilidade)
                  |
                  +--> ProcurementSource --> Procurement (canônico)
                  +--> FieldObservation (campo, valor, status, confiança)
                  +--> Evidence (trecho, localizador, página, offsets)
                             |
                             +--> ProcurementEvent / Participant / Deadline / Lead
```

- **`SourceRecord`** guarda a resposta HTTP original, o hash do conteúdo, os
  parâmetros da requisição e a `availability`; é a raiz de auditoria.
- **`ProcurementSource`** liga o registro canônico à sua origem (fonte,
  `external_id`, URL, coleta, `is_primary`).
- **`FieldObservation`** registra cada campo normalizado com `value_status`
  (`observed`, `unknown`, `not_available`, `requires_manual_review`), fonte,
  confiança e vínculo com `SourceRecord` ou `Evidence`.
- **`Evidence`** exige rastreabilidade: `document_id` OU `source_record_id` OU
  `source_url` (`traceable_source_required`); guarda trecho literal, página,
  localizador e offsets.
- **`DocumentChunk`** preserva a localização do trecho no documento (página,
  seção, planilha, intervalo de células, offsets).

## Entidades

| Entidade | Papel | Pontos-chave |
|---|---|---|
| `Procurement` | Contratação canônica | `pncp_control_number` único; dedup por fonte+`external_id`; índice composto (órgão, UASG, número, ano, modalidade); `procurement_type` (`licitacao`/`contratacao_direta`/`procedimento_auxiliar`), `is_srp` e `legal_basis` observados da fonte |
| `CrawlRun` | Execução de coleta | status, contadores, filtros e diagnósticos por fonte no `cursor` |
| `SourceRecord` | Resposta bruta | raiz de auditoria |
| `ProcurementSource` | Vínculo de origem | um registro por fonte+`external_id` |
| `FieldObservation` | Observação de campo | valor + status + confiança + origem |
| `FieldState` / `FieldChange` | Estado de campo e diário de decisões | data da fonte e da consulta, origem, antes/depois e transições repetidas |
| `CollectionBatch` / `CollectionWorkItem` | Lote congelado e trabalho persistido | filtros e período fixos; páginas, pendências e confirmação transacional |
| `ProcurementItem` | Item/lote | único por contratação+número; quantidades e valores estimados |
| `Company` | Empresa | `cnpj` único; `normalized_name`; site/domínio |
| `Document` | Documento oficial | URL original, hash do arquivo, status de extração, `is_active`/`deactivated_at` para retratação, caminho local, pai/filho para ZIPs |
| `DocumentChunk` | Trecho extraído | localizador para citar evidência |
| `Evidence` | Prova | trecho literal + rastreabilidade obrigatória |
| `Participant` | Participante/vencedor | papel, item, valor, confiança e evidência; `status_code` normaliza o desfecho (`winner`/`awarded`/`participant`/`disqualified`/`ineligible`) mantendo o texto bruto em `status` |
| `ProcurementEvent` | Evento jurídico | tipo, motivo normalizado/categoria, confiança, revisão manual, evidência |
| `Deadline` | Prazo | método de cálculo, prazo explícito/estimado, status e explicação textual |
| `CompanyContact` | Contato corporativo | único por empresa+tipo+valor; `is_corporate`/`is_personal` mutuamente exclusivos |
| `Lead` | Oportunidade | score 0–100 decomposto; vínculo com evento, prazo, empresa e processo |
| `LeadReview` | Decisão humana | `approved`, `rejected`, `needs_changes`, com autor e notas |
| `PriceRegistry` | Ata de registro de preços | cabeçalho oficial (PNCP/Compras): número, ano, órgão, vigência, valores, vínculo com a contratação (`linked_pncp_control_number`) e com a resposta bruta (`source_record_id`) |
| `PriceRegistryItem` | Item registrado da ata | único por ata+item+fornecedor; descrição, quantidades, valores, fornecedor (empresa por CNPJ validado) e adesão máxima |
| `Contract` / `ContractItem` / `ContractTerm` | Contrato, itens e termos | Valores e vigência com origem, separados de pagamentos |
| `Payment` | Fato financeiro | Fonte, identidade, empenho e conciliação; agregado não é pagamento individual |
| `OutreachDraft` | Rascunho | `facts_hash`/`template_hash` para idempotência; `sent` exige `approved` |
| `JobLease` | Lease de job | evita execução concorrente do scheduler/pipeline |

`FieldChange` é um diário cronológico: A → B → A permanece como três
observações, mesmo que a deduplicação de valores seja reutilizada. Ausência,
`null` ou texto vazio não apagam valor conhecido por padrão; `0` e `false` são
valores válidos. Remoção explícita depende da completude da resposta. Valores
anteriores rejeitados e divergências ficam auditáveis; dados históricos não
recebem versões inventadas. Veja [coletas incrementais](coletas_incrementais.md).

## Deduplicação

- Chave preferencial: `numeroControlePNCP` (`pncp_control_number`).
- Sem ele: CNPJ do órgão + unidade/UASG + número + ano + modalidade
  normalizada, **somente quando todos os campos estiverem completos**.
- `Procurement` é único por (`source`, `external_id`); `ProcurementSource`
  idem.
- Empresas são unificadas por CNPJ validado (numérico ou alfanumérico); nome
  normalizado serve para comparação, nunca para fusão automática.
- Participantes, eventos e demais entidades derivadas usam `fingerprint`
  determinístico para upsert idempotente.
- Similaridade textual nunca auto-funde empresas ou processos.

## Enums principais

| Enum | Valores |
|---|---|
| `DataAvailability` | `available`, `empty`, `not_supported`, `not_published`, `access_restricted`, `temporary_error` |
| `FieldValueStatus` | `observed`, `unknown`, `not_available`, `requires_manual_review` |
| `ProcurementEventType` | `PROPOSAL_SUBMITTED`, `PROPOSAL_ACCEPTED`, `PROPOSAL_REJECTED`, `DISQUALIFIED`, `QUALIFIED`, `INELIGIBLE`, `INTENT_TO_APPEAL`, `APPEAL_SUBMITTED`, `COUNTERARGUMENT_OPENED`, `COUNTERARGUMENT_SUBMITTED`, `APPEAL_DECIDED`, `WINNER_DECLARED`, `ADJUDICATED`, `HOMOLOGATED`, `SESSION_SUSPENDED`, `SESSION_REOPENED`, `PARTICIPATION_DETECTED`, `UNKNOWN` |
| `ReasonCategory` | `TECHNICAL_SPECIFICATION`, `MISSING_DOCUMENT`, `INVALID_DOCUMENT`, `FISCAL_REGULARITY`, `LABOR_REGULARITY`, `ECONOMIC_FINANCIAL`, `TECHNICAL_QUALIFICATION`, `PRICE_INEXEQUIBILITY`, `PRICE_ABOVE_ESTIMATE`, `LATE_SUBMISSION`, `PROPOSAL_FORMAT`, `SAMPLE_REJECTED`, `BRAND_OR_MODEL_NONCOMPLIANT`, `FAILURE_TO_RESPOND`, `OTHER`, `UNKNOWN` |
| `ParticipantRole` | `participant`, `winner`, `awarded`, `contractor`, `unknown` |
| `ParticipantStatus` | `winner`, `awarded`, `participant`, `disqualified`, `ineligible`, `cancelled`, `unknown` |
| `ContractingType` | `licitacao`, `contratacao_direta`, `procedimento_auxiliar` |
| `ExtractionStatus` | `pending`, `extracted`, `ocr_required`, `failed`, `unsupported` |
| `DeadlineCalculationMethod` | `EXPLICIT`, `DOCUMENT_EXTRACTED`, `EDITAL_RULE`, `LEGAL_ESTIMATE`, `MANUAL`, `UNKNOWN` |
| `DeadlineStatus` | `OPEN`, `DUE_TODAY`, `DUE_WITHIN_24H`, `EXPIRED`, `UNKNOWN`, `REQUIRES_REVIEW` |
| `ContactType` | `email`, `phone`, `whatsapp`, `form`, `website`, `linkedin` |
| `ContactStatus` | `discovered`, `verified`, `invalid`, `requires_review` |
| `LeadStatus` | `new`, `below_threshold`, `pending_review`, `approved`, `rejected`, `needs_changes`, `archived` |
| `ReviewDecision` | `approved`, `rejected`, `needs_changes` |
| `CrawlRunStatus` | `pending`, `running`, `completed`, `partial`, `failed`, `cancelled` |
| `OutreachChannel` | `email`, `whatsapp`, `phone`, `generic` |

## Sincronização prospectiva

`collection_sync_states` conserva a cobertura confirmada e o lote pendente por
fonte/fluxo/filtros. `verified_collection_versions` referencia as respostas brutas
que comprovam cada versão e operação. `source_response_validators` permite uma
consulta condicional HTTP sem modificar a data da resposta original.
`collection_recheck_states` registra a rotação das identidades revisitadas.
Essas tabelas começam vazias e participam da limpeza de dados.

As tabelas `llm_batches`, `llm_demands`, `llm_work_items`, `llm_results`,
`llm_attempts` e `llm_provider_states` registram demandas, trabalho, resultados,
tentativas e bloqueios da fila de IA. Uso e custo desconhecidos permanecem
desconhecidos; preços históricos não são recalculados. Detalhes em [fila de IA](fila_ia.md).

`documents.resolved_url` guarda o endereço efetivamente baixado após validação
e confirmação de versão; `original_url` continua sendo o endereço recebido.


### Revisões de pontuação

A revisão `0025_company_scoring` amplia os limites de evidência para 30 e
relevância para 25, preservando os valores anteriores. `LeadScoreRevision`
registra transições da nota com versão, antes/depois, componentes, entradas,
referências de origem e momento do cálculo. `LeadScoreRequest` guarda o recibo
transacional de cada solicitação explícita, incluindo resultados inelegíveis.
Não há preenchimento retroativo. Ambas as tabelas participam da limpeza de dados.
