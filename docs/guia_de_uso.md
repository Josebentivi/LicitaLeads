# Guia de uso

## Instalação

```powershell
# Windows
py -3.12 -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -e ".[dev]"
copy .env.example .env
python scripts/setup.py
python run.py
```

```bash
# Linux/macOS
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[dev]'
cp .env.example .env
python scripts/setup.py
python run.py
```

`python scripts/setup.py` cria `.env` (se ausente) e aplica
`alembic upgrade head`. `python run.py` exige o banco na revisão Alembic head;
use `--migrate` para migrar antes de subir e `--reload` para desenvolvimento.

## Fluxo operacional (CLI)

1. **Coletar** contratações:
   `python -m app.cli crawl pncp --uf MA --days 7`
2. **Processar** documentos pendentes (download, extração, eventos):
   `python -m app.cli process-documents`
3. **Reprocessar** detecção se necessário:
   `python -m app.cli detect-events`
4. **Recalcular** prazos:
   `python -m app.cli calculate-deadlines`
5. **Enriquecer** contatos corporativos:
   `python -m app.cli enrich-contacts`
6. **Criar/atualizar** leads:
   `python -m app.cli create-leads`
7. **Exportar**:
   `python -m app.cli export-leads leads.csv`

Ou tudo de uma vez com `python -m app.cli run-pipeline --uf MA --days 7`.

## Interface web

| Tela | Uso |
|---|---|
| **Dashboard** | Métricas (contratações, eventos, empresas, leads, prazos em 24 h, vencidos, revisões pendentes, erros de coleta) e últimos leads |
| **Contratações** | Filtros por UF, município e órgão; detalhe com itens, documentos, participantes, eventos e origem de cada dado |
| **Leads** | Filtros por score mínimo, tipo de evento e revisão pendente; detalhe com evidências e rascunho |
| **Coletas** | Status por fonte, erro amigável, detalhe técnico e **retry somente da fonte que falhou** |
| **Configurações** | Visão pública das configurações, sem segredos |
| **Fontes** | Matriz de capacidades auditadas |

Em **Coletas**, uma execução pode ficar `partial` quando uma fonte falha e a
outra conclui; os registros válidos são preservados. O fragmento HTMX
`/crawls/table` é repollado enquanto há coleta ativa.

## Revisão de leads

- O score (0–100) vem de urgência (0–30), evidência (0–25), relevância jurídica
  (0–20), contato (0–15) e valor econômico (0–10), menos penalidades
  (empresa não confirmada, CNPJ ausente, evidência fraca, motivo desconhecido,
  prazo estimado, OCR pendente, processo encerrado, prazo vencido).
- Leads abaixo de `MIN_LEAD_SCORE` (padrão 50) ficam `below_threshold`.
- Eventos com empresa ambígua/ausente ou confiança abaixo de
  `MANUAL_REVIEW_CONFIDENCE_THRESHOLD` (padrão 0.75) exigem revisão.
- Decisões de revisão: `approved`, `rejected` ou `needs_changes`, com autor e
  notas (`LeadReview`).
- Eventos sem empresa identificada podem ser vinculados manualmente a uma
  empresa **já participante** na tela de detalhe da contratação.
- Rascunhos de outreach são gerados sob demanda e permanecem como rascunho; o
  sistema não envia mensagens.

## Prazos e feriados

O motor prioriza, nesta ordem: prazo explícito → prazo extraído de documento →
regra de dias úteis do edital → estimativa jurídica configurada (somente para
eventos compatíveis e com marco inicial confiável). Toda estimativa registra
método, base, dias excluídos, confiança e necessidade de revisão.

Feriados nacionais são calculados automaticamente. Feriados estaduais e
municipais podem ser adicionados em `data/holidays.csv` com as colunas
`date,name,scope,uf,municipality_ibge` (`scope`: `national`, `state` ou
`municipal`).

## Limites e avisos

- Estimativa de prazo não substitui a conferência do edital, da ata, da
  plataforma oficial e da legislação aplicável.
- Resultado homologado é o vencedor/adjudicatário, não a lista de participantes.
- Sem documentos, inabilitação/desclassificação/recursos podem não aparecer.
- PDF escaneado fica `ocr_required` sem OCR configurado.
- LLM e busca de contatos são opcionais e ficam desligados por padrão.
