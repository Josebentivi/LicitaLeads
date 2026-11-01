# Guia de uso

## Instalação

```powershell
# Windows
py -3.12 -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -e ".[dev,ocr]"
copy .env.example .env
python scripts/setup.py
python run.py
```

```bash
# Linux/macOS
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[dev,ocr]'
cp .env.example .env
python scripts/setup.py
python run.py
```

`python scripts/setup.py` cria `.env` (se ausente) e aplica
`alembic upgrade head`. `python run.py` exige o banco na revisão Alembic head;
use `--migrate` para migrar antes de subir e `--reload` para desenvolvimento.
No Windows, `iniciar.bat` instala por padrão os extras de desenvolvimento e OCR,
inicia API e scheduler e prepara o navegador de testes. Em unidade sincronizada,
o launcher pode usar Python e SQLite em `%LOCALAPPDATA%\LicitaLeads`; confira
`python scripts/launcher.py --check` antes de usar a CLI ou Alembic diretamente.
Eles podem usar outro banco se não receberem o `DATABASE_URL` efetivo.

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
| **Empresas** | CNPJ, participações comprovadas, eventos e contatos corporativos com origem |
| **Atas (ARP)** | Consulta manual por UF e órgãos/unidades conhecidos, fontes e período; itens e fornecedores registrados |
| **Contratos** | Coleta por fonte, CNPJ do órgão e período; itens, termos e pagamentos conciliados |
| **Pagamentos** | Fontes municipais e importação CSV/JSON; pagamentos conciliados ou para revisão |
| **Leads** | Filtros por score mínimo, tipo de evento e revisão pendente; detalhe com evidências e rascunho |
| **Coletas** | Progresso, histórico, fontes, continuação, repetição, falhas documentais e fila de IA |
| **Configurações** | Configuração pública sem segredos, pausa do scheduler e manutenção |
| **Fontes** | Matriz de capacidades auditadas |
| **Testes** | Diagnóstico rápido e suíte local com etapas e relatórios identificados |
| **Ajuda** | Orientações de uso e operação dentro da aplicação |

No Dashboard, selecione UF e dias, mantenha **Baixar/processar documentos**
marcado quando quiser identificar eventos em documentos e clique em **Executar
coleta**. Em **Coletas**, acompanhe fontes, estágio, falhas e documentos. O
total informado pela fonte é o total da consulta, não uma garantia de que
todos serão processados naquela execução. Uma execução parcial também pode
decorrer de limite ou resposta incompleta. Os resultados confirmados permanecem
disponíveis; leia o diagnóstico antes de agir.

**Continuar coleta** retoma as pendências do lote com filtros e período
congelados. **Repetir coleta** ou **Tentar fonte novamente** inicia uma nova
execução com os filtros anteriores; detalhes comprovadamente inalterados podem
ser reaproveitados. Marque **Reconsultar detalhes já verificados** para forçar
consulta completa. Continuações mantêm a opção original. Documentos com falha
elegíveis têm reprocessamento próprio, sem repetir a descoberta.

Em **Atas**, selecione UF, órgão/unidade e fonte ou as opções de todos os
órgãos conhecidos e todas as fontes disponíveis. As unidades vêm de
contratações já identificadas e podem não cobrir toda a UF. O formulário manual
começa com o menor valor entre 7 dias e a janela automática configurada; o job
automático usa a janela geral de 30 dias por padrão ou
o override de atas. A CLI `crawl-atas` usa 365 dias se `--days` for omitido.
O formulário manual não muda o agendamento.

Em **Contratos**, informe o CNPJ do órgão e o período; a janela é de publicação
no PNCP e de início de vigência no Compras.gov.br. Em **Pagamentos**, escolha
uma fonte municipal ou importe CSV/JSON do portal. Um identificador oficial,
quando disponível, é vinculado ao portal de origem. Sem ele, uma linha alterada
pode gerar outro registro. Empenho, contrato, aditivo e pagamento são fatos
distintos; pagamento não conciliado não prova inadimplência.

## Revisão de leads

- O score **2.0** (0–100) vem de urgência (0–30), evidência (0–30), relevância jurídica
  (0–25), contato (0–5) e valor econômico (0–10), menos penalidades
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

Use o lead como indicação para análise, não como conclusão de irregularidade.
Confira identidade, trecho, contexto, fase e prazo na fonte oficial antes de
decidir. `PARTICIPATION_DETECTED` comprova participação, sem indicar derrota.
Em Empresas, **Em processo ativo** significa proposta aberta, e pode excluir
processos em julgamento ou recurso. A data de adição do lead não é a data do ato.
Antes de qualquer contato profissional, avalie as regras éticas aplicáveis,
inclusive o [Provimento 205/2021 da OAB](https://www.oab.org.br/leisnormas/legislacao/provimentos/205-2021)
e as orientações da seccional; rascunho gerado não autoriza abordagem automática.

## IA, acompanhamento e testes

A análise de IA é opcional e fica em fila separada da extração. Em **Coletas**,
acompanhe pendentes, concluídas, bloqueios, custo conhecido e retentativas.
Resultados sujeitos a revisão continuam exigindo conferência humana. Consulte
[fila de IA](fila_ia.md) para ativação, custos e limites.

Em **Testes**, o diagnóstico rápido e a suíte completa têm relatórios próprios.
A suíte local usa dados temporários e verifica Python, processos API/scheduler,
OCR, navegador e qualidade de código. Relatórios antigos ou de outra versão
não comprovam o código atual; a tela informa pendências e falhas. Fontes reais
e serviços pagos não são verificados pela suíte offline. Veja [testes locais](testes_locais.md).

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


## Visão completa da empresa

Abra **Empresas** e selecione o CNPJ, ou use **Visão completa da empresa** no
lead, na ata ou no contrato. A página reúne o acervo coletado em seções
expansíveis: leads, licitações e participações, atas, contratos, pagamentos,
eventos, prazos, documentos e contatos corporativos.

Leads abrem inicialmente. As demais listas carregam quando expandidas, em
páginas de 20 registros; os detalhes também abrem na mesma tela. Os links
continuam funcionando sem JavaScript. **Incluir representações históricas**
permite consultar documentos retirados e representações financeiras anteriores.
Documentos e saldos gerais da contratação/ata aparecem como contexto e não
como fatos exclusivos da empresa. A ausência de registros no acervo não
comprova ausência de atividade.

Dentro do lead, confira a explicação da nota e suas evidências, registre a
revisão e gere ou recupere o rascunho. Nenhuma mensagem é enviada. Decisões
humanas, responsáveis, notas e rascunhos são preservados pelo recálculo.

Novos leads usam a pontuação **2.0**. Leads antigos mostram sua versão e só
migram por **Recalcular pontuação** ou **Recalcular selecionados**. O cálculo
usa somente dados locais e registra antes/depois, entradas e fontes. Os
prazos continuam sendo atualizados independentemente dessa escolha.

Na versão 2.0, inabilitação, desclassificação, rejeição de proposta e fases de
recurso têm maior relevância. Prazos desconhecidos ou vencidos não recebem
urgência; estimativas têm urgência limitada. A parcela econômica usa o maior
valor individual comprovado da empresa no mesmo processo, respeitando o item
do evento. Valores conflitantes ficam fora da conta. O orçamento geral e os
pagamentos permanecem contexto: não se somam contrato, ata, empenho e pagamento,
nem se interpreta ausência de pagamento como inadimplência.


## Participantes e valores documentais

A contratação distingue participantes comprovados, referências para revisão e atribuições históricas invalidadas. Uma empresa citada como fornecedora de custo, fabricante, cliente ou seguradora não é, por isso, licitante.

O CNPJ do próprio órgão comprador também não comprova participação como fornecedor. Números de cláusulas do edital e de itens locais não identificam automaticamente um produto do PNCP. Vínculos antigos sem correspondência comprovada ficam para revisão; decisões e vínculos manuais são preservados e sinalizados.

Os valores mostram empresa, lote/item local, natureza da proposta e evidência. Proposta publicada, inicial, readequada e resultado final são fatos diferentes. Custos unitários, garantias e orçamento estimado não são totais da participante. A numeração local só é vinculada ao item do PNCP com correspondência comprovada.

Totais publicados para vários itens, sem valores individuais e sem correspondência suficiente de escopo, não são atribuídos ao processo inteiro. Consulte a evidência e o histórico quando não houver um valor individual confirmado.

Divergências ficam identificadas e não entram na referência econômica dos leads. O cálculo usa o maior valor individual elegível, sem somar lotes. O reparo do acervo preserva notas antigas, decisões humanas e rascunhos; o operador pode solicitar recálculo explícito posteriormente. Leads com atribuição invalidada ficam históricos e não permitem gerar outro rascunho.
