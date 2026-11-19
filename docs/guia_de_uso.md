# Guia de uso

## Comece pela sua agenda

No início, **Minha agenda de análise** oferece filas de revisão, prazos nas
próximas 72 horas, novas oportunidades e ajustes solicitados. Abra um cartão
para ver a lista já filtrada. Em **Oportunidades**, confira o fato, a natureza
do prazo e a prova; registre sua decisão e, se desejar, analise a próxima.
O retorno à lista mantém a busca e a página. Veja o
[guia da experiência do analista](experiencia_usuario.md).

## Encerrar o programa

Use **Encerrar a aplicação**, no rodapé do menu lateral, e confirme. O sistema
cancela coletas, análises de IA e testes ativos ou pendentes, encerra os processos
filhos e fecha API, scheduler e launcher. Dados já gravados, evidências e
resultados concluídos são preservados.

A próxima abertura manterá coletas automáticas e IA pausadas. Retomá-las ou
continuar trabalhos cancelados exige uma ação explícita. O botão não apaga o
acervo e não equivale apenas a pausar o scheduler.

O encerramento completo está disponível para a instância iniciada pelo
`iniciar.bat`, com acesso pela própria máquina. O navegador pode impedir que a
aba seja fechada; nesse caso ela mantém a mensagem **Aplicação encerrada**.
Se a comunicação cair antes da confirmação, confira o log do launcher. Há até
60 segundos de limpeza cooperativa antes de finalizar processos travados.

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
| **Minha agenda de análise** | Filas de prioridades, atualização das fontes, oportunidades, acesso à coleta e indicadores do acervo |
| **Contratações** | Filtros por UF, município e órgão; detalhe com itens, documentos, participantes, eventos e origem de cada dado |
| **Empresas** | Dados corporativos com origem, histórico semestral ou anual, participações comprovadas e eventos |
| **Atas (ARP)** | Busca no acervo; unidades, itens e fornecedores registrados |
| **Contratos** | Busca por número, órgão ou CNPJ do fornecedor; itens, termos e pagamentos conciliados |
| **Pagamentos** | Pagamentos conciliados ou para revisão e importação CSV/JSON de arquivos oficiais |
| **Oportunidades** | Busca por empresa/CNPJ/processo, filas por situação, prazo, filtros avançados, evidências legíveis e revisão contínua |
| **Coletas** | Progresso, histórico, fontes, continuação, repetição, falhas documentais e fila de IA |
| **Configurações** | Configuração pública sem segredos, pausa do scheduler e manutenção |
| **Fontes** | Matriz em português com a cobertura de cada integração e acesso à consulta oficial |
| **Testes** | Diagnóstico rápido e suíte local com etapas e relatórios identificados |
| **Ajuda** | Orientações de uso e operação dentro da aplicação |

Clique em **Coletar dados** no topo de qualquer página. Para mudar a UF ou o
período, abra **Coletas → Escopo da próxima coleta** e use o mesmo botão.
O período inclui as duas datas e pode recuar até dois anos. O padrão usa `DEFAULT_UF` e
`DEFAULT_LOOKBACK_DAYS`; os atalhos de calendário permitem trocar a janela.
Em **Coletas**, cada etapa mostra o estado e as pendências. Assim que a etapa
de contratações e documentos terminar, o atalho **Ver oportunidades vinculadas**
abre a fila daquela coleta. O
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

As atas usam órgãos e unidades conhecidos na UF. A coleta de contratos usa os
órgãos configurados ou os já identificados na UF; a janela é de publicação no
PNCP e de início de vigência no Compras.gov.br. Os pagamentos municipais são
consultados pelos exercícios que o período inclui. Pagamentos federais
dependem da chave do Portal da Transparência e dos CNPJs de favorecidos
configurados. Em **Pagamentos**, também é possível importar CSV/JSON oficial. Um
identificador oficial,
quando disponível, é vinculado ao portal de origem. Sem ele, uma linha alterada
pode gerar outro registro. Empenho, contrato, aditivo e pagamento são fatos
distintos; pagamento não conciliado não prova inadimplência.

Nas atas, o PNCP fornece o cabeçalho e o módulo ARP do Compras.gov.br pode
complementar com itens, fornecedores, unidades, saldos, empenhos e adesões.
**Não informado** representa campo sem valor no acervo. Confira a fonte e a
execução da coleta para investigar seções vazias.

O **TCE-MA** complementa os pregões eletrônicos da Prefeitura de São Luís
(UASG 980921) com vínculo confirmado pelo controle PNCP. No detalhe do
pregão, consulte a plataforma de origem, quando o PNCP publica o link, e a
seção **TCE-MA · dados complementares**, com referências oficiais e registros
para revisão quando o controle coincide e a identificação diverge. A lista do tribunal não vira evento ou oportunidade
automaticamente.

## Revisão de leads

- A pontuação de prioridade **2.0** (0–100) vem de urgência (0–30), evidência (0–30), relevância jurídica
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

Em **Dados corporativos**, confira razão social, CNPJ, endereço, telefone e
e-mail, além da origem e da situação da verificação das informações disponíveis.
Use **Registrar um dado corporativo** para completar o cadastro com um dado
público referente ao CNPJ exibido. O CSV de contatos também aceita o tipo
`address`. Em **Histórico de participações**, selecione semestre ou ano. Os
totais contam contratações distintas pela data de publicação dos últimos dois
anos. Abra um total para consultar as respectivas contratações.

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
## Central de IA

Em **Operação de dados → Central de IA**, acompanhe análises de documentos já
extraídos, bloqueios, consumo e cobertura parcial. A IA pode ser pausada ou
retomada independentemente das coletas automáticas. Enfileirar ou tentar IA
novamente não baixa documentos nem inicia OCR.

Confira **Sugestões para revisão** antes de confirmar empresa e ato. O candidato
não é um fato comprovado e não gera leads ou prazos automaticamente. Os detalhes
da contratação e da empresa oferecem acessos filtrados à central. Veja o
[guia da fila de IA](fila_ia.md) para limites, custos e cancelamento.
