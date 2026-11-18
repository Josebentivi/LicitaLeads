# Experiência do advogado e analista

## Agenda e fila de análise

`/dashboard` abre a **Minha agenda de análise**. Os cartões Aguardando revisão,
Prazos próximos, Novas oportunidades e Ajustes solicitados consultam o mesmo
serviço de filtros usado em `/leads`. Cada cartão abre sua lista correspondente.
Prazos próximos consideram datas expressas ou estimadas nas próximas 72 horas para
oportunidades novas, pendentes de revisão ou com ajustes solicitados. A lista da
agenda usa esses três estados e atribuições atuais. A nota ordena a fila.

O início também mostra a última consulta HTTP bem-sucedida às fontes e o estado
das atualizações automáticas. Esse horário é de consulta, não uma afirmação de
que todo o acervo está atualizado. Documentos pendentes ou que precisam de OCR
aparecem como pendências. **Coletar dados** fica sempre no topo; em **Coletas**
é possível ajustar o escopo antes de usar a ação.

## Analisar e decidir

O detalhe da oportunidade organiza fato, prioridade, prazo, evidência, decisão
e rascunho. A data do fato e a data-base de cálculo nunca são exibidas como
data-limite na ausência de prazo. A tela identifica prazo expresso, estimado,
manual, histórico ou não identificado. O cálculo e suas explicações permanecem
acessíveis. A indicação temporal é calculada para a data atual sem alterar a
situação armazenada no último cálculo, que continua acessível. Decisões sobre
atribuições históricas ou invalidadas são bloqueadas.

**Conferir evidência** abre `/evidencias/{id}` com o trecho escapado, página
quando registrada, data da coleta e links para documento armazenado/fonte.
Conteúdo histórico é identificado. A API de metadados continua disponível em
detalhes de rastreabilidade; o endpoint da API não mudou.

Após revisar, há confirmação e opção **Registrar e analisar próxima**. A próxima
oportunidade usa a mesma busca, incluindo situação, evento, prazo e recorte de
coleta. Voltar à lista preserva filtros e página. Destinos de retorno são
restritos às listas locais correspondentes.

## Buscas e preferências

- Oportunidades: busca por nome, CNPJ ou processo, situação e proximidade do prazo.
- Contratações: busca por processo/objeto/órgão, UF, publicação e situação;
  campos especializados ficam em **Filtros avançados**.
- Empresas: busca existente com filtros, totais e retorno preservado.
- Atas e contratos: totais, filtros removíveis e retorno aos filtros no detalhe;
  nas atas, os campos especializados ficam recolhidos.
- Pagamentos: fontes e fases selecionáveis, totais e filtros removíveis; os
  avisos de importação/coleta não são contados como filtros. Ausência de vínculo
  exato continua distinta de ausência de pagamento.
- Filtros aplicados aparecem como atalhos removíveis; **Limpar filtros** volta
  à lista completa. CNPJ inválido e intervalos invertidos mantêm os campos e
  não ampliam a consulta silenciosamente.
- **Guardar esta busca** salva a URL no `localStorage` deste navegador. O nome
  do revisor também é lembrado localmente, sem alterar autoria anterior.
- **Copiar rascunho** confirma a cópia; se a área de transferência estiver
  indisponível, seleciona o texto para cópia manual. O sistema não envia mensagens.

## Coletas

De/Até são inclusivos. Há atalhos Últimos 7 dias, Últimos 30 dias, Este mês e
Personalizado. Os atalhos usam o dia local da aplicação exibido ao abrir a página.
O escopo também permite pedir nova consulta de detalhes já verificados. O JavaScript
previne submissões repetidas enquanto o formulário navega e restaura os botões
ao voltar pelo histórico. A execução continua protegida pelas leases do pipeline.

Na agenda, **Última coleta de contratações** resume o que a execução registrou
sobre descoberta e processamento. A etapa documental pode trabalhar em arquivos
pendentes de contratações anteriores. Os links abrem a execução, os documentos
do lote e as contratações e oportunidades vinculadas. Na ficha de contratação,
Itens e Documentos mostram a cobertura das fontes identificadas, com data e
origem quando há resposta preservada. Um erro de consulta ou ausência de
histórico fica visível mesmo quando o acervo está vazio.

Erros de formulário reapresentam os campos para correção. O painel acompanha
contratações e documentos, TCE-MA, atas, contratos e pagamentos em etapas
distintas. Pagamentos podem avançar enquanto contratações aguardam uma fonte.
O histórico de cada execução dá acesso aos diagnósticos e às ações disponíveis.
O histórico separa período consultado e horários de execução. Para execuções
antigas sem período registrado, a interface informa a ausência sem reconstruí-lo.

**Ver oportunidades vinculadas** e **Ver contratações vinculadas** usam registros
de origem, histórico de campos e documentos do lote. Não usam coincidência de
datas. São registros das contratações relacionadas à coleta, podendo incluir
oportunidades anteriores; não são uma contagem de novas oportunidades produzidas.
Atas/contratos/pagamentos apontam explicitamente para seus acervos.

## Consultar o acervo

Em **Compras públicas → Itens de compra**, busque uma descrição em todas as
contratações e refine por órgão e unidade. A descrição abre a linha do item
na ficha de origem. Valores unitários estimados preservam quatro casas decimais.

Em **Empresas**, use **Origem da presença** junto de **Local do órgão ou portal**
para encontrar, por exemplo, empresas com execução financeira registrada em
Caxias. **UF da sede** consulta o cadastro da empresa. Cada contagem de origem
abre a seção correspondente do dossiê.

Em **Pagamentos**, selecione município, exercício e função orçamentária.
O acervo usa a classificação publicada pelo portal e liga cada registro ao
empenho e à resposta de origem. A visão de fornecedores reúne as empresas
identificadas por CNPJ no mesmo recorte. Referências locais de processo ou
licitação servem à conferência na fonte. O vínculo contratual continua
dependendo da conciliação por identificadores oficiais.

Em **Operação de dados → Alterações do acervo**, confira as alterações aplicadas
em vigências, valores e situações. A lista usa o momento do registro local para
incluir observações que chegaram com atraso. O primeiro preenchimento de cada
campo fica no histórico da entidade. Ao terminar a última página, **Marcar como
consultado** guarda o corte neste navegador. **Todas as alterações** permite
rever o período anterior.

## Validação com analistas

Os testes de integração e navegador cobrem os fluxos técnicos. A validação com
advogados/analistas exige observação de usuários reais, segundo o
[roteiro de usabilidade](validacao_usabilidade.md).
