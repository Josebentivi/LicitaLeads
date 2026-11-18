# Roteiro de validação de usabilidade

**Estado: preparado para execução com usuários. Nenhuma sessão com pessoas foi
realizada ou resultado de usabilidade presumido.**

## Preparação

Convidar dois ou três advogados/analistas com níveis distintos de familiaridade
com licitações. Reservar 20 a 30 minutos por pessoa. Usar uma instância isolada de
teste com dados sintéticos e documentos de exemplo, incluindo uma oportunidade
com prazo expresso, outra estimada, uma sem prazo e uma evidência histórica.
Não inserir dados fictícios no banco operacional nem iniciar coletas externas
como parte da sessão.

Explicar apenas o objetivo da aplicação e pedir que a pessoa diga o que está
tentando fazer. O facilitador observa antes de orientar. Registrar separadamente
ações espontâneas e tarefas concluídas com ajuda.

## Tarefas

| Tarefa | O que observar | Critério proposto |
|---|---|---|
| Encontrar algo para analisar a partir do início | Reconhece a fila e os cartões? | Abre uma oportunidade sem ajuda |
| Explicar o fato e localizar a prova | Encontra trecho, página e documento? | Abre a evidência em até 3 interações a partir da lista |
| Distinguir prazo expresso, estimado e não identificado | Confunde data do fato com limite? | Identifica corretamente os três casos |
| Buscar empresa/CNPJ e revisar a oportunidade | Entende situação e registra autoria? | Vê confirmação e mudança de situação |
| Continuar a análise e voltar à lista | Perde filtros ou posição? | Reconhece que o contexto foi preservado |
| Preparar e copiar um rascunho | Entende que não houve envio? | Copia ou usa a alternativa manual |
| Escolher uma UF e um período de 30 dias | Descobre o atalho e as opções avançadas? | Confere datas inclusivas e identifica como iniciar |
| Corrigir um filtro inválido | Entende o erro e recupera a busca? | Corrige sem redigitar os outros campos |

Repetir ao menos busca e consulta da evidência em uma janela estreita/celular.
As metas são critérios para avaliação futura, não resultados já alcançados.

## Ficha de observação — preencher durante cada sessão

| Campo | Registro |
|---|---|
| Sessão / data / dispositivo | A preencher |
| Perfil e experiência do participante | A preencher |
| Tarefa e tempo aproximado | A preencher |
| Concluída sem ajuda / com ajuda / não concluída | A preencher |
| Cliques, erros e retornos | A preencher |
| Frase ou dúvida espontânea | A preencher |
| Dificuldade de 1 (fácil) a 5 (difícil) | A preencher |
| Ajuste sugerido / gravidade | A preencher |

Ao terminar, perguntar: “O que você esperava encontrar?”, “Em qual informação
ficou em dúvida?” e “Qual etapa deu mais trabalho?”. Priorizar erros de
interpretação de fatos/prazos, depois bloqueios de tarefa e por último atritos
visuais. Após correções, repetir as tarefas afetadas com os participantes.

## Verificação técnica complementar

```powershell
python -m pytest tests/integration/test_analyst_workspace.py --no-cov -q
python -m app.cli prepare-test-browser
python -m pytest tests/browser/test_analyst_workspace.py --no-cov -q
```

Esses testes usam banco/armazenamento temporários. A preparação explícita do
navegador pode obter o driver; a execução dos testes não faz esse download.
Passar nos testes não substitui a observação de usuários.
