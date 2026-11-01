# Diagnóstico de desempenho da coleta de atas — 27/10/2026

> **Registro histórico:** descreve o estado e os resultados da data indicada. Consulte a [documentação atual](README.md).

## Resultado

O principal gargalo observado foi o controle adaptativo do próprio cliente:
no fim da execução, os inícios das requisições ficaram separados por cerca de
30 segundos. A latência HTTP e a inserção das respostas no banco eram menores.
Aumentar o teto de concorrência não remove esse intervalo entre inícios.

## Medição observada

Execução `1bf43315-a951-4744-9c89-bfb74ae773e8`, cancelada durante a descoberta.
Período: 16:54:12 a 16:59:54 em America/Chicago, em 27/10/2026.
Foram observados 5 min 41,56 s de execução. A configuração carregada pelo processo
era concorrência 8; a alteração posterior do arquivo para 20 depende de reinício.

| Indicador | Resultado |
|---|---:|
| Tentativas HTTP com resposta | 36 |
| Respostas HTTP 200 | 20 |
| Respostas HTTP 429 | 16 (44,4%) |
| Espera média pelos cabeçalhos HTTP | 2,69 s por tentativa |
| Maior espera de retentativa registrada após 429 | 8,14 s |
| Últimos oito intervalos entre inícios HTTP | aproximadamente 30 s cada |
| Resposta recebida → término do INSERT bruto | média 0,049 s; máximo 0,097 s |
| Cabeçalhos recebidos → término do INSERT bruto | média 0,326 s; máximo 0,545 s |
| Atas novas/atualizadas na execução | 0 / 0; a descoberta ainda não havia terminado |

Os últimos oito intervalos medidos foram: 30,042; 29,881; 30,211; 29,797;
29,991; 30,047; 29,931; 30,190 segundos.

## Método e limites

- Consulta somente leitura ao banco efetivo do launcher, em
  `%LOCALAPPDATA%\LicitaLeads\data\licita_lead.db`, usando `CrawlRun` e `SourceRecord`.
- Logs estruturados do processo em
  `%TEMP%\LicitaLeads-continuous-20261027\launcher.log`, restritos ao período da execução.
- A média de cabeçalhos usa os 36 pares de início/término registrados pelo
  httpcore. Todos tinham término no intervalo analisado; a soma dos tempos foi
  96,67 s. A média independe da ordem de conclusão das chamadas simultâneas.
- Os INSERTs foram correlacionados pelo ID dos 20 registros de origem nos logs
  do aiosqlite. O intervalo até o término do INSERT inclui preparação e espera
  anteriores ao comando; não é uma medição completa do commit da transação.
- A soma de esperas programadas para retentativas foi 29,42 s. Esperas e chamadas
  simultâneas se sobrepõem: esses números não são uma divisão percentual do tempo real.
- Não houve medição da persistência final dos cadastros de atas, pois a execução
  foi cancelada antes dessa etapa. Nenhuma resposta ou dado cadastral foi alterado
  para produzir o diagnóstico.

## Correções decorrentes

- Distinguir 429 de chamadas antigas de um novo sinal de excesso de ritmo,
  evitando escaladas repetidas causadas por respostas que já estavam em trânsito.
- Acordar e reagendar reservas pendentes quando a política muda, inclusive na
  recuperação, sem antecipar uma chamada além do intervalo mínimo configurado.
- Manter `Retry-After`, o cancelamento, o teto de concorrência do operador e a
  recuperação gradual após respostas saudáveis.
- Registrar tempos de admissão, rede e gravação bruta, além das mudanças do
  intervalo, para permitir comparação nas próximas execuções sem estimativas.
