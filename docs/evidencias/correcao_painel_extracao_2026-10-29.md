# Correção do painel e da extração documental — 29/10/2026

> **Registro histórico:** descreve o estado e os resultados da data indicada. Consulte a [documentação atual](../README.md).

Coleta de origem: `56a23fd5-b788-48e7-8a16-748a44659903`, encerrada com pendências
às 21:25 UTC (16:25 em America/Chicago). O histórico dessa execução permanece
inalterado. Os dois documentos classificados como formato não suportado não
fazem parte desta correção.

## Diagnóstico e mudanças

- O painel falhava com `TemplateNotFound` ao procurar `app/templates` por um
  caminho relativo. Templates e arquivos estáticos agora usam a localização
  absoluta do pacote, sem mudar o diretório global do processo.
- `ETP_PESQUISA_DE_PRECO_E_PLANILHA_DE_CONTRATACAO.zip` e
  `39303005900952025000` continham ZIPs internos, bloqueados pelo limite anterior.
  `ARCHIVE_MAX_DEPTH=2` permite o contêiner externo e um nível interno; valores
  diferentes de 1 ou 2 são rejeitados.
- `Sems__MA.zip` continha seis auxiliares AppleDouble. Nome e assinatura precisam
  coincidir para ignorá-los, depois das verificações de segurança. Arquivos reais
  sob `__MACOSX` continuam sendo examinados.
- Quantidade e tamanho usam um orçamento cumulativo em toda a árvore, incluindo
  contêineres Office e auxiliares ignorados. Continuam valendo os limites de
  tamanho individual, compressão, criptografia e caminhos.
- Os membros são persistidos recursivamente, com origem, hierarquia, localização
  e evidências. A detecção e a IA recebem apenas documentos finais. Membros não
  são baixados independentemente por URLs com fragmentos.
- Erros do worker trazem categoria, arquivo interno, hash de entrada quando
  disponível e classe da falha. Mensagens arbitrárias do parser ou stderr não
  são usadas como diagnóstico de exceções do worker; a captura de stderr é
  limitada a 8 KiB. O supervisor usa diretamente o interpretador real no Windows
  para preservar a propriedade dos processos filhos pelo Job Object.

## Escopo da tentativa corretiva

| Documento | ID | Falhas anteriores |
|---|---|---|
| ETP_PESQUISA_DE_PRECO_E_PLANILHA_DE_CONTRATACAO.zip | `b2ff533c-2587-4191-9baa-fc7fadd8ea96` | 4 |
| Sems__MA.zip | `88204bd8-d709-4602-9b56-7e36aac3d105` | 1 |
| 39303005900952025000 | `22a0a775-aee0-459b-9eeb-55c6c19f1783` | 1 |

A identificação persistente `zip-appledouble-20261029-v1` impede a criação
concorrente ou repetida da mesma correção. O lote guarda justificativa,
contadores e prazos anteriores; cada autorização de dispensa é consumida antes
do download, na transação protegida pelo fencing. Atividade, versão e limite de
falhas continuam sendo verificados. O processamento aceito mantém o comportamento
normal de zerar a sequência de falhas; nenhum contador é zerado para permitir
uma tentativa. Não foi acrescentado parâmetro público para ignorar a espera.

O teste prévio dos três conteúdos armazenados, em processos isolados e sem
gravação no banco de produção, extraiu respectivamente 17, 6 e 5 documentos finais.

## Validação e aplicação

As execuções offline usam bancos e armazenamento temporários com as migrações
aplicadas, serviços externos desabilitados e exclusão dos testes `live`.
Os artefatos desta validação ficam em
`%TEMP%\LicitaLeads-doc-fix-validation-svpfbd9x`.

- Suíte offline completa: **915 testes aprovados**, dois testes live excluídos.
  Um grupo inicialmente travou no harness; foi interrompido e repetido por
  completo, com seus 316 testes aprovados. Os registros de ambas as execuções
  foram preservados.
- Regressões específicas: **97 testes aprovados**. Após os últimos ajustes de
  supervisão e diagnóstico: **45 testes aprovados**, incluindo encerramento real
  de descendentes no Windows, falhas sanitizadas, segurança dos arquivos e
  correção documental. Há sobreposição entre esses grupos.
- Cobertura consolidada: **85,47%**, acima do mínimo de 80%. Os dados anteriores
  dos módulos alterados durante a validação foram substituídos pelas regressões
  executadas sobre suas versões finais.
- Ruff, `ruff format --check .` e mypy: aprovados (131 arquivos de aplicação
  verificados pelo mypy).
- Backup consistente com `integrity_check` e SHA-256 verificados:
  `%LOCALAPPDATA%\LicitaLeads\data\backups\20261029T224257Z-3ee7678f36ea45129c00bfc749f0482f.sqlite3`.
  SHA-256: `65d9606803a9786eca25ec34cd549cf1b78362affa71aab5bd7f9334a4ba8684`.
- Banco efetivo conferido em `%LOCALAPPDATA%\LicitaLeads\data\licita_lead.db`,
  revisão `0024_llm_queue`. Não há mudança de esquema nesta correção.
- Aplicação reiniciada por `iniciar.bat --no-browser`, após confirmar ausência
  de coletas e leases ativos. `/health`, `/crawls`, `/crawls/progress`,
  `/crawls/table`, `/static/app.css` e os documentos da coleta original retornaram
  HTTP 200 (entre 0,45 e 1,27 segundo).

## Resultado em produção

Execução corretiva: `b32cb22e-edd1-455a-9061-692a94cfcf17`, concluída às
22:51 UTC (17:51 em America/Chicago), com **3/3 arquivos processados e zero
falhas**. A execução original foi comparada integralmente com o backup e
permanece inalterada, assim como os dois documentos de formato não suportado.

| Arquivo | Documentos finais | Componentes persistidos, incluindo contêineres |
|---|---:|---:|
| ETP_PESQUISA_DE_PRECO_E_PLANILHA_DE_CONTRATACAO.zip | 17 | 20 |
| Sems__MA.zip | 6 | 7 |
| 39303005900952025000 | 5 | 7 |
| Total | **28** | **34** |

Todos os componentes estão ativos, com referência à resposta oficial de origem.
Foram preservadas as URLs originais e preenchidos os endereços públicos seguros
após downloads bem-sucedidos. As seis entradas AppleDouble ficaram registradas
como `appledouble_metadata_ignored: 6` no segundo arquivo.

As regras existentes produziram 121 eventos, 118 registros de participação e
118 leads em rascunho; as conclusões continuam vinculadas às evidências e às
regras de revisão humana. Foram enfileiradas 441 análises de IA para os documentos
finais. Isso não reativou o lote cancelado
`678d077b-2535-437e-80c4-628bbb28e9a9`: as **8.396 análises canceladas** permaneceram
nessa condição.

A dispensa de espera foi efetivamente utilizada apenas no primeiro arquivo,
cujo prazo original era 23:10 UTC. Nos outros dois, a espera já havia terminado;
os três consumos e os contadores anteriores constam no cursor do lote corretivo.
O scheduler foi restaurado ao estado anterior, ativo, após a conclusão.
