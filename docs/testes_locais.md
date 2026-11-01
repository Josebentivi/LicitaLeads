# Validação local e alcance dos testes

`/testes` mantém o diagnóstico rápido separado da suíte completa. A suíte roda
sequencialmente: descoberta, Ruff, formatação (somente verificação), mypy, testes
Python offline, processos API/scheduler, OCR português e navegador. O relatório
consolida cobertura de linhas e decisões, incluindo `app/jobs/scheduler.py`, com
mínimo global de 80%. Cobertura de código não comprova todos os comportamentos.

Cada execução guarda o hash dos arquivos de aplicação, testes, fixtures,
migrações e configurações versionadas relevantes, além das versões Python e
dependências. `.env`, banco, segredos, logs e relatórios ficam fora. Relatórios
anteriores sem identificação são históricos; alteração durante a execução ou
diferença posterior de código/ambiente impede sua classificação como atual.

Os arquivos `test-report-quick.json`, `test-report-full.json` e a pasta
`test-report-history` ficam ao lado de `test-report.json`, fora do versionamento.
As contagens de testes são independentes dos resultados de qualidade. Etapas
obrigatórias ausentes/ignoradas ficam pendentes; falha ou timeout reprova a
validação. Eventos parciais sobrevivem ao encerramento do subprocesso.

`TEST_SUITE_TIMEOUT_SECONDS=3600` limita a execução inteira (60 a 21600 segundos).
O executor renova uma lease exclusiva e encerra os descendentes no cancelamento,
timeout ou perda da lease. Todo subprocesso de teste usa banco migrado e
armazenamento temporários. LLM, contatos, OCR geral e contratos live ficam
desabilitados; o cenário OCR habilita somente o executável local e fontes
controladas são injetadas exclusivamente por `tests/support`.

## Preparação

Use Python 3.12 do launcher com dependências de desenvolvimento/OCR. O launcher
prepara o driver no fluxo de desenvolvimento. Para preparar explicitamente:

```powershell
python -m app.cli prepare-test-browser
```

O comando pode baixar o driver antes da suíte. Os testes usam Edge (ou Chrome),
driver em cache local e perfil temporário, em modo headless; não invocam Selenium
Manager para baixar componentes durante a execução. Atualizações incompatíveis
do navegador exigem nova preparação. `iniciar.bat --check` continua somente leitura.
OCR requer Tesseract e idioma `por`. Dependência ausente fica visível como pendência.

## Matriz de jornadas

| Área | Verificação local | Limite da comprovação |
|---|---|---|
| Atas | UF/unidade/fonte, envio, filtros persistidos, atualização HTMX e banco | Fontes simuladas; não confirma disponibilidade oficial |
| Coletas | Início, responsividade, cancelamento, continuação e retomada após reinício | Dados sintéticos e paginação controlada |
| Documentos | Retentativas, extração, evidência e histórico | Proteções e formatos adicionais cobertos por testes Python |
| IA | Cancelamento/retentativa pelos handlers, estados e demanda compartilhada | Provedor controlado; nenhuma chamada paga |
| Tela de testes | Relatório legado/atual, progresso, parcial e cancelamento | Executor pequeno evita recursão da suíte |
| Scheduler | Processo separado, pausa/retomada, disputa de lease e recuperação | Callback determinístico, sem coleta oficial |
| OCR | Palavras e identificadores de página sintética em português | Não mede qualidade em todo documento real |
| APIs/ações | Importação/consulta financeira, vínculo de empresa, validação, origem, recursos ausentes e concorrência | Sem autenticação remota: aplicação local usa política de Host/origem |
| Contratos offline | Códigos numéricos, vazios, duplicações, páginas incompletas e mudanças nos filhos | Formatos documentados com valores sintéticos |

Integrações oficiais continuam opcionais e duplamente habilitadas por marcador
`live` e `RUN_LIVE_CONTRACT_TESTS=true`. Fontes oficiais e serviços pagos devem ser
considerados **não verificados** quando não executados. Esta matriz descreve
alcance funcional e não afirma cobertura integral da plataforma.
