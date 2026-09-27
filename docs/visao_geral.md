# Visão geral

LicitaLead Monitor é uma aplicação **local e auditável** para monitorar
contratações públicas brasileiras, processar documentos oficiais e produzir
oportunidades de atendimento jurídico em modo **rascunho**. As fontes usadas são
o PNCP e os Dados Abertos do Compras.gov.br.

> MVP para apoio à análise. Uma estimativa de prazo não substitui a conferência
> do edital, da ata, da plataforma oficial e da legislação aplicável.

## Objetivo

Descobrir contratações publicadas, preservar os dados de origem, processar
documentos oficiais, identificar empresas e eventos somente quando houver
evidência, estimar prazos de modo conservador e produzir leads com rascunhos de
contato. O sistema **não envia mensagens**.

## Princípios

1. **Auditabilidade é o produto.** Cada campo relevante pode ser rastreado até o
   payload bruto (`SourceRecord`), a observação de campo (`FieldObservation`) ou
   o trecho de documento (`Evidence`).
2. **Nada é inventado.** Participantes, eventos, prazos e valores só existem se
   constarem de fonte oficial. Ausência de dado é representada por estado
   tipado (`unknown`, `not_published`, `not_supported` etc.) e `NULL`.
3. **Conservadorismo.** Na dúvida, o resultado fica `requires_manual_review`.
   Similaridade textual nunca unifica empresas ou processos automaticamente.
4. **Rascunho apenas.** `OUTREACH_MODE=draft_only`; o banco impede registrar
   envio sem aprovação (`sent_requires_approval`).
5. **Operação determinística.** OCR e LLM são integrações opcionais e
   desligadas por padrão.

## O que o sistema faz

- Coleta contratações, itens, documentos e resultados homologados no PNCP e no
  Compras.gov.br, guardando o payload bruto de cada resposta.
- Baixa documentos públicos com validações de segurança e extrai texto de PDF,
  HTML, DOCX, XLSX, CSV, TXT e ZIP.
- Detecta eventos (inabilitação, desclassificação, recursos, homologação etc.)
  por regras determinísticas sobre o texto extraído, com trecho e localizador.
- Calcula prazos com prioridade para prazo explícito ou extraído de documento;
  regras de dias úteis (edital ou estimativa) só quando seguras.
- Enriquece contatos corporativos a partir de domínio já evidenciado, com
  respeito a `robots.txt`.
- Pontua leads (0–100) com breakdown explicável e gera rascunhos de outreach.
- Expõe API REST, interface web em português, CLI e scheduler em processo
  separado.

## O que o sistema não faz

- Não envia e-mail, WhatsApp ou qualquer mensagem.
- Não infere a lista de participantes a partir do resultado homologado.
- Não usa CPF para prospecção ou enriquecimento.
- Não cria participantes perdedores, eventos ou prazos sem evidência.
- Não garante o prazo legal: estimativas exigem conferência humana.
- Não deve ser exposto publicamente sem camada de autenticação.

## Limitações conhecidas

- PNCP fornece contratações, itens, documentos e resultados publicados.
- Compras.gov.br fornece contratações, itens, resultados, fornecedores, atas e
  contratos, mas **não** documentos da sessão.
- Resultado homologado não equivale à lista de participantes.
- Inabilitação, desclassificação e recursos normalmente dependem de documentos.
- PDF escaneado é marcado `ocr_required` quando não há OCR configurado.
- LLM e busca de contatos ficam desligados por padrão.
- CNPJ numérico e alfanumérico são aceitos; CPF não é usado.

Consulte a [matriz de fontes](matriz_de_fontes.md) para o detalhamento por
endpoint e situação (`available`, `partial`, `document_only`, `not_supported`).
