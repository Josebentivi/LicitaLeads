# Segurança e privacidade

## Downloads de documentos

`app/services/ingestion/documents.py` aceita apenas HTTP(S) público:

- esquema HTTP(S), sem credenciais embutidas na URL;
- resolução DNS validada; endereços privados, loopback, link-local,
  multicast, reservados ou não especificados são rejeitados;
- redirects limitados, revalidando o destino;
- tamanho máximo por documento (`DOCUMENT_MAX_BYTES`, padrão 50 MB);
- tipo detectado pelo conteúdo (assinatura), não apenas pelo nome;
- gravação confinada ao diretório de documentos configurado.

A visualização (`GET /api/documents/{id}/view`) resolve o caminho e recusa
arquivos fora de `DOCUMENT_STORAGE_PATH` com `403`.

## Arquivos ZIP

`app/services/documents/security.py` protege a extração:

- nomes normalizados; caminhos absolutos, unidades Windows e `..` rejeitados
  (zip-slip);
- membros criptografados rejeitados;
- limite de membros (`ARCHIVE_MAX_MEMBERS`);
- limite de tamanho por membro e total descompactado (`ARCHIVE_MAX_BYTES`);
- razão de compressão máxima (zip-bomb);
- profundidade de arquivos aninhados limitada;
- tamanho declarado conferido após a leitura.

O exemplo permite dois níveis de ZIP, com orçamento cumulativo de membros e
bytes em toda a árvore. Auxiliares AppleDouble são ignorados somente após as
verificações de segurança; documentos reais continuam sujeitos à extração.
Respostas incompletas ou bloqueadas não autorizam retratação de fatos anteriores.

O resolvedor específico do PNCP pode reconstruir uma URL pública a partir da
identidade oficial armazenada. Ele não altera a URL original e a resolução,
cada redirecionamento e o download passam pelas mesmas validações de endereço,
DNS, conteúdo e limites. O endereço resolvido é registrado após sucesso.

## Interface web

- A UI não renderiza HTML bruto de fontes externas.
- Templates Jinja2 não expõem segredos; `/settings` usa `public_view()`.
- Erros de página retornam HTML amigável; erros de `/api` retornam JSON.
- O servidor vincula-se a `127.0.0.1` por padrão e o MVP não deve ser exposto
  publicamente sem autenticação.
- `ALLOWED_HOSTS` restringe hosts; formulários de alteração verificam a origem.
  Esses controles não substituem autenticação para exposição pública.

## Segredos e logs

- `LLM_API_KEY`, `CONTACT_SEARCH_API_KEY` e `TRANSPARENCIA_API_KEY` são aceitos do ambiente e nunca
  entram em logs estruturados ou na página de configurações.
- Nenhuma chave deve ser commitada; use `.env` (ignorado) e documente em
  `.env.example`.

## Outreach

- `OUTREACH_MODE=draft_only`: o serviço rejeita qualquer outro modo.
- O sistema **não envia mensagens**; rascunhos nascem `approved=false` e
  `sent=false`, e o banco impede `sent=true` sem `approved=true`
  (`sent_requires_approval`).
- A existência de um contato ou rascunho não autoriza uma abordagem. Avalie o
  [Provimento 205/2021 da OAB](https://www.oab.org.br/leisnormas/legislacao/provimentos/205-2021)
  e as orientações da seccional antes de qualquer comunicação profissional.

## Contatos e privacidade

- Apenas contatos explicitamente corporativos são aceitos na importação CSV.
- O enriquecimento visita somente domínios corporativos já evidenciados,
  respeita `robots.txt` e limita a 5 páginas e 2 MB por página.
- CPF não é usado para prospecção; CNPJ (numérico ou alfanumérico) é validado
  com os dígitos verificadores oficiais.
- Similaridade textual não funde empresas automaticamente.

## Execução

- Scheduler e API são processos separados; jobs usam leases no banco para
  evitar execução concorrente.
- Não há Docker nem Node no projeto; o script de verificação de ambiente falha
  se esses arquivos aparecerem.
