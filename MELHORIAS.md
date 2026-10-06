# Melhorias desta branch (`melhorias-newsbot`)

Resumo do que mudou em relação à `main`, por quê, e como conferir.

## O que foi corrigido

| # | Problema encontrado | O que mudou |
|---|---|---|
| 1 | O `INSERT` gravava as colunas na ordem errada (tipo na coluna da data, data de publicação na de envio, data de envio na do tipo). O **resumo diário** agrupava por data de envio e gerava ~146 linhas (>4096 caracteres), que o Telegram recusa. | `INSERT` com colunas nomeadas + **migração automática** que corrige as linhas já gravadas (idempotente). Resumo agora conta pelo dia de Brasília e ordena por quantidade. |
| 2 | **DOU**: 0 itens em 9 meses; id gerado com `hash()` (muda a cada execução, reenviaria tudo); resumo sem escape de HTML (o Telegram recusava a mensagem). | `hashlib.sha1` estável; leitura do JSON embutido na página (`script#params`) com fallback para os seletores antigos; escape do resumo; **alerta no Telegram (máx. 1x/dia)** se o site não responder ou mudar de layout; busca só nos horários de `dou.run_hours`. |
| 3 | Planilha aberta e salva **a cada notícia** (~11 s cada, 8 MB); banco de 32 MB commitado a cada rodada, perto de bater no limite de 100 MB do GitHub. | Planilha gravada **uma vez por rodada**; banco **podado** (`keep_days`, padrão 90) com `VACUUM`; planilha arquivada com data ao passar de 40 MB; uma conexão SQLite por execução. |
| 4 | Pauta/resumo dependiam de o minuto de início ser < 15 (atrasos do GitHub os faziam pular). `cancel-in-progress: true` cancelava rodadas no meio e gerava reenvios. | Modo escolhido por `github.event.schedule`; `cancel-in-progress: false`; estado salvo com `if: always()` e `pull --rebase` antes do push. |
| 5 | Busca por pedaço de palavra ("pix" pegava "pixel", "cac" pegava "cacique"); Zanatta podia ficar atrás de matérias genéricas e ser cortada pelo limite de 30. | Casamento por **palavra inteira** (use `*` no fim para variações, ex.: `tribut*`); nova lista `priority_keywords` (🚨, vai primeiro e **não conta** no limite); similaridade com pré-checagem rápida e janela de 24 h. |
| 6 | Resumo do RSS trazia HTML cru no "contexto". | HTML removido; estilo de mensagem opcional `whatsapp` (`settings.message_style`). |

Extras: feeds agora são baixados com `timeout` de 20 s (um feed travado não segura mais a rodada) e mensagens acima de 4096 caracteres são cortadas em vez de recusadas.

## Opções novas no `config.yaml`

```yaml
settings:
  keep_days: 90
  message_style: "completa"     # ou "whatsapp"
priority_keywords: [Julia Zanatta, Júlia Zanatta, Zanatta]
dou:
  run_hours: [7, 9, 12, 16, 19]
  alert_on_failure: true
```

O formato padrão das mensagens **não mudou**, exceto o 🚨 nos itens prioritários.

## Como testar com segurança

- As rodadas agendadas só rodam na branch principal, então esta branch não faz nada sozinha.
- Para testar **antes** de mesclar, rode o workflow manualmente nesta branch no modo `summary` (só lê o banco e manda o resumo). Cuidado: os modos `normal` e `dou` mandam mensagens de verdade e gravam o estado **na branch**, não na `main`.
- Depois de mesclar na `main`, a primeira rodada aplica a migração das colunas e a poda sozinha (o `sent_items.db` vai encolher).

## Para desfazer

Reverta o commit de merge, ou volte ao commit anterior pela página do repositório. O banco migrado continua válido para o código antigo, mas o código antigo voltaria a gravar com as colunas trocadas.

## Limites conhecidos

- A busca no DOU foi escrita contra o formato que o site costuma usar, mas **não pôde ser testada contra o site real** no ambiente em que foi desenvolvida. Se o layout for diferente, o alerta diário avisa.
- O repositório é público: o feed do Google Alerts e a lista de palavras ficam visíveis. Considere tornar o repositório privado (confira antes a cota de minutos grátis do seu plano).
