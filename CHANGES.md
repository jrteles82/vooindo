# Changelog

## [wygo-log] — 2026-04-22

### feat: 4 workers paralelos com perfis Chrome isolados (`151d9b7`)
- `run_all.py`: spawna `NUM_JOB_WORKERS` (padrão 4) instâncias de `job_worker.py`, cada uma com `GOOGLE_PERSISTENT_PROFILE_DIR=google_session_N`
- `.py`: passa `GOOGLE_PERSISTENT_PROFILE_DIR` para subprocesso executor; lock derivado do nome do diretório (`google_session_N.lock`) — workers não bloqueiam uns aos outros
- `google_flights_executor.py`: `SESSION_DIR` lê env var `GOOGLE_PERSISTENT_PROFILE_DIR`
- Perfis criados: `google_session_{1..4}` (~97MB cada, sem Cache/Code Cache)

### fix: run_all.py reinicia processos individualmente (`361ab98`)
- `signal.pause()` → `time.sleep(2)` polling loop
- `shutdown()` só chamado em crash loop (< `RESTART_GRACE_SECONDS`), não sempre
- Processos mortos são reiniciados individualmente sem derrubar a stack

### fix: recuperação de jobs travados 10→5 min (`13c46b3`)
- `recover_stale_jobs`: timeout de `running` reduzido de 10 para 5 minutos
- Adicionado `_send_links_message` em `job_worker.py` e `bot_scheduler.py` com fallback HTML→texto puro em `TelegramError`

### feat: cancelar job manual existente ao pedir novo (`8123c11`)
- Ao pedir nova consulta manual, cancela todos os jobs `pending`/`running` do usuário antes de enfileirar novo
- Antes: bloqueava até terminar

### feat: usuário bloqueado vê menu completo mas só pode usar "Fale conosco" e "Voltar" (`a2589c0` / `f2831d5`)
- Ações restritas → toast `🚫 Conta suspensa` + botão "Abrir menu principal"
- `support` e `back` passam normalmente

### fix: parsing de action em painel_callback (`b0a3fd8`)
- `parts[1]` → `':'.join(parts[1:])` — sub-ações como `usr:8070572579` passavam truncadas

### fix: removido clear_pending_input_ui (`70d0759`)
- `ReplyKeyboardMarkup` nunca usado no bot → função inútil causava flash de "." na tela

## [metro-airport-expansion] — 2026-05-14

### fix: expandir códigos metropolitanos e bloquear preço falso (`SAO`, `RIO`, `BHZ`)
- `main.py`: rotas com códigos metropolitanos agora testam aeroportos reais antes de escolher preço:
  - `SAO` → `GRU`, `CGH`, `VCP`
  - `RIO` → `GIG`, `SDU`
  - `BHZ` → `CNF`, `PLU`
- Mantém a rota original cadastrada na exibição (ex.: `PVH → SAO`), mas registra a variante escolhida em `notes` como `google_variant=...`.
- Bloqueia resultados não confiáveis vindos de página genérica/fallback bruto, especialmente:
  - `price_fallback_body_parse_min` com `click_candidates=0` / `clicked_result_tab=none`
  - `minimal_scraper_fallback + search_url_fallback` em `/search?q=`
- Corrige caso real em que `PVH → SAO` pegou preço falso de página genérica "Voos baratos para São Paulo" (ex.: `GIG → VCP` por R$316).
- Validação sem envio: usuário 2 `PVH → SAO 01/10/2026` retornou preço confiável `R$ 1.092` via variante `PVH → VCP`, com booking URL.
