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

## [route-cache-dedupe-test] — 2026-05-14

### test: cache/deduplicação por rota antes de abrir Chrome
- `job_worker.py`: adiciona tabela `scan_route_cache` com TTL padrão de 1h (`VOOINDO_ROUTE_CACHE_TTL_SECONDS`).
- Jobs per-route agora tentam reutilizar resultado recente por chave de rota/data/modo antes de abrir Google Flights.
- Se outra worker já estiver pesquisando a mesma rota, usa `GET_LOCK` por rota; workers duplicadas aguardam cache por até 420s (`VOOINDO_ROUTE_CACHE_WAIT_SECONDS`) antes de fazer busca própria.
- Cacheia apenas resultados com preço para não propagar falhas transitórias.
- Objetivo do teste: reduzir Chromes duplicados e tempo total sem aumentar workers/CPU.
- Checkpoint anterior seguro: `28fdfcb chore: checkpoint before route cache test`.

## [booking-link-travel-search-fallback] — 2026-05-14

### fix: não enviar `/travel/search?ts=...` como link de voo
- `main.py`: `build_booking_links_message()` agora rejeita links genéricos do Google Travel (`/travel/search?ts=...`) quando não são Google Flights.
- Se o scraper só trouxer esse link quebrado, o bot reconstrói um fallback seguro do Google Flights via `/travel/flights/search?q=ORIGEM to DESTINO DATA`.
- Caso real: usuário 11 recebeu `NAT → PVH 16/06/26` com `/travel/search?ts=...`; agora vira busca segura `NAT to PVH 2026-06-16 one way`.

## [enqueue-route-dedupe-test] — 2026-05-14

### test: deduplicar rotas antes de ocupar worker/Chrome
- `bot_scheduler.py`: na criação da rodada, rotas agendadas idênticas (`origin`, `destination`, `outbound_date`, `inbound_date`, modo scheduled sem agências) recebem a mesma `dedupe_key`.
- A primeira rota idêntica entra como `pending`; duplicadas entram como `waiting_route_dedupe`, não são capturadas por workers e não abrem Chrome.
- `job_worker.py`: quando o job primário finaliza com resultado, copia o resultado para os jobs `waiting_route_dedupe`, marca como `done` e dispara a consolidação do usuário duplicado.
- Se o primário falhar sem resultado, os duplicados são liberados para `pending` e rodam normalmente, evitando perda de busca.
- Validação unitária em banco com dry-run: job duplicado `PVH→SAO` foi finalizado sem Chrome via `[route-dedupe] ... resultado copiado`; consolidação DRY RUN sem envio.
- Checkpoint anterior: `0157e46 chore: checkpoint before enqueue dedupe test`.
