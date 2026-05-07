# Limites do Servidor — Vooindo

## Especificações do Servidor

| Item | Valor |
|------|-------|
| vCPU | 2 |
| RAM  | 8 GB |
| Swap | 2 GB |
| SO   | Linux |

## Limites Máximos Comprovados (06/05/2026)

### 🔴 10 workers / Chrome 10 — ❌ SATURADO
- **Workers**: 8 schedule + 2 manual = 10 total
- **Chrome concurrency**: 10
- **Load average**: 7.71 (3.85x em 2 CPUs)
- **Memória**: 5.5G/7.9G usado + 1.5G swap
- **Resultado**: Round travou, múltiplos jobs órfãos, guardian reiniciando Chrome em loop
- **Veredito**: ❌ Inviável

### 🟡 8 workers / Chrome 8 — ⚠️ INSTÁVEL
- **Workers**: 6 schedule + 2 manual = 8 total
- **Chrome concurrency**: 8
- **Load average**: ~4.0-5.5
- **Memória**: ~3.0G usado, ok
- **Resultado**: Jobs zumbis (14/24 travados), guardian reiniciou Chrome no meio
- **Veredito**: ⚠️ Funciona parcialmente, mas arriscado

### 🟢 7 workers / Chrome 7 — ✅ ESTÁVEL (recomendado)
- **Workers**: 5 schedule + 2 manual = 7 total
- **Chrome concurrency**: 7
- **Load average**: 0.22 (ocioso) / ~3.0 (pico)
- **Memória**: ~2.8G usado, 3.3G livre (madrugada)
- **Resultado**: 141 jobs na madrugada, 0 erros, round em ~6-10 min
- **Veredito**: ✅ Ideal para este servidor

## Recomendações

| Parâmetro | Máximo seguro | Padrão atual |
|-----------|:------------:|:------------:|
| Job workers (schedule) | 5 | 5 |
| Job workers (manual) | 2 | 2 |
| Total workers | 7 | 7 |
| Chrome simultâneo | 7 | 7 |
| Timeout por job | 600s | 300-600s |
| Intervalo entre rodadas | — | 60 min |

## Como Alterar

### Workers
Editar `/opt/vooindo/run_all.py`, seção `children`:
```python
# Adicionar/remover linhas:
{'cmd': [py, str(BASE_DIR / 'job_worker.py'), '--pool', 'scheduled'], 'env': _worker_env(N)},
```

Cada worker precisa de um perfil Google dedicado: `google_session_N`

### Chrome Concurrency
Editar `/opt/vooindo/main.py`:
```python
_CHROME_MAX_CONCURRENT = 7  # máximo comprovado: 7
```

### Pós-alteração
```bash
sudo systemctl restart vooindo
```

## Bottleneck Real

O tempo de cada job NÃO é limitado por CPU, RAM, workers ou Chrome.
O gargalo real é o **Google Flights**:

| Etapa | Tempo típico |
|-------|:-----------:|
| Navegação até página de busca | ~3-5s |
| Aguardar resultados carregarem | ~3-9s |
| Expandir resultados (show more) | ~9-13s |
| Booking loop (2 cartões × ~12s) | ~24s |
| Extração de preço + URL | ~2-5s |
| **Total por job** | **~40-75s** (sem fila) |
| + Fila de semáforo (contenção) | +30-120s |
| **Total observado** | **~100-190s** |

### Por que aumentar workers não acelera?

Com 7 workers e Chrome 7, cada job leva ~40-75s de tempo real de Google Flights.
O restante (~60-115s) é espera no semáforo de Chrome — todos os workers disputam os mesmos 7 slots.

Aumentar workers de 7 → 10 **não reduz o tempo por job**, apenas aumenta a contenção:
- Mais workers disputando os mesmos 7 Chromes
- Mesmo tempo de espera no semáforo
- Mais CPU/RAM para gerenciar workers ociosos

### O que REALMENTE reduz o tempo?

As únicas otimizações que impactam:
1. **Booking loop**: processar menos cartões (já em 2, limite inferior)
2. **Timeout de booking**: reduzir de 12s para menos (risco de perder URL)
3. **skip_booking**: pula navegação dos cards (já desligado no scheduler — booking é obrigatório)
4. **Google Flights**: fora do nosso controle

### Conclusão

7 workers + Chrome 7 é o **ponto ótimo** para 2 vCPUs / 8GB RAM.
A curva de performance é plana acima disso — mais recursos do servidor não traduzem em scans mais rápidos.

## Nota
Os perfis de worker (google_session_N) são sincronizados automaticamente do profile base (`google_session/`) via `sync_current_worker_profile_from_base()` na inicialização de cada worker. Novos perfis são criados automaticamente se não existirem.

## Histórico de Correções

### 06/05/2026 — rc1 crashes (booking URL ausente em rotas lentas)

**Sintoma:** Algumas rotas (ex: FOR→PVH 16/06) retornavam sem URL de booking,
mostrando apenas `minimal_scraper_fallback` a partir da rodada das 13:00.

**Causa raiz:** O executor `google_flights_executor.py` roda como subprocesso
com um SIGALRM interno de 120s. O Chrome do guardian acumula memória ao longo
do dia (~5h+ uptime), deixando o Google Flights mais lento para responder.
Quando o executor não conclui em 120s, o SIGALRM mata o processo com exit code 1
sem stderr → `rc1`. O retry loop tenta 3x mas todas falham pelo mesmo motivo
→ fallback para `minimal_scraper` que só extrai preço (sem URL).

**Correções (commit a9ff5f1):**
1. **SIGALRM: 120s → 180s** — margem extra para rotas lentas com Chrome degradado
2. **Guardian auto-restart a cada 4h** — previne degradação do Chrome
   kill+restart preventivo para manter Chrome responsivo

### 06/05/2026 — Google Flights DOM change (booking URLs missing from ~13:00)

**Sintoma:** Todas as rotas pararam de extrair URL de booking a partir das 13:00.
Resultados mostravam `final_price_source=booking_agency_fallback` (sem URL).

**Causa raiz:** Google Flights mudou a estrutura do DOM — três problemas:

1. **Preços inline sumiram** — cards passaram a mostrar "R$ " sem valor.
   O filtro `parse_prices(txt)` eliminava todos os candidatos (0 cards).

2. **Clique JS no seletor errado** — `document.querySelector(sel)` sempre clicava
   no 1º elemento da página, nunca no N-ésimo card.

3. **Elementos stale após go_back()** — referências Playwright ficavam inválidas
   após navegar para booking e voltar.

**Correções (commits d104515 + 1eb31a0 + 7c714b7):**
1. **Filtro ampliado** — aceita cards com horário + cia aérea (sem exigir R$)
2. **`.click(force=True)` direto no elemento** — não usa `document.querySelector` global
3. **Re-find cards após go_back** — reconstroi lista de candidatos após navegação
4. **Booking loop deadline 20s → 60s** — margem para UI mais lenta do novo Google Flights
5. **DOM Detective** — script que roda a cada 2h, detecta mudanças no DOM e
   atualiza seletores automaticamente. Notifica admin via Telegram se todos falharem.

**Resultado:** URLs voltaram (~75-80% de taxa de sucesso vs 0% antes). O ~20% restante
é crash normal de executor (rc1) que já existia antes da mudança do DOM.

**Arquivos criados:**
- `dom_detective.py` — script de auto-detecção
- `systemd/dom-detective.service` + `systemd/dom-detective.timer` — execução a cada 2h
