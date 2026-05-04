# Estratégia de Booking — Vooindo

## Objetivo

Encontrar o menor preço de **companhia aérea** abrindo cards de resultados no Google Flights e extraindo opções reais da página de booking.

> O preço real **não está no card da lista de resultados** — está na página de booking que se abre ao clicar no card. A página de booking mostra as opções de cada companhia/agência com seus preços reais.

---

## Fluxo Resumido

1. Escaneia a página de resultados do Google Flights
2. Extrai cards com preços visíveis (candidatos)
3. Filtra apenas cards que parecem ser de **companhias aéreas** (se `allow_agencies=False`)
4. Abre os bookings **do menor para o maior preço**
5. Em cada booking, extrai as opções de companhias aéreas e seus preços reais
6. Para quando encontra **2 opções de companhia aérea** ou esgota o limite

---

## Parâmetros (via env)

| Variável | Default | Descrição |
|----------|---------|-----------|
| `GOOGLE_FLIGHTS_MAX_CARDS` | 5 | Cards iniciais a tentar |
| `GOOGLE_FLIGHTS_MAX_CARDS_MAX` | 12 | Limite máximo de cards |
| `GOOGLE_FLIGHTS_MAX_CARDS_STEP` | 1 | Incremento quando expande |
| `GOOGLE_FLIGHTS_MIN_AIRLINE_PRICES_TO_COMPARE` | 2 | Quantas opções de companhia queremos para comparar |

---

## Estratégia Passo a Passo

### 1. Extrair candidatos da página

Varre a página de resultados com múltiplos seletores e coleta cards que contenham preços (R$, $, € etc.). Deduplica cards com mesmo texto+preço (±R$2 de tolerância). Ordena **do menor para o maior preço**.

### 2. Filtrar companhias aéreas

Se `allow_agencies=False` (padrão no scheduler), filtra apenas cards que *parecem* ser de companhia aérea (heurística no texto do card). Se não sobrar nenhum, usa todos.

### 3. Abrir bookings — estratégia adaptativa

```
start_cards = max(1, MAX_CARDS)        # normalmente 5
max_cards   = max(MAX_CARDS_MAX, 5)    # normalmente 12
step        = max(1, MAX_CARDS_STEP)   # normalmente 1
```

**Janela inicial:** abre os `start_cards` cards de menor preço.

**A cada janela:**
- Abre cada card via clique
- Espera a página de booking carregar
- Extrai opções (vendor, preço, tipo, link)
- Volta (`go_back`) para a página de resultados
- Acumula os preços encontrados em `found_airline_prices`

**Decisão de continuar ou parar:**

| Condição | Ação |
|----------|------|
| `found_airline_prices >= MIN_AIRLINE_PRICES_TO_COMPARE` (≥2) | ✅ **Para** — já temos preços para comparar |
| `processed_cards >= max_cards` (12) | ✅ **Para** — limite máximo atingido |
| É rota internacional e ainda não achou nenhum preço de companhia | 🔄 **Varre TODOS os cards** (sem limite) até achar ao menos 1 |
| Ainda não atingiu limite e não tem 2 preços | 🔄 **Avança +step cards** e continua |

### 4. Seleção do melhor resultado

Depois de parar:

- **Se encontrou ≥2 opções de companhia aérea:**
  Escolhe o **menor preço entre elas**. Se forem iguais, escolhe qualquer uma.

- **Se encontrou exatamente 1 opção de companhia aérea:**
  Usa essa mesmo — é o único disponível.

- **Se encontrou 0 opções de companhia aérea:**
  (Com `allow_agencies`) — usa a melhor agência como fallback.

- **Comparação companhia × agência** (apenas se `allow_agencies=True`):
  Escolhe o menor entre o melhor preço de companhia e o da agência.

> A extração do booking SEMPRE tem prioridade sobre o preço visível no card — o preço do card é apenas um indicador para ordenar os cliques, não o preço final.

---

## Tratamento de Fluxos Especiais

### Booking em 2 etapas
Algumas rotas abrem primeiro uma página de **detalhes do voo** (não o booking diretamente). O código detecta isso e clica em "Selecionar voo" / "Select flight" para avançar.

### Timeout na página de booking
Se o conteúdo do booking não carregar em 3s (`BOOKING_CONTENT_TIMEOUT_MS=3000`), volta e tenta o próximo card.

### Sem preço no card — refresh
Se após carregar a página de resultados não encontrar nenhum preço, faz até **3 refreshes** com reload da página antes de desistir.

---

## Exemplo Prático

Para uma rota PVH-FOR com 15 cards de companhias aéreas candidatos:

1. Abre cards 1 a 5 (menores preços)
2. Se em algum deles achar 2 opções de companhia aérea → **para aqui**, escolhe o menor
3. Se achar apenas 1 → abre os próximos: card 6, 7, 8... (1 a 1)
4. Se achar 0 → abre cards 1 a 5 → 6 → 7 → ... até 12
5. Se mesmo assim não achar → abre os próximos (sem limite pra internacional, ou até 12 pra doméstica)
6. Resultado final:
   - ≥2 opções → menor preço entre elas
   - 1 opção → usa essa
   - 0 opções → fallback agência (se permitido) ou nada
