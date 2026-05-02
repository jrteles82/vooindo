#!/opt/vooindo/.venv/bin/python
from __future__ import annotations

import json
import os
import random
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote

from db import connect as connect_db, sql

os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(Path.home() / ".cache/ms-playwright"))

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright
    from playwright_stealth import Stealth
except ImportError as exc:
    print(json.dumps({"ok": False, "error": "missing_dependency", "message": str(exc)}, ensure_ascii=False))
    raise SystemExit(1)

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'

SESSION_DIR = Path(os.getenv("GOOGLE_PERSISTENT_PROFILE_DIR", str(Path(__file__).resolve().with_name("google_session"))))
BASE_URL = os.getenv("GOOGLE_FLIGHTS_BASE_URL", "https://www.google.com/travel/flights")
HL = os.getenv("GOOGLE_HL", "pt-BR")
GL = os.getenv("GOOGLE_GL", "BR")
CURR = "BRL"
HEADLESS = os.getenv("GOOGLE_FLIGHTS_EXECUTOR_HEADLESS", "1").strip().lower() in {"1", "true", "yes", "on"}
TIMEOUT_MS = int(os.getenv("GOOGLE_FLIGHTS_EXECUTOR_TIMEOUT_MS", os.getenv("GOOGLE_TIMEOUT_MS", "45000")))

# Se recebeu timeout reduzido via env, respeitar
if os.getenv("GOOGLE_FLIGHTS_SHORT_TIMEOUT"):
    TIMEOUT_MS = min(TIMEOUT_MS, 60000)
SLOW_MO = int(os.getenv("GOOGLE_FLIGHTS_EXECUTOR_SLOW_MO_MS", "125"))
BOOKING_CONTENT_TIMEOUT_MS = int(os.getenv("GOOGLE_FLIGHTS_BOOKING_CONTENT_TIMEOUT_MS", "3000"))
ALLOW_AGENCIES = os.getenv("GOOGLE_FLIGHTS_ALLOW_AGENCIES", "1").strip().lower() in {"1", "true", "yes", "on"}
SKIP_BOOKING = os.getenv("GOOGLE_FLIGHTS_SKIP_BOOKING", "0").strip().lower() in {"1", "true", "yes", "on"}
MAX_CARDS = int(os.getenv("GOOGLE_FLIGHTS_MAX_CARDS", "5"))
MAX_CARDS_MAX = int(os.getenv("GOOGLE_FLIGHTS_MAX_CARDS_MAX", "12"))
MAX_CARDS_STEP = int(os.getenv("GOOGLE_FLIGHTS_MAX_CARDS_STEP", "1"))
MIN_AIRLINE_PRICES_TO_COMPARE = int(os.getenv("GOOGLE_FLIGHTS_MIN_AIRLINE_PRICES_TO_COMPARE", "2"))


def configure_context_routing(context) -> None:
    def _handle_route(route):
        try:
            request = route.request
            resource_type = (request.resource_type or "").lower()
            url = (request.url or "").lower()
            blocked_resource_types = {"image", "media", "font"}
            blocked_url_terms = (
                "doubleclick",
                "google-analytics",
                "googletagmanager",
                "facebook",
                "hotjar",
                "segment",
                "analytics",
                "pixel",
            )
            if resource_type in blocked_resource_types or any(term in url for term in blocked_url_terms):
                route.abort()
                return
        except Exception:
            pass
        route.continue_()

    try:
        context.route("**/*", _handle_route)
    except Exception:
        pass

def build_url(origin: str, destination: str, outbound_date: str, inbound_date: str = "") -> str:
    trip = f"{origin} to {destination} {outbound_date} one way" if not inbound_date else f"{origin} to {destination} {outbound_date} return {inbound_date}"
    base = BASE_URL.rstrip("/")
    if base.endswith("/travel/flights"):
        base = f"{base}/search"
    elif "/travel/flights/search" not in base:
        base = "https://www.google.com/travel/flights/search"
    return f"{base}?q={quote(trip)}&hl={quote(HL)}&gl={quote(GL)}&curr={CURR}"


def parse_prices(text: str) -> list[float]:
    vals = []
    currency_pattern = r"(?:R\$|\$|€|ARS|USD|BRL|[\w]{3})\s*"
    price_val = r"([\d\.]+(?:,\d{2})?)"
    for raw in re.findall(rf"{currency_pattern}{price_val}", text or ""):
        try:
            # Remove pontos de milhar e converte vírgula decimal para ponto
            cleaned = raw.replace('.', '').replace(',', '.')
            vals.append(float(cleaned))
        except Exception:
            pass
    return vals


def parse_price(text: str) -> float | None:
    vals = parse_prices(text)
    return vals[0] if vals else None


def _valid_price(value) -> float | None:
    return float(value) if isinstance(value, (int, float)) and value >= 100 else None


def extract_section(text: str, start_label: str, end_label: str | None = None) -> str:
    if not text:
        return ""
    if end_label:
        m = re.search(rf"{re.escape(start_label)}([\s\S]*?){re.escape(end_label)}", text, flags=re.I)
    else:
        m = re.search(rf"{re.escape(start_label)}([\s\S]+)$", text, flags=re.I)
    return m.group(1) if m else ""


def human_pause(a: float = 0.35, b: float = 0.95) -> None:
    time.sleep(random.uniform(a, b))


def human_scroll(page, delta: int) -> None:
    try:
        page.mouse.wheel(random.randint(-40, 40), delta)
    except Exception:
        pass
    human_pause(0.45, 1.1)


def human_move(page) -> None:
    try:
        vp = page.viewport_size or {"width": 1280, "height": 900}
        page.mouse.move(random.randint(80, vp["width"] - 80), random.randint(120, vp["height"] - 120), steps=random.randint(8, 20))
    except Exception:
        pass


def check_session_health(page) -> dict:
    """Score 0-3. Score >= 1 = sessão válida. Verifica seletores de perfil, ausência de tela de login e presença de resultados."""
    score = 0
    indicators: dict = {}

    # Check 1: Resultados visíveis (se tem voo, a sessão permitiu a busca)
    result_indicators = [
        "Melhor opção", "Menores preços", "Principais voos", "Outros voos",
        "Best flights", "Top flights", "More flights"
    ]
    try:
        body_text = page.locator("body").inner_text(timeout=2000)
        if any(kw in body_text for kw in result_indicators):
            score += 2  # Se tem resultado, ignoramos quase todo o resto
            indicators["result_found"] = "ok"
    except Exception:
        pass

    # Check 2: seletores do perfil na página atual
    profile_selectors = [
        'a[aria-label*="Conta do Google"]',
        'a[aria-label*="Google Account"]',
        'img[alt*="Foto do perfil"]',
        'img[alt*="Profile picture"]',
        'a[aria-label*="jr"]',     # fallback: apelido do usuario
        '[data-ogsr-up]',
        'img[class*="gb"]',        # Google header avatar class
        'a[class*="gb_A"]',        # Google account button
        '[aria-haspopup] svg[aria-label]',  # generic account icon
        'a[href*="SignOutOptions"]',  # fallback: link de sign out = logado
        '[jsname*="account"]',
        'a[href*="https://myaccount.google"]',
    ]
    for sel in profile_selectors:
        try:
            if page.locator(sel).count() > 0:
                score += 1
                indicators["profile_selector"] = sel
                break
        except Exception:
            pass

    # Check 2: sem tela de login visível no body
    try:
        body = page.locator("body").inner_text(timeout=2000)
        low = (body or "").lower()
        if "fazer login" not in low and "entrar" not in low and "sign in" not in low:
            score += 1
            indicators["no_login_prompt"] = "ok"
        else:
            indicators["no_login_prompt"] = "login_prompt_detected"
    except Exception:
        pass

    return {"ok": score >= 1, "score": score, "indicators": indicators}


def is_authenticated_google_session(page) -> bool:
    """Compat wrapper — usa check_session_health internamente."""
    return check_session_health(page)["ok"]


def wait_for_results(page) -> float:
    started = time.perf_counter()
    try:
        page.wait_for_load_state("domcontentloaded", timeout=TIMEOUT_MS)
    except Exception:
        pass
    for locator in [
        page.get_by_text("Melhor opção", exact=False),
        page.get_by_text("Menores preços", exact=False),
        page.get_by_text("Principais voos", exact=False),
        page.get_by_text("Outros voos", exact=False),
        page.locator("[role='main'] [role='listitem']"),
    ]:
        try:
            locator.first.wait_for(timeout=5000)
            break
        except Exception:
            pass
    human_pause(1.2, 2.1)
    return round(time.perf_counter() - started, 3)


def expand_results(page, notes: list[str], is_international: bool = False) -> float:
    started = time.perf_counter()
    human_move(page)
    pre_scrolls = 6 if is_international else 4
    post_scrolls = 4 if is_international else 3
    for _ in range(pre_scrolls):
        human_scroll(page, random.randint(650, 980))
    clicked = False
    for label in ["Mostrar mais voos", "More flights"]:
        try:
            btn = page.get_by_text(label, exact=False)
            if btn.count() > 0:
                btn.first.scroll_into_view_if_needed(timeout=2500)
                human_pause(0.5, 1.0)
                btn.first.click(timeout=4000)
                notes.append("clicked_show_more=1")
                human_pause(1.5, 2.6)
                clicked = True
                break
        except Exception:
            pass
    if not clicked:
        notes.append("clicked_show_more=0")
    for _ in range(post_scrolls):
        human_scroll(page, random.randint(720, 1050))
    return round(time.perf_counter() - started, 3)


def extract_summary_price(body: str) -> float | None:
    # Suporta R$, $, €, ARS, etc. e códigos de moeda de 3 letras
    currency_pattern = r"(?:R\$|\$|€|ARS|USD|BRL|[\w]{3})\s*"
    price_val = r"([\d\.]+(?:,\d{2})?)"
    for label in ["Melhor opção", "Menores preços", "Principais voos"]:
        for pattern in [
            rf"{label}\s*a partir de\s*{currency_pattern}{price_val}",
            rf"{label}.*?{currency_pattern}{price_val}",
        ]:
            m = re.search(pattern, body or "", flags=re.I | re.S)
            if m:
                try:
                    raw = m.group(1).replace('.', '').replace(',', '.')
                    return float(raw)
                except Exception:
                    pass
    return None


def try_click_result_tab(page, notes: list[str]) -> float:
    started = time.perf_counter()
    label = "Melhor opção"
    for target in [
        lambda: page.get_by_text(label, exact=False),
        lambda: page.get_by_role("button", name=re.compile(label, re.I)),
        lambda: page.get_by_role("tab", name=re.compile(label, re.I)),
    ]:
        try:
            loc = target()
            if loc.count() > 0:
                loc.first.click(timeout=3500)
                notes.append("clicked_result_tab=best_option")
                human_pause(0.9, 1.8)
                return round(time.perf_counter() - started, 3)
        except Exception:
            pass
    notes.append("clicked_result_tab=none")
    return round(time.perf_counter() - started, 3)


def wait_for_booking_content(page, timeout_ms: int = 12000) -> bool:
    """Aguarda vendors de booking: 'Reserve com', 'Reservar com' ou 'Comprar com'.
    Não retorna True em 'Opções de reserva' (header carregado antes dos vendors)."""
    try:
        page.wait_for_function(
            """() => {
                const t = document.body ? document.body.innerText : '';
                return ['Reserve com','Reservar com','Comprar com']
                    .some(k => t.includes(k));
            }""",
            timeout=timeout_ms,
        )
        return True
    except Exception:
        pass
    return False


def wait_for_booking_options_stable(page, settle_timeout_ms: int = 3000) -> None:
    """Após wait_for_booking_content, aguarda a lista de opções estabilizar (todas renderizadas).
    Usa wait_for_function buscando >= 2 ocorrências de 'Reserve com'; timeout é best-effort."""
    try:
        page.wait_for_function(
            """() => (document.body ? document.body.innerText : '').split('Reserve com').length >= 3""",
            timeout=settle_timeout_ms,
        )
    except Exception:
        pass


def wait_for_booking(page) -> bool:
    if "/travel/flights/booking" in (page.url or ""):
        return wait_for_booking_content(page)
    # Painel lateral: verifica body por texto específico
    try:
        body = page.locator("body").inner_text(timeout=2000)
        if any(kw in (body or "") for kw in ["Reserve com", "Reservar com", "Comprar com", "Opções de reserva"]):
            if "Carregando" not in (body or ""):
                return True
    except Exception:
        pass
    return False


def is_details_panel_open(page) -> bool:
    """Detecta o painel intermediário de detalhes do voo (antes do booking)."""
    try:
        body = page.locator("body").inner_text(timeout=1200)
        return any(kw in (body or "") for kw in ["Selecionar voo", "Select flight"])
    except Exception:
        return False


def extract_continuar_link(page, vendor: str) -> str:
    """Tenta capturar o href do botão 'Continuar' do vendor na página de booking."""
    try:
        result = page.evaluate("""(vendor) => {
            const vendorLow = vendor.toLowerCase();
            const allLinks = Array.from(document.querySelectorAll('a[href]'));
            // Procura <a> com "Continuar" ou "Selecionar" que esteja dentro de um container com o nome do vendor
            for (const a of allLinks) {
                const txt = (a.textContent || '').trim().toLowerCase();
                if (txt !== 'continuar' && txt !== 'selecionar' && txt !== 'select') continue;
                const container = a.closest('[data-vendor], [jsname], li, [role="listitem"], div');
                if (container && container.textContent.toLowerCase().includes(vendorLow)) {
                    return a.href || '';
                }
            }
            // Fallback: qualquer <a> com "Continuar" na página
            for (const a of allLinks) {
                const txt = (a.textContent || '').trim().toLowerCase();
                if (txt === 'continuar' || txt === 'selecionar') {
                    return a.href || '';
                }
            }
            return '';
        }""", vendor)
        return str(result or "").strip()
    except Exception:
        return ""


def _extract_price_insight_from_body(body: str) -> str:
    """Extrai dicas de previsão de preço do body da página principal."""
    if not body:
        return ""
    body_clean = body.replace('\n', ' ')
    m_range = re.search(r"(Os voos mais baratos para viagens semelhantes.*?custam R\$\s*[\d\.]+\s*a\s*[\d\.]+\.)", body_clean, flags=re.I)
    m_normal = re.search(r"(O preço normal para.*?é R\$\s*[\d\.]+(?:,\d{2})?)", body_clean, flags=re.I)
    parts = []
    if m_normal:
        parts.append(m_normal.group(1).strip())
    if m_range:
        parts.append(m_range.group(1).strip())
    if parts:
        return " ".join(parts)
    m_fallback = re.search(r"(O preço normal para[\s\S]+?custam R\$[\s\S]+?[\d\.]+)", body_clean, flags=re.I)
    if m_fallback:
        return m_fallback.group(1).strip()
    return ""


def extract_booking_options(page, allow_agencies: bool = False) -> tuple[str, float | None, list[dict], str]:
    try:
        body = page.evaluate("document.body.innerText")
    except Exception:
        try:
            body = page.locator("body").inner_text(timeout=4000)
        except Exception:
            body = ""

    options: list[dict] = []
    lines = [ln.strip() for ln in (body or '').splitlines() if ln.strip()]

    # Extração de insights de preço (ex: "O preço normal para a Econômica é...")
    price_insight = ""
    # Remove quebras de linha para facilitar o regex
    body_clean = body.replace('\n', ' ')
    
    # Procura pela frase completa de comparação de preços, pegando até o ponto final apos a faixa de valores
    # Ex: "Os voos mais baratos para viagens semelhantes ... geralmente custam R$ 1.900 a 6.100."
    m_range = re.search(r"(Os voos mais baratos para viagens semelhantes.*?custam R\$\s*[\d\.]+\s*a\s*[\d\.]+\.)", body_clean, flags=re.I)
    
    m_normal = re.search(r"(O preço normal para.*?é R\$\s*[\d\.]+(?:,\d{2})?)", body_clean, flags=re.I)
    
    parts = []
    if m_normal:
        parts.append(m_normal.group(1).strip())
    if m_range:
        parts.append(m_range.group(1).strip())
        
    if parts:
        price_insight = " ".join(parts)
    else:
        # Fallback genérico se os padrões específicos falharem
        m_fallback = re.search(r"(O preço normal para[\s\S]+?custam R\$[\s\S]+?[\d\.]+)", body_clean, flags=re.I)
        if m_fallback:
            price_insight = m_fallback.group(1).strip()

    def _parse_price_near(start_i: int, window: int = 12) -> float | None:
        reserve_pattern = re.compile(r"(?:Reserve com|Reservar com|Comprar com|Vendido por)", re.I)
        installment_pattern = re.compile(r"\d+\s*x\s*(?:de\s*)?R\$", re.I)
        end = min(len(lines), start_i + window)
        for j in range(start_i, end):
            if j > start_i and reserve_pattern.search(lines[j]):
                break
            # Pula linhas que contêm texto de parcelamento ("10x de R$" ou "12x R$")
            if installment_pattern.search(lines[j]):
                continue
            p = re.search(r"R\$\s*([\d\.]+(?:,\d{2})?)", lines[j])
            if p:
                try:
                    price = float(p.group(1).replace('.', '').replace(',', '.'))
                    # Pula preços que parecem parcelas (< 300 reais)
                    if price < 300:
                        continue
                    return price
                except Exception:
                    pass
        return None

    seen: set[tuple] = set()

    def _add(vendor: str, price: float, is_airline: bool) -> None:
        # Limpeza agressiva do nome do vendor
        vendor = re.sub(r"Companhia[ns]?\s+a[ée]rea[ns]?", "", vendor, flags=re.I).strip(" :-\n")
        if not vendor or price <= 0:
            return
        key = (vendor.lower()[:40], round(price), is_airline)
        if key not in seen:
            seen.add(key)
            options.append({"vendor": vendor, "price": price, "is_airline": is_airline})

    for i, line in enumerate(lines):
        # Método 1: "Reserve com [a] Vendor"
        m = re.search(
            r"(?:Reserve com(?: a)?|Reservar com(?: a)?|Comprar com(?: a)?|Vendido por)\s*(.+?)(?:\s*Companhia[ns]?\s+a[ée]rea[ns]?|$)",
            line, re.I
        )
        if m:
            vendor = re.sub(r"Companhia[ns]?\s+a[ée]rea[ns]?\s*$", "", m.group(1).strip(), flags=re.I).strip()
            is_airline = bool(re.search(r"Companhia[ns]?\s+a[ée]rea[ns]?", line, re.I))
            if not is_airline and i + 1 < len(lines):
                is_airline = bool(re.search(r"Companhia[ns]?\s+a[ée]rea[ns]?", lines[i + 1], re.I))
            if not is_airline:
                is_airline = is_probable_airline_vendor(vendor)
            price = _parse_price_near(i)
            if price:
                _add(vendor, price, is_airline)
            continue

        if re.search(r"Companhia[ns]?\s+a[ée]rea[ns]?", line, re.I):
            m2 = re.search(r"^(.+?)Companhia[ns]?\s+a[ée]rea[ns]?", line, re.I)
            vendor = m2.group(1).strip() if m2 else (lines[i - 1] if i > 0 else "")
            if vendor:
                price = _parse_price_near(i)
                if price:
                    _add(vendor, price, True)

    if not options:
        return "", parse_price(body), [], price_insight

    airline_opts = [o for o in options if o["is_airline"]]
    pool = options if allow_agencies else airline_opts
    if not pool:
        return "", None, options, price_insight
    best = min(pool, key=lambda o: o["price"])
    return best["vendor"], best["price"], options, price_insight


def load_active_airlines() -> list[tuple[str, str]]:
    aliases: list[tuple[str, str]] = []
    try:
        conn = connect_db()
        try:
            rows = conn.execute(sql("SELECT iata_code, name FROM airlines WHERE is_active = 1 ORDER BY iata_code")).fetchall()
            for row in rows:
                iata = str((row.get('iata_code') if isinstance(row, dict) else row['iata_code']) or '').strip().upper()
                name = str((row.get('name') if isinstance(row, dict) else row['name']) or '').strip()
                if iata and name:
                    aliases.append((iata, name))
        finally:
            conn.close()
    except Exception:
        pass
    return aliases


def airline_alias_tokens() -> list[tuple[str, str]]:
    rows = load_active_airlines()
    tokens: list[tuple[str, str]] = []
    for _iata, name in rows:
        canonical = name
        low = name.lower().strip()
        tokens.append((low, canonical))
        if 'latam' in low:
            tokens.append(('latam', canonical))
        if 'aerolineas' in low or 'aerolíneas' in low:
            tokens.append(('aerolineas argentinas', canonical))
            tokens.append(('aerolíneas argentinas', canonical))
        if 'gol' in low:
            tokens.append(('gol', canonical))
        if 'azul' in low:
            tokens.append(('azul', canonical))
        if 'avianca' in low:
            tokens.append(('avianca', canonical))
        if 'jetsmart' in low:
            tokens.append(('jetsmart', canonical))
        if 'flybondi' in low:
            tokens.append(('flybondi', canonical))
        if 'arajet' in low:
            tokens.append(('arajet', canonical))
    seen = set()
    deduped: list[tuple[str, str]] = []
    for token, canonical in tokens:
        key = (token, canonical)
        if token and key not in seen:
            seen.add(key)
            deduped.append((token, canonical))
    return deduped


_AIRLINE_TOKENS = airline_alias_tokens()

# Fallback para companhias internacionais comuns não cobertas pelo DB
_INTL_AIRLINE_KEYWORDS: frozenset[str] = frozenset([
    'american', 'united', 'delta', 'copa', 'air france', 'iberia', 'tap',
    'aeromexico', 'turkish', 'emirates', 'air canada', 'british airways',
    'lufthansa', 'klm', 'swiss', 'qatar', 'ethiopian', 'alitalia',
    'norwegian', 'spirit', 'frontier', 'jetblue', 'southwest',
    'gol', 'latam', 'azul', 'avianca', 'voepass', 'passaredo',
    'arajet', 'jetsmart', 'flybondi', 'aerolineas',
])


def load_br_airports() -> set[str]:
    codes: set[str] = set()
    try:
        conn = connect_db()
        try:
            rows = conn.execute(sql("SELECT code FROM airports WHERE name LIKE ?"), ('%Brasil%',)).fetchall()
            for row in rows:
                code = str((row.get('code') if isinstance(row, dict) else row['code']) or '').strip().upper()
                if code:
                    codes.add(code)
        finally:
            conn.close()
    except Exception:
        pass
    return codes


_BR_AIRPORTS: set[str] = load_br_airports()


def is_international_route(origin: str, dest: str) -> bool:
    o = origin.strip().upper()
    d = dest.strip().upper()
    return o not in _BR_AIRPORTS or d not in _BR_AIRPORTS


def extract_vendor_from_body(body: str) -> str:
    """Extrai o nome da companhia aérea do texto do card principal do Google Flights.
    Procura por padrões como:
        R$ 1.234
        LATAM
        Companhia aérea

    Ou texto concatenado:
        Aerolineas ArgentinasCompanhia aérea
        GolCompanhia
    """
    lines = [ln.strip() for ln in (body or '').splitlines() if ln.strip()]
    for i, line in enumerate(lines):
        if re.search(r'Companhia[ns]?\s+a[ée]rea[ns]?', line, re.I):
            # Tenta extrair da própria linha (antes de "Companhia")
            before = re.sub(r'Companhia[ns]?\s+a[ée]rea[ns]?.*', '', line, flags=re.I).strip()
            before = re.sub(r'R\$[\s\d.,]+', '', before).strip()
            if before and is_probable_airline_vendor(before):
                return before
            # Fallback: linha anterior
            candidate = lines[i - 1] if i > 0 else ''
            if candidate:
                candidate = re.sub(r'R\$[\s\d.,]+', '', candidate).strip()
                candidate = re.sub(r'Companhia[ns]?\s+a[ée]rea[ns]?.*', '', candidate, flags=re.I).strip()
                if candidate and is_probable_airline_vendor(candidate):
                    return candidate
        # Padrão: nome em linha curta, logo acima de "Companhia" na linha seguinte
        if i + 1 < len(lines) and re.search(r'Companhia[ns]?\s+a[ée]rea[ns]?', lines[i + 1], re.I):
            candidate = line.strip()
            candidate = re.sub(r'R\$[\s\d.,]+', '', candidate).strip()
            candidate = re.sub(r'Companhia[ns]?\s+a[ée]rea[ns]?.*', '', candidate, flags=re.I).strip()
            if candidate and is_probable_airline_vendor(candidate):
                return candidate
    # Fallback final: qualquer linha com nome de companhia antes de "Companhia"
    for line in lines:
        # Procura padrão onde "Companhia" vem grudado no nome (pode ser multi-palavra)
        # Ex: "Aerolineas ArgentinasCompanhia aérea"
        m = re.search(r'((?:[A-Z][a-zA-ZÀ-ÿ]+\s+)*[A-Z][a-zA-ZÀ-ÿ]+)Companhia[ns]?\s+a?', line)
        if m:
            candidate = m.group(1).strip()
            if is_probable_airline_vendor(candidate):
                return candidate
        # Fallback mais agressivo: tudo antes de "Companhia"
        m = re.search(r'(.+?)Companhia[ns]?\s+a?', line)
        if m:
            candidate = m.group(1).strip()
            # Remove preço se tiver na mesma linha
            candidate = re.sub(r'R\$[\s\d.,]+', '', candidate).strip()
            if candidate and is_probable_airline_vendor(candidate):
                return candidate

    # Fallback extra: procurar nomes de companhias no texto SEM precisar de "Companhia"
    # Isso captura cards que só mostram "LATAM" ou "Gol" na linha do preço
    for i, line in enumerate(lines):
        has_price = bool(re.search(r'R\$[\s\d.,]+', line))
        low = line.lower()
        best_match = None
        best_match_len = 999
        for token, canonical in _AIRLINE_TOKENS:
            if token in low:
                # Pega o token mais curto que casa (evita "latam airlines brasil" quando "latam" serve)
                if len(token) < best_match_len:
                    best_match = token
                    best_match_len = len(token)
        if best_match:
            short_name = best_match.upper() if len(best_match) <= 6 else best_match.title()
            if has_price:
                return short_name
            # Busca preço num raio maior (o card pode ter texto quebrado em várias linhas)
            for j in range(1, 15):
                if i + j < len(lines) and bool(re.search(r'R\$[\s\d.,]+', lines[i + j])):
                    return short_name
            for j in range(1, 10):
                if i - j >= 0 and bool(re.search(r'R\$[\s\d.,]+', lines[i - j])):
                    return short_name
    
    # Último recurso: procurar nome de companhia perto de "Companhias aéreas" no plural
    for i, line in enumerate(lines):
        if 'companhia' in line.lower() or 'companhias' in line.lower():
            # Pega o que vem depois de qualquer texto que não seja o nome
            before = re.sub(r'Companhi[as].*', '', line, flags=re.I).strip()
            before = re.sub(r'R\$[\s\d.,]+', '', before).strip()
            # Remove números, símbolos
            before = re.sub(r'[.,\-]+', ' ', before).strip()
            # Pega a última palavra (provavelmente o nome da cia)
            words = before.split()
            for w in reversed(words):
                if len(w) >= 3 and w[0].isupper():
                    for token, canonical in _AIRLINE_TOKENS:
                        if token in w.lower():
                            return canonical
                    if w.lower() in _INTL_AIRLINE_KEYWORDS:
                        return w.title()
    return ''


def is_probable_airline_vendor(vendor: str) -> bool:
    low = (vendor or '').strip().lower()
    if not low:
        return False
    if any(token in low for token, _canonical in _AIRLINE_TOKENS):
        return True
    return any(kw in low for kw in _INTL_AIRLINE_KEYWORDS)


def _card_looks_like_airline(txt: str) -> bool:
    low = txt.lower()
    return any(token in low for token, _canonical in _AIRLINE_TOKENS)


def _vendor_from_card_text(txt: str) -> str:
    """Extrai nome da companhia aérea do texto do card. Retorna '' se não reconhecido."""
    low = txt.lower()
    for token, canonical in _AIRLINE_TOKENS:
        if token in low:
            return canonical
    # Fallback para keywords conhecidas se não achou token exato
    for kw in _INTL_AIRLINE_KEYWORDS:
        if kw in low:
            return kw.title()
    return ''


def maybe_open_booking(page, summary_price: float | None, notes: list[str], allow_agencies: bool = False, is_international: bool = False) -> tuple[bool, str, float | None, float | None, list[dict], str, tuple[str, float, float | None, list[dict], str] | None, tuple[str, float, float | None, list[dict], str] | None, str]:
    booking_started = time.perf_counter()
    # Seletores semânticos estáveis — sem classes obfuscadas que mudam a cada deploy do Google
    candidate_locators = [
        "[role='main'] [role='listitem']",
        "[role='main'] li",
        "[role='main'] [role='link']",
        "[role='listitem']",
        "div[data-ved]",
    ]
    raw_candidates: list[tuple[float, object, str, str]] = []
    seen: set[tuple[str, float]] = set()
    for selector in candidate_locators:
        try:
            cards = page.locator(selector)
            count = min(cards.count(), 60)
        except Exception:
            count = 0
            cards = None
        for i in range(count):
            card = cards.nth(i)
            try:
                txt = card.inner_text(timeout=1000).strip()
            except Exception:
                continue
            # Suporta R$, $, €, ARS, etc.
            if not any(curr in txt for curr in ["R$", "$", "€", "ARS", "BRL", "USD"]):
                continue
            prices = [p for p in parse_prices(txt) if p >= 300]
            if not prices:
                continue
            price = min(prices)
            # dedup por conteúdo + preço (independente do seletor)
            key = (txt[:220], round(price, 2))
            if key in seen:
                continue
            seen.add(key)
            raw_candidates.append((price, card, txt, selector))

    # Ordena por preço crescente e deduplica por faixa de preço (±2 BRL)
    # Colapsa variações do mesmo voo com preço idêntico mas horários diferentes,
    # sem eliminar voos distintos com preços próximos (ex: Gol 1970 e Aerolineas 1977)
    candidates_sorted = sorted(raw_candidates, key=lambda x: x[0])
    candidates: list[tuple[float, object, str, str]] = []
    last_price: float | None = None
    for cand in candidates_sorted:
        if last_price is None or (cand[0] - last_price) > 2:
            candidates.append(cand)
            last_price = cand[0]
    start_cards = max(1, MAX_CARDS)
    max_cards_limit = max(start_cards, MAX_CARDS_MAX)
    step_cards = max(1, MAX_CARDS_STEP)

    # Sempre abre bookings — preço do card pode não ser o menor do booking
    airline_candidates = candidates if allow_agencies else [c for c in candidates if _card_looks_like_airline(c[2])]
    if not airline_candidates:
        airline_candidates = candidates
    notes.append(f"click_candidates={len(raw_candidates)} deduped={len(candidates)} airline_candidates={len(airline_candidates)} start={start_cards} step={step_cards} max={max_cards_limit} intl={is_international}")

    # tupla: (vendor, booking_price, visible_card_price, options, booking_url)
    best_airline: tuple[str, float, float | None, list[dict], str] | None = None
    best_agency: tuple[str, float, float | None, list[dict], str] | None = None
    first_agency_fallback: tuple[str, float, str] | None = None
    found_airline_prices: dict[tuple[str, float], tuple[str, float, float | None, list[dict], str]] = {}
    final_price_insight = ""

    def _try_go_back() -> float:
        started = time.perf_counter()
        try:
            page.go_back(wait_until='domcontentloaded')
            for kw in ["Principais voos", "Outros voos", "Melhor opção", "Menores preços"]:
                try:
                    page.get_by_text(kw, exact=False).first.wait_for(timeout=2000)
                    break
                except Exception:
                    pass
            human_pause(0.1, 0.3)
        except Exception:
            human_pause(0.2, 0.4)
        return round(time.perf_counter() - started, 3)

    def _try_click_selecionar_voo() -> bool:
        """Clica em 'Selecionar voo' se estiver na etapa intermediária de detalhes do voo."""
        for label in ["Selecionar voo", "Select flight"]:
            for role in ["button", "link"]:
                try:
                    loc = page.get_by_role(role, name=re.compile(label, re.I))
                    if loc.count() > 0:
                        loc.first.click(timeout=2500)
                        human_pause(0.2, 0.4)
                        return True
                except Exception:
                    pass
        return False

    def _extract_booking_with_two_step(idx: int, card_price: float, page_booking_url: str = "") -> bool:
        """Extrai opções de booking, lidando com fluxo em 2 etapas (detalhes → booking).
        Continua varrendo todos os cards para garantir prioridade absoluta a qualquer companhia aérea encontrada."""
        nonlocal best_airline, best_agency, first_agency_fallback, final_price_insight
        
        # Espera o texto de insights de preço se não tiver capturado
        if not final_price_insight:
            try:
                page.wait_for_selector("text=O preço normal para", timeout=4500)
            except Exception:
                pass

        vendor, vendor_price, options, insight = extract_booking_options(page, allow_agencies=True)
        if insight:
            final_price_insight = insight
            
        if not vendor:
            # Possivelmente na etapa intermediária de detalhes — tenta clicar "Selecionar voo"
            if _try_click_selecionar_voo():
                notes.append(f"clicked_selecionar_voo_card_{idx}")
                vendor, vendor_price, options, insight = extract_booking_options(page, allow_agencies=True)
                if insight: final_price_insight = insight
                
        booking_price = _valid_price(vendor_price) or _valid_price(card_price)
        _n_airline = sum(1 for o in options if o.get('is_airline'))
        _n_agency = sum(1 for o in options if not o.get('is_airline'))
        if options:
            notes.append(f"options_card_{idx}=airline:{_n_airline} agency:{_n_agency} vendors:{[o['vendor'] for o in options[:6]]}")

        current_booking_url = page_booking_url or page.url or ""
        airline_options = [o for o in options if o.get('is_airline')]
        best_airline_in_booking = None
        if airline_options:
            best_airline_in_booking = min(airline_options, key=lambda o: float(o['price']))
            vendor_link = extract_continuar_link(page, best_airline_in_booking['vendor']) or current_booking_url
            key = (str(best_airline_in_booking['vendor']), float(best_airline_in_booking['price']))
            found_airline_prices[key] = (
                best_airline_in_booking['vendor'],
                best_airline_in_booking['price'],
                card_price,
                options,
                vendor_link,
            )
            if best_airline is None or float(best_airline_in_booking['price']) < best_airline[1]:
                notes.append(
                    f"booking_airline_found_card_{idx}={best_airline_in_booking['vendor']} booking={best_airline_in_booking['price']} card={card_price}"
                )
                best_airline = (
                    best_airline_in_booking['vendor'],
                    float(best_airline_in_booking['price']),
                    card_price,
                    options,
                    vendor_link,
                )

        # Atualiza melhor agência
        for ao in [o for o in options if not o.get('is_airline')]:
            vendor_link = extract_continuar_link(page, ao['vendor']) or current_booking_url
            if first_agency_fallback is None:
                first_agency_fallback = (str(ao['vendor']), float(ao['price']), vendor_link)
            if best_agency is None or ao['price'] < best_agency[1]:
                notes.append(f"booking_agency_found_card_{idx}={ao['vendor']} booking={ao['price']} card={card_price}")
                best_agency = (ao['vendor'], ao['price'], card_price, options, vendor_link)

        go_back_s = _try_go_back()
        return False  # sempre continua — varre todos os cards

    def _effective_max() -> int:
        if is_international and not found_airline_prices:
            return len(airline_candidates)
        return max_cards_limit

    processed_cards = 0
    current_limit = min(len(airline_candidates), start_cards)
    while processed_cards < min(len(airline_candidates), _effective_max()):
        window_end = min(len(airline_candidates), current_limit)
        for idx in range(processed_cards + 1, window_end + 1):
            price, card, txt, selector_used = airline_candidates[idx - 1]
            try:
                card.scroll_into_view_if_needed(timeout=1500)
            except Exception: pass
            
            click_targets = [(card, selector_used)]
            for sel in ["div.JMc5Xc[role='link']", "[jsaction*='click:O1htCb']"]:
                try:
                    loc = card.locator(sel)
                    if loc.count() > 0 and sel != selector_used:
                        click_targets.append((loc.first, sel))
                except Exception: pass

            for target, target_name in click_targets:
                try:
                    target.dispatch_event('click')
                    human_pause(0.2, 0.4)
                    current_url = page.url or ""
                    if "/travel/flights/booking" in current_url:
                        if wait_for_booking_content(page, timeout_ms=BOOKING_CONTENT_TIMEOUT_MS):
                            wait_for_booking_options_stable(page)
                            _extract_booking_with_two_step(idx, price, page_booking_url=current_url)
                            break
                        else:
                            _try_go_back()
                            break
                    elif wait_for_booking(page):
                        _extract_booking_with_two_step(idx, price)
                        break
                    elif is_details_panel_open(page):
                        if _try_click_selecionar_voo():
                            if wait_for_booking(page):
                                _extract_booking_with_two_step(idx, price)
                                break
                        _try_go_back()
                        break
                except Exception: pass

        processed_cards = window_end
        if is_international and not found_airline_prices and processed_cards < len(airline_candidates):
            current_limit = min(len(airline_candidates), processed_cards + step_cards)
            continue
        if len(found_airline_prices) >= max(1, MIN_AIRLINE_PRICES_TO_COMPARE):
            break
        if processed_cards >= min(len(airline_candidates), _effective_max()):
            break
        current_limit = min(len(airline_candidates), min(_effective_max(), processed_cards + step_cards))

    res_best = None
    if allow_agencies:
        if is_international:
            res_best = best_airline if best_airline else best_agency
        else:
            if best_airline and best_agency:
                res_best = best_airline if best_airline[1] <= best_agency[1] else best_agency
            else:
                res_best = best_airline or best_agency
    else:
        res_best = best_airline

    if res_best:
        v, p, vp, opts, url = res_best
        return True, v, p, vp, opts, url, best_airline, best_agency, final_price_insight
    
    return False, "", None, None, [], "", best_airline, best_agency, final_price_insight


def run(origin: str, destination: str, outbound_date: str, inbound_date: str = "") -> dict:
    notes: list[str] = []
    url = build_url(origin, destination, outbound_date, inbound_date)
    with sync_playwright() as p:
        proxy_settings = {}
        proxy_url = os.getenv('GOOGLE_FLIGHTS_PROXY')
        if proxy_url:
            proxy_settings = {'server': proxy_url}
            proxy_user = os.getenv('GOOGLE_FLIGHTS_PROXY_USER')
            proxy_pass = os.getenv('GOOGLE_FLIGHTS_PROXY_PASS')
            if proxy_user and proxy_pass:
                proxy_settings['username'] = proxy_user
                proxy_settings['password'] = proxy_pass
        # Apenas Chrome (Firefox removido — instável com recursos do VPS)
        context = p.chromium.launch_persistent_context(
            str(SESSION_DIR),
            headless=HEADLESS,
            slow_mo=SLOW_MO,
                locale="pt-BR",
                user_agent=USER_AGENT,
                proxy=proxy_settings if proxy_settings else None,
                viewport={"width": 1280, "height": 900},
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-infobars",
                    "--ignore-certifcate-errors",
                    "--remote-debugging-port=0",
                    "--disable-extensions",
                    "--disable-component-extensions-with-background-pages",
                    "--disable-software-rasterizer",
                    "--single-process",
                    "--disable-crashpad",
                    "--disable-features=Translate,OptimizationHints,MediaRouter,DialMediaRouteProvider",
                ],
            )
        configure_context_routing(context)
        page = context.pages[0] if context.pages else context.new_page()
        Stealth().apply_stealth_sync(page)
        page.set_default_timeout(TIMEOUT_MS)
        try:
            nav_started = time.perf_counter()
            page.goto("https://www.google.com/", wait_until="domcontentloaded")
            notes.append(f"home_nav_s={round(time.perf_counter() - nav_started, 3)}")
            human_pause(1.0, 1.8)
            health = check_session_health(page)
            if not health["ok"]:
                return {
                    "ok": False,
                    "error": "google_auth_required",
                    "message": f"Sessão Google inválida (score={health['score']}/3)",
                    "health": health,
                    "notes": [f"auth_score={health['score']}", "auth_probe=google_home"],
                }
            # Retry automático se auth_score < 1 (sessão completamente inválida)
            # NOTA: Não limpa cookies se score >= 1 (tem sessão parcial mas sem avatar visível)
            # Limpar cookies quebra sessões válidas de navegadores headless sem avatar
            if health['score'] < 1:
                human_pause(0.5, 1.0)
                context.clear_cookies()
                time.sleep(1)
                page.goto("https://accounts.google.com/signin", wait_until="domcontentloaded")
                human_pause(1.0, 2.0)
                # Apos limpar cookies, volta pro Google.com e verifica de novo
                page.goto("https://www.google.com/", wait_until="domcontentloaded")
                human_pause(1.0, 1.5)
                health = check_session_health(page)
                if not health["ok"]:
                    return {
                        "ok": False,
                        "error": "google_auth_required",
                        "message": f"Sessão Google inválida (score={health['score']}/3) após clear_cookies",
                        "health": health,
                        "notes": [f"auth_score={health['score']}", "auth_probe=google_home_retry"],
                    }
                notes.append(f"auth_score_retry={health['score']}")
            notes.append(f"auth_score={health['score']}")
            search_nav_started = time.perf_counter()
            page.goto(url, wait_until="domcontentloaded")
            notes.append(f"search_nav_s={round(time.perf_counter() - search_nav_started, 3)}")
            notes.append(f"wait_results_s={wait_for_results(page)}")
            notes.append(f"click_result_tab_s={try_click_result_tab(page, notes)}")
            is_intl = is_international_route(origin, destination)
            notes.append(f"route_international={is_intl}")
            notes.append(f"expand_results_s={expand_results(page, notes, is_international=is_intl)}")
            try:
                body = page.locator("body").inner_text(timeout=8000)
            except Exception:
                body = ""
            # Salva o body dos cards antes de tentar booking (antes do DOM mudar)
            cards_body = body
            # Log do body bruto para debug (primeiros 500 chars)
            notes.append(f'cards_body_len={len(cards_body)}')
            _debug = cards_body[:500].replace('\n', '\\n')
            notes.append(f'cards_body_preview={_debug}')
            # Tenta extrair preços — se não encontrar, faz refresh e tenta de novo
            REFRESH_MAX_RETRIES = 3
            retry_count = 0
            overall_min = None
            summary_price = None
            while retry_count <= REFRESH_MAX_RETRIES:
                body = page.locator("body").inner_text(timeout=8000)
                main_prices = [p for p in parse_prices(extract_section(body, "Principais voos", "Outros voos")) if p >= 300]
                other_prices = [p for p in parse_prices(extract_section(body, "Outros voos", "Mostrar mais voos")) if p >= 300]
                current_summary = extract_summary_price(body)
                main_min = min(main_prices) if main_prices else None
                other_min = min(other_prices) if other_prices else None
                current_overall = min(main_prices + other_prices) if (main_prices or other_prices) else None
                if current_overall is not None or current_summary is not None:
                    summary_price = current_summary
                    overall_min = current_overall
                    notes.append(f'price_retry_{retry_count}_found=True')
                    break
                retry_count += 1
                if retry_count <= REFRESH_MAX_RETRIES:
                    notes.append(f'price_retry_{retry_count}_sem_preco=True')
                    page.reload(wait_until='domcontentloaded')
                    time.sleep(2)
                    wait_for_results(page)
                    try_click_result_tab(page, notes)
                    expand_results(page, notes, is_international=is_intl)
            if overall_min is None and summary_price is None:
                # Mesmo após retries, sem preço — varre HTML bruto por qualquer menção de valor
                # pra tentar extrair algo que o seletor de sessão não pegou
                try:
                    body = page.locator("body").inner_text(timeout=8000)
                except Exception:
                    body = ""
                # Tenta qualquer padrão de preço no body inteiro
                all_prices = [p for p in parse_prices(body) if p >= 300]
                if all_prices:
                    overall_min = min(all_prices)
                    notes.append(f'price_fallback_body_parse_min={overall_min}')
                    if not summary_price:
                        summary_price = overall_min
                else:
                    notes.append('price_fallback_body_parse_sem_preco=True')

            booking_followed = False
            best_vendor = ""
            best_vendor_price = None
            visible_card_price = None
            booking_options: list[dict] = []
            booking_url = ""
            best_airline = None
            best_agency = None
            price_insight = ""
            _booking_price = overall_min if overall_min is not None else summary_price
            if SKIP_BOOKING:
                # Modo rápido: pula navegação do booking, usa dados da página principal
                notes.append('skip_booking=True')
                if cards_body:
                    card_vendor = extract_vendor_from_body(cards_body)
                    if card_vendor:
                        best_vendor = card_vendor
                        notes.append(f'vendor_from_card_summary={best_vendor}')
                    price_insight = _extract_price_insight_from_body(cards_body)
                    if price_insight:
                        notes.append('price_insight_from_main_page')
                if best_vendor and _booking_price:
                    best_vendor_price = _booking_price
                    visible_card_price = _booking_price
                    final_price_source = 'summary_fast'
            else:
                if _booking_price is not None:
                    try:
                        followed, best_vendor, best_vendor_price, visible_card_price, booking_options, booking_url, best_airline, best_agency, price_insight = maybe_open_booking(page, _booking_price, notes, allow_agencies=ALLOW_AGENCIES, is_international=is_intl)
                        booking_followed = followed
                    except Exception as _be:
                        # Booking crashou mas já temos dados do card principal — retorna parcial
                        notes.append(f'booking_crashed={type(_be).__name__}: {_be}')
                        # Tenta extrair vendor do body salvo antes do crash
                        if not best_vendor and cards_body:
                            card_vendor = extract_vendor_from_body(cards_body)
                            if card_vendor:
                                best_vendor = card_vendor
                                notes.append(f'vendor_from_crash_fallback={best_vendor}')
                        if best_vendor and _booking_price:
                            best_vendor_price = _booking_price
                            visible_card_price = _booking_price

            best_vendor_price = _valid_price(best_vendor_price)
            visible_card_price = _valid_price(visible_card_price)
            summary_price = _valid_price(summary_price)
            overall_min = _valid_price(overall_min)
            main_min = _valid_price(main_min)
            other_min = _valid_price(other_min)
            final_price = None
            if best_vendor_price is not None and best_vendor:
                final_price = best_vendor_price
                notes.append('final_price_source=booking_validated')
            elif summary_price is not None:
                final_price = summary_price
                if best_vendor_price is None:
                    best_vendor_price = summary_price
                notes.append('final_price_source=booking_agency_fallback')
            else:
                notes.append('final_price_rejected_no_validated_booking_price')
            notes.append(f"run_total_s={round(time.perf_counter() - nav_started, 3)}")
            best_airline_vendor = best_airline_price = best_airline_url = best_airline_visible_price = None
            best_agency_vendor = best_agency_price = best_agency_url = best_agency_visible_price = None
            if best_airline:
                best_airline_vendor = best_airline[0]
                best_airline_price = best_airline[1]
                best_airline_visible_price = best_airline[2]
                best_airline_url = best_airline[4]
            if best_agency:
                best_agency_vendor = best_agency[0]
                best_agency_price = best_agency[1]
                best_agency_visible_price = best_agency[2]
                best_agency_url = best_agency[4]
            # Se booking não retornou vendor, tenta extrair do card principal (antes do booking alterar DOM)
            if not best_vendor and cards_body:
                card_vendor = extract_vendor_from_body(cards_body)
                if card_vendor:
                    best_vendor = card_vendor
                    notes.append(f'vendor_from_card={best_vendor}')
            # Propaga nome da companhia aérea para best_vendor quando disponível
            if not best_vendor and best_airline_vendor:
                best_vendor = best_airline_vendor
            return {
                "ok": final_price is not None,
                "origin": origin,
                "destination": destination,
                "outbound_date": outbound_date,
                "inbound_date": inbound_date,
                "trip_type": "roundtrip" if inbound_date else "oneway",
                "url": page.url,
                "booking_url": booking_url,
                "summary_price": summary_price,
                "main_min": main_min,
                "other_min": other_min,
                "overall_min": overall_min,
                "price": final_price,
                "best_vendor": best_vendor,
                "best_vendor_price": best_vendor_price,
                "visible_card_price": visible_card_price,
                "best_airline_vendor": best_airline_vendor,
                "best_airline_price": best_airline_price,
                "best_airline_url": best_airline_url,
                "best_airline_visible_price": best_airline_visible_price,
                "airline": best_airline_vendor or "",
                "best_agency_vendor": best_agency_vendor,
                "best_agency_price": best_agency_price,
                "best_agency_url": best_agency_url,
                "best_agency_visible_price": best_agency_visible_price,
                "booking_options": booking_options,
                "booking_followed": booking_followed,
                "price_insight": price_insight,
                "notes": notes,
            }
        finally:
            context.close()


def _has_valid_vendor(result: dict) -> bool:
    vendor = str(result.get('best_vendor') or '').strip()
    if vendor in ('', 'google_flights', 'google', '-', 'N/D'):
        return False
    return True


def _try_renew_session(profile_dir: str | None = None) -> bool:
    """Tenta renovar a sessão Google usando google_login_stdin.py."""
    import subprocess
    import sys as _sys
    script = '/opt/vooindo/google_login_stdin.py'
    if not os.path.exists(script):
        return False
    try:
        app_password = 'rcwv jvmu yyyx okto'
        env = os.environ.copy()
        if profile_dir:
            env['GOOGLE_PERSISTENT_PROFILE_DIR'] = profile_dir
        proc = subprocess.run(
            [_sys.executable, script, '--email', 'vooindo.bot@gmail.com'],
            input=app_password + '\n',
            env=env, capture_output=True, timeout=120, text=True
        )
        if proc.returncode == 0:
            # Desativa manutenção após renovar
            try:
                from db import connect as _db_connect
                from access_policy import set_maintenance_mode
                _conn = _db_connect()
                set_maintenance_mode(_conn, False)
                _conn.close()
            except Exception:
                pass
            return True
        return False
    except Exception:
        return False


def main(argv: list[str]) -> int:
    if len(argv) < 4:
        print(json.dumps({"ok": False, "error": "usage", "message": "expected origin destination outbound_date [inbound_date]"}, ensure_ascii=False))
        return 2
    
    origin = argv[1].upper()
    destination = argv[2].upper()
    outbound_date = argv[3]
    inbound_date = argv[4] if len(argv) > 4 else ""
    profile_dir = os.environ.get('GOOGLE_PERSISTENT_PROFILE_DIR', '/opt/vooindo/google_session')
    
    max_retries = 2
    last_result = None
    renewed = False
    for attempt in range(1 + max_retries):
        try:
            # Timeout global de 150s por tentativa via signal.alarm
            import signal as _sig
            def _timeout_handler(_signum, _frame):
                raise TimeoutError('scan_timeout_150s')
            _sig.signal(_sig.SIGALRM, _timeout_handler)
            _sig.alarm(150)
            try:
                result = run(origin, destination, outbound_date, inbound_date)
            finally:
                _sig.alarm(0)
            
            last_result = result
            if result.get("ok") and _has_valid_vendor(result) and result.get("price") is not None:
                # Tudo certo: preço + vendor identificados
                print(json.dumps(result, ensure_ascii=False))
                return 0
            # Faltou preço ou vendor — faz retry
            notes = result.get('notes', [])
            notes.append(f'retry_{attempt+1}_ok={result.get("ok")}_vendor={result.get("best_vendor", "")}_price={result.get("price")}')
            result['notes'] = notes
        except PlaywrightTimeoutError as exc:
            last_result = {"ok": False, "error": "timeout", "message": str(exc)}
        except TimeoutError as exc:
            last_result = {"ok": False, "error": "timeout", "message": str(exc)}
        except Exception as exc:
            last_result = {"ok": False, "error": exc.__class__.__name__, "message": str(exc)}
        # Se tentou 2x e ainda falhou, tenta renovar sessão antes do último retry
        if attempt >= 1 and not renewed:
            renewed = _try_renew_session(profile_dir)
            if renewed:
                if last_result is None:
                    last_result = {'notes': []}
                notes = last_result.get('notes', [])
                notes.append('session_renewed_after_retries')
                last_result['notes'] = notes
    
    # Último resultado (com ou sem vendor/preço)
    print(json.dumps(last_result, ensure_ascii=False))
    return 0 if last_result and last_result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
