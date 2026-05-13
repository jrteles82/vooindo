#!/opt/vooindo/.venv/bin/python
"""Minimal Google Flights scraper - fallback when main executor crashes.

Carrega a página, tenta extrair URL de booking clicando no card mais barato.
Chrome próprio (não CDP compartilhado), sem stealth.
Se booking falhar, retorna ao menos preço + URL da busca."""
from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote

os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(Path.home() / ".cache/ms-playwright"))

from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

SESSION_DIR = Path(os.getenv("GOOGLE_PERSISTENT_PROFILE_DIR", str(Path(__file__).resolve().with_name("google_session"))))
HL = "pt-BR"
GL = "BR"
CURR = "BRL"

_AIRLINE_TOKENS = [
    ("latam", "LATAM"), ("gol", "GOL"), ("azul", "Azul"),
    ("avianca", "Avianca"), ("american", "American"), ("united", "United"),
    ("delta", "Delta"), ("copa", "Copa"), ("voepass", "Voepass"),
    ("passaredo", "Passaredo"), ("arajet", "Arajet"), ("jetsmart", "JetSMART"),
    ("flybondi", "Flybondi"), ("aerolineas", "Aerolineas Argentinas"),
    ("iberia", "Iberia"), ("tap", "TAP"), ("air france", "Air France"),
    ("emirates", "Emirates"), ("qatar", "Qatar"), ("lufthansa", "Lufthansa"),
    ("klm", "KLM"), ("british", "British Airways"), ("swiss", "Swiss"),
    ("etihad", "Etihad"), ("turkish", "Turkish"), ("aeromexico", "Aeromexico"),
    ("spirit", "Spirit"), ("frontier", "Frontier"), ("jetblue", "JetBlue"),
    ("southwest", "Southwest"),
]


def parse_prices(text: str) -> list[float]:
    vals = []
    for raw in re.findall(r"(?:R\$|[$])\s*([\d.]+(?:,\d{2})?)", text or ""):
        try:
            cleaned = raw.replace(".", "").replace(",", ".")
            vals.append(float(cleaned))
        except Exception:
            pass
    return vals


def extract_vendor_and_price(body: str) -> tuple[str | None, float | None]:
    lines = [ln.strip() for ln in (body or "").splitlines() if ln.strip()]
    seen_tokens: set[int] = set()
    results: list[tuple[str, float]] = []

    for token, canonical in _AIRLINE_TOKENS:
        for i, line in enumerate(lines):
            low = line.lower()
            if token in low:
                for j in range(-15, 16):
                    idx = i + j
                    if 0 <= idx < len(lines):
                        prices = parse_prices(lines[idx])
                        for p in prices:
                            if p >= 300:
                                if id(token) not in seen_tokens:
                                    seen_tokens.add(id(token))
                                    results.append((canonical, p))
                                break
    if results:
        results.sort(key=lambda x: x[1])
        return results[0]

    for i, line in enumerate(lines):
        low = line.lower()
        if re.search(r'R\$[\s\d.,]+', line):
            for j in range(max(0, i - 10), min(len(lines), i + 11)):
                ln = lines[j].lower()
                for token, canonical in _AIRLINE_TOKENS:
                    if token in ln:
                        prices = parse_prices(line)
                        if prices and prices[0] >= 300:
                            return (canonical, prices[0])

    all_prices = [p for p in parse_prices(body) if p >= 300]
    if all_prices:
        price = min(all_prices)
        for i, line in enumerate(lines):
            low = line.lower()
            if str(price)[:4] in line:
                for j in range(max(0, i - 10), min(len(lines), i + 11)):
                    ln = lines[j].lower()
                    for token, canonical in _AIRLINE_TOKENS:
                        if token in ln:
                            return (canonical, price)
        return (None, price)
    return (None, None)


def _try_open_booking(page, url: str) -> tuple[str | None, str]:
    """Tenta clicar no primeiro card de booking e extrair a URL real.
    Retorna (booking_url, page_url_fallback)."""
    import urllib.parse

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except Exception:
        pass

    time.sleep(4)

    booking_url = None
    page_url = url

    try:
        page_url = page.url
    except Exception:
        pass

    try:
        body_text = page.locator("body").inner_text(timeout=4000)
    except Exception:
        body_text = ""

    try:
        # Tenta clicar no primeiro card de companhia aerea com resultado
        # Seletores comuns do Google Flights para opções de voo
        selectors = [
            "div[role='listbox'] a",          # lista de resultados
            "div[role='listbox'] div[role='option']",  # opções de resultado
            "a[href*='flights/booking']",      # link direto de booking
            "a[href*='flights/search']",       # link de resultado
            "div[jsname]",                     # cards genéricos
        ]
        clicked = False
        for sel in selectors:
            try:
                candidates = page.locator(sel).all()
                for cand in candidates:
                    try:
                        txt = cand.inner_text(timeout=500)
                        if 'R$' in txt or re.search(r'R\$\s*[\d.,]+', txt):
                            href = cand.get_attribute('href') or ''
                            if href and ('flights/booking' in href or 'flights/search' in href):
                                cand.click(timeout=4000, force=True)
                                clicked = True
                                break
                    except Exception:
                        continue
                if clicked:
                    break
            except Exception:
                continue

        if clicked:
            time.sleep(4)
            try:
                booking_url = page.url
                if 'flights/search' in booking_url and 'flights/booking' not in booking_url:
                    booking_url = None
            except Exception:
                pass

            # Se clicou mas caiu na mesma página (não abriu booking), tenta 2ª via
            if not booking_url:
                try:
                    all_links = page.locator("a").all()
                    for link in all_links:
                        try:
                            href = link.get_attribute('href') or ''
                            if 'flights/booking' in href:
                                booking_url = href
                                if not href.startswith('http'):
                                    booking_url = urllib.parse.urljoin('https://www.google.com', href)
                                break
                        except Exception:
                            continue
                except Exception:
                    pass
    except Exception:
        pass

    if not booking_url:
        try:
            page_url = page.url
        except Exception:
            pass

    return booking_url, page_url


def main(argv: list[str]) -> int:
    if len(argv) < 4:
        print(json.dumps({"ok": False, "error": "missing_args"}))
        return 1
    origin = argv[1].upper()
    destination = argv[2].upper()
    outbound_date = argv[3]
    inbound_date = argv[4] if len(argv) > 4 else ""

    trip = f"{origin} to {destination} {outbound_date} one way" if not inbound_date else f"{origin} to {destination} {outbound_date} return {inbound_date}"
    search_url = f"https://www.google.com/travel/flights/search?q={quote(trip)}&hl={HL}&gl={GL}&curr={CURR}"

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-gpu",
                ],
            )
            ctx = browser.new_context(
                locale="pt-BR",
                viewport={"width": 1280, "height": 900},
            )
            page = ctx.new_page()
            page.set_default_timeout(30000)

            booking_url, page_url = _try_open_booking(page, search_url)

            try:
                body = page.locator("body").inner_text(timeout=3000)
            except Exception:
                body = ""
            ctx.close()
            browser.close()

        vendor, price = extract_vendor_and_price(body)
        notes = ["minimal_scraper_fallback"]
        if booking_url:
            notes.append("booking_extracted_by_minimal")
        else:
            notes.append("search_url_fallback")

        result = {
            "ok": price is not None,
            "origin": origin,
            "destination": destination,
            "outbound_date": outbound_date,
            "inbound_date": inbound_date,
            "trip_type": "roundtrip" if inbound_date else "oneway",
            "price": price,
            "currency": "BRL",
            "url": page_url or search_url,
            "booking_url": booking_url or "",
            "best_vendor": vendor or "",
            "best_vendor_price": price,
            "notes": notes,
        }
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result["ok"] else 1
    except Exception as exc:
        print(json.dumps({"ok": False, "error": type(exc).__name__, "message": str(exc)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
