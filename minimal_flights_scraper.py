#!/opt/vooindo/.venv/bin/python
"""Minimal Google Flights scraper - fallback when main executor crashes.
Carrega a página, extrai body, sem stealth/blocks - Chrome mais vanilla possível."""
from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote

os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(Path.home() / ".cache/ms-playwright"))

from playwright.sync_api import sync_playwright

SESSION_DIR = Path(os.getenv("GOOGLE_PERSISTENT_PROFILE_DIR", str(Path(__file__).resolve().with_name("google_session"))))
HL = "pt-BR"
GL = "BR"
CURR = "BRL"

# Tokens de airline (simplificado)
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
                # Procura preço perto (até 15 linhas)
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
    
    # Fallback 2: qualquer linha com nome de cia perto de preço (sem token)
    for i, line in enumerate(lines):
        low = line.lower()
        if re.search(r'R\$[\s\d.,]+', line):
            # Esta linha tem preço - procura nome de cia nas redondezas
            for j in range(max(0,i-10), min(len(lines),i+11)):
                ln = lines[j].lower()
                for token, canonical in _AIRLINE_TOKENS:
                    if token in ln:
                        prices = parse_prices(line)
                        if prices and prices[0] >= 300:
                            return (canonical, prices[0])
    
    # Fallback 3: qualquer preço > 300
    all_prices = [p for p in parse_prices(body) if p >= 300]
    if all_prices:
        price = min(all_prices)
        # Tenta achar vendor próximo a esse preço
        for i, line in enumerate(lines):
            low = line.lower()
            if str(price)[:4] in line:
                for j in range(max(0,i-10), min(len(lines),i+11)):
                    ln = lines[j].lower()
                    for token, canonical in _AIRLINE_TOKENS:
                        if token in ln:
                            return (canonical, price)
        return (None, price)
    return (None, None)


def main(argv: list[str]) -> int:
    if len(argv) < 4:
        print(json.dumps({"ok": False, "error": "missing_args"}))
        return 1
    origin = argv[1].upper()
    destination = argv[2].upper()
    outbound_date = argv[3]
    inbound_date = argv[4] if len(argv) > 4 else ""

    trip = f"{origin} to {destination} {outbound_date} one way" if not inbound_date else f"{origin} to {destination} {outbound_date} return {inbound_date}"
    url = f"https://www.google.com/travel/flights/search?q={quote(trip)}&hl={HL}&gl={GL}&curr={CURR}"

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
            page.goto(url, wait_until="domcontentloaded", timeout=25000)
            time.sleep(5)
            body = page.locator("body").inner_text(timeout=5000)
            ctx.close()
            browser.close()

        vendor, price = extract_vendor_and_price(body)
        result = {
            "ok": price is not None,
            "origin": origin,
            "destination": destination,
            "outbound_date": outbound_date,
            "inbound_date": inbound_date,
            "trip_type": "roundtrip" if inbound_date else "oneway",
            "price": price,
            "currency": "BRL",
            "best_vendor": vendor or "",
            "best_vendor_price": price,
            "notes": ["minimal_scraper_fallback"],
        }
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result["ok"] else 1
    except Exception as exc:
        print(json.dumps({"ok": False, "error": type(exc).__name__, "message": str(exc)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
