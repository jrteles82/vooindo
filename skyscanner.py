
from __future__ import annotations
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

CONFIG = {
    "timeout_ms": 45000,
    "settle_seconds": 3,
    "headless": True,
}

from models import FlightResult, RouteQuery

def format_brl(value: float) -> str:
    if value is None: return "N/D"
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def build_google_flights_url(route: RouteQuery) -> str:
    from urllib.parse import quote
    trip = f"{route.origin} to {route.destination} {route.outbound_date} one way" if not route.inbound_date else f"{route.origin} to {route.destination} {route.outbound_date} return {route.inbound_date}"
    return f"https://www.google.com/travel/flights/search?q={quote(trip)}&hl=pt-BR&gl=BR&curr=BRL"

class GoogleFlightsScraper:
    def __init__(self, browser):
        self.browser = browser

    def _accept_cookies_if_present(self, page) -> None:
        labels = ["Aceitar tudo", "Aceito", "I agree", "Accept all"]
        for label in labels:
            try:
                page.get_by_role("button", name=label).click(timeout=2000)
                time.sleep(1)
                return
            except Exception:
                pass

    def _extract_summary_price(self, page) -> float | None:
        patterns = [
            r"Menores preços\s+a partir de\s+R\$\s*([\d\.]+(?:,\d{2})?)",
            r"Menores preços.*?R\$\s*([\d\.]+(?:,\d{2})?)",
        ]
        for sel in ["body", "main", "[role='main']"]:
            try:
                txt = page.locator(sel).first.inner_text(timeout=3000)
                if not txt: continue
                for pattern in patterns:
                    m = re.search(pattern, txt, flags=re.IGNORECASE | re.DOTALL)
                    if m:
                        try:
                            return float(m.group(1).replace(".", "").replace(",", "."))
                        except ValueError:
                            pass
            except Exception:
                pass
        return None

    def _click_lowest_prices_tab(self, page) -> bool:
        candidates = [
            lambda: page.get_by_text("Menores preços", exact=False),
            lambda: page.get_by_role("button", name=re.compile(r"Menores preços", re.I)),
            lambda: page.get_by_role("tab", name=re.compile(r"Menores preços", re.I)),
        ]
        for factory in candidates:
            try:
                loc = factory()
                if loc.count() > 0:
                    loc.first.click(timeout=4000)
                    time.sleep(2.5)
                    try:
                        page.wait_for_load_state("networkidle", timeout=8000)
                    except Exception:
                        pass
                    time.sleep(2.0)
                    return True
            except Exception:
                pass
        return False

    def _extract_visible_flight_cards(self, page) -> list[dict]:
        cards = []
        selectors = ["[role='listitem']", "li", "div[jscontroller]", "div[role='button']"]
        for sel in selectors:
            try:
                loc = page.locator(sel)
                count = min(loc.count(), 50)
                for i in range(count):
                    card = loc.nth(i)
                    try:
                        txt = card.inner_text(timeout=1000).strip()
                    except:
                        continue
                    if not txt or "R$" not in txt: continue
                    nums = re.findall(r"R\$\s*([\d\.]+(?:,\d{2})?)", txt)
                    if nums:
                        try:
                            price = float(nums[-1].replace('.', '').replace(',', '.'))
                            cards.append({"price": price, "loc": card})
                        except: pass
            except: pass
            if cards: break
        return cards

    def _open_card_and_extract_vendor(self, page, card) -> tuple[str, float | None, list[dict]]:
        try:
            card.click(timeout=3000)
            time.sleep(2)
        except: pass
        try:
            body = page.locator("body").inner_text(timeout=5000)
        except: return "", None, []
        
        options = []
        patterns = [r"Reserve com a\s+([^\n\r]+?)\s+R\$\s*([\d\.]+(?:,\d{2})?)", r"Reservar com\s+([^\n\r]+?)\s+R\$\s*([\d\.]+(?:,\d{2})?)"]
        for pattern in patterns:
            for vendor, raw_price in re.findall(pattern, body, flags=re.IGNORECASE):
                try:
                    price = float(raw_price.replace(".", "").replace(",", "."))
                    options.append({"vendor": vendor.strip(), "price": price})
                except: continue
        if not options: return "", None, []
        best = sorted(options, key=lambda x: x["price"])[0]
        return best["vendor"], best["price"], options

    def search(self, route: RouteQuery, profile_dir: Optional[str] = None) -> FlightResult:
        page = self.browser.new_page()
        page.set_default_timeout(CONFIG["timeout_ms"])
        url = build_google_flights_url(route)
        notes = []
        try:
            page.goto(url, wait_until="domcontentloaded")
            self._accept_cookies_if_present(page)
            time.sleep(CONFIG["settle_seconds"])
            summary_price = self._extract_summary_price(page)
            if summary_price: notes.append(f"summary={format_brl(summary_price)}")
            self._click_lowest_prices_tab(page)
            cards = self._extract_visible_flight_cards(page)
            best_vendor, best_vendor_price, booking_options = "", None, []
            final_price = summary_price
            if cards:
                cheapest = sorted(cards, key=lambda x: x["price"])[0]
                final_price = cheapest["price"]
                best_vendor, best_vendor_price, booking_options = self._open_card_and_extract_vendor(page, cheapest["loc"])
            
            return FlightResult(
                site="google_flights",
                origin=route.origin,
                destination=route.destination,
                outbound_date=route.outbound_date,
                inbound_date=route.inbound_date,
                price=final_price,
                url=page.url,
                notes=" | ".join(notes),
                best_vendor=best_vendor,
                best_vendor_price=best_vendor_price,
                booking_options_json=json.dumps(booking_options)
            )
        except Exception as e:
            return FlightResult(site="google_flights", origin=route.origin, destination=route.destination, outbound_date=route.outbound_date, inbound_date=route.inbound_date, price=None, notes=str(e))
        finally:
            page.close()

def build_google_flights_worker(playwright, browser=None):
    if not browser:
        browser = playwright.chromium.launch(headless=CONFIG["headless"])
    return GoogleFlightsScraper(browser)

def classify_price(price, min_p, avg_p):
    if not min_p: return "⚪️"
    if price <= min_p: return "🟢"
    if price <= avg_p: return "🟡"
    return "🔴"

def parse_price_brl(txt):
    try: return float(txt.replace("R$", "").replace(".", "").replace(",", ".").strip())
    except: return 0.0

def sync_playwright():
    from playwright.sync_api import sync_playwright
    return sync_playwright()

def build_db_queries(conn): return None
