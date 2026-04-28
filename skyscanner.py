
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
        from google_flights_executor import run as run_executor
        import os
        
        # Override profile_dir if provided
        if profile_dir:
            os.environ["GOOGLE_PERSISTENT_PROFILE_DIR"] = profile_dir
            
        try:
            res = run_executor(route.origin, route.destination, route.outbound_date, route.inbound_date or "")
            
            if res.get("ok"):
                return FlightResult(
                    site="google_flights",
                    origin=route.origin,
                    destination=route.destination,
                    outbound_date=route.outbound_date,
                    inbound_date=route.inbound_date,
                    trip_type=route.trip_type,
                    price=res.get("price"),
                    currency="BRL",
                    url=res.get("url", ""),
                    booking_url=res.get("booking_url", ""),
                    notes=" | ".join(res.get("notes", [])),
                    best_vendor=res.get("best_vendor", ""),
                    best_vendor_price=res.get("best_vendor_price"),
                    booking_options_json=json.dumps(res.get("booking_options", []), ensure_ascii=False),
                    price_insight=res.get("price_insight", ""),
                    best_airline_vendor=res.get("best_airline_vendor"),
                    best_airline_price=res.get("best_airline_price"),
                    best_airline_url=res.get("best_airline_url"),
                    best_airline_visible_price=res.get("best_airline_visible_price"),
                    best_agency_vendor=res.get("best_agency_vendor"),
                    best_agency_price=res.get("best_agency_price"),
                    best_agency_url=res.get("best_agency_url"),
                    best_agency_visible_price=res.get("best_agency_visible_price")
                )
            else:
                return FlightResult(
                    site="google_flights", 
                    origin=route.origin, 
                    destination=route.destination, 
                    outbound_date=route.outbound_date, 
                    inbound_date=route.inbound_date, 
                    price=None, 
                    notes=res.get("error", "unknown_executor_error")
                )
        except Exception as e:
            import traceback
            return FlightResult(
                site="google_flights", 
                origin=route.origin, 
                destination=route.destination, 
                outbound_date=route.outbound_date, 
                inbound_date=route.inbound_date, 
                price=None, 
                notes=f"executor_exception: {str(e)}"
            )

def build_db_queries(conn): return None
