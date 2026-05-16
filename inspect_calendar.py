#!/opt/vooindo/.venv/bin/python
"""Inspect Google Flights page to find date picker / calendar elements."""
from playwright.sync_api import sync_playwright
from google_flights_executor import _get_guardian_ws
import time

ws = None
for i in range(3):
    ws = _get_guardian_ws()
    if ws: break
    time.sleep(2)

if not ws:
    print("NO GUARDIAN")
    exit()

with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(ws)
    context = browser.new_context()
    page = context.new_page()
    
    # Load one-way search
    page.goto("https://www.google.com/travel/flights/search?q=PVH+to+NAT+2026-06-01+one+way&hl=pt-BR&gl=BR&curr=BRL", wait_until="domcontentloaded")
    time.sleep(5)
    
    print("=== ALL elements with aria-label containing date/price ===")
    try:
        all_els = page.locator("[aria-label]").all()
        for el in all_els[:50]:  # limit to 50
            try:
                label = el.get_attribute("aria-label") or ""
                if any(k in label.lower() for k in ["data", "preço", "preco", "r$", "dia", "partida", "volta"]):
                    role = el.get_attribute("role") or ""
                    tag = el.evaluate("el => el.tagName")
                    clas = (el.get_attribute("class") or "")[:40]
                    jsname = el.get_attribute("jsname") or ""
                    print(f"  {tag}[{role}] jsname={jsname[:20]} class={clas}")
                    print(f"    -> {label[:150]}")
            except:
                pass
    except Exception as e:
        print(f"  Error: {e}")
    
    # Try to click the date button
    print("\n=== Trying to open date picker ===")
    for sel in [
        'div[role="button"][aria-label*="Data"]',
        'div[aria-label*="Data de ida"]',
        'div[aria-label*="data de ida"]',
        'input[aria-label*="data"]',
        'input[aria-label*="Data"]',
    ]:
        try:
            el = page.locator(sel).first
            c = el.count()
            if c > 0:
                label = el.get_attribute("aria-label") or ""
                print(f"  FOUND: {sel} count={c} label={label[:80]}")
                el.click()
                time.sleep(2)
                break
        except Exception as e:
            print(f"  {sel}: {str(e)[:80]}")
    
    # After clicking, check for calendar cells
    print("\n=== After click: elements with role=gridcell or date in aria ===")
    time.sleep(1)
    try:
        all_els = page.locator("[aria-label]").all()
        for el in all_els[:50]:
            try:
                label = el.get_attribute("aria-label") or ""
                role = el.get_attribute("role") or ""
                if role == "gridcell" or any(k in label.lower() for k in ["r$", "preço", "preco", "junho", "julho"]):
                    tag = el.evaluate("el => el.tagName")
                    print(f"  {tag}[{role}]: {label[:150]}")
            except:
                pass
    except Exception as e:
        print(f"  Error: {e}")
    
    context.close()
