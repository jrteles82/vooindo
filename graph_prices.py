#!/opt/vooindo/.venv/bin/python
"""Extrai preços do gráfico de preços do Google Flights."""
from playwright.sync_api import sync_playwright
from google_flights_executor import _get_guardian_ws
import time, re, json

ws = None
for i in range(5):
    ws = _get_guardian_ws()
    if ws: break
    time.sleep(3)
if not ws:
    print(json.dumps({"error":"no_guardian"}))
    exit(1)

with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(ws)
    ctx = browser.new_context()
    page = ctx.new_page()
    
    page.goto("https://www.google.com/travel/flights/search?q=PVH+to+FOR+2026-11-01+one+way&hl=pt-BR&gl=BR&curr=BRL", wait_until="domcontentloaded")
    time.sleep(5)
    
    # Click "Gráfico de preços" button
    print("=== Clicking Grafico de preços ===")
    try:
        # The button might be hidden in a menu first
        # Try clicking it directly
        btns = page.locator('text="Gráfico de preços"').all()
        print(f"  Found {len(btns)} 'Gráfico de preços' buttons")
        if btns:
            btns[0].click()
            time.sleep(3)
            print("  Clicked!")
    except Exception as e:
        print(f"  Click error: {e}")
    
    # After click, look for any elements containing multiple R$ prices
    print("\n=== Searching for price data after graph click ===")
    body = page.inner_text("body")
    
    # Find all R$ prices with nearby dates
    price_matches = re.findall(r'(R\$\s*[\d.]+,\d{2})', body)
    print(f"  All R$ prices: {len(price_matches)} total")
    
    # Look for patterns like: dia X R$ Y
    date_price = re.findall(r'(\d{1,2})\s*(?:de\s+\w+\s*(?:de\s+)?)?.*?(R\$\s*[\d.]+,\d{2})', body, re.IGNORECASE)
    if date_price:
        print(f"\n  Date-price pairs found: {len(date_price)}")
        for d, p in date_price[:15]:
            print(f"    Day {d}: {p}")
    
    # Try to find graph bars/rects with data
    print("\n=== Looking for graph elements ===")
    for sel in ['svg rect', 'svg [data-price]', '[aria-label*="R$"]', 'canvas']:
        els = page.locator(sel).all()
        if els:
            print(f"  {sel}: {len(els)} elements")
    
    # All aria-labels with R$
    r_aria = page.locator('[aria-label*="R$"]').all()
    print(f"\n  Elements with R$ in aria: {len(r_aria)}")
    for el in r_aria[:10]:
        label = el.get_attribute("aria-label") or ""
        role = el.get_attribute("role") or ""
        tag = el.evaluate("el => el.tagName")
        print(f"    {tag}[{role}]: {label[:120]}")
    
    # Try to find data in the page's DOM that's price-related
    print("\n=== Full page text search for November prices ===")
    nov_price_lines = re.findall(r'.{0,60}(?:nov\w+|Nov\w+|11/\d+).{0,30}(?:R\$|a partir).{0,60}', body, re.IGNORECASE)
    for line in nov_price_lines[:10]:
        print(f"  {line.strip()[:120]}")
    
    ctx.close()
