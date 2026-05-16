#!/opt/vooindo/.venv/bin/python
"""Click Gráfico de preços and extract data."""
from playwright.sync_api import sync_playwright
from google_flights_executor import _get_guardian_ws
import time, re, json

ws = None
for i in range(3):
    ws = _get_guardian_ws()
    if ws: break
    time.sleep(2)

with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(ws)
    context = browser.new_context()
    page = context.new_page()
    
    url = "https://www.google.com/travel/flights/search?q=PVH+to+NAT+2026-06-01+one+way&hl=pt-BR&gl=BR&curr=BRL"
    page.goto(url, wait_until="domcontentloaded")
    time.sleep(5)
    
    # Click "Gráfico de preços"
    print("=== Clicking Gráfico de preços ===")
    try:
        btn = page.locator('text="Gráfico de preços"').first
        if btn.count() > 0:
            btn.click()
            time.sleep(3)
            print("  Clicked!")
    except Exception as e:
        print(f"  Error: {e}")
    
    # Look for any price data in the page now
    print("\n=== Elements with price data ===")
    for sel in ['[aria-label*="R$"]', '[data-price]', '.price-graph', 'svg', 'canvas']:
        try:
            els = page.locator(sel).all()
            if els:
                print(f"  {sel}: {len(els)} elements")
                for e in els[:3]:
                    aria = e.get_attribute("aria-label") or ""
                    text = e.inner_text()[:100]
                    if aria or text.strip():
                        print(f"    aria={aria[:120]}")
                        print(f"    text={text[:120]}")
        except:
            pass
    
    # Try to extract from script tags JSON data
    print("\n=== Searching for price data in page source ===")
    html = page.content()
    # Common Google Flights data patterns
    for pattern in ['"price"', '"prices"', '"dayPrice"', '"calendarPrice"']:
        idx = html.find(pattern)
        if idx >= 0:
            print(f"  Found '{pattern}' at position {idx}: ...{html[max(0,idx-50):idx+150]}...")
    
    # Check if there are new elements after clicking
    print("\n=== New aria-labels with R$ after click ===")
    all_els = page.locator('[aria-label*="R$"]').all()
    for el in all_els[:10]:
        aria = el.get_attribute("aria-label") or ""
        role = el.get_attribute("role") or ""
        print(f"  [{role}] {aria[:150]}")
    
    context.close()
