#!/opt/vooindo/.venv/bin/python
"""Extrai preços do gráfico de barras do Google Flights."""
from playwright.sync_api import sync_playwright
from google_flights_executor import _get_guardian_ws
import time, re

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
    
    url = "https://www.google.com/travel/flights/search?q=PVH+to+NAT+2026-06-01+one+way&hl=pt-BR&gl=BR&curr=BRL"
    page.goto(url, wait_until="domcontentloaded")
    time.sleep(5)
    
    # Try to find the price graph toggle/button
    print("=== Looking for price graph / date bar ===")
    
    # Method 1: Look for graph SVG or canvas elements
    for tag in ['svg', 'canvas', '[role="img"]']:
        try:
            els = page.locator(tag).all()
            for el in els[:5]:
                try:
                    aria = el.get_attribute("aria-label") or ""
                    if any(k in aria.lower() for k in ['preço', 'preco', 'price', 'gráfico', 'grafico']):
                        print(f"  {tag} aria={aria[:120]}")
                except:
                    pass
        except:
            pass
    
    # Method 2: Try to click "Gráfico de preços" or similar toggle
    for text in ['Gráfico de preços', 'gráfico', 'Histórico', 'Datas flexíveis', 'Menores preços']:
        try:
            btn = page.locator(f'text="{text}"').first
            if btn.count() > 0:
                print(f"  Found button: '{text}'")
        except:
            pass
    
    # Method 3: Dump all text near date/price elements
    print("\n=== Page text containing price data patterns ===")
    try:
        body = page.inner_text("body")
        # Look for patterns like "R$ 1.200" near dates
        for match in re.finditer(r'.{0,30}(R\$\s*[\d.]+,\d{2}).{0,30}', body):
            print(f"  {match.group().strip()[:100]}")
    except:
        pass
    
    # Method 4: Try clicking the date button differently
    print("\n=== Trying to open date picker with JS click ===")
    try:
        el = page.locator('div[aria-label*="data de ida"]').first
        if el.count() > 0:
            # Try JS click instead of Playwright click
            page.evaluate('el => el.click()', el.element_handle())
            time.sleep(2)
            print("  JS click done")
    except Exception as e:
        print(f"  JS click failed: {e}")
    
    # After click attempt, look for calendar cells
    print("\n=== Looking for calendar/price cells ===")
    for sel in ['div[role="gridcell"]', 'td', 'div[data-iso]', '[jsname]']:
        try:
            cells = page.locator(sel).all()
            for c in cells[:20]:
                try:
                    text = c.inner_text()[:60]
                    aria = c.get_attribute("aria-label") or ""
                    role = c.get_attribute("role") or ""
                    jsname = c.get_attribute("jsname") or ""
                    if text.strip() or aria:
                        print(f"  {sel}[{role}] jsname={jsname[:15]}: text={text} aria={aria[:80]}")
                except:
                    pass
        except:
            pass
    
    context.close()
