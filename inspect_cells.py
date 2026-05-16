#!/opt/vooindo/.venv/bin/python
"""Extrai preços das células do calendário."""
from playwright.sync_api import sync_playwright
from google_flights_executor import _get_guardian_ws
import time

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
    
    # Open date picker
    el = page.locator('div[aria-label*="data de ida"]').first
    page.evaluate('el => el.click()', el.element_handle())
    time.sleep(2)
    
    # Get all gridcells
    cells = page.locator('div[role="gridcell"][data-iso]').all()
    print(f"Found {len(cells)} gridcells with data-iso")
    
    for c in cells[:35]:
        iso = c.get_attribute("data-iso") or ""
        inner = c.inner_html()[:300]
        # Look for price patterns in inner HTML
        import re
        prices = re.findall(r'R\$\s*[\d.]+,\d{2}', inner)
        text = c.inner_text()[:100]
        print(f"  {iso}: text={text!r} prices={prices}")
    
    context.close()
