#!/opt/vooindo/.venv/bin/python
"""Extrai preços do calendário do Google Flights - inspeção profunda."""
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
    
    # Load PVH->FOR November 1st one-way
    page.goto("https://www.google.com/travel/flights/search?q=PVH+to+FOR+2026-11-01+one+way&hl=pt-BR&gl=BR&curr=BRL", wait_until="domcontentloaded")
    time.sleep(5)
    
    # Open date picker
    el = page.locator('div[aria-label*="data de ida"]').first
    page.evaluate('el => el.click()', el.element_handle())
    time.sleep(3)
    
    # Get gridcells with data-iso in November
    cells = page.locator('div[role="gridcell"][data-iso]').all()
    print(f"Total cells: {len(cells)}")
    
    nov_cells = []
    for c in cells:
        iso = c.get_attribute("data-iso") or ""
        if iso.startswith("2026-11"):
            nov_cells.append((iso, c))
    
    print(f"November cells: {len(nov_cells)}")
    
    # Deep inspect first 5 cells
    for iso, c in nov_cells[:5]:
        # Full HTML
        html = c.inner_html()
        
        # Look for R$ in raw HTML
        r_matches = re.findall(r'R\$\s*[\d.,]+', html)
        
        # Look for any numeric price pattern
        price_patterns = re.findall(r'[\d.]+,\d{2}', html)
        
        # Check for specific aria attributes
        aria = c.get_attribute("aria-label") or ""
        
        # Check child elements
        children = c.locator('*').all()
        child_texts = []
        for ch in children[:5]:
            try:
                txt = ch.inner_text()[:50]
                ch_aria = ch.get_attribute("aria-label") or ""
                if txt.strip() or ch_aria:
                    child_texts.append({"text": txt, "aria": ch_aria[:80]})
            except:
                pass
        
        print(f"\n{iso}:")
        print(f"  aria={aria[:100]}")
        print(f"  R$ in HTML: {r_matches}")
        print(f"  Price patterns: {price_patterns[:5]}")
        if child_texts:
            print(f"  Children: {child_texts[:3]}")
        
        # Try to find ALL nested elements with any text
        all_nested = c.locator('//*[text()]').all()
        nested_info = []
        for n in all_nested[:10]:
            try:
                txt = n.inner_text().strip()
                role = n.get_attribute("role") or ""
                clas = (n.get_attribute("class") or "")[:30]
                if txt:
                    nested_info.append(f"[{role}] class={clas}: {txt[:60]}")
            except:
                pass
        if nested_info:
            print(f"  Nested: {nested_info}")
    
    ctx.close()
