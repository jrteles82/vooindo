#!/opt/vooindo/.venv/bin/python
"""Extrai preços do gráfico de barras do Google Flights."""
from playwright.sync_api import sync_playwright
from google_flights_executor import _get_guardian_ws
import time, json

ws = None
for i in range(5):
    ws = _get_guardian_ws()
    if ws: break
    time.sleep(3)
if not ws: exit(1)

with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(ws)
    ctx = browser.new_context()
    page = ctx.new_page()
    
    page.goto("https://www.google.com/travel/flights/search?q=PVH+to+FOR+2026-11-01+one+way&hl=pt-BR&gl=BR&curr=BRL", wait_until="domcontentloaded")
    time.sleep(5)
    
    page.locator('text=Gráfico de preços').first.click()
    time.sleep(3)
    
    result = page.evaluate("""() => {
        const bars = document.querySelectorAll("path.ZMv3u");
        const data = [];
        bars.forEach(b => {
            const rect = b.getBoundingClientRect();
            const graphTop = 262, graphHeight = 294, maxPrice = 3000;
            const barTop = rect.y - graphTop;
            const price = Math.round((graphHeight - barTop) / graphHeight * maxPrice);
            data.push({x: Math.round(rect.x), y: Math.round(rect.y), h: Math.round(rect.height), price: price});
        });
        return data;
    }""")
    
    print(f"Bars: {len(result)}")
    
    # First 30 bars = November, next 30 = December
    nov = result[:30] if len(result) >= 30 else result
    dez = result[30:60] if len(result) >= 60 else []
    
    print("\nNOVEMBER 2026:")
    for i, b in enumerate(nov):
        p = b['price']
        print(f"  Day {i+1:2d}: R$ {p:,.0f}".replace(",", "."))
    
    if dez:
        print("\nDECEMBER 2026:")
        for i, b in enumerate(dez):
            p = b['price']
            print(f"  Day {i+1:2d}: R$ {p:,.0f}".replace(",", "."))
    
    if nov:
        cheapest_idx = min(range(len(nov)), key=lambda i: nov[i]['price'])
        cheapest_price = nov[cheapest_idx]['price']
        print(f"\n✅ Cheapest November: Day {cheapest_idx + 1} at R$ {cheapest_price:,.0f}".replace(",", "."))
    
    ctx.close()
