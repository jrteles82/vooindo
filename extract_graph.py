#!/opt/vooindo/.venv/bin/python
"""Extrai dados do gráfico de preços do Google Flights."""
from playwright.sync_api import sync_playwright
from google_flights_executor import _get_guardian_ws
import time, re, json

ws = None
for i in range(5):
    ws = _get_guardian_ws()
    if ws: break
    time.sleep(3)

if not ws:
    print(json.dumps({"error": "no_guardian"}))
    exit()

with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(ws)
    context = browser.new_context()
    page = context.new_page()
    
    # Load the page Teles shared
    url = "https://www.google.com/travel/flights/search?tfs=CBwQAhoeEgoyMDI2LTExLTAxagcIARIDUFZIcgcIARIDRk9SQAFIAXABggELCP___wGYAQI&tfu=EgYIACABKAEiAA&hl=pt-BR&gl=BR&curr=BRL"
    page.goto(url, wait_until="domcontentloaded")
    time.sleep(5)
    
    # Try to find price data in the page
    result = {"prices": {}, "graph_data": None}
    
    # Method 1: Look for script data
    try:
        scripts = page.locator("script[type='application/json'], script[type='text/json']").all()
        for s in scripts:
            try:
                data = s.inner_text()
                if '"price"' in data.lower() and len(data) < 50000:
                    result["script_data"] = data[:3000]
            except:
                pass
    except:
        pass
    
    # Method 2: Try to access JS window data
    try:
        js_data = page.evaluate("""() => {
            const result = {};
            // Try common data stores
            for (const key of ['__DATA__', '__INITIAL_STATE__', '__remixContext', '__NEXT_DATA__', 'APP_STATE']) {
                if (window[key]) result[key] = 'found';
            }
            // Try to find any global with price data
            const globals = Object.keys(window).filter(k => 
                typeof window[k] === 'object' && window[k] !== null && 
                !k.startsWith('webkit') && !k.startsWith('on')
            ).slice(0, 50);
            result.globals = globals;
            return result;
        }""")
        result["js_globals"] = js_data
    except Exception as e:
        result["js_globals_error"] = str(e)
    
    # Method 3: Look for elements with price data
    try:
        all_text = page.inner_text("body")
        # Find all R$ prices near dates
        matches = re.findall(r'.{0,40}(R\$\s*[\d.]+,\d{2}).{0,40}', all_text)
        result["price_matches"] = matches[:20]
    except:
        pass
    
    # Method 4: Try clicking the date to open calendar and extract prices
    try:
        el = page.locator('div[aria-label*="data de ida"]').first
        if el.count() > 0:
            page.evaluate('el => el.click()', el.element_handle())
            time.sleep(3)
            
            cells = page.locator('div[role="gridcell"][data-iso]').all()
            prices_found = {}
            for c in cells:
                iso = c.get_attribute("data-iso") or ""
                if not iso.startswith("2026-11"):
                    continue
                # Get ALL text including hidden elements
                full_html = c.inner_html()
                # Look for R$ in the HTML
                r_matches = re.findall(r'R\$\s*([\d.]+,\d{2})', full_html)
                if r_matches:
                    prices_found[iso] = r_matches[0]
            result["calendar_prices"] = prices_found
            result["total_cells"] = len(cells)
    except Exception as e:
        result["calendar_error"] = str(e)
    
    print(json.dumps(result, indent=2, ensure_ascii=False))
    context.close()
