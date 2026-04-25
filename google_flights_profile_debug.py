import json
import os
import re
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

URL = "https://www.google.com/travel/flights/search?q=AEP%20to%20PVH%202026-06-15%20one%20way&hl=pt-BR&gl=BR&curr=BRL"
SESSION_DIR = Path("./google_session")
OUT_DIR = Path("./debug_profile_compare")
OUT_DIR.mkdir(exist_ok=True)


def parse_prices(text: str):
    vals = []
    for raw in re.findall(r"R\$\s*([\d\.]+(?:,\d{2})?)", text or ""):
        try:
            vals.append(float(raw.replace('.', '').replace(',', '.')))
        except Exception:
            pass
    return vals


def extract_section(text: str, start_label: str, end_label: str | None = None):
    if not text:
        return ""
    if end_label:
        m = re.search(rf"{re.escape(start_label)}([\s\S]*?){re.escape(end_label)}", text, flags=re.I)
    else:
        m = re.search(rf"{re.escape(start_label)}([\s\S]+)$", text, flags=re.I)
    return m.group(1) if m else ""


def human_expand(page):
    for _ in range(4):
        try:
            page.mouse.wheel(0, 900)
        except Exception:
            pass
        time.sleep(1.0)
    try:
        btn = page.get_by_text("Mostrar mais voos", exact=False)
        if btn.count() > 0:
            btn.first.click(timeout=4000)
            time.sleep(2.0)
    except Exception:
        pass
    for _ in range(3):
        try:
            page.mouse.wheel(0, 900)
        except Exception:
            pass
        time.sleep(1.0)


def dump_page(page, prefix: str):
    body = ""
    try:
        body = page.locator("body").inner_text(timeout=8000)
    except Exception:
        pass
    (OUT_DIR / f"{prefix}_body.txt").write_text(body or "", encoding="utf-8")
    try:
        page.screenshot(path=str(OUT_DIR / f"{prefix}.png"), full_page=True)
    except Exception:
        pass

    main_prices = parse_prices(extract_section(body, "Principais voos", "Outros voos"))
    other_prices = parse_prices(extract_section(body, "Outros voos", "Mostrar mais voos"))
    summary_match = re.search(r"Menores preços\s*a partir de\s*R\$\s*([\d\.]+(?:,\d{2})?)", body or "", flags=re.I)
    summary_price = None
    if summary_match:
        try:
            summary_price = float(summary_match.group(1).replace('.', '').replace(',', '.'))
        except Exception:
            pass

    data = {
        "url": page.url,
        "summary_price": summary_price,
        "main_prices": main_prices,
        "other_prices": other_prices,
        "main_min": min(main_prices) if main_prices else None,
        "other_min": min(other_prices) if other_prices else None,
        "overall_min": min(main_prices + other_prices) if (main_prices or other_prices) else None,
    }
    (OUT_DIR / f"{prefix}.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data


def run_anonymous(p):
    browser = p.chromium.launch(headless=False, args=["--disable-blink-features=AutomationControlled"])
    page = browser.new_page(locale="pt-BR")
    page.goto(URL, wait_until="domcontentloaded")
    time.sleep(3)
    human_expand(page)
    data = dump_page(page, "anonymous")
    browser.close()
    return data


def run_logged(p):
    context = p.chromium.launch_persistent_context(
        str(SESSION_DIR),
        headless=False,
        slow_mo=100,
        locale="pt-BR",
        args=["--disable-blink-features=AutomationControlled"],
    )
    page = context.pages[0] if context.pages else context.new_page()
    page.goto("https://www.google.com/", wait_until="domcontentloaded")
    print("Se necessário, faça login manualmente. Depois pressione ENTER aqui no terminal para continuar...")
    input()
    page.goto(URL, wait_until="domcontentloaded")
    time.sleep(3)
    human_expand(page)
    data = dump_page(page, "logged")
    context.close()
    return data


def main():
    with sync_playwright() as p:
        anon = run_anonymous(p)
        logged = run_logged(p)
        compare = {
            "anonymous": anon,
            "logged": logged,
            "logged_price": logged.get("overall_min"),
            "anonymous_price": anon.get("overall_min"),
        }
        (OUT_DIR / "compare.json").write_text(json.dumps(compare, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(compare, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
