#!/usr/bin/env python3
"""
DOM Detective — Auto-detects Google Flights UI changes and updates selectors.

Runs periodically (cron/systemd timer) or on-demand:
1. Opens Google Flights search page via guardian Chrome
2. Tries current selectors to find flight cards
3. If current selectors fail (0 cards), scans DOM for new candidates
4. Updates google_flights_executor.py with new selectors
5. Logs changes and notifies via Telegram

Usage:
  python3 dom_detective.py              # check + auto-fix (dry-run)
  python3 dom_detective.py --apply      # check + auto-fix (apply changes)
  python3 dom_detective.py --test       # just report, no changes
"""

import os
import sys
import json
import time
import re
import urllib.request
import subprocess
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent
EXECUTOR_FILE = BASE_DIR / "google_flights_executor.py"
GUARDIAN_STATUS_URL = "http://127.0.0.1:9230/status"
LOG_FILE = BASE_DIR / "logs" / "dom_detective.log"

# Route to test (needs to be a route that always returns results)
TEST_ORIGIN = "FOR"
TEST_DEST = "PVH"
TEST_DATE = "2026-06-16"

# Current selectors (should match what's in google_flights_executor.py)
CURRENT_SELECTORS = [
    ".mxvQLc",
    ".BVAVmf",
    ".POX3ye",
    ".jLMuyc",
]

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"[{ts}] {msg}"
    print(line)
    LOG_FILE.parent.mkdir(exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def get_guardian_ws() -> str | None:
    """Get the Chrome guardian WebSocket endpoint."""
    try:
        resp = urllib.request.urlopen(GUARDIAN_STATUS_URL, timeout=5)
        data = json.loads(resp.read().decode())
        inst = data.get("instances", [{}])[0]
        return inst.get("ws_endpoint") if inst.get("alive") else None
    except Exception as e:
        log(f"guardian_unreachable: {e}")
        return None


def find_cards(ws: str) -> dict[str, int]:
    """Find flight cards using various selectors. Returns {selector: count}."""
    try:
        os.environ["GOOGLE_FLIGHTS_EXECUTOR_HEADLESS"] = "1"
        os.environ["GOOGLE_FLIGHTS_USE_GUARDIAN"] = "1"
        os.environ["GOOGLE_PERSISTENT_PROFILE_DIR"] = str(BASE_DIR / "google_session")

        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(ws)
            ctx = browser.new_context()
            page = ctx.new_page()
            page.set_default_timeout(30000)

            url = f"https://www.google.com/travel/flights/search?q={TEST_ORIGIN}+to+{TEST_DEST}+{TEST_DATE}+one+way&hl=pt-BR&gl=BR&curr=BRL"
            page.goto(url)

            try:
                page.wait_for_function('document.body.innerText.includes("R$")', timeout=15000)
                time.sleep(3)
            except Exception:
                log("timeout_waiting_results")

            # Click "Melhor opção" to ensure expanded view
            for label in ["Melhor opção", "Principais voos"]:
                try:
                    btn = page.get_by_text(label, exact=False).first
                    btn.click(timeout=3000)
                    time.sleep(2)
                except Exception:
                    pass

            # Try each current selector
            results = {}
            for sel in CURRENT_SELECTORS:
                try:
                    cards = page.locator(sel)
                    count = cards.count()
                    if count > 0:
                        txt = cards.nth(0).inner_text(timeout=1000)[:100]
                        has_price = "R$" in txt or "$" in txt
                        log(f"selector_ok: {sel} → {count} cards (has_price={has_price})")
                    results[sel] = count
                except Exception as e:
                    log(f"selector_error: {sel} → {e}")
                    results[sel] = 0

            ctx.close()
            return results

    except Exception as e:
        log(f"find_cards_error: {e}")
        return {sel: -1 for sel in CURRENT_SELECTORS}


def discover_new_selectors(ws: str) -> list[str]:
    """Scan DOM for new card selector candidates."""
    try:
        os.environ["GOOGLE_FLIGHTS_EXECUTOR_HEADLESS"] = "1"
        os.environ["GOOGLE_FLIGHTS_USE_GUARDIAN"] = "1"
        os.environ["GOOGLE_PERSISTENT_PROFILE_DIR"] = str(BASE_DIR / "google_session")

        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(ws)
            ctx = browser.new_context()
            page = ctx.new_page()
            page.set_default_timeout(30000)

            url = f"https://www.google.com/travel/flights/search?q={TEST_ORIGIN}+to+{TEST_DEST}+{TEST_DATE}+one+way&hl=pt-BR&gl=BR&curr=BRL"
            page.goto(url)

            try:
                page.wait_for_function('document.body.innerText.includes("R$")', timeout=15000)
                time.sleep(3)
            except Exception:
                pass

            for label in ["Melhor opção", "Principais voos"]:
                try:
                    page.get_by_text(label, exact=False).first.click(timeout=3000)
                    time.sleep(2)
                except Exception:
                    pass

            # Discover: elements with flight info (time + R$ or airline)
            candidates = page.evaluate('''() => {
                const found = {};
                const all = document.querySelectorAll("*");
                for (const el of all) {
                    const t = el.textContent || "";
                    // Skip tiny elements
                    if (t.length < 15) continue;
                    const hasCurrency = /R\\$|\\$|€/.test(t);
                    const hasTime = /\\d{1,2}:\\d{2}/.test(t);
                    const hasAirline = /Azul|Gol|LATAM|COPA|American|United|Avianca/i.test(t);
                    if (!hasCurrency && !hasTime) continue;
                    if (!hasAirline) continue;

                    const classes = el.className?.toString()?.trim();
                    if (!classes) continue;
                    for (const cls of classes.split(" ")) {
                        if (cls.length >= 5 && cls.length <= 12) {
                            found[cls] = (found[cls] || 0) + 1;
                        }
                    }
                }
                // Return classes sorted by frequency
                return Object.entries(found)
                    .filter(([_,c]) => c >= 2)
                    .sort((a,b) => b[1] - a[1])
                    .map(([c,_]) => "." + c);
            }''')

            ctx.close()

            # Filter out known non-card classes
            exclude = {"ghyPEc", "IqBfM", "tQj5Y", "VUoKZ", "TRHLAc", "CQYfx",
                       "EIlDfe", "EWZcud", "Fvk98b", "LcUz9d"}
            new_selectors = [s for s in candidates if s[1:] not in exclude]

            log(f"discovered {len(new_selectors)} candidates: {new_selectors[:5]}")
            return new_selectors

    except Exception as e:
        log(f"discover_error: {e}")
        return []


def update_executor_selectors(new_selectors: list[str], dry_run: bool = True) -> bool:
    """Update candidate_locators in google_flights_executor.py."""
    if not new_selectors:
        return False

    content = EXECUTOR_FILE.read_text()
    pattern = r'(candidate_locators\s*=\s*\[)(.*?)(\])'

    match = re.search(pattern, content, re.DOTALL)
    if not match:
        log("could_not_find_candidate_locators")
        return False

    # Build new list, keeping proven selectors first
    proven = [s for s in CURRENT_SELECTORS if s not in new_selectors]
    all_selectors = new_selectors + proven

    selector_lines = "\n        ".join(f'"{s}",' for s in all_selectors)
    new_block = f'candidate_locators = [\n        {selector_lines}\n    ]'

    new_content = content[:match.start()] + new_block + content[match.end():]

    if dry_run:
        log(f"DRY_RUN: would update selectors to: {all_selectors}")
        return True

    EXECUTOR_FILE.write_text(new_content)
    log(f"UPDATED: selectors → {all_selectors}")
    return True


def notify_telegram(message: str) -> None:
    """Send notification to admin Telegram."""
    try:
        import urllib.parse
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        admin_chat = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")
        if not token or not admin_chat:
            return
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=f"chat_id={admin_chat}&text={urllib.parse.quote(message)}".encode(),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


def main():
    import argparse
    parser = argparse.ArgumentParser(description="DOM Detective for Google Flights")
    parser.add_argument("--apply", action="store_true", help="Apply detected changes")
    parser.add_argument("--test", action="store_true", help="Test only, no changes")
    args = parser.parse_args()

    dry_run = not args.apply
    log(f"starting dry_run={dry_run}")

    # 1. Get Chrome connection
    ws = get_guardian_ws()
    if not ws:
        log("no_chrome_guardian")
        return 1

    # 2. Try current selectors
    results = find_cards(ws)
    working = {s: c for s, c in results.items() if c > 0}

    if working:
        log(f"selectors_ok: {len(working)} working, {len(results)-len(working)} dead")
        if args.test:
            for s, c in working.items():
                print(f"  ✓ {s}: {c} cards")
            for s, c in results.items():
                if c == 0:
                    print(f"  ✗ {s}: 0 cards")
        return 0

    # 3. All selectors failed — discover new ones
    log("ALL_SELECTORS_DEAD — discovering new selectors")
    notify_telegram("🚨 Google Flights mudou o DOM — todos os seletores falharam. Buscando novos...")

    ws2 = get_guardian_ws()
    if not ws2:
        log("no_chrome_guardian_retry")
        return 1

    new_selectors = discover_new_selectors(ws2)

    if not new_selectors:
        log("no_new_selectors_found")
        notify_telegram("❌ DOM Detective: Nenhum seletor novo encontrado. Intervenção manual necessária!")
        return 1

    # 4. Update executor
    if update_executor_selectors(new_selectors, dry_run=dry_run):
        msg = f"✅ Seletores atualizados: {', '.join(new_selectors[:3])}"
        log(msg)
        if not dry_run:
            # Restart the service
            try:
                subprocess.run(["sudo", "systemctl", "restart", "vooindo"], timeout=15, capture_output=True)
                log("service_restarted")
                msg += " | Serviço reiniciado."
            except Exception as e:
                log(f"restart_failed: {e}")
            notify_telegram(msg)
    else:
        log("update_failed")

    return 0


if __name__ == "__main__":
    sys.exit(main())
