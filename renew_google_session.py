#!/opt/vooindo/.venv/bin/python
"""
Renova a sessão Google abrindo o Chrome visualmente na pasta google_session/.

Como usar:
    .venv/bin/python renew_google_session.py

O Chrome vai abrir em modo headful. Faça login na sua conta Google,
aguarde a página carregar normalmente e pressione Enter no terminal.
A sessão fica salva no perfil persistente e será usada pelo bot.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(Path(__file__).with_name(".playwright-browsers")))

try:
    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth
except ImportError:
    print("Dependências faltando. Execute: .venv/bin/pip install playwright playwright-stealth")
    raise SystemExit(1)

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'

BASE_DIR = Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / "google_session"

from google_session_sync import sync_base_session_to_worker_profiles


def check_health(page) -> dict:
    score = 0
    profile_selectors = [
        'a[aria-label*="Conta do Google"]',
        'a[aria-label*="Google Account"]',
        'img[alt*="Foto do perfil"]',
        'img[alt*="Profile picture"]',
        '[data-ogsr-up]',
    ]
    for sel in profile_selectors:
        try:
            if page.locator(sel).count() > 0:
                score += 1
                break
        except Exception:
            pass
    try:
        page.goto("https://myaccount.google.com/", wait_until="domcontentloaded", timeout=8000)
        body = page.locator("body").inner_text(timeout=3000)
        if "@" in body and "fazer login" not in body.lower():
            score += 1
        page.go_back(wait_until="domcontentloaded")
    except Exception:
        pass
    return {"ok": score >= 2, "score": score}


def main() -> int:
    print(f"Perfil persistente: {SESSION_DIR}")
    print("Abrindo Chrome... faça login se solicitado.\n")
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            str(SESSION_DIR),
            headless=False,
            slow_mo=0,
            locale="pt-BR",
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 900},
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-infobars",
                "--ignore-certifcate-errors",
            ],
        )
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        Stealth().apply_stealth_sync(page)
        page.goto("https://accounts.google.com/", wait_until="domcontentloaded")

        health_before = check_health(page)
        if health_before["ok"]:
            print(f"✓ Sessão já válida (score={health_before['score']}/3). Nenhuma ação necessária.")
            print("  Pressione Enter para fechar.")
        else:
            print(f"✗ Sessão inválida (score={health_before['score']}/3).")
            print("  Faça login no Chrome e pressione Enter quando terminar.")

        try:
            input()
        except (EOFError, KeyboardInterrupt):
            pass

        health_after = check_health(page)
        print(f"\nVerificação final: score={health_after['score']}/3 — {'✓ OK' if health_after['ok'] else '✗ Sessão ainda inválida'}")
        ok = health_after["ok"]
        ctx.close()

    if ok:
        copied = sync_base_session_to_worker_profiles()
        if copied:
            print("\nPerfis replicados:")
            for path in copied:
                print(f"  - {path}")

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
