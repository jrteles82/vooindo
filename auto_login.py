#!/opt/vooindo/.venv/bin/python
"""Tenta login automático no Google. Se falhar, aguarda manual."""
import os, sys, subprocess, time, re
from pathlib import Path

os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH",
    str(Path(__file__).resolve().parent / ".cache/ms-playwright"))
os.environ.setdefault("DISPLAY", ":99")

BASE = Path(__file__).resolve().parent
SESSION_DIR = BASE / "google_session"

for f in SESSION_DIR.glob("Singleton*"):
    try: f.unlink()
    except: pass

sys.path.insert(0, str(BASE))
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    ctx = p.chromium.launch_persistent_context(
        str(SESSION_DIR), headless=False, slow_mo=0,
        locale="pt-BR", viewport={"width": 1280, "height": 900},
        args=["--disable-blink-features=AutomationControlled",
              "--disable-gpu", "--disable-dev-shm-usage",
              "--no-sandbox", "--disable-infobars"],
    )
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    
    from playwright_stealth import Stealth
    Stealth().apply_stealth_sync(page)

    print("📄 Navegando para accounts.google.com...")
    page.goto("https://accounts.google.com/", wait_until="networkidle", timeout=30000)
    time.sleep(3)

    url = page.url
    print(f"📍 URL atual: {url}")

    # Detecta se já está logado
    if "myaccount" in url or "signin" not in url.lower():
        print("✅ Já está logado!")
        ctx.close()
        print("Sessão OK! systemctl start vooindo.service")
        sys.exit(0)

    # Tenta preencher email
    print("📝 Tentando preencher email...")
    email_field = page.locator('input[type="email"]').first
    if not email_field.is_visible(timeout=3000):
        email_field = page.locator('#identifierId').first

    if email_field.is_visible(timeout=5000):
        email_field.fill("vooindo.bot@gmail.com")
        time.sleep(1)
        page.keyboard.press("Enter")
        print("✉️ Email enviado, aguardando senha...")
        time.sleep(5)
    else:
        print("⚠️ Campo de email não encontrado")
        page.screenshot(path="/tmp/login_debug.png")
    
    # Salva screenshot para debug
    page.screenshot(path="/tmp/login_debug.png")
    
    # Tenta senha
    try:
        pwd = page.locator('input[type="password"]').first
        if pwd.is_visible(timeout=5000):
            pwd.fill("Vooindo#8212")
            time.sleep(1)
            page.keyboard.press("Enter")
            print("🔑 Senha enviada!")
            time.sleep(5)
    except:
        print("⚠️ Campo de senha não apareceu")
    
    page.screenshot(path="/tmp/login_debug2.png")
    
    # Verifica resultado
    url2 = page.url
    print(f"📍 URL após login: {url2}")
    page_source = page.content()
    
    with open("/tmp/login_page_source.html", "w") as f:
        f.write(page_source)

    if "signin/rejected" in page_source or "challenge" in url2.lower():
        blocked = "❌ Login barrado pelo Google (signin/rejected ou challenge)"
        print(blocked)
    elif "myaccount" in url2 or "signin" not in url2.lower():
        print("✅ ✅ ✅ LOGIN BEM SUCEDIDO!")
    else:
        print(f"⚠️ Estado incerto. URL: {url2}")

    # Se falhou, aguarda login manual
    if "signin/rejected" in page_source or "challenge" in url2.lower():
        print("⏳ Aguardando login manual via VNC (10 min timeout)...")
        from google_flights_executor import check_session_health
        deadline = time.time() + 600
        ok = False
        while time.time() < deadline:
            time.sleep(10)
            try:
                h = check_session_health(page)
                s = h.get('score', 0)
                if s >= 2:
                    ok = True
                    break
            except:
                pass
        if ok:
            print("✅ Sessão OK após login manual!")
        else:
            print("⏰ Tempo esgotado.")

    ctx.close()

if ok:
    print("\n✅ Pronto! systemctl start vooindo.service")
else:
    print("\n⚠️ Sessão não OK. Tente manual: systemctl start vooindo.service")
