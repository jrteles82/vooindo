#!/opt/vooindo/.venv/bin/python
"""
Login automático no Google usando Firefox headless.
Cria sessão persistente para o Firefox no diretório google_session_firefox/.
"""
import os, sys, json, time, getpass, re
from pathlib import Path

os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(Path.home() / ".cache/ms-playwright"))
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from playwright_stealth import Stealth

SESSION_DIR = Path('/opt/vooindo/google_session_firefox')
USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64; rv:146.0) Gecko/20100101 Firefox/146.0'

print("[*] Abrindo Firefox...")
with sync_playwright() as p:
    ctx = p.firefox.launch_persistent_context(
        str(SESSION_DIR),
        headless=False,
        slow_mo=80,
        locale='pt-BR',
        viewport={'width': 1280, 'height': 900},
        args=['-no-remote'],
    )
    
    # Determinar qual página usar
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    
    # Stealth básico (sem opções Chrome-specific)
    Stealth(
        chrome_app=False, chrome_csi=False, chrome_load_times=False,
        chrome_runtime=False, sec_ch_ua=False
    ).apply_stealth_sync(page)
    
    page.set_default_timeout(30000)
    
    # Navegar para o Google Flights
    print("[*] Indo para google.com/travel/flights...")
    page.goto("https://www.google.com/travel/flights?hl=pt-BR", wait_until="domcontentloaded")
    time.sleep(2)
    
    # Verificar se já está logado
    cookies = ctx.cookies()
    session_cookies = [c for c in cookies if c['name'] in ('SAPISID', 'SSID', 'APISID', 'SID', 'HSID', '__Secure-1PSID', '__Secure-3PSID')]
    
    if session_cookies:
        print(f"[✓] Já autenticado! ({len(session_cookies)} cookies de sessão)")
        ctx.close()
        sys.exit(0)
    
    print("[!] Não autenticado. Abrindo página de login...")
    page.goto("https://accounts.google.com/signin/v2/identifier?hl=pt-BR&flowName=GlifWebSignIn&flowEntry=ServiceLogin", wait_until="domcontentloaded")
    time.sleep(2)
    
    print("[*] Login manual necessário. Complete o login no navegador aberto.")
    print("[*] Quando terminar, volte ao terminal e pressione Enter...")
    input()
    
    # Verificar se logou
    time.sleep(2)
    cookies = ctx.cookies()
    session_cookies = [c for c in cookies if c['name'] in ('SAPISID', 'SSID', 'APISID', 'SID', 'HSID', '__Secure-1PSID', '__Secure-3PSID')]
    
    if session_cookies:
        print(f"[✓] Login OK! ({len(session_cookies)} cookies de sessão salvos)")
    else:
        print("[!] Ainda sem cookies de sessão. Pressione Enter quando terminar o login...")
        input()
        cookies = ctx.cookies()
        session_cookies = [c for c in cookies if c['name'] in ('SAPISID', 'SSID', 'APISID', 'SID', 'HSID', '__Secure-1PSID', '__Secure-3PSID')]
        print(f"[{'✓' if session_cookies else '✗'}] Cookies: {len(session_cookies)}")
    
    ctx.close()

if session_cookies:
    print(f"\n[✓] Sessão Firefox salva em: {SESSION_DIR}")
else:
    print(f"\n[✗] Login falhou. Tente novamente.")
    sys.exit(1)
