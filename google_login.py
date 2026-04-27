"""
Script para autenticar o Chrome profile do bot no Google.
Execute: sudo -u ubuntu /opt/vooindo/.venv/bin/python /opt/vooindo/google_login.py
"""
import subprocess, sys, os, time, getpass
from pathlib import Path

print("[*] Iniciando display virtual...")
xvfb = subprocess.Popen(
    ['Xvfb', ':98', '-screen', '0', '1280x900x24'],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
)
time.sleep(1.5)
os.environ['DISPLAY'] = ':98'

sys.path.insert(0, '/opt/vooindo')
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

SESSION_DIR = Path('/opt/vooindo/google_session')
DUMP_DIR = Path('/opt/vooindo/debug_dumps')
DUMP_DIR.mkdir(exist_ok=True)

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'

def screenshot(page, name):
    p = DUMP_DIR / f'login_{name}.png'
    try:
        page.screenshot(path=str(p), full_page=False)
        print(f"    screenshot: {p}")
    except Exception as e:
        print(f"    screenshot falhou: {e}")

def get_body(page):
    try:
        return page.locator('body').inner_text(timeout=3000).lower()
    except Exception:
        return ''

password = getpass.getpass("[?] Senha do jrteles.moreira@gmail.com: ")

print("[*] Abrindo Chrome...")
with sync_playwright() as p:
    ctx = p.chromium.launch_persistent_context(
        str(SESSION_DIR),
        headless=False,
        slow_mo=80,
        locale='pt-BR',
        user_agent=USER_AGENT,
        viewport={'width': 1280, 'height': 900},
        args=[
            '--disable-blink-features=AutomationControlled',
            '--disable-gpu',
            '--disable-dev-shm-usage',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-infobars',
            '--window-position=0,0',
            '--ignore-certifcate-errors',
            '--ignore-certifcate-errors-spki-list',
        ],
    )
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    Stealth().apply_stealth_sync(page)
    page.set_default_timeout(30000)

    page.goto('https://accounts.google.com/signin', wait_until='domcontentloaded')
    time.sleep(2)
    screenshot(page, '01_start')

    # Step 1: Account chooser — click on existing account
    body = get_body(page)
    if 'escolha uma conta' in body or 'choose an account' in body or 'accountchooser' in page.url:
        print("[*] Tela 'Escolha uma conta' detectada — clicando na conta existente...")
        try:
            # Click the account row (first li or div containing the email)
            account = page.locator('li').filter(has_text='jrteles.moreira@gmail.com').first
            if account.count() == 0:
                account = page.locator('[data-email]').first
            if account.count() == 0:
                # Fallback: click first account-like element
                account = page.locator('div[role="link"]').first
            account.click()
            time.sleep(2.5)
            screenshot(page, '02_after_account_click')
        except Exception as e:
            print(f"[!] Erro ao clicar na conta: {e}")
            screenshot(page, '02_error_click')

    # Step 2: Email field (if shown — skip hidden/aria-hidden fields)
    body = get_body(page)
    email_input = page.locator('input[type="email"]:visible')
    if email_input.count() > 0:
        print("[*] Campo de email detectado — preenchendo...")
        email_input.first.fill('jrteles.moreira@gmail.com')
        time.sleep(0.5)
        page.keyboard.press('Enter')
        time.sleep(2.5)
        screenshot(page, '03_after_email')

    # Step 3: Password
    body = get_body(page)
    pwd_input = page.locator('input[type="password"]')
    if pwd_input.count() > 0:
        print("[*] Campo de senha detectado — preenchendo...")
        pwd_input.first.fill(password)
        time.sleep(0.5)
        page.keyboard.press('Enter')
        time.sleep(3)
        screenshot(page, '04_after_password')
    else:
        print(f"[!] Campo de senha não encontrado. URL: {page.url[:80]}")
        screenshot(page, '04_no_password_field')

    # Step 4: Handle challenges (2FA, confirm, etc.)
    for attempt in range(12):
        time.sleep(2)
        url = page.url
        body = get_body(page)
        print(f"[*] Step {attempt+1} | URL: {url[:70]}")
        screenshot(page, f'05_step_{attempt:02d}')

        # 2FA / verification code
        if any(k in body for k in ['verificação em duas', '2-step', 'código', 'confirme seu telefone',
                                     'autenticador', 'authenticator', 'código de verificação', 'totp']):
            print("[*] 2FA detectado!")
            code = input("[?] Código 2FA/verificação: ").strip()
            if code:
                try:
                    code_input = page.locator('input[type="tel"], input[name="totpPin"], input[type="number"], input[autocomplete="one-time-code"]')
                    if code_input.count() > 0:
                        code_input.first.fill(code)
                    else:
                        page.keyboard.type(code)
                    time.sleep(0.5)
                    page.keyboard.press('Enter')
                    time.sleep(3)
                except Exception as ex:
                    print(f"[!] Erro 2FA: {ex}")
            continue

        # "Continue" / "Avançar" button
        if any(k in body for k in ['avançar', 'continuar', 'next', 'continue']) and 'accounts.google' in url:
            try:
                btn = page.locator('button:has-text("Avançar"), button:has-text("Continuar"), button:has-text("Next")').first
                if btn.count() > 0:
                    btn.click()
                    time.sleep(2)
                    continue
            except Exception:
                pass

        # Success
        if 'myaccount.google.com' in url:
            print("[✓] Redirecionado para myaccount — login OK!")
            break
        if 'google.com' in url and 'accounts' not in url and 'signin' not in url:
            print("[✓] Fora do accounts — provável sucesso!")
            break
        if 'accounts.google' not in url and 'signin' not in url:
            print(f"[✓] URL fora do signin: {url[:80]}")
            break

    # Final verification
    print("\n[*] Verificando autenticação final...")
    page.goto('https://www.google.com/', wait_until='domcontentloaded')
    time.sleep(2.5)
    screenshot(page, '99_final')

    profile_selectors = [
        'a[aria-label*="Conta do Google"]',
        'a[aria-label*="Google Account"]',
        'img[alt*="Foto do perfil"]',
        'img[alt*="Profile picture"]',
        '[data-ogsr-up]',
    ]
    found = None
    for sel in profile_selectors:
        try:
            if page.locator(sel).count() > 0:
                found = sel
                break
        except Exception:
            pass

    body = get_body(page)
    has_login = any(k in body for k in ['fazer login', 'entrar', 'sign in'])
    score = (1 if found else 0) + (1 if not has_login else 0)

    print(f"\n{'='*40}")
    print(f"auth_score: {score}/2")
    print(f"profile_selector: {found}")
    print(f"login_prompt: {has_login}")
    if score == 2:
        print("[✓] SUCESSO! Agências vão aparecer na próxima pesquisa.")
    elif score == 1 and not has_login:
        print("[~] Sessão parcial — sem prompt de login mas sem foto de perfil.")
        print("    Verifique screenshot login_99_final.png")
    else:
        print("[✗] Login não concluído. Veja screenshots login_05_step_*.png")
    print(f"{'='*40}")

    ctx.close()

xvfb.terminate()
print("\n[*] Pronto.")
