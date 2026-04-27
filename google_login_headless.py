#!/opt/vooindo/.venv/bin/python
"""
Tentativa de login headless no Google — sem Xvfb, sem display.
Se falhar, tenta novamente com um perfil limpo.
"""
import sys, os, time
sys.path.insert(0, '/opt/vooindo')

from playwright.sync_api import sync_playwright
from google_session_sync import purge_chrome_singleton_artifacts

SESSION_DIR = '/opt/vooindo/google_session'

USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36'

def step(msg):
    print(f'STATUS:STEP:{msg}', flush=True)

email = 'vooindo.bot@gmail.com'
password = 'rcwv jvmu yyyx okto'

# Lê senha do stdin (pra compatibilidade com o bot)
password_from_stdin = ''
import select
r, _, _ = select.select([sys.stdin], [], [], 3)
if r:
    password_from_stdin = sys.stdin.readline().strip()
if password_from_stdin:
    password = password_from_stdin

step('Limpando singletons...')
from pathlib import Path
purge_chrome_singleton_artifacts(Path(SESSION_DIR))

step('Abrindo Chrome headless...')
with sync_playwright() as p:
    ctx = p.chromium.launch_persistent_context(
        SESSION_DIR,
        headless=True,
        user_agent=USER_AGENT,
        locale='pt-BR',
        timezone_id='America/Porto_Velho',
        viewport={'width': 1280, 'height': 900},
        args=[
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-dev-shm-usage',
        ],
    )
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    page.set_default_timeout(30000)
    
    step('Acessando accounts.google.com...')
    page.goto('https://accounts.google.com/signin/v2/identifier?hl=pt-BR&flowName=GlifWebSignIn&flowEntry=ServiceLogin', wait_until='domcontentloaded')
    time.sleep(2)
    
    body = page.locator('body').inner_text(timeout=3000)
    if 'não foi possível' in body.lower() or 'not secure' in body.lower() or 'rejected' in body.lower():
        step(f'Google rejeitou: {body[:200]}')
        print('STATUS:AUTH_SCORE:0')
        ctx.close()
        sys.exit(1)
    
    # Tenta preencher email
    try:
        email_input = page.locator('input[type="email"], input[name="identifier"]')
        email_input.wait_for(timeout=5000)
        email_input.fill(email)
        step('Email preenchido')
        page.click('#identifierNext')
        time.sleep(3)
    except Exception as e:
        step(f'Campo de email não encontrado: {e}')
        print('STATUS:AUTH_SCORE:0')
        ctx.close()
        sys.exit(1)
    
    body2 = page.locator('body').inner_text(timeout=3000)
    if 'não foi possível' in body2.lower() or 'rejected' in body2.lower():
        step(f'Google rejeitou após email: {body2[:200]}')
        print('STATUS:AUTH_SCORE:0')
        ctx.close()
        sys.exit(1)
    
    # Tenta preencher senha
    try:
        pass_input = page.locator('input[type="password"], input[name="Passwd"]')
        pass_input.wait_for(timeout=5000)
        pass_input.fill(password)
        step('Senha preenchida')
        page.click('#passwordNext')
        time.sleep(3)
    except Exception as e:
        step(f'Campo de senha não encontrado: {e}')
        print('STATUS:NEED_2FA')
        # Google pediu 2FA - usuário precisa confirmar no celular
        step('Google solicitou 2FA. Confirme no celular e digite OK...')
        # Aguarda confirmação
        for i in range(30):
            time.sleep(2)
            current_url = page.url
            body3 = page.locator('body').inner_text(timeout=2000)
            if 'myaccount' in current_url or 'myaccount' in body3 or 'Veri' in body3 and 'ficação' in body3:
                step('2FA concluído!')
                break
            if i % 5 == 0:
                step(f'Aguardando confirmação 2FA ({i*2}s)...')
        else:
            step('Timeout aguardando 2FA')
    
    # Verifica se logou
    page.goto('https://google.com', wait_until='domcontentloaded', timeout=15000)
    time.sleep(2)
    body_final = page.locator('body').inner_text(timeout=3000)
    
    if 'fazer login' not in body_final.lower():
        step('Login parece OK!')
        print('STATUS:AUTH_SCORE:2')
    else:
        step('Sessão ainda não autenticada')
        print('STATUS:AUTH_SCORE:1')
    
    ctx.close()
