#!/opt/vooindo/.venv/bin/python
"""
Versão stdin/stdout do google_login.py — usada pelo bot Telegram.
Protocolo stdout:
  STATUS:STEP:<texto>     → atualização de progresso
  STATUS:NEED_2FA         → aguardando código 2FA no stdin
  STATUS:AUTH_SCORE:<n>   → resultado final (0/1/2)
  STATUS:ERROR:<msg>      → erro fatal
"""
import subprocess
import sys
import os
import time
import argparse
from pathlib import Path

# Configura path dos browsers antes de importar playwright
BASE_DIR = Path(__file__).resolve().parent
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(BASE_DIR / ".cache/ms-playwright"))

# Flush imediato em cada print para bot ler linha a linha
_real_print = print
def print(*args, **kwargs):  # noqa: A001
    kwargs.setdefault('flush', True)
    _real_print(*args, **kwargs)

parser = argparse.ArgumentParser()
parser.add_argument('--email', help='Email do Google')
parser.add_argument('--force', action='store_true', help='Forçar login mesmo que pareça logado')
args, unknown = parser.parse_known_args()

email = args.email or 'vooindo.bot@gmail.com'

DISPLAY_NUM = ":99"
SESSION_DIR = Path('/opt/vooindo/google_session')
DUMP_DIR = Path('/opt/vooindo/debug_dumps')
DUMP_DIR.mkdir(exist_ok=True)

print('STATUS:STEP:Iniciando display virtual...')
subprocess.run(['pkill', '-f', f'Xvfb.*{DISPLAY_NUM}'], capture_output=True)
xvfb = subprocess.Popen(
    ['Xvfb', DISPLAY_NUM, '-screen', '0', '1280x900x24'],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
)
time.sleep(1.5)
os.environ['DISPLAY'] = DISPLAY_NUM

sys.path.insert(0, '/opt/vooindo')
from playwright.sync_api import sync_playwright  # noqa: E402
from playwright_stealth import Stealth
from google_session_sync import purge_chrome_singleton_artifacts, is_profile_in_use  # noqa: E402

USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36'

def _screenshot(page, name: str) -> None:
    p = DUMP_DIR / f'login_{name}.png'
    try:
        page.screenshot(path=str(p), full_page=False)
    except Exception:
        pass


def _get_body(page) -> str:
    try:
        return page.locator('body').inner_text(timeout=3000).lower()
    except Exception:
        return ''


def _read_stdin_line(timeout: int = 60) -> str:
    import select
    try:
        if timeout > 0:
            r, _, _ = select.select([sys.stdin], [], [], timeout)
            if not r:
                return ''
        line = sys.stdin.readline()
        return line.rstrip('\n').strip()
    except Exception:
        return ''


# Lê senha do stdin
password = _read_stdin_line()
if not password:
    print('STATUS:ERROR:Senha vazia recebida')
    xvfb.terminate()
    sys.exit(1)

print('STATUS:STEP:Abrindo Chrome...')
purge_chrome_singleton_artifacts(SESSION_DIR)
try:
    proxy_settings = {}
    proxy_url = os.getenv('GOOGLE_FLIGHTS_PROXY')
    if proxy_url:
        proxy_settings = {'server': proxy_url}
        proxy_user = os.getenv('GOOGLE_FLIGHTS_PROXY_USER')
        proxy_pass = os.getenv('GOOGLE_FLIGHTS_PROXY_PASS')
        if proxy_user and proxy_pass:
            proxy_settings['username'] = proxy_user
            proxy_settings['password'] = proxy_pass

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            str(SESSION_DIR),
            headless=True,
            proxy=proxy_settings if proxy_settings else None,
            ignore_default_args=['--enable-automation'],
            slow_mo=80,
            locale='pt-BR',
            timezone_id='America/Porto_Velho',
            viewport={'width': 1280, 'height': 900},
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-gpu',
                '--disable-dev-shm-usage',
                '--disable-setuid-sandbox',
                '--disable-infobars',
                '--ignore-certifcate-errors',
                '--ignore-certifcate-errors-spki-list',
            ],
        )
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        Stealth().apply_stealth_sync(page)
        page.set_default_timeout(30000)

        # Vai direto para o login — pula google.com que tem mais detecção
        print('STATUS:STEP:Acessando contas Google...')
        page.goto('https://accounts.google.com/signin', wait_until='domcontentloaded')
        time.sleep(3)
        _screenshot(page, '00_google_accounts')

        time.sleep(2)
        _screenshot(page, '01_start')

        # Account chooser
        body = _get_body(page)
        if 'escolha uma conta' in body or 'choose an account' in body or 'accountchooser' in page.url:
            print('STATUS:STEP:Selecionando conta existente...')
            try:
                account = page.locator('li').filter(has_text=email).first
                if account.count() == 0:
                    account = page.locator('[data-email]').first
                if account.count() == 0:
                    account = page.locator('div[role="link"]').first
                account.click()
                time.sleep(2.5)
                _screenshot(page, '02_after_account_click')
            except Exception as e:
                print(f'STATUS:STEP:Erro ao clicar na conta: {e}')

        # Email field
        email_input = page.locator('input[type="email"]:visible')
        if email_input.count() > 0:
            print('STATUS:STEP:Preenchendo email...')
            email_input.first.click()
            time.sleep(0.5)
            page.keyboard.type(email, delay=100)
            time.sleep(0.5)
            page.keyboard.press('Enter')
            time.sleep(2.5)
            _screenshot(page, '03_after_email')

        # Password — se não aparecer, faz logout forçado e tenta de novo
        pwd_input = page.locator('input[type="password"]')
        if pwd_input.count() > 0:
            print('STATUS:STEP:Preenchendo senha...')
            pwd_input.first.click()
            time.sleep(0.5)
            page.keyboard.type(password, delay=110)
            time.sleep(0.5)
            page.keyboard.press('Enter')
            time.sleep(3)
            _screenshot(page, '04_after_password')
        else:
            print(f'STATUS:STEP:Campo de senha não encontrado (URL: {page.url[:80]}). Tentando tela de rejeição...')
            _screenshot(page, '04_no_password_field')
            rejected_handled = False
            if 'signin/rejected' in page.url or 'challenge' in page.url:
                for _ in range(8):
                    time.sleep(2)
                    body = _get_body(page)
                    print('STATUS:STEP:Tela de verificação...')
                    _screenshot(page, '04_reject_step')
                    pwd_input = page.locator('input[type="password"]')
                    if pwd_input.count() > 0:
                        print('STATUS:STEP:Campo senha apareceu!')
                        pwd_input.first.click()
                        time.sleep(0.5)
                        page.keyboard.type(password, delay=110)
                        time.sleep(0.5)
                        page.keyboard.press('Enter')
                        time.sleep(3)
                        rejected_handled = True
                        break
                    for label in ['Tentar de outra forma', 'Try another way']:
                        try:
                            btn = page.get_by_text(label, exact=False).first
                            if btn.count() > 0:
                                btn.click(timeout=3000)
                                print(f'STATUS:STEP:Clicou "{label}"')
                                time.sleep(2)
                                break
                        except: pass
                    for label in ['Continuar', 'Next', 'Avançar']:
                        try:
                            btn = page.get_by_text(label, exact=False).first
                            if btn.count() > 0:
                                btn.click(timeout=3000)
                                time.sleep(2)
                                break
                        except: pass
                    for label in ['SMS', 'Telefone', 'Phone', 'mensagem de texto']:
                        try:
                            btn = page.get_by_text(label, exact=False).first
                            if btn.count() > 0:
                                btn.click(timeout=3000)
                                print(f'STATUS:STEP:Selecionou SMS')
                                time.sleep(1)
                                for lbl2 in ['Continuar', 'Next', 'Enviar', 'Send']:
                                    try:
                                        btn2 = page.get_by_text(lbl2, exact=False).first
                                        if btn2.count() > 0:
                                            btn2.click(timeout=3000)
                                            time.sleep(1)
                                            break
                                    except: pass
                                break
                        except: pass
            if not rejected_handled:
                ctx.clear_cookies()
                time.sleep(0.5)
                page.goto('https://accounts.google.com/signin', wait_until='domcontentloaded')
                time.sleep(2)
                email_input = page.locator('input[type="email"]:visible')
                if email_input.count() > 0:
                    print('STATUS:STEP:Preenchendo email (retry)...')
                    email_input.first.click()
                    page.keyboard.type(email, delay=120)
                    page.keyboard.press('Enter')
                    time.sleep(2.5)
                time.sleep(2)
                pwd_input = page.locator('input[type="password"]')
                if pwd_input.count() > 0:
                    print('STATUS:STEP:Preenchendo senha (retry)...')
                    pwd_input.first.click()
                    page.keyboard.type(password, delay=130)
                    page.keyboard.press('Enter')
                    time.sleep(3)
                else:
                    print('STATUS:ERROR:Sem campo senha mesmo limpando cookies')

        # Handle challenges
        for attempt in range(12):
            time.sleep(2)
            url = page.url
            body = _get_body(page)
            print(f'STATUS:STEP:Verificando passo {attempt + 1}...')
            _screenshot(page, f'05_step_{attempt:02d}')

            # 2FA
            if any(k in body for k in [
                'verificação em duas', '2-step', 'código', 'confirme seu telefone',
                'autenticador', 'authenticator', 'código de verificação', 'totp',
            ]):
                print('STATUS:NEED_2FA')
                print('STATUS:STEP:Aguardando Google Prompt por 30s...')
                code = _read_stdin_line(timeout=30)
                if code:
                    try:
                        code_input = page.locator(
                            'input[type="tel"], input[name="totpPin"], '
                            'input[type="number"], input[autocomplete="one-time-code"]'
                        )
                        if code_input.count() > 0:
                            code_input.first.fill(code)
                        else:
                            page.keyboard.type(code)
                        time.sleep(0.5)
                        page.keyboard.press('Enter')
                        time.sleep(3)
                    except Exception as ex:
                        print(f'STATUS:STEP:Erro 2FA: {ex}')
                else:
                    print('STATUS:STEP:Timeout 2FA, verificando se Google Prompt resolveu...')
                continue

            # Continue/Next button
            if any(k in body for k in ['avançar', 'continuar', 'next', 'continue']) and 'accounts.google' in url:
                try:
                    btn = page.locator(
                        'button:has-text("Avançar"), button:has-text("Continuar"), button:has-text("Next")'
                    ).first
                    if btn.count() > 0:
                        btn.click()
                        time.sleep(2)
                        continue
                except Exception:
                    pass

            # Success checks
            if 'myaccount.google.com' in url:
                print('STATUS:STEP:Redirecionado para myaccount — login OK!')
                break
            if 'google.com' in url and 'accounts' not in url and 'signin' not in url:
                print('STATUS:STEP:Login concluído!')
                break
            if 'accounts.google' not in url and 'signin' not in url:
                break

        # Final verification
        print('STATUS:STEP:Verificando sessão final...')
        page.goto('https://www.google.com/', wait_until='domcontentloaded')
        time.sleep(2.5)
        _screenshot(page, '99_final')

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

        body = _get_body(page)
        has_login_prompt = any(k in body for k in ['fazer login', 'entrar', 'sign in'])
        score = (1 if found else 0) + (1 if not has_login_prompt else 0)

        ctx.close()

        # Sync session to worker profiles
        if score >= 1:
            try:
                from google_session_sync import sync_base_session_to_worker_profiles
                sync_base_session_to_worker_profiles(force=True, skip_in_use=False)
                print('STATUS:STEP:Sessão sincronizada para workers.')
            except Exception as e:
                print(f'STATUS:STEP:Aviso: sync falhou: {e}')
        
        # Garantir permissão ubuntu em TODAS as sessões (base + workers)
        import subprocess as _sp
        for _d in sorted(SESSION_DIR.parent.glob('google_session*')):
            if _d.is_dir():
                _sp.run(['chown', '-R', 'ubuntu:ubuntu', str(_d)], capture_output=True, timeout=15)
        print('STATUS:STEP:Permissões corrigidas para ubuntu:ubuntu (base + workers).')

        print(f'STATUS:AUTH_SCORE:{score}')

except Exception as exc:
    print(f'STATUS:ERROR:{exc}')
    print('STATUS:AUTH_SCORE:0')
finally:
    xvfb.terminate()
