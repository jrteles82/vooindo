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
from pathlib import Path

# Flush imediato em cada print para bot ler linha a linha
_real_print = print
def print(*args, **kwargs):  # noqa: A001
    kwargs.setdefault('flush', True)
    _real_print(*args, **kwargs)

DISPLAY_NUM = ':99'
SESSION_DIR = Path('/opt/vooindo/google_session')
DUMP_DIR = Path('/opt/vooindo/debug_dumps')
DUMP_DIR.mkdir(exist_ok=True)

print('STATUS:STEP:Iniciando display virtual...')
xvfb = subprocess.Popen(
    ['Xvfb', DISPLAY_NUM, '-screen', '0', '1280x900x24'],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
)
time.sleep(1.5)
os.environ['DISPLAY'] = DISPLAY_NUM

sys.path.insert(0, '/opt/vooindo')
from playwright.sync_api import sync_playwright  # noqa: E402
from google_session_sync import purge_chrome_singleton_artifacts, is_profile_in_use  # noqa: E402


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


def _read_stdin_line() -> str:
    line = sys.stdin.readline()
    return line.rstrip('\n').strip()


# Lê senha do stdin
password = _read_stdin_line()
if not password:
    print('STATUS:ERROR:Senha vazia recebida')
    xvfb.terminate()
    sys.exit(1)

print('STATUS:STEP:Abrindo Chrome...')
purge_chrome_singleton_artifacts(SESSION_DIR)
try:
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            str(SESSION_DIR),
            channel='chrome',
            headless=False,
            slow_mo=80,
            locale='pt-BR',
            viewport={'width': 1280, 'height': 900},
            args=['--disable-blink-features=AutomationControlled'],
        )
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.set_default_timeout(30000)

        page.goto('https://accounts.google.com/signin', wait_until='domcontentloaded')
        time.sleep(2)
        _screenshot(page, '01_start')

        # Account chooser
        body = _get_body(page)
        if 'escolha uma conta' in body or 'choose an account' in body or 'accountchooser' in page.url:
            print('STATUS:STEP:Selecionando conta existente...')
            try:
                account = page.locator('li').filter(has_text='jrteles.moreira@gmail.com').first
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
            email_input.first.fill('jrteles.moreira@gmail.com')
            time.sleep(0.5)
            page.keyboard.press('Enter')
            time.sleep(2.5)
            _screenshot(page, '03_after_email')

        # Password — se não aparecer, faz logout forçado e tenta de novo
        pwd_input = page.locator('input[type="password"]')
        if pwd_input.count() > 0:
            print('STATUS:STEP:Preenchendo senha...')
            pwd_input.first.fill(password)
            time.sleep(0.5)
            page.keyboard.press('Enter')
            time.sleep(3)
            _screenshot(page, '04_after_password')
        else:
            print(f'STATUS:STEP:Campo de senha não encontrado (URL: {page.url[:80]}). Limpando cookies e tentando de novo...')
            _screenshot(page, '04_no_password_field')
            # Limpa todos os cookies do perfil para forçar reautenticação real
            ctx.clear_cookies()
            time.sleep(0.5)
            page.goto('https://accounts.google.com/signin', wait_until='domcontentloaded')
            time.sleep(2)
            _screenshot(page, '04_after_clear_cookies')
            # Email field (agora DEVE aparecer)
            email_input = page.locator('input[type="email"]:visible')
            if email_input.count() > 0:
                print('STATUS:STEP:Preenchendo email...')
                email_input.first.fill('jrteles.moreira@gmail.com')
                time.sleep(0.5)
                page.keyboard.press('Enter')
                time.sleep(2.5)
                _screenshot(page, '04_after_email_retry')
            # Password field (agora DEVE aparecer)
            time.sleep(2)
            pwd_input = page.locator('input[type="password"]')
            if pwd_input.count() > 0:
                print('STATUS:STEP:Preenchendo senha...')
                pwd_input.first.fill(password)
                time.sleep(0.5)
                page.keyboard.press('Enter')
                time.sleep(3)
                _screenshot(page, '04_after_password_retry')
            else:
                print('STATUS:ERROR:Campo de senha não apareceu mesmo limpando cookies')
                _screenshot(page, '04_still_no_password')

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
                code = _read_stdin_line()
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

        print(f'STATUS:AUTH_SCORE:{score}')

except Exception as exc:
    print(f'STATUS:ERROR:{exc}')
    print('STATUS:AUTH_SCORE:0')
finally:
    xvfb.terminate()
