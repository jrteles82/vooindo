#!/opt/vooindo/.venv/bin/python
"""
Versão Firefox do login automático no Google — usada pelo bot Telegram.
Protocolo stdout (igual ao google_login_stdin.py):
  STATUS:STEP:<texto>     → atualização de progresso
  STATUS:NEED_2FA         → aguardando código 2FA no stdin
  STATUS:AUTH_SCORE:<n>   → resultado final (0/1/2)
  STATUS:ERROR:<msg>      → erro fatal

Fluxo:
  1. Login no Google via Firefox (Xvfb, headful, menos detectado que Chrome)
  2. Extrai cookies do Firefox
  3. Injeta cookies no perfil Chrome persistente (google_session/)
  4. Sincroniza perfil Chrome para workers
  5. Corrige permissões
"""
import subprocess
import sys
import os
import time
import argparse
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(BASE_DIR / ".cache/ms-playwright"))

_real_print = print
def print(*args, **kwargs):
    kwargs.setdefault('flush', True)
    _real_print(*args, **kwargs)

parser = argparse.ArgumentParser()
parser.add_argument('--email', help='Email do Google')
args, unknown = parser.parse_known_args()

email = args.email or 'vooindo.bot@gmail.com'

DISPLAY_NUM = ":98"  # diferente do :99 do Chrome pra não conflitar
SESSION_DIR = Path('/opt/vooindo/google_session')
FIREFOX_SESSION_DIR = Path('/opt/vooindo/google_session_firefox')
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
from google_session_sync import purge_chrome_singleton_artifacts  # noqa: E402


def _screenshot(page, name: str) -> None:
    p = DUMP_DIR / f'firefox_login_{name}.png'
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


def _write_firefox_cookies_to_chrome_db(firefox_session_dir: Path, chrome_session_dir: Path) -> bool:
    """Lê cookies do Firefox SQLite e escreve direto no DB do Chrome.
    
    O add_cookies() do Playwright não persiste cookies __Host-* 
    corretamente. Escrevemos direto no SQLite do Chrome.
    """
    import sqlite3, glob
    # Firefox cookies DB
    ff_dbs = glob.glob(str(firefox_session_dir / '**' / 'cookies.sqlite'), recursive=True)
    if not ff_dbs:
        return False
    
    chrome_db = chrome_session_dir / 'Default' / 'Cookies'
    if not chrome_db.exists():
        return False
    
    try:
        # Read from Firefox
        ff_conn = sqlite3.connect(ff_dbs[0])
        ff_cur = ff_conn.cursor()
        ff_cur.execute('SELECT name, value, host, path, expiry, lastAccessed, \
            creationTime, isSecure, isHttpOnly, sameSite \
            FROM moz_cookies')
        rows = ff_cur.fetchall()
        ff_conn.close()

        if not rows:
            print('STATUS:STEP:Nenhum cookie no Firefox para transferir.')
            return False

        # Write to Chrome
        # Chrome schema: creation_utc, host_key, top_frame_site_key, name, value,
        #   encrypted_value, path, expires_utc, is_secure, is_httponly,
        #   last_access_utc, has_expires, is_persistent, priority, samesite,
        #   source_scheme, source_port, last_update_utc, source_type, has_cross_site_ancestor
        chrome_db_conn = sqlite3.connect(str(chrome_db))
        chrome_cur = chrome_db_conn.cursor()

        # Clear existing Google cookies
        chrome_cur.execute('DELETE FROM cookies WHERE host_key LIKE "%.google.com" '  
                           'OR host_key LIKE "google.com"')

        inserted = 0
        for row in rows:
            name, value, host, path, expiry, last_accessed, created, secure, httponly, samesite = row

            # Firefox expiry is in milliseconds (13-digit unix ts); Chrome uses
            # microseconds since Windows epoch (1601-01-01).
            # Chrome format = (unix_seconds + 11644473600) * 1000000
            WINDOWS_EPOCH_DELTA = 11644473600  # seconds from 1601-01-01 to 1970-01-01
            chrome_expiry = (int(expiry / 1000) + WINDOWS_EPOCH_DELTA) * 1000000 if expiry > 0 else 0
            chrome_last_access = (int(last_accessed / 1000) + WINDOWS_EPOCH_DELTA) * 1000000 if last_accessed > 0 else 0
            chrome_created = (int(created / 1000) + WINDOWS_EPOCH_DELTA) * 1000000 if created > 0 else 0

            # SameSite: Firefox uses values 0-256 (bitmask); Chrome uses -1 to 3
            if samesite == 0:
                chrome_samesite = -1  # UNSPECIFIED
            elif samesite == 1:
                chrome_samesite = 1   # STRICT
            elif samesite == 2:
                chrome_samesite = 2   # LAX
            elif samesite == 256:
                chrome_samesite = 3   # NONE
            elif samesite == 3:
                chrome_samesite = 3   # NONE
            elif samesite == 4:
                chrome_samesite = 3   # NONE (Firefox 4=NoSameSite)
            else:
                chrome_samesite = -1  # UNSPECIFIED

            # Ensure path starts with /
            if not path.startswith('/'):
                path = '/' + path

            # host_key: Chrome uses leading dot for domain cookies, Firefox stores without
            if not host.startswith('.'):
                host = '.' + host

            try:
                chrome_cur.execute(
                    """INSERT OR REPLACE INTO cookies
                    (creation_utc, host_key, top_frame_site_key, name, value,
                     encrypted_value, path, expires_utc, is_secure, is_httponly,
                     last_access_utc, has_expires, is_persistent, priority, samesite,
                     source_scheme, source_port, last_update_utc, source_type,
                     has_cross_site_ancestor)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        chrome_created,       # creation_utc
                        host,                 # host_key
                        '',                   # top_frame_site_key
                        name,                 # name
                        value,                # value
                        b'',                  # encrypted_value
                        path,                 # path
                        chrome_expiry,        # expires_utc
                        int(secure),          # is_secure
                        int(httponly),        # is_httponly
                        chrome_last_access,   # last_access_utc
                        1 if expiry > 0 else 0,  # has_expires
                        1,                    # is_persistent
                        1,                    # priority
                        chrome_samesite,      # samesite
                        0,                    # source_scheme (0=UNKNOWN)
                        0,                    # source_port
                        chrome_created,       # last_update_utc
                        1,                    # source_type (1=FIRST_PARTY)
                        0,                    # has_cross_site_ancestor
                    )
                )
                inserted += 1
            except Exception as row_e:
                pass

        chrome_db_conn.commit()
        chrome_db_conn.close()

        print(f'STATUS:STEP:Cookies transferidos: {inserted} de {len(rows)} para Chrome')
        return inserted > 0
    except Exception as e:
        print(f'STATUS:STEP:Erro transferência cookies: {e}')
        import traceback
        traceback.print_exc()
        return False


# ─── Lê senha do stdin ───────────────────────────────
password = _read_stdin_line()
if not password:
    print('STATUS:ERROR:Senha vazia recebida')
    xvfb.terminate()
    sys.exit(1)


try:
    with sync_playwright() as p:
        # ─── PASSO 1: Login com Firefox ──────────────────────
        print('STATUS:STEP:Abrindo Firefox...')
        FIREFOX_SESSION_DIR.mkdir(parents=True, exist_ok=True)
        purge_chrome_singleton_artifacts(FIREFOX_SESSION_DIR)

        firefox_ctx = p.firefox.launch_persistent_context(
            str(FIREFOX_SESSION_DIR),
            headless=False,
            slow_mo=80,
            locale='pt-BR',
            timezone_id='America/Porto_Velho',
            viewport={'width': 1280, 'height': 900},
            args=['-no-remote'],
        )
        page = firefox_ctx.pages[0] if firefox_ctx.pages else firefox_ctx.new_page()
        Stealth(
            chrome_app=False, chrome_csi=False, chrome_load_times=False,
            chrome_runtime=False, sec_ch_ua=False
        ).apply_stealth_sync(page)
        page.set_default_timeout(30000)

        # Vai pro Google Flights pra ver se já tá logado
        print('STATUS:STEP:Verificando sessão existente...')
        page.goto('https://www.google.com/travel/flights?hl=pt-BR', wait_until='domcontentloaded')
        time.sleep(3)
        _screenshot(page, '00_initial')

        # Verifica se já logado pelo perfil ou cookies de sessão
        sesh_cookies = [c for c in firefox_ctx.cookies()
                        if c['name'] in ('SAPISID', 'SSID', 'APISID', 'SID', 'HSID',
                                         '__Secure-1PSID', '__Secure-3PSID',
                                         '__Host-GAPS') and c.get('value')]
        host_gaps = [c for c in sesh_cookies if c['name'] == '__Host-GAPS']
        if sesh_cookies:
            print(f'STATUS:STEP:Firefox já tem {len(sesh_cookies)} cookies de sessão (__Host-GAPS: {"✅" if host_gaps else "❌"})')

        if not host_gaps:
            # Login flow
            print('STATUS:STEP:Acessando página de login...')
            page.goto('https://accounts.google.com/signin', wait_until='domcontentloaded')
            time.sleep(3)
            _screenshot(page, '01_signin')

            body = _get_body(page)
            if 'escolha uma conta' in body or 'choose an account' in body:
                print('STATUS:STEP:Selecionando conta...')
                try:
                    account = page.locator('li').filter(has_text=email).first
                    if account.count() == 0:
                        account = page.locator('div[role="link"]').first
                    account.click()
                    time.sleep(2.5)
                    _screenshot(page, '02_account_click')
                except Exception as e:
                    print(f'STATUS:STEP:Erro ao clicar na conta: {e}')

            # Email
            email_input = page.locator('input[type="email"]:visible')
            if email_input.count() > 0:
                print('STATUS:STEP:Preenchendo email...')
                email_input.first.click()
                time.sleep(0.3)
                page.keyboard.type(email, delay=80)
                page.keyboard.press('Enter')
                time.sleep(2.5)
                _screenshot(page, '03_after_email')

            # Password
            pwd_input = page.locator('input[type="password"]')
            if pwd_input.count() > 0:
                print('STATUS:STEP:Preenchendo senha...')
                pwd_input.first.click()
                time.sleep(0.3)
                page.keyboard.type(password, delay=100)
                page.keyboard.press('Enter')
                time.sleep(3)
                _screenshot(page, '04_after_password')
            else:
                print(f'STATUS:STEP:Campo senha não encontrado ({page.url[:60]}). Verificando...')

            # Espera a página processar e trata challenges
            for attempt in range(12):
                time.sleep(2)
                url = page.url
                body = _get_body(page)
                print(f'STATUS:STEP:Passo {attempt + 1}...')
                _screenshot(page, f'05_step_{attempt:02d}')

                # Tenta senha novamente se apareceu
                pwd = page.locator('input[type="password"]')
                if pwd.count() > 0:
                    print('STATUS:STEP:Senha detectada, preenchendo...')
                    pwd.first.click(); time.sleep(0.3)
                    page.keyboard.type(password, delay=100)
                    page.keyboard.press('Enter')
                    time.sleep(3)
                    continue

                # 2FA
                if any(k in body for k in [
                    'verificação em duas', '2-step', 'código', 'autenticador',
                    'authenticator', 'código de verificação', 'totp',
                    'confirme seu telefone',
                ]):
                    print('STATUS:NEED_2FA')
                    print('STATUS:STEP:Aguardando código 2FA por 30s...')
                    code = _read_stdin_line(timeout=30)
                    if code:
                        try:
                            inp = page.locator(
                                'input[type="tel"], input[name="totpPin"], '
                                'input[type="number"], input[autocomplete="one-time-code"]'
                            )
                            if inp.count() > 0:
                                inp.first.fill(code)
                            else:
                                page.keyboard.type(code)
                            page.keyboard.press('Enter')
                            time.sleep(3)
                        except Exception as ex:
                            print(f'STATUS:STEP:Erro 2FA: {ex}')
                    continue

                # Continue / Next buttons
                if any(k in body for k in ['avançar', 'continuar', 'next', 'continue']):
                    try:
                        btn = page.locator(
                            'button:has-text("Avançar"), button:has-text("Continuar"), '
                            'button:has-text("Next"), button:has-text("Continue")'
                        ).first
                        if btn.count() > 0:
                            btn.click()
                            time.sleep(2)
                            continue
                    except Exception:
                        pass

                # Tentar de outra forma
                if 'signin/rejected' in url or 'challenge' in url:
                    for label in ['Tentar de outra forma', 'Try another way']:
                        try:
                            btn = page.get_by_text(label, exact=False).first
                            if btn.count() > 0:
                                btn.click(timeout=3000)
                                print(f'STATUS:STEP:Clicou "{label}"')
                                time.sleep(2)
                                break
                        except: pass

                # Saiu do accounts.google
                if 'accounts.google' not in url and 'signin' not in url:
                    print('STATUS:STEP:Saiu da página de login.')
                    break
                if 'myaccount.google.com' in url:
                    print('STATUS:STEP:Redirecionado myaccount — OK!')
                    break

            # Verifica cookies de sessão no Firefox
            time.sleep(2)
            sesh_cookies = [c for c in firefox_ctx.cookies()
                            if c['name'] in ('SAPISID', 'SSID', 'APISID',
                                             '__Secure-1PSID', '__Secure-3PSID',
                                             '__Host-GAPS') and c.get('value')]

            host_gaps = [c for c in sesh_cookies if c['name'] == '__Host-GAPS']
            if not host_gaps:
                print(f'STATUS:STEP:Firefox não obteve __Host-GAPS ({len(sesh_cookies)} cookies outros). Tentando navegação...')
                page.goto('https://www.google.com/', wait_until='domcontentloaded')
                time.sleep(3)
                sesh_cookies = [c for c in firefox_ctx.cookies()
                                if c['name'] in ('SAPISID', 'SSID', 'APISID',
                                                 '__Secure-1PSID', '__Secure-3PSID',
                                                 '__Host-GAPS') and c.get('value')]
                host_gaps = [c for c in sesh_cookies if c['name'] == '__Host-GAPS']

        firefox_ok = len(host_gaps) > 0

        # ─── PASSO 2: Transferir cookies para o Chrome ───────
        if firefox_ok:
            print(f'STATUS:STEP:Firefox OK ({len(sesh_cookies)} cookies de sessão). Transferindo para Chrome...')
            firefox_ctx.close()

            # Remove singletons do Chrome
            purge_chrome_singleton_artifacts(SESSION_DIR)

            # Escreve cookies do Firefox direto no DB do Chrome
            session_valid = _write_firefox_cookies_to_chrome_db(FIREFOX_SESSION_DIR, SESSION_DIR)

            if session_valid:
                # Verifica se o Chrome consegue ler os cookies injetados
                print('STATUS:STEP:Verificando sessão Chrome...')
                chrome_ctx = p.chromium.launch_persistent_context(
                    str(SESSION_DIR),
                    headless=True,
                    ignore_default_args=['--enable-automation'],
                    locale='pt-BR',
                    timezone_id='America/Porto_Velho',
                    viewport={'width': 1280, 'height': 900},
                    args=['--no-sandbox', '--disable-blink-features=AutomationControlled',
                          '--disable-gpu', '--disable-dev-shm-usage'],
                )
                chrome_page = chrome_ctx.pages[0] if chrome_ctx.pages else chrome_ctx.new_page()
                chrome_page.goto('https://www.google.com/travel/flights?hl=pt-BR', wait_until='domcontentloaded')
                time.sleep(3)
                _screenshot(chrome_page, '06_chrome_verify')

                chrome_cookies = chrome_ctx.cookies()
                chrome_host_gaps = [c for c in chrome_cookies if c['name'] == '__Host-GAPS' and c.get('value')]
                chrome_ok = len(chrome_host_gaps) > 0
                chrome_ctx.close()

                if not chrome_ok:
                    print('STATUS:STEP:Cookies no Chrome mas Chrome não os reconheceu. Continuando de qualquer forma.')

                # Sync workers
                try:
                    from google_session_sync import sync_base_session_to_worker_profiles
                    sync_base_session_to_worker_profiles(force=True, skip_in_use=False)
                    print('STATUS:STEP:Sessão sincronizada para workers.')
                except Exception as e:
                    print(f'STATUS:STEP:Aviso sync: {e}')

                # Permissões
                import subprocess as _sp
                for _d in sorted(SESSION_DIR.parent.glob('google_session*')):
                    if _d.is_dir():
                        _sp.run(['chown', '-R', 'ubuntu:ubuntu', str(_d)], capture_output=True, timeout=15)
                print('STATUS:STEP:Permissões corrigidas para ubuntu:ubuntu.')

                print('STATUS:AUTH_SCORE:2')
                sys.exit(0)
            else:
                print('STATUS:STEP:Falha ao transferir cookies para Chrome.')
                print('STATUS:AUTH_SCORE:1')
                sys.exit(0)
        else:
            firefox_ctx.close()
            print(f'STATUS:STEP:Firefox não obteve cookies suficientes ({len(sesh_cookies)}).')
            print('STATUS:AUTH_SCORE:0')
            sys.exit(0)

except Exception as exc:
    print(f'STATUS:ERROR:{exc}')
    print('STATUS:AUTH_SCORE:0')
    sys.exit(1)
finally:
    xvfb.terminate()
