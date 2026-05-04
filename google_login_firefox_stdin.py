#!/opt/vooindo/.venv/bin/python
"""
Firefox Google login - stdin/stdout protocol.
ATTENTION: This script currently DOES NOT WORK because the Google account
vooindo.bot@gmail.com is flagged. Both Chrome and Firefox automation via
Playwright hit signin/rejected. Only manual login (Chrome headful) works.

Protocol:
  STATUS:STEP:<text>     → update
  STATUS:NEED_2FA        → waiting for 2FA code on stdin
  STATUS:AUTH_SCORE:<n>  → result (0=no session, 1=partial, 2=full)
  STATUS:ERROR:<msg>     → fatal
"""
import subprocess, sys, os, time, argparse
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH",
                       str(BASE_DIR / ".cache/ms-playwright"))

_real_print = print
def print(*args, **kwargs):
    kwargs.setdefault('flush', True)
    _real_print(*args, **kwargs)

parser = argparse.ArgumentParser()
parser.add_argument('--email', help='Email do Google')
args, unknown = parser.parse_known_args()
email = args.email or 'vooindo.bot@gmail.com'

DISPLAY_NUM = ":98"
SESSION_DIR      = Path('/opt/vooindo/google_session')
FIREFOX_SESSION  = Path('/opt/vooindo/google_session_firefox')
DUMP_DIR = Path('/opt/vooindo/debug_dumps')
DUMP_DIR.mkdir(exist_ok=True)

print('STATUS:STEP:Iniciando display virtual...')
subprocess.run(['pkill', '-f', f'Xvfb.*{DISPLAY_NUM}'], capture_output=True)
xvfb = subprocess.Popen(
    ['Xvfb', DISPLAY_NUM, '-screen', '0', '1280x900x24'],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(1.5)
os.environ['DISPLAY'] = DISPLAY_NUM

sys.path.insert(0, str(BASE_DIR))
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
from google_session_sync import purge_chrome_singleton_artifacts

DUMP_DIR.mkdir(exist_ok=True)

def _ss(page, name: str):
    try: page.screenshot(path=str(DUMP_DIR / f'firefox_{name}.png'), full_page=False)
    except: pass

def body_text(page) -> str:
    try: return page.locator('body').inner_text(timeout=3000).lower()
    except: return ''

def read_stdin(timeout: int = 60) -> str:
    import select
    try:
        r, _, _ = select.select([sys.stdin], [], [], timeout)
        if r: return sys.stdin.readline().rstrip('\n').strip()
    except: pass
    return ''

def _transfer_cookies(firefox_dir: Path, chrome_dir: Path, pw_man) -> bool:
    """Copy Firefox cookies to Chrome SQLite DB."""
    import glob, sqlite3 as _sql, subprocess as _sp
    ff_dbs = glob.glob(str(firefox_dir / '**' / 'cookies.sqlite'), recursive=True)
    if not ff_dbs:
        print('STATUS:STEP:Nenhum cookie DB Firefox encontrado')
        return False
    chrome_db = chrome_dir / 'Default' / 'Cookies'
    if not chrome_db.exists():
        print('STATUS:STEP:Chrome Cookies DB não encontrado')
        return False
    try:
        ff = _sql.connect(ff_dbs[0])
        rows = ff.execute('SELECT name,value,host,path,expiry,isSecure,isHttpOnly,sameSite FROM moz_cookies').fetchall()
        ff.close()
        if not rows:
            print('STATUS:STEP:Nenhum cookie Firefox encontrado')
            return False
        WDE = 11644473600  # Windows epoch delta (seconds)
        now_chrome = (int(time.time()) + WDE) * 1000000
        ch = _sql.connect(str(chrome_db))
        ch.execute("DELETE FROM cookies WHERE host_key LIKE '%google.com'")
        ins = 0
        for name, value, host, path, expiry, secure, httponly, samesite in rows:
            if not path.startswith('/'): path = '/' + path
            if name.startswith('__Host-'):
                fix_host = host  # no leading dot for __Host- prefix
            elif not host.startswith('.'):
                fix_host = '.' + host
            else:
                fix_host = host
            ce = (int(expiry / 1000) + WDE) * 1000000 if expiry > 0 else 0
            cs = {-1: -1, 0: -1, 1: 1, 2: 2, 3: 3, 256: 3}.get(samesite, -1)
            try:
                ch.execute(
                    '''INSERT OR REPLACE INTO cookies
                    (creation_utc,host_key,top_frame_site_key,name,value,
                     encrypted_value,path,expires_utc,is_secure,is_httponly,
                     last_access_utc,has_expires,is_persistent,priority,samesite,
                     source_scheme,source_port,last_update_utc,source_type,
                     has_cross_site_ancestor)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (now_chrome, fix_host, '', name, value,
                     b'', path, ce,
                     int(secure), int(httponly),
                     now_chrome, 1 if expiry > 0 else 0, 1, 1, cs,
                     0, 0, now_chrome, 1, 0))
                ins += 1
            except: pass
        ch.commit(); ch.close()
        print(f'STATUS:STEP:Cookies transferidos: {ins}/{len(rows)}')
        if ins > 0:
            # Sync to workers
            try:
                from google_session_sync import sync_base_session_to_worker_profiles
                sync_base_session_to_worker_profiles(force=True, skip_in_use=False)
                print('STATUS:STEP:Sincronizado para workers')
            except Exception as e:
                print(f'STATUS:STEP:Erro sync: {e}')
            # Permissions
            for d in sorted(chrome_dir.parent.glob('google_session*')):
                if d.is_dir():
                    _sp.run(['chown', '-R', 'ubuntu:ubuntu', str(d)],
                            capture_output=True, timeout=15)
            print('STATUS:STEP:Permissões OK')
        return ins > 0
    except Exception as e:
        print(f'STATUS:STEP:Erro transferência: {e}')
        return False

# ─── Read password from stdin ───
password = read_stdin()
if not password:
    print('STATUS:ERROR:Senha vazia recebida')
    xvfb.terminate(); sys.exit(1)

try:
    with sync_playwright() as pw_man:
        FIREFOX_SESSION.mkdir(parents=True, exist_ok=True)
        purge_chrome_singleton_artifacts(FIREFOX_SESSION)

        print('STATUS:STEP:Abrindo Firefox...')
        firefox_ctx = pw_man.firefox.launch_persistent_context(
            str(FIREFOX_SESSION), headless=False, slow_mo=80,
            locale='pt-BR', timezone_id='America/Porto_Velho',
            viewport={'width': 1280, 'height': 900}, args=['-no-remote'])
        page = firefox_ctx.pages[0] if firefox_ctx.pages else firefox_ctx.new_page()
        Stealth(chrome_app=False, chrome_csi=False, chrome_load_times=False,
                chrome_runtime=False, sec_ch_ua=False).apply_stealth_sync(page)
        page.set_default_timeout(30000)

        # Check if already authenticated
        print('STATUS:STEP:Verificando sessão Firefox...')
        page.goto('https://www.google.com/travel/flights?hl=pt-BR',
                   wait_until='domcontentloaded')
        time.sleep(3)
        _ss(page, 'initial')

        import importlib.util
        health_spec = importlib.util.spec_from_file_location(
            'check_session_health',
            str(BASE_DIR / 'google_flights_executor.py'))
        health_mod = importlib.util.module_from_spec(health_spec)
        health_spec.loader.exec_module(health_mod)
        health = health_mod.check_session_health(page)
        score = health.get('score', 0)
        msg = health.get('message', '')

        if score >= 2:
            # Already authenticated
            print(f'STATUS:STEP:Firefox já autenticado (score {score}/3). Transferindo cookies...')
            firefox_ctx.close()
            # Transfer cookies from Firefox to Chrome
            chrome_synced = _transfer_cookies(FIREFOX_SESSION, SESSION_DIR, pw_man)
            if chrome_synced:
                print('STATUS:AUTH_SCORE:2')
            else:
                print('STATUS:AUTH_SCORE:1')
            sys.exit(0)

        # --- LOGIN FLOW ---
        print('STATUS:STEP:Acessando página de login...')
        page.goto('https://accounts.google.com/signin', wait_until='domcontentloaded')
        time.sleep(3)
        _ss(page, 'signin')

        bt = body_text(page)
        if 'escolha uma conta' in bt or 'choose an account' in bt:
            print('STATUS:STEP:Selecionando conta...')
            try:
                acc = page.locator('li').filter(has_text=email).first
                if acc.count() == 0:
                    acc = page.locator('div[role="link"]').first
                acc.click(); time.sleep(2.5)
                _ss(page, 'account')
            except: pass

        # Email
        el = page.locator('input[type="email"]:visible')
        if el.count() > 0:
            print('STATUS:STEP:Preenchendo email...')
            el.first.click(); time.sleep(0.3)
            page.keyboard.type(email, delay=80)
            page.keyboard.press('Enter')
            time.sleep(3)
            _ss(page, 'after_email')

        # Check for signin/rejected
        if 'signin/rejected' in page.url:
            _ss(page, 'rejected')
            bt = body_text(page)
            print(f'STATUS:STEP:⚠️ Google rejeitou - "{bt[:100]}"')
            print('STATUS:STEP:A conta está marcada pelo Google. Login automático inviável.')
            firefox_ctx.close()
            print('STATUS:AUTH_SCORE:0')
            sys.exit(0)

        # Password
        pwd = page.locator('input[type="password"]')
        if pwd.count() > 0:
            print('STATUS:STEP:Preenchendo senha...')
            pwd.first.click(); time.sleep(0.3)
            page.keyboard.type(password, delay=100)
            page.keyboard.press('Enter')
            time.sleep(3)
            _ss(page, 'after_password')
        else:
            print(f'STATUS:STEP:Campo senha não encontrado ({page.url[:60]})')

        # Wait for login to complete
        for attempt in range(12):
            time.sleep(2)
            url = page.url
            bt = body_text(page)
            print(f'STATUS:STEP:Passo {attempt+1}...')
            _ss(page, f'step_{attempt}')

            if 'signin/rejected' in url:
                _ss(page, 'rejected')
                print(f'STATUS:STEP:⚠️ Google rejeitou login ({bt[:100]})')
                print('STATUS:STEP:Tentar novamente manualmente: https://google.com')
                firefox_ctx.close()
                print('STATUS:AUTH_SCORE:0')
                sys.exit(0)

            pwd_new = page.locator('input[type="password"]')
            if pwd_new.count() > 0:
                print('STATUS:STEP:Senha novamente...')
                pwd_new.first.click(); time.sleep(0.3)
                page.keyboard.type(password, delay=100)
                page.keyboard.press('Enter'); time.sleep(3); continue

            if any(k in bt for k in ['verificação em duas', '2-step', 'código',
                                       'autenticador', 'totp', 'confirm']):
                print('STATUS:NEED_2FA')
                print('STATUS:STEP:Aguardando código 2FA...')
                code = read_stdin(timeout=30)
                if code:
                    inp = page.locator('input[type="tel"],input[name="totpPin"],'
                                       'input[autocomplete="one-time-code"]')
                    if inp.count() > 0: inp.first.fill(code)
                    else: page.keyboard.type(code)
                    page.keyboard.press('Enter'); time.sleep(3)
                continue

            if any(k in bt for k in ['avançar','continuar','next','continue']):
                try:
                    btn = page.locator('button').filter(has_text='Avançar').or_(
                        page.locator('button').filter(has_text='Continuar')).first
                    if btn.count() > 0: btn.click(); time.sleep(2); continue
                except: pass

            if 'accounts.google' not in url and 'signin' not in url.lower():
                print(f'STATUS:STEP:Saiu do login: {url[:60]}')
                break
            if 'myaccount' in url:
                print('STATUS:STEP:Redirecionado myaccount - OK!')
                break

        # --- Check result ---
        time.sleep(2)
        page.goto('https://www.google.com/travel/flights?hl=pt-BR',
                   wait_until='domcontentloaded')
        time.sleep(3)
        _ss(page, 'final')

        health = health_mod.check_session_health(page)
        score = health.get('score', 0)
        msg = health.get('message', '')
        print(f'STATUS:STEP:Score final: {score}/3')

        firefox_ctx.close()

        if score >= 2:
            print('STATUS:STEP:Firefox autenticado! Transferindo cookies para Chrome...')
            chrome_ok = _transfer_cookies(FIREFOX_SESSION, SESSION_DIR, pw_man)
            if chrome_ok:
                print('STATUS:AUTH_SCORE:2')
            else:
                print('STATUS:AUTH_SCORE:1')
        else:
            print(f'STATUS:STEP:Firefox não autenticou (score {score}/3)')
            print('STATUS:AUTH_SCORE:0')
        sys.exit(0)

except Exception as exc:
    print(f'STATUS:ERROR:{exc}')
    print('STATUS:AUTH_SCORE:0')
    sys.exit(1)
finally:
    xvfb.terminate()
