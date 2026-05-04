#!/opt/vooindo/.venv/bin/python
"""
Login Google usando Chrome via subprocess + xdotool.
Nao usa Playwright - evita deteccao de automacao do Google.
Protocolo stdout (mesmo do google_login_stdin.py):
  STATUS:STEP:<texto>     → atualizacao de progresso
  STATUS:NEED_2FA         → aguardando codigo 2FA no stdin
  STATUS:AUTH_SCORE:<n>   → resultado final (0/1/2/3)
  STATUS:ERROR:<msg>      → erro fatal
"""
import os, sys, time, subprocess, shutil, pathlib, signal, json

# Ensure flush on every print
_real_print = print
def print(*a, **kw):
    kw.setdefault('flush', True)
    _real_print(*a, **kw)

DISPLAY = ':99'
BASE_DIR = pathlib.Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / 'google_session'
SESSIONS_BKP = BASE_DIR / 'google_session_bkp'
LOG_FILE = BASE_DIR / 'logs' / 'login_subprocess.log'

def log(msg):
    with open(LOG_FILE, 'a') as f:
        f.write(f'{time.strftime("%H:%M:%S")} {msg}\n')

def step(text):
    print(f'STATUS:STEP:{text}')
    log(f'STEP: {text}')

def error(msg):
    print(f'STATUS:ERROR:{msg}')
    log(f'ERROR: {msg}')

def auth_score(score):
    print(f'STATUS:AUTH_SCORE:{score}')
    sys.stdout.flush()
    log(f'AUTH_SCORE: {score}')
    # Also write to status file for reliable reading
    status_file = BASE_DIR / 'logs' / 'login_result.json'
    try:
        status_file.write_text(json.dumps({'score': score, 'time': time.time()}))
        # Ensure readable by bot (which runs as root)
        import stat as st
        status_file.chmod(st.S_IRUSR | st.S_IWUSR | st.S_IRGRP | st.S_IROTH)
    except Exception as e:
        log(f'Failed to write status file: {e}')

def xkey(wid, *keys):
    try:
        subprocess.run(['xdotool', 'key', '--window', str(wid)] + [str(k) for k in keys],
                       env={**os.environ, 'DISPLAY': DISPLAY}, capture_output=True, timeout=5)
    except Exception as e:
        log(f'xkey error ({keys}): {e}')

def xtype(wid, text):
    for ch in text:
        if ch == '@':
            xkey(wid, 'at')
        elif ch == '.':
            xkey(wid, 'period')
        elif ch == '#':
            xkey(wid, 'Shift+3')
        elif ch == '!':
            xkey(wid, 'Shift+1')
        elif ch == '_':
            xkey(wid, 'Shift+underscore')
        elif ch == '-':
            xkey(wid, 'minus')
        elif ch == ' ':
            xkey(wid, 'space')
        elif ch.isupper():
            xkey(wid, f'Shift+{ch.lower()}')
        else:
            xkey(wid, ch)
        time.sleep(0.03)

def get_window(timeout=20):
    patterns = ['Sign in', 'Google', 'Chrome', 'myaccount', 'conta', '2-Step', 'verifica']
    deadline = time.time() + timeout
    while time.time() < deadline:
        for pat in patterns:
            result = subprocess.run(['xdotool', 'search', '--name', pat],
                                    env={**os.environ, 'DISPLAY': DISPLAY}, capture_output=True, text=True, timeout=3)
            for wid in result.stdout.strip().split():
                r2 = subprocess.run(['xdotool', 'getwindowname', wid],
                                    env={**os.environ, 'DISPLAY': DISPLAY}, capture_output=True, text=True, timeout=2)
                t = r2.stdout.strip()
                if 'google' in t.lower() or 'chrome' in t.lower() or 'conta' in t.lower() or 'sign' in t.lower():
                    return wid
        time.sleep(1)
    return None

def kill_chrome():
    import signal
    pids = []
    for proc_file in pathlib.Path('/proc').iterdir():
        if not proc_file.name.isdigit():
            continue
        try:
            cmdline = (proc_file / 'cmdline').read_bytes().replace(b'\x00', b' ').decode(errors='replace')
            if str(SESSION_DIR) in cmdline and 'chrome' in cmdline.lower():
                pids.append(int(proc_file.name))
        except (PermissionError, FileNotFoundError, ProcessLookupError, OSError):
            pass
    
    if pids:
        log(f'Gracefully stopping Chrome PIDs: {pids}')
        # Send SIGTERM only - let Chrome flush cookies cleanly
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        # Wait generously for Chrome to flush and exit
        deadline = time.time() + 15
        while time.time() < deadline:
            still_running = False
            for pid in pids[:]:
                try:
                    os.kill(pid, 0)  # Check if alive
                    still_running = True
                except ProcessLookupError:
                    pids.remove(pid)
            if not still_running:
                break
            time.sleep(0.5)
    
    time.sleep(1)
    for f in pathlib.Path(str(SESSION_DIR)).glob('Singleton*'):
        f.unlink()

def check_auth_score():
    """Check session auth score by reading cookies SQLite DB directly."""
    import sqlite3
    cookies_file = SESSION_DIR / 'Default' / 'Cookies'
    if not cookies_file.exists():
        log('Cookie file not found')
        return 0
    try:
        conn = sqlite3.connect(str(cookies_file))
        cur = conn.cursor()
        cur.execute("SELECT host_key, name FROM cookies WHERE host_key LIKE '%google%' AND name IN ('SAPISID','APISID','HSID','SSID','SID','OSID','__Host-GAPS')")
        cookies = cur.fetchall()
        names = {r[1] for r in cookies}
        conn.close()
        log(f'Auth cookies found: {len(cookies)} -> {names}')
        # __Host-GAPS = logged in session (Google's current auth cookie)
        if '__Host-GAPS' in names:
            return 3
        if len(cookies) >= 3:
            return 3
        if len(cookies) >= 1:
            return 1
        return 0
    except Exception as e:
        log(f'check_auth_score error (sqlite): {e}')
        return 0


def main():
    # Read email and password from stdin
    email = sys.stdin.readline().strip()
    password = sys.stdin.readline().strip()

    if not email or not password:
        error('Email e senha sao obrigatorios')
        return 1

    step('Preparando ambiente...')

    # Ensure Xvfb is running
    result = subprocess.run(['ps', 'aux'], capture_output=True, text=True, timeout=5)
    if 'Xvfb :99' not in result.stdout:
        subprocess.Popen(['Xvfb', DISPLAY, '-screen', '0', '1280x900x24'],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(2)
        log('Xvfb started')

    # Use current profile - just clean stale locks
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    (SESSION_DIR / 'Default').mkdir(parents=True, exist_ok=True)
    for f in SESSION_DIR.glob('Singleton*'):
        f.unlink()
    log('Session ready (preserved existing cookies)')

    # Set permissions
    subprocess.run(['chown', '-R', 'ubuntu:ubuntu', str(SESSION_DIR)], capture_output=True, timeout=5)

    step('Abrindo Chrome...')

    # Start Chrome via subprocess (as ubuntu user, no root)
    chrome_proc = subprocess.Popen(
        ['sudo', '-u', 'ubuntu', 'google-chrome',
         '--no-sandbox', '--no-first-run',
         '--window-size=1280,900',
         f'--user-data-dir={SESSION_DIR}',
         '--disable-gpu', '--disable-software-rasterizer',
         '--disable-session-crashed-bubble',
         'https://accounts.google.com/signin'],
        env={**os.environ, 'DISPLAY': DISPLAY},
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    try:
        # Wait for Chrome window
        wid = get_window(timeout=25)
        if not wid:
            error('Janela do Chrome nao apareceu')
            return 1

        time.sleep(2)
        step('Digitando email...')

        # Type email (field is auto-focused)
        xtype(wid, email)
        time.sleep(0.3)
        xkey(wid, 'Return')
        log('Email submitted')
        time.sleep(4)

        step('Digitando senha...')
        xtype(wid, password)
        time.sleep(0.3)
        xkey(wid, 'Return')
        log('Password submitted')
        time.sleep(5)

        # Check if we're on 2FA page
        title_result = subprocess.run(['xdotool', 'getwindowname', wid],
                                       env={**os.environ, 'DISPLAY': DISPLAY}, capture_output=True, text=True, timeout=5)
        title = title_result.stdout.strip() or ''

        if '2-Step' in title or 'Verification' in title or 'verifica' in title.lower():
            step('2FA detectado - aguardando confirmacao')
            print('STATUS:NEED_2FA')
            sys.stdout.flush()
            log('2FA requested')

            # Wait for phone approval OR SMS code (max 120s, non-blocking)
            import select
            step('Aguardando aprovacao no celular...')
            code = ''
            deadline = time.time() + 120
            while time.time() < deadline:
                # Check if phone was approved
                try:
                    r = subprocess.run(['xdotool', 'getwindowname', wid],
                        env={**os.environ, 'DISPLAY': DISPLAY}, capture_output=True, text=True, timeout=3)
                    t = r.stdout.strip() or ''
                    if 'myaccount' in t.lower() or 'conta' in t.lower() or 'account' in t.lower():
                        if 'sign' not in t.lower():
                            log('Phone approval detected!')
                            code = None
                            break
                except:
                    pass

                # Check if SMS code arrived via stdin
                try:
                    if select.select([sys.stdin], [], [], 0.5)[0]:
                        line = sys.stdin.readline().strip()
                        if line:
                            code = line
                            log(f'Code received via stdin')
                            break
                except:
                    pass

            if code:
                # Admin provided SMS code - type it
                step('Digitando codigo de verificacao...')
                xtype(wid, code)
                time.sleep(0.3)
                xkey(wid, 'Return')
                log('Verification code submitted')
                time.sleep(8)

        # Wait for Chrome to complete login - watch for myaccount page
        step('Aguardando Chrome salvar sessao...')
        logged_in = False
        for _ in range(30):
            time.sleep(1)
            try:
                r = subprocess.run(['xdotool', 'getwindowname', wid],
                    env={**os.environ, 'DISPLAY': DISPLAY}, capture_output=True, text=True, timeout=3)
                t = r.stdout.strip() or ''
                tl = t.lower()
                if 'conta' in tl or 'account' in tl:
                    if 'sign' not in tl and '2-step' not in tl and 'verif' not in tl:
                        logged_in = True
                        log(f'Login confirmed - navigated to: {t[:60]}')
                        # Extra wait for cookies to flush
                        for _ in range(10):
                            time.sleep(1)
                            try:
                                r2 = subprocess.run(['xdotool', 'getwindowname', wid],
                                    env={**os.environ, 'DISPLAY': DISPLAY}, capture_output=True, text=True, timeout=2)
                                if 'conta' in (r2.stdout.strip() or '').lower():
                                    pass
                            except:
                                pass
                        break
                elif 'google' in tl and 'sign' not in tl and '2-step' not in tl:
                    logged_in = True
                    log('Login confirmed (on google.com)')
                    time.sleep(5)
                    break
            except:
                pass

        if not logged_in:
            log('Chrome did not navigate to account page - trying anyway')
            time.sleep(5)

        # Kill Chrome gracefully
        step('Salvando e verificando sessao...')
        kill_chrome()
        time.sleep(3)

        # Check auth score
        score = check_auth_score()
        log(f'Final auth score: {score}/3')

        if score >= 2:
            step('Sessao sincronizada para workers...')
            # Sync to all worker profiles
            for uid in [2, 3, 4, 5, 6, 7, 11]:
                dst = BASE_DIR / f'google_session_{uid}'
                if dst.exists():
                    for f in ['Default/Cookies', 'Default/Cookies-journal']:
                        sf = SESSION_DIR / f
                        df = dst / f
                        if sf.exists():
                            shutil.copy2(sf, df)
                    for sf in dst.glob('Singleton*'):
                        sf.unlink()
            log('Session synced to all workers')
            step('Login concluido!')

        auth_score(score)
        return 0

    finally:
        # Clean up
        if chrome_proc:
            chrome_proc.terminate()
            time.sleep(1)
            chrome_proc.kill()
        kill_chrome()


if __name__ == '__main__':
    sys.exit(main())
