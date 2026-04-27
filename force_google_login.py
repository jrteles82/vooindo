#!/opt/vooindo/.venv/bin/python
"""
Login Google forcado — abre Chrome real (nao Playwright) para gerar refresh token valido.
O usuario precisa digitar a senha no terminal.
O Chrome abre com DISPLAY=:99 (Xvfb) e o usuario interage via prompts.
"""
import os, sys, time, subprocess, shutil, json
from pathlib import Path

SESSION_DIR = Path('/opt/vooindo/google_session')
DISPLAY_NUM = ':99'

# Garantir Xvfb rodando
subprocess.run(['pkill', '-f', 'Xvfb.*:99'], capture_output=True)
time.sleep(0.5)
xvfb = subprocess.Popen(
    ['Xvfb', DISPLAY_NUM, '-screen', '0', '1280x900x24'],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
)
time.sleep(1)
os.environ['DISPLAY'] = DISPLAY_NUM

# Backup do profile atual
backup_dir = SESSION_DIR.parent / 'google_session_bkp'
if backup_dir.exists():
    shutil.rmtree(backup_dir)
if SESSION_DIR.exists():
    shutil.copytree(SESSION_DIR, backup_dir, ignore=shutil.ignore_patterns(
        'Cache', 'Code Cache', 'GPUCache', 'DawnGraphiteCache', 'DawnWebGPUCache',
        'Service Worker', 'Session Storage', 'Local Storage',
        '*.tmp', '*.log', 'Singleton*',
    ))
    # Limpar Cookies para forçar login do zero
    cookies_db = SESSION_DIR / 'Default' / 'Cookies'
    if cookies_db.exists():
        cookies_db.unlink()
        print(f"Cookies removidos: {cookies_db}")
    login_data = SESSION_DIR / 'Default' / 'Login Data'
    if login_data.exists():
        login_data.unlink()
        print(f"Login Data removido: {login_data}")
    # Limpar também token service
    for f in SESSION_DIR.glob('Default/Token Service*'):
        f.unlink()
        print(f"Token Service removido: {f}")

print("\n🟢 Chrome será aberto em modo normal (DISPLAY=:99)")
print("   Siga os passos:")
print("   1. Digite o email: jrteles.moreira@gmail.com → Enter")
print("   2. Digite a senha → Enter")
print("   3. Se houver 2FA, digite o código → Enter")
print("   4. Após login, digite 'ok' neste terminal\n")

# Abrir Chrome em modo normal (no Xvfb)
chrome_proc = subprocess.Popen([
    'google-chrome',
    f'--user-data-dir={SESSION_DIR}',
    '--no-first-run',
    '--disable-blink-features=AutomationControlled',
    '--window-size=1280,900',
    'https://accounts.google.com/signin',
], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

print("Chrome aberto. Faça o login na janela virtual.")
print("Digite 'ok' quando terminar o login:")
while True:
    line = sys.stdin.readline().strip().lower()
    if line == 'ok':
        break
    print("Digite 'ok' para continuar...")

# Fechar Chrome
chrome_proc.terminate()
time.sleep(2)
chrome_proc.kill()

# Remover arquivos temporários do Chrome
for f in SESSION_DIR.glob('Singleton*'):
    f.unlink(missing_ok=True)
for f in SESSION_DIR.glob('.com.google.Chrome*'):
    f.unlink(missing_ok=True)

# Sincronizar para workers escravos
print("Sincronizando para workers...")
sys.path.insert(0, '/opt/vooindo')
from google_session_sync import sync_base_session_to_worker_profiles
sync_base_session_to_worker_profiles(force=True, skip_in_use=False)

# Verificar auth_score
import sqlite3
cookies_db = SESSION_DIR / 'Default' / 'Cookies'
if cookies_db.exists():
    conn = sqlite3.connect(f'file:{cookies_db}?mode=ro', uri=True)
    c = conn.cursor()
    c.execute("SELECT name, host_key FROM cookies WHERE name IN ('SAPISID','SSID') AND host_key='.google.com'")
    cnt = len(c.fetchall())
    conn.close()
    print(f"\nauth_score real: {cnt}/2")

print("✅ Login concluído!")
xvfb.terminate()
