#!/opt/vooindo/.venv/bin/python
"""Login manual - usa profile temp pra nao corromper o original."""
import os, sys, subprocess, time, shutil, tempfile
from pathlib import Path

BASE = Path("/opt/vooindo")

# Sobe Xvfb
subprocess.run(["pkill", "-f", "Xvfb.*:99"], capture_output=True)
xvfb = subprocess.Popen(["Xvfb", ":99", "-screen", "0", "1280x900x24"],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(2)
os.environ["DISPLAY"] = ":99"

# Cria profile TEMPORARIO (copia do fresh)
TMP_PROFILE = Path(tempfile.mkdtemp(prefix="vooindo_login_"))
FRESH = BASE / "google_session_fresh"
if FRESH.is_dir():
    shutil.copytree(str(FRESH), str(TMP_PROFILE), dirs_exist_ok=True,
                    ignore=lambda d,f: [x for x in f if x in ("Cache","Code Cache","GPUCache")])

sys.path.insert(0, str(BASE))
os.environ["USE_SYSTEM_CHROME"] = "1"
from playwright.sync_api import sync_playwright

with sync_playwright() as pw:
    ctx = pw.chromium.launch_persistent_context(
        str(TMP_PROFILE), headless=False, channel="chrome",
        slow_mo=50, locale="pt-BR",
        viewport={"width": 1280, "height": 900},
        args=["--disable-blink-features=AutomationControlled","--no-sandbox"],
    )
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    page.goto("https://accounts.google.com/signin", wait_until="domcontentloaded")

    print("Chrome aberto em :99 (perfil TEMPORARIO)")
    print("1) Faca login manual (email + senha)")
    print("2) Se pedir confirmacao, escolha SMS")
    print("3) Confirme no celular")
    print("4) Depois de logado, pressione ENTER aqui")
    input()

    # Verifica se logou
    page.goto("https://www.google.com/", wait_until="domcontentloaded")
    time.sleep(2)
    ctx.close()

# Se logou com sucesso, copia pro perfil real
print("Verificando sessao...")
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(BASE / ".cache/ms-playwright")
from google_flights_executor import check_session_health

with sync_playwright() as pw:
    ctx2 = pw.chromium.launch_persistent_context(
        str(TMP_PROFILE), headless=True, channel="chrome",
        args=["--no-sandbox"], timeout=20000)
    page2 = ctx2.pages[0] if ctx2.pages else ctx2.new_page()
    page2.goto("https://www.google.com/", wait_until="domcontentloaded", timeout=20000)
    health = check_session_health(page2)
    score = health.get("score", 0)
    ctx2.close()

if score >= 2:
    print(f"Login OK! Score {score}/3. Copiando pro perfil real...")
    REAL = BASE / "google_session"
    shutil.rmtree(str(REAL), ignore_errors=True)
    shutil.copytree(str(TMP_PROFILE), str(REAL), dirs_exist_ok=True)
    subprocess.run(["chown", "-R", "ubuntu:ubuntu", str(REAL)], capture_output=True)
    from google_session_sync import sync_base_session_to_worker_profiles
    sync_base_session_to_worker_profiles(force=True, skip_in_use=False)
    print("Sessao copiada e sincronizada pros 7 perfis!")
    print("Agora rode: systemctl start vooindo.service")
else:
    print(f"Falha: score {score}/3. Tentando com perfil fresh...")

shutil.rmtree(str(TMP_PROFILE), ignore_errors=True)
xvfb.terminate()
