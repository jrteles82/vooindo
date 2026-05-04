#!/opt/vooindo/.venv/bin/python
"""Login manual no Google — abre Chrome com Xvfb pra login interativo."""
import os, sys, subprocess, time
from pathlib import Path

os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(Path(__file__).resolve().parent / ".cache/ms-playwright"))
os.environ.setdefault("USE_SYSTEM_CHROME", "1")

BASE = Path("/opt/vooindo/google_session")

# Garante Xvfb
subprocess.run(["pkill", "-f", "Xvfb.*:99"], capture_output=True)
xvfb = subprocess.Popen(["Xvfb", ":99", "-screen", "0", "1280x900x24"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(2)
os.environ["DISPLAY"] = ":99"

# Limpa locks
for f in BASE.glob("Singleton*"):
    try: f.unlink()
    except: pass

sys.path.insert(0, "/opt/vooindo")
from playwright.sync_api import sync_playwright

with sync_playwright() as pw:
    ctx = pw.chromium.launch_persistent_context(
        str(BASE),
        headless=False,
        channel="chrome",
        slow_mo=100,
        locale="pt-BR",
        viewport={"width": 1280, "height": 900},
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--start-maximized",
        ],
    )
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    page.goto("https://accounts.google.com/signin", wait_until="domcontentloaded")

    print("✅ Chrome aberto em :99")
    print("Faça login manual no navegador (email + senha + SMS se pedir).")
    print("Depois de logado, volte aqui e pressione ENTER para finalizar.")
    input()

    ctx.close()

# Sincroniza pros workers
sys.path.insert(0, "/opt/vooindo")
from google_session_sync import sync_base_session_to_worker_profiles
sync_base_session_to_worker_profiles(force=True, skip_in_use=False)
subprocess.run(["chown", "-R", "ubuntu:ubuntu", str(BASE)], capture_output=True)

print("✅ Sessão sincronizada pros 7 perfis. Pode startar o serviço:")
print("   systemctl start vooindo.service")
xvfb.terminate()
