#!/opt/vooindo/.venv/bin/python
"""Chrome Guardian — mantém 1 Chrome logado no Google 24/7.

O bot se conecta via CDP (connect_over_cdp) em vez de abrir instância nova.
Se a sessão expirar, tenta re-login automático com xdotool (indetectável).
Se aparecer challenge, notifica no Telegram pro usuário confirmar no celular.

Uso:
    python3 chrome_guardian.py [--port CDP_PORT] [--display DISPLAY]

Como systemd service:
    [Service]
    ExecStart=/opt/vooindo/.venv/bin/python /opt/vooindo/chrome_guardian.py
    Restart=always
    RestartSec=10
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import threading
import urllib.request
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Optional

# ── Config ──────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / "google_session"
CDP_PORT = 9222
STATUS_PORT = 9230
HEALTH_INTERVAL = 60
AUTORELOGIN_INTERVAL = 300
POLL_INTERVAL = 10

GOOGLE_EMAIL = "vooindo.bot@gmail.com"
GOOGLE_PASS = "Vooindo#8212"

TELEGRAM_BOT_TOKEN = os.getenv("VOOINDO_BOT_TOKEN", "")
TELEGRAM_ADMIN_ID = os.getenv("VOOINDO_ADMIN_CHAT_ID", "")

CHROME_CANDIDATES = [
    "/opt/google/chrome/chrome",
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
]

# ── Logger ──────────────────────────────────────────────────────────
def log(msg: str, **kw):
    extra = " ".join(f"{k}={v}" for k, v in kw.items())
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] guardian: {msg}" + (f" ({extra})" if extra else ""), flush=True)

# ── Telegram ────────────────────────────────────────────────────────
def notify_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_ADMIN_ID:
        return
    try:
        data = json.dumps({"chat_id": TELEGRAM_ADMIN_ID, "text": text, "parse_mode": "HTML"}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data=data, headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        log("telegram_notify_failed", error=str(e))

# ── Chrome finder ───────────────────────────────────────────────────
def find_chrome() -> Optional[str]:
    for c in CHROME_CANDIDATES:
        if Path(c).exists():
            return c
    return None

# ── Xvfb ────────────────────────────────────────────────────────────
def ensure_xvfb(display: str = ":99") -> bool:
    subprocess.run(["killall", "Xvfb"], capture_output=True, timeout=3)
    time.sleep(0.5)
    try:
        proc = subprocess.Popen(
            ["Xvfb", display, "-screen", "0", "1280x900x24", "-ac"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(2)
        if proc.poll() is not None:
            log("xvfb_start_failed", returncode=proc.returncode)
            return False
        r = subprocess.run(["xdpyinfo", "-display", display], capture_output=True, timeout=5)
        if r.returncode != 0:
            log("xvfb_display_not_responding")
            return False
        log("xvfb_started", display=display)
        os.environ["DISPLAY"] = display
        return True
    except Exception as e:
        log("xvfb_error", error=str(e))
        return False

# ── xdotool helpers ─────────────────────────────────────────────────
def _focus_window(name_substr: str = "Google") -> Optional[int]:
    """Foca janela do Chrome pelo nome. Retorna window id ou None."""
    try:
        wins = subprocess.run(
            ["xdotool", "search", "--name", name_substr],
            capture_output=True, text=True, timeout=5,
        ).stdout.strip().split()
        if not wins:
            return None
        wid = wins[0]
        subprocess.run(["xdotool", "windowfocus", wid], capture_output=True, timeout=3)
        time.sleep(0.3)
        return int(wid)
    except Exception:
        return None

def _type_text(text: str, delay_ms: int = 50):
    """Digita texto via xdotool com delay entre caracteres (mais humano)."""
    subprocess.run(
        ["xdotool", "type", "--delay", str(delay_ms), text],
        capture_output=True, timeout=30,
    )

def _press_key(key: str):
    """Pressiona tecla (Return, Tab, etc)."""
    subprocess.run(["xdotool", "key", key], capture_output=True, timeout=5)

# ── Chrome Guardian ─────────────────────────────────────────────────
class ChromeGuardian:
    def __init__(self, cdp_port: int = CDP_PORT, display: str = ":99"):
        self.cdp_port = cdp_port
        self.display = display
        self.chrome_path = find_chrome()
        self.process: Optional[subprocess.Popen] = None
        self.ws_endpoint: Optional[str] = None
        self.session_ok: bool = False
        self.last_auth_check: float = 0
        self.running = True
        self._lock = threading.Lock()

        if not self.chrome_path:
            raise RuntimeError("Chrome não encontrado")
        log("chrome_detected", path=self.chrome_path)

    # ── Chrome lifecycle ─────────────────────────────────────────────
    def start_chrome(self) -> bool:
        with self._lock:
            if self.process and self.process.poll() is None:
                return True

            for f in ["SingletonLock", "SingletonCookie", "SingletonSocket"]:
                (SESSION_DIR / f).unlink(missing_ok=True)
            (SESSION_DIR / "DevToolsActivePort").unlink(missing_ok=True)

            cmd = [
                self.chrome_path,
                f"--remote-debugging-port={self.cdp_port}",
                f"--user-data-dir={str(SESSION_DIR)}",
                "--window-size=1280,900",
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--disable-crashpad",
                "--lang=pt-BR",
                "--start-maximized",
                f"--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            ]
            log("starting_chrome", port=self.cdp_port)

            env = os.environ.copy()
            env["DISPLAY"] = self.display
            self.process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)

            for attempt in range(30):
                time.sleep(1)
                if self.process.poll() is not None:
                    log("chrome_died_early", attempt=attempt, rc=self.process.returncode)
                    return False
                ws = self._get_ws_endpoint()
                if ws:
                    self.ws_endpoint = ws
                    log("chrome_ready", port=self.cdp_port, ws=ws[:60])
                    return True

            log("chrome_not_ready_timeout")
            self.kill_chrome()
            return False

    def kill_chrome(self) -> None:
        with self._lock:
            if self.process:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except Exception:
                    self.process.kill()
                    self.process.wait(timeout=3)
                self.process = None
            self.ws_endpoint = None

    # ── CDP ──────────────────────────────────────────────────────────
    def _get_ws_endpoint(self) -> Optional[str]:
        try:
            url = f"http://127.0.0.1:{self.cdp_port}/json/version"
            resp = urllib.request.urlopen(url, timeout=5)
            return json.loads(resp.read().decode()).get("webSocketDebuggerUrl")
        except Exception:
            return None

    def get_ws_endpoint(self) -> Optional[str]:
        with self._lock:
            return self.ws_endpoint

    def get_status(self) -> dict:
        with self._lock:
            alive = self.process is not None and self.process.poll() is None
            return {
                "ready": 1 if (alive and self.session_ok) else 0,
                "session_ok": self.session_ok,
                "alive": alive,
                "cdp_port": self.cdp_port,
                "ws_endpoint": self.ws_endpoint if alive else "",
                "profile": str(SESSION_DIR),
                "chrome_pid": self.process.pid if alive else None,
            }

    # ── Session check (CDP Playwright in-process) ────────────────────
    def _run_cdp_script(self, script: str) -> dict:
        """Executa snippet Python via CDP num subprocess Playwright."""
        ws = self.get_ws_endpoint()
        if not ws:
            return {"ok": False, "error": "chrome_not_running"}
        pw_path = str(BASE_DIR / ".cache/ms-playwright")
        # Identa cada linha do script com 4 espaços pra ficar dentro do 'with sync_playwright()'
        indented = "\n".join("    " + line if line.strip() else "" for line in script.split("\n"))
        code = (
            '#!/opt/vooindo/.venv/bin/python\n'
            'from __future__ import annotations\n'
            'import json, sys, os, time\n'
            f'os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "{pw_path}")\n'
            'from playwright.sync_api import sync_playwright\n'
            'from playwright_stealth import Stealth\n'
            '\n'
            'with sync_playwright() as p:\n'
            f'    browser = p.chromium.connect_over_cdp("{ws}")\n'
            '    context = browser.contexts[0] if browser.contexts else browser.new_context()\n'
            '    page = context.pages[0] if context.pages else context.new_page()\n'
            + indented + '\n'
        )
        tmp = BASE_DIR / f".guardian_{os.getpid()}_{int(time.time()*1000)}.py"
        tmp.write_text(code)
        try:
            r = subprocess.run([sys.executable, str(tmp)], capture_output=True, text=True, timeout=60,
                               env={"DISPLAY": self.display, **os.environ})
            tmp.unlink(missing_ok=True)
            if r.returncode != 0:
                return {"ok": False, "error": r.stderr.strip()[:500]}
            for line in r.stdout.strip().split("\n"):
                line = line.strip()
                if line.startswith("{"):
                    return json.loads(line)
            return {"ok": True, "stdout": r.stdout.strip()[:500]}
        except subprocess.TimeoutExpired:
            tmp.unlink(missing_ok=True)
            return {"ok": False, "error": "timeout"}

    def check_session(self) -> dict:
        """Verifica sessão Google navegando a google.com e checando avatar.
        Cria página própria pra não conflitar com workers."""
        code = '''
import traceback
page = context.new_page()  # página própria, isolada dos workers
result = {"ok": False, "score": 0, "error": None}
try:
    page.goto("https://www.google.com/", wait_until="domcontentloaded", timeout=30000)
    time.sleep(2)
    Stealth().apply_stealth_sync(page)
    time.sleep(1)
    avatar = page.locator('img[class*="gb"]').count() > 0
    signin_a = page.locator('a:has-text("Fazer login")').count() > 0
    signin_b = page.locator('a:has-text("Sign in")').count() > 0
    acct_btn = page.locator('a[aria-label*="Google"i]').count() > 0
    if avatar or acct_btn:
        score = 3; ok = True
    elif not signin_a and not signin_b:
        score = 2; ok = True
    else:
        score = 0; ok = False
    result = {"ok": ok, "score": score, "avatar": avatar, "signin": signin_a or signin_b}
except Exception as e:
    result = {"ok": False, "score": 0, "error": type(e).__name__ + ": " + str(e)[:200]}
finally:
    try: page.close()
    except: pass
print(json.dumps(result))
'''
        return self._run_cdp_script(code)

    # ── Login via xdotool (indetectável) ─────────────────────────────
    def _navigate_to_accounts(self) -> bool:
        """Usa CDP para navegar a accounts.google.com (página própria)."""
        ws = self.get_ws_endpoint()
        if not ws:
            return False
        code = '''
import traceback
page = context.new_page()
try:
    page.goto("https://accounts.google.com/", wait_until="domcontentloaded", timeout=30000)
    time.sleep(2)
    Stealth().apply_stealth_sync(page)
    time.sleep(1)
    print(json.dumps({"ok": True, "url": page.url}))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)[:200]}))
finally:
    try: page.close()
    except: pass
'''
        r = self._run_cdp_script(code)
        return r.get("ok", False)

    def _xdotool_login(self) -> Optional[str]:
        """Faz login via xdotool. Retorna 'ok', 'challenge' ou None (falha)."""
        # Foca janela do Chrome
        wid = _focus_window("Google")
        if not wid:
            log("xdotool_window_not_found")
            return None

        time.sleep(1)

        # Email
        _type_text(GOOGLE_EMAIL, delay_ms=40)
        time.sleep(0.5)
        _press_key("Return")
        time.sleep(4)

        # Senha
        _type_text(GOOGLE_PASS, delay_ms=30)
        time.sleep(0.5)
        _press_key("Return")
        time.sleep(5)

        # Verifica resultado via CDP (página própria)
        ws = self.get_ws_endpoint()
        if not ws:
            return None

        code = '''
import traceback
page = context.new_page()
try:
    page.wait_for_load_state("domcontentloaded", timeout=10000)
    url = page.url
    body = page.locator("body").inner_text(timeout=3000)
    if "challenge" in url.lower() or "idv" in url.lower() or "confirm" in body.lower()[:200]:
        print(json.dumps({"status": "challenge", "url": url}))
    elif "vooindo" in body.lower() or "myaccount" in url or page.locator('img[class*="gb"]').count() > 0:
        print(json.dumps({"status": "ok", "url": url}))
    elif "Couldn't sign you in" in body or "não foi possível" in body:
        print(json.dumps({"status": "blocked", "url": url}))
    else:
        print(json.dumps({"status": "unknown", "url": url, "body": body[:200]}))
except Exception as e:
    print(json.dumps({"status": "error", "error": str(e)[:200]}))
finally:
    try: page.close()
    except: pass
'''
        r = self._run_cdp_script(code)
        return r.get("status")

    def try_login(self) -> dict:
        """Tenta login completo: navega + xdotool.
        Retorna {"ok": bool, "status": str, ...}."""
        log("attempting_login_via_xdotool")

        if not self._navigate_to_accounts():
            log("nav_to_accounts_failed")
            return {"ok": False, "status": "nav_failed"}

        time.sleep(3)  # tempo pra página carregar completamente
        result = self._xdotool_login()

        if result == "ok":
            log("xdotool_login_success")
            return {"ok": True, "status": "login_success"}
        elif result == "challenge":
            log("xdotool_challenge_detected")
            notify_telegram(
                "⚠️ <b>Challenge do Google detectado!</b>\n\n"
                "Confirme no seu celular pra renovar a sessão do bot.\n"
                "O guardian vai esperar 5 minutos."
            )
            return {"ok": False, "status": "challenge_required"}
        else:
            log("xdotool_login_failed", status=result)
            return {"ok": False, "status": result or "xdotool_failed"}

    # ── Main loop ────────────────────────────────────────────────────
    def run(self) -> None:
        log("guardian_starting")
        SESSION_DIR.mkdir(exist_ok=True)
        last_relogin = 0
        health_count = 0

        while self.running:
            try:
                alive = self.process is not None and self.process.poll() is None
                if not alive:
                    log("chrome_down_restarting")
                    self.kill_chrome()
                    time.sleep(2)
                    if not self.start_chrome():
                        log("chrome_restart_failed_retry")
                        time.sleep(10)
                        continue
                    notify_telegram("🔄 Guardian: Chrome reiniciado")

                now = time.time()
                if now - self.last_auth_check > HEALTH_INTERVAL:
                    health = self.check_session()
                    health_count += 1
                    log("auth_check", attempt=health_count, ok=health.get("ok"), score=health.get("score"))

                    if health.get("ok"):
                        self.session_ok = True
                        self.last_auth_check = now
                    else:
                        self.session_ok = False
                        log("session_bad", score=health.get("score", 0),
                            error=health.get("error", "")[:100])

                        if now - last_relogin > AUTORELOGIN_INTERVAL:
                            last_relogin = now
                            r = self.try_login()
                            if r.get("ok"):
                                self.session_ok = True
                                self.last_auth_check = now
                                notify_telegram("✅ Guardian: Sessão renovada automaticamente")
                            elif r.get("status") == "challenge_required":
                                log("waiting_for_challenge_5min")
                                time.sleep(300)

                time.sleep(POLL_INTERVAL)

            except Exception as e:
                log("guardian_loop_error", error=str(e))
                time.sleep(5)

    def stop(self):
        self.running = False

# ── HTTP Status Server ──────────────────────────────────────────────
_guardian: Optional[ChromeGuardian] = None

class StatusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if _guardian:
            s = _guardian.get_status()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ready": s["ready"],
                "session_ok": s["session_ok"],
                "instances": [{
                    "id": 0, "port": s["cdp_port"],
                    "profile": s["profile"], "ready": bool(s["session_ok"]),
                    "alive": s["alive"], "ws_endpoint": s["ws_endpoint"],
                }],
            }).encode())
        else:
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ready": 0}).encode())

    def log_message(self, fmt, *args):
        pass

def run_status_server(port: int = STATUS_PORT):
    try:
        server = HTTPServer(("127.0.0.1", port), StatusHandler)
        log("status_server", port=port)
        server.serve_forever()
    except OSError:
        log("status_server_port_in_use", port=port)

# ── Main ────────────────────────────────────────────────────────────
def main():
    global _guardian
    parser = argparse.ArgumentParser(description="Chrome Guardian")
    parser.add_argument("--port", type=int, default=CDP_PORT)
    parser.add_argument("--display", type=str, default=":99")
    parser.add_argument("--status-port", type=int, default=STATUS_PORT)
    args = parser.parse_args()

    if not ensure_xvfb(args.display):
        log("xvfb_required")
        sys.exit(1)

    _guardian = ChromeGuardian(cdp_port=args.port, display=args.display)
    threading.Thread(target=run_status_server, args=(args.status_port,), daemon=True).start()

    def handle_signal(sig, frame):
        log("shutdown_signal", signal=sig)
        if _guardian:
            _guardian.stop()
            _guardian.kill_chrome()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    _guardian.run()

if __name__ == "__main__":
    main()
