#!/opt/vooindo/.venv/bin/python
"""Chrome Pool Manager — mantém N instâncias Chrome persistentes.

Cada worker se conecta via remote debugging port em vez de abrir/fechar Chrome.
Isso economiza ~3-5s de inicialização + login por rota e reduz uso de RAM.

Uso:
    python3 chrome_pool_manager.py [--pool-size N] [--port BASE]

Design:
- Pool size fixo (default: 2)
- Cada instância em porta incremental: 9222, 9223, etc.
- Health check a cada 30s — se uma instância morre, spawna substituta
- Sinal SIGTERM/SIGINT fecha tudo gracefulmente
- Estado via HTTP em /status (JSON)
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
from pathlib import Path
from typing import Optional

from app_logging import get_logger

logger = get_logger("chrome_pool")

BASE_DIR = Path(__file__).resolve().parent
BASE_PORT = 9222
HEALTH_CHECK_INTERVAL = 30  # segundos


class ChromeInstance:
    """Uma instância Chrome gerenciada."""

    def __init__(self, instance_id: int, port: int, profile_dir: str):
        self.instance_id = instance_id
        self.port = port
        self.profile_dir = profile_dir
        self.process: Optional[subprocess.Popen] = None
        self.ready = False
        self.lock = threading.Lock()

    def start(self) -> bool:
        """Inicia Chrome com remote debugging port."""
        env = os.environ.copy()
        env["GOOGLE_PERSISTENT_PROFILE_DIR"] = self.profile_dir

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error("playwright não instalado")
            return False

        # Inicia Chrome diretamente (sem Playwright, só o browser)
        # Localiza executável do Chrome
        chrome_path = self._find_chrome()
        if not chrome_path:
            logger.error("chrome_bin_not_found")
            return False

        cmd = [
            chrome_path,
            f"--remote-debugging-port={self.port}",
            f"--user-data-dir={self.profile_dir}",
            "--headless=new",
            "--no-sandbox",
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--window-size=1280,900",
            f"--lang=pt-BR",
        ]

        logger.info(
            "starting chrome instance=%s port=%s profile=%s",
            self.instance_id, self.port, self.profile_dir
        )

        try:
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env,
            )
        except Exception as e:
            logger.error("chrome_start_failed instance=%s error=%s", self.instance_id, e)
            return False

        # Aguarda até 15s pro Chrome ficar pronto
        for attempt in range(15):
            time.sleep(1)
            if self._is_alive():
                ws_url = self._get_ws_endpoint()
                if ws_url:
                    self.ready = True
                    logger.info(
                        "chrome_ready instance=%s port=%s ws=%s",
                        self.instance_id, self.port, ws_url
                    )
                    return True

        logger.warning("chrome_not_ready instance=%s port=%s", self.instance_id, self.port)
        self.kill()
        return False

    def _find_chrome(self) -> Optional[str]:
        """Encontra executável do Chrome."""
        candidates = [
            "/opt/vooindo/.venv/lib/python3.13/site-packages/playwright/driver/package/.local-browsers/chromium-1140/chrome-linux/chrome",
            "/root/.cache/ms-playwright/chromium-1140/chrome-linux/chrome",
            "/usr/bin/google-chrome",
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
        ]
        # Tenta achar via Playwright
        try:
            import playwright._repo
            browser_path = Path(playwright._repo.__file__).parent.parent / "driver" / "package" / ".local-browsers"
            for p in browser_path.glob("*/chrome-linux/chrome"):
                candidates.insert(0, str(p))
        except Exception:
            pass

        for c in candidates:
            if Path(c).exists():
                return c
        return None

    def _is_alive(self) -> bool:
        """Chrome ainda rodando?"""
        if self.process is None:
            return False
        return self.process.poll() is None

    def _get_ws_endpoint(self) -> Optional[str]:
        """Obtém URL WebSocket do DevTools."""
        try:
            import urllib.request
            url = f"http://127.0.0.1:{self.port}/json/version"
            resp = urllib.request.urlopen(url, timeout=5)
            data = json.loads(resp.read().decode())
            return data.get("webSocketDebuggerUrl")
        except Exception:
            return None

    def get_ws_endpoint(self) -> Optional[str]:
        """Ponto de entrada público (thread-safe)."""
        return self._get_ws_endpoint()

    def get_browser_info(self) -> dict:
        """Info de diagnóstico."""
        ws = self.get_ws_endpoint()
        return {
            "id": self.instance_id,
            "port": self.port,
            "profile": self.profile_dir,
            "ready": self.ready,
            "alive": self._is_alive(),
            "ws_endpoint": ws or "",
        }

    def kill(self) -> None:
        """Mata instância Chrome."""
        with self.lock:
            if self.process:
                try:
                    self.process.terminate()
                    self.process.wait(timeout=5)
                except Exception:
                    self.process.kill()
                self.process = None
            self.ready = False


class ChromePool:
    """Gerenciador do pool de Chrome."""

    def __init__(self, pool_size: int = 2, base_port: int = 9222):
        self.pool_size = pool_size
        self.base_port = base_port
        self.instances: list[ChromeInstance] = []
        self.running = True
        self._health_thread: Optional[threading.Thread] = None

        # Cria profiles se não existirem
        self._ensure_profiles()

    def _ensure_profiles(self) -> None:
        """Cria diretórios de profile se necessário."""
        for i in range(self.pool_size):
            profile = BASE_DIR / f"google_session_pool_{i}"
            profile.mkdir(parents=True, exist_ok=True)

    def start(self) -> None:
        """Inicia todas as instâncias do pool."""
        logger.info("pool_start size=%s base_port=%s", self.pool_size, self.base_port)

        for i in range(self.pool_size):
            port = self.base_port + i
            profile = str(BASE_DIR / f"google_session_pool_{i}")
            inst = ChromeInstance(instance_id=i, port=port, profile_dir=profile)
            self.instances.append(inst)
            ok = inst.start()
            if not ok:
                logger.warning("pool_instance_failed id=%s", i)

        # Health check thread
        self._health_thread = threading.Thread(target=self._health_loop, daemon=True)
        self._health_thread.start()

        ready_count = sum(1 for i in self.instances if i.ready)
        logger.info("pool_status ready=%s/%s", ready_count, self.pool_size)

    def acquire(self, timeout: float = 60.0) -> Optional[dict]:
        """Pega uma instância disponível (round-robin simples)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            for inst in self.instances:
                if inst.ready and inst._is_alive():
                    ws = inst.get_ws_endpoint()
                    if ws:
                        return {
                            "ws_endpoint": ws,
                            "port": inst.port,
                            "profile": inst.profile_dir,
                            "instance_id": inst.instance_id,
                        }
            time.sleep(0.5)
        return None

    def release(self, instance_id: int) -> None:
        """Libera instância (volta pro pool). No design atual é no-op porque
        o Chrome fica aberto — mas podemos resetar contexto aqui se necessário."""
        pass

    def _health_loop(self) -> None:
        """Monitora instâncias, revive as mortas."""
        while self.running:
            time.sleep(HEALTH_CHECK_INTERVAL)
            for inst in self.instances:
                if not inst._is_alive():
                    logger.warning(
                        "pool_instance_dead id=%s port=%s — restarting",
                        inst.instance_id, inst.port
                    )
                    inst.kill()
                    ok = inst.start()
                    if ok:
                        logger.info(
                            "pool_instance_restarted id=%s port=%s",
                            inst.instance_id, inst.port
                        )

    def status(self) -> dict:
        """Estado do pool."""
        return {
            "pool_size": self.pool_size,
            "instances": [inst.get_browser_info() for inst in self.instances],
            "ready": sum(1 for i in self.instances if i.ready),
        }

    def shutdown(self) -> None:
        """Graceful shutdown."""
        logger.info("pool_shutdown")
        self.running = False
        for inst in self.instances:
            inst.kill()


# Flask HTTP server para status/pool management
def run_http_server(pool: ChromePool, http_port: int = 9230):
    """Servidor HTTP simples pra health check."""
    from http.server import HTTPServer, BaseHTTPRequestHandler

    class PoolHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/status":
                data = pool.status()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(data, ensure_ascii=False).encode())
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, fmt, *args):
            pass  # silencia logs HTTP

    server = HTTPServer(("127.0.0.1", http_port), PoolHandler)
    while pool.running:
        server.handle_request()


def main():
    parser = argparse.ArgumentParser(description="Chrome Pool Manager")
    parser.add_argument("--pool-size", type=int, default=2, help="Número de instâncias Chrome (default: 2)")
    parser.add_argument("--port", type=int, default=9222, help="Porta base (default: 9222)")
    parser.add_argument("--http-port", type=int, default=9230, help="Porta HTTP status (default: 9230)")
    args = parser.parse_args()

    # Garante que só uma instância roda
    pid_file = Path("/tmp/chrome_pool.pid")
    if pid_file.exists():
        try:
            old_pid = int(pid_file.read_text().strip())
            os.kill(old_pid, 0)
            logger.info("pool_already_running pid=%s", old_pid)
            # Atualiza estado
            print(json.dumps({"status": "already_running", "pid": old_pid}))
            return 0
        except (OSError, ValueError):
            pid_file.unlink(missing_ok=True)

    pid_file.write_text(str(os.getpid()))

    pool = ChromePool(pool_size=args.pool_size, base_port=args.port)
    pool.start()

    # Trata sinais
    def _signal_handler(sig, frame):
        logger.info("pool_signal sig=%s", sig)
        pool.shutdown()
        pid_file.unlink(missing_ok=True)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    print(json.dumps({"status": "started", "pool_size": args.pool_size, "port": args.port}))
    sys.stdout.flush()

    # HTTP server loop
    run_http_server(pool, http_port=args.http_port)


if __name__ == "__main__":
    main()
