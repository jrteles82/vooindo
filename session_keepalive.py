#!/opt/vooindo/.venv/bin/python
"""
Mantém a sessão Google viva visitando periodicamente google.com.
Roda headless — chamado pelo systemd timer a cada 6h.
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(Path(__file__).with_name(".playwright-browsers")))

BASE_DIR = Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / "google_session"

from config import load_env
load_env()

import httpx
from playwright.sync_api import sync_playwright

from google_session_sync import sync_base_session_to_worker_profiles, purge_chrome_singleton_artifacts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("session_keepalive")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_CHAT_ID = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "").strip()
TELEGRAM_API_BASE = os.getenv("TELEGRAM_API_BASE_URL", "https://api.telegram.org").rstrip("/")

KEEPALIVE_URLS = [
    "https://www.google.com/",
    "https://www.google.com/travel/flights",
]

PROFILE_SELECTORS = [
    'a[aria-label*="Conta do Google"]',
    'a[aria-label*="Google Account"]',
    'img[alt*="Foto do perfil"]',
    'img[alt*="Profile picture"]',
    '[data-ogsr-up]',
]


def _send_telegram(text: str) -> None:
    if not BOT_TOKEN or not ADMIN_CHAT_ID:
        logger.warning("BOT_TOKEN ou ADMIN_CHAT_ID ausente — alerta não enviado")
        return
    url = f"{TELEGRAM_API_BASE}/bot{BOT_TOKEN}/sendMessage"
    try:
        httpx.post(url, json={"chat_id": ADMIN_CHAT_ID, "text": text, "parse_mode": "Markdown"}, timeout=10)
    except Exception as exc:
        logger.warning("Falha ao enviar Telegram: %s", exc)


def _fix_ownership() -> None:
    """Garante que google_session pertence ao usuário atual (evita Permission denied)."""
    try:
        uid = os.getuid()
        if uid == 0:
            return
        result = subprocess.run(
            ["find", str(SESSION_DIR), "-not", "-user", str(uid)],
            capture_output=True, text=True
        )
        if result.stdout.strip():
            subprocess.run(["chown", "-R", f"{uid}:{uid}", str(SESSION_DIR)], check=False)
            logger.info("Permissões corrigidas em google_session")
    except Exception as exc:
        logger.warning("Falha ao corrigir permissões: %s", exc)


def check_health(page) -> dict:
    score = 0
    indicators: dict[str, str] = {}

    for sel in PROFILE_SELECTORS:
        try:
            if page.locator(sel).count() > 0:
                score += 1
                indicators["profile_element"] = sel
                break
        except Exception:
            pass

    try:
        body = page.locator("body").inner_text(timeout=4000)
        low = body.lower()
        if "fazer login" not in low and "entrar" not in low and "sign in" not in low:
            score += 1
            indicators["no_login_prompt"] = "ok"
        else:
            indicators["no_login_prompt"] = "login_prompt_detected"
    except Exception:
        pass

    return {"ok": score >= 1, "score": score, "indicators": indicators}


def run_keepalive() -> bool:
    if not SESSION_DIR.is_dir():
        logger.error("SESSION_DIR não existe: %s", SESSION_DIR)
        return False

    _fix_ownership()
    purge_chrome_singleton_artifacts(SESSION_DIR)

    logger.info("Iniciando keepalive headless para %s", SESSION_DIR)

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            str(SESSION_DIR),
            headless=True,
            locale="pt-BR",
            viewport={"width": 1280, "height": 900},
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
            ],
        )
        try:
            page = ctx.pages[0] if ctx.pages else ctx.new_page()

            for url in KEEPALIVE_URLS:
                try:
                    logger.info("Visitando %s", url)
                    page.goto(url, wait_until="domcontentloaded", timeout=20_000)
                    page.wait_for_timeout(2000)
                except Exception as exc:
                    logger.warning("Falha ao visitar %s: %s", url, exc)

            health = check_health(page)
            logger.info("Health check: score=%s/2 ok=%s indicators=%s",
                        health["score"], health["ok"], health["indicators"])
        finally:
            ctx.close()

    if health["ok"]:
        copied = sync_base_session_to_worker_profiles(skip_in_use=True)
        if copied:
            logger.info("Perfis sincronizados: %s", [str(p) for p in copied])
        else:
            logger.info("Perfis já atualizados, nenhuma sincronização necessária")
        return True
    else:
        logger.error("Sessão Google inválida! score=%s", health["score"])
        _send_telegram(
            "⚠️ *Sessão Google expirada*\n\n"
            "Keepalive detectou sessão inválida.\n"
            "Execute no servidor:\n"
            "`cd /opt/vooindo-bot && .venv/bin/python renew_google_session.py`"
        )
        return False


def main() -> int:
    ok = run_keepalive()
    if ok:
        logger.info("Keepalive concluído com sucesso")
        return 0
    else:
        logger.error("Keepalive falhou — sessão expirada")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
