#!/opt/vooindo/.venv/bin/python
"""
Verifica se a sessão Google está válida (score 3/3).
Usado antes de rodadas agendadas e manuais.
Se score < 3, notifica admin via Telegram.
"""
import sqlite3
import sys
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
COOKIES_DB = BASE_DIR / 'google_session' / 'Default' / 'Cookies'
REQUIRED_COOKIE = '__Host-GAPS'
ADMIN_IDS = [1748352987]  # Teles


def check_session_score(verify_with_browser: bool = True) -> int:
    """Retorna 0-3. 3 = autenticado.
    
    Se verify_with_browser=True e SQLite der score >= 2, faz verificação
    real abrindo Chrome com Playwright (check_session_health) pra evitar
    falsos positivos (cookies no DB mas sessão não reconhecida).
    """
    score = _score_sqlite()
    if score >= 2 and verify_with_browser:
        browser_score = _score_with_browser()
        if browser_score < 2:
            print(f'⚠️ SQLite deu {score}/3 mas navegação real deu {browser_score}/3')
            print('⚠️ Os cookies estão no DB mas o Google não reconhece a sessão')
            return browser_score
    return score


def _score_sqlite() -> int:
    if not COOKIES_DB.exists():
        print(f'❌ Cookie DB não encontrado: {COOKIES_DB}')
        return 0
    try:
        conn = sqlite3.connect(str(COOKIES_DB))
        cur = conn.cursor()
        cur.execute("SELECT name, value FROM cookies WHERE name = ?", (REQUIRED_COOKIE,))
        gaps_row = cur.fetchone()
        has_gaps = gaps_row is not None and len(gaps_row[1] or '') > 0
        cur.execute("SELECT name, value FROM cookies WHERE name IN ('SAPISID','APISID','HSID','SSID','SID','OSID')")
        legacy_rows = cur.fetchall()
        legacy_with_value = [r for r in legacy_rows if len(r[1] or '') > 0]
        conn.close()
        if has_gaps:
            return 3
        if len(legacy_with_value) >= 3:
            return 3
        if len(legacy_with_value) >= 1:
            return 1
        return 0
    except Exception as e:
        print(f'❌ Erro ao ler cookies: {e}')
        return 0


def _score_with_browser() -> int:
    """Abre Chrome e verifica sessão real com check_session_health."""
    try:
        from playwright.sync_api import sync_playwright
        from google_flights_executor import check_session_health
        for f in BASE_DIR.glob('google_session/Singleton*'):
            try: f.unlink()
            except: pass
        with sync_playwright() as pw:
            ctx = pw.chromium.launch_persistent_context(
                str(BASE_DIR / 'google_session'), headless=True, channel='chrome',
                args=['--no-sandbox'], timeout=20000)
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto('https://www.google.com/', wait_until='domcontentloaded', timeout=20000)
            health = check_session_health(page)
            ctx.close()
            return health.get('score', 0)
    except Exception as e:
        print(f'⚠️ Browser check falhou: {e}')
        return -1


def notify_admin(score: int, via_bot: bool = True):
    """Envia notificação para o admin via Telegram."""
    msg = (
        f'⚠️ *Sessão Google com score {score}/3*\n\n'
        f'A próxima rodada pode falhar pois a sessão não está autenticada.\n\n'
        f'Renove a sessão:\n'
        f'`/renovar_sessao`'
    )
    if via_bot:
        # Tenta enviar pelo bot em execução
        try:
            sys.path.insert(0, str(BASE_DIR))
            from bot import send_message_sync
            for admin_id in ADMIN_IDS:
                send_message_sync(admin_id, msg)
            print(f'✅ Notificação enviada para admins')
            return
        except ImportError:
            pass
        except Exception as e:
            print(f'⚠️ Erro ao notificar via bot: {e}')

    # Fallback: escreve log
    print(f'⚠️ [ADMIN ALERT] {msg}')


def main():
    score = check_session_score()
    print(f'Score: {score}/3')

    if score >= 3:
        print('✅ Sessão Google válida')
        return 0
    else:
        print('❌ Sessão Google inválida')
        # Só notifica se chamado com --notify
        if '--notify' in sys.argv:
            notify_admin(score)
        return 1


if __name__ == '__main__':
    sys.exit(main())
