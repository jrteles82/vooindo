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


def check_session_score() -> int:
    """Retorna 0-3. 3 = autenticado."""
    if not COOKIES_DB.exists():
        print(f'❌ Cookie DB não encontrado: {COOKIES_DB}')
        return 0
    try:
        conn = sqlite3.connect(str(COOKIES_DB))
        cur = conn.cursor()
        cur.execute("SELECT name FROM cookies WHERE name = ?", (REQUIRED_COOKIE,))
        has_gaps = cur.fetchone() is not None
        cur.execute("SELECT name FROM cookies WHERE name IN ('SAPISID','APISID','HSID','SSID','SID','OSID')")
        legacy = len(cur.fetchall())
        conn.close()
        if has_gaps:
            return 3
        if legacy >= 3:
            return 3
        if legacy >= 1:
            return 1
        return 0
    except Exception as e:
        print(f'❌ Erro ao ler cookies: {e}')
        return 0


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
