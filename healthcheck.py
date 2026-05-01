#!/usr/bin/env python3
"""Healthcheck do Vooindo — verifica serviço, notifica admin, tenta auto-fix."""
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
ADMIN_CHAT_ID = os.getenv('TELEGRAM_ADMIN_CHAT_ID', '').strip()
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
STATE_FILE = Path('/tmp/vooindo_healthcheck_state.json')

LOG_THRESHOLD_MINUTES = 10  # Alerta se último log de erro for mais recente que isso
RESTART_COOLDOWN_SECONDS = 300  # 5 min entre auto-restarts


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {'last_restart': 0, 'last_alert': 0, 'consecutive_failures': 0, 'known_bugs': []}


def save_state(state: dict):
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def now_ts() -> int:
    return int(time.time())


def get_recent_errors(minutes: int = LOG_THRESHOLD_MINUTES) -> list[dict]:
    """Pega erros recentes do journal do Vooindo."""
    since = (datetime.now() - timedelta(minutes=minutes)).strftime('%Y-%m-%d %H:%M:%S')
    try:
        result = subprocess.run(
            ['journalctl', '-u', 'vooindo.service', '--no-pager', '--since', since, '--output', 'cat'],
            capture_output=True, text=True, timeout=10
        )
        lines = result.stdout.split('\n')
    except Exception as e:
        return [{'level': 'ERROR', 'msg': f'Falha ao ler journal: {e}'}]

    errors = []
    for line in lines:
        if any(kw in line.upper() for kw in ['ERROR', 'CRITICAL', 'CRASH', 'TRACEBACK', 'IMPORTERROR']):
            errors.append({
                'level': 'ERROR',
                'msg': line.strip()[:200],
                'ts': datetime.now().isoformat()
            })
    return errors


def check_service() -> dict:
    """Verifica status do serviço e processos filhos."""
    result = {
        'healthy': False,
        'service_active': False,
        'scheduler_alive': False,
        'bot_alive': False,
        'workers_alive': 0,
        'recent_errors': [],
        'last_restart_ago': None,
        'message': ''
    }

    # 1. systemd status
    try:
        r = subprocess.run(['systemctl', 'is-active', 'vooindo.service'], capture_output=True, text=True, timeout=5)
        result['service_active'] = (r.stdout.strip() == 'active')
    except Exception:
        pass

    # 2. Processos filhos vivos
    try:
        ps = subprocess.run(['pgrep', '-af', 'bot_scheduler.py'], capture_output=True, text=True, timeout=5)
        result['scheduler_alive'] = bool(ps.stdout.strip())

        ps = subprocess.run(['pgrep', '-af', 'bot.py'], capture_output=True, text=True, timeout=5)
        result['bot_alive'] = bool(ps.stdout.strip())

        ps = subprocess.run(['pgrep', '-af', 'job_worker.py'], capture_output=True, text=True, timeout=5)
        result['workers_alive'] = len([l for l in ps.stdout.split('\n') if l.strip()])
    except Exception:
        pass

    # 3. Erros recentes
    result['recent_errors'] = get_recent_errors(minutes=LOG_THRESHOLD_MINUTES)

    # 4. Último restart do service
    try:
        r = subprocess.run(
            ['journalctl', '-u', 'vooindo.service', '--no-pager', '--output', 'cat',
             '--since', '1 hour ago'],
            capture_output=True, text=True, timeout=10
        )
        last_start_match = re.findall(r'Started vooindo\.service', r.stdout)
        result['last_restart_count'] = len(last_start_match)
    except Exception:
        pass

    # Determinar saúde geral
    all_ok = (
        result['service_active'] and
        result['scheduler_alive'] and
        result['bot_alive'] and
        len(result['recent_errors']) == 0
    )
    result['healthy'] = all_ok

    if all_ok:
        result['message'] = '✅ Vooindo saudável'
    elif not result['service_active']:
        result['message'] = '❌ Serviço parado'
    elif not result['scheduler_alive']:
        result['message'] = '⚠️ Scheduler morto'
    elif result['recent_errors']:
        result['message'] = f"⚠️ {len(result['recent_errors'])} erros recentes"
    else:
        result['message'] = '❓ Estado desconhecido'

    return result


def try_auto_fix(health: dict, state: dict) -> bool:
    """Tenta corrigir problemas comuns automaticamente."""
    now = now_ts()

    if not health['service_active']:
        # Serviço parado — restart
        if now - state.get('last_restart', 0) > RESTART_COOLDOWN_SECONDS:
            subprocess.run(['systemctl', 'restart', 'vooindo.service'], timeout=30)
            state['last_restart'] = now
            state['consecutive_failures'] = state.get('consecutive_failures', 0) + 1
            save_state(state)
            return True
        return False

    if not health['scheduler_alive'] and health['bot_alive']:
        # Scheduler morreu mas bot ainda vive — restart total
        if now - state.get('last_restart', 0) > RESTART_COOLDOWN_SECONDS:
            subprocess.run(['systemctl', 'restart', 'vooindo.service'], timeout=30)
            state['last_restart'] = now
            state['consecutive_failures'] = state.get('consecutive_failures', 0) + 1
            save_state(state)
            return True
        return False

    return False


def send_alert(message: str, state: dict) -> None:
    """Envia alerta pro admin via Telegram."""
    now = now_ts()
    cooldown = 300  # 5 min entre alertas do mesmo tipo
    last_alert = state.get('last_alert', 0)
    if now - last_alert < cooldown:
        return

    state['last_alert'] = now
    save_state(state)

    if not ADMIN_CHAT_ID or not TOKEN:
        return

    try:
        import asyncio
        from telegram import Bot
        from telegram.request import HTTPXRequest
        request = HTTPXRequest(connection_pool_size=5, pool_timeout=10.0)
        bot = Bot(token=TOKEN, request=request)
        asyncio.run(bot.send_message(chat_id=ADMIN_CHAT_ID, text=message))
    except Exception as exc:
        print(f'Falha ao enviar alerta: {exc}', file=sys.stderr)


def main():
    state = load_state()
    health = check_service()
    fixed = False

    if not health['healthy']:
        print(f"[HEALTHCHECK] {health['message']}")
        if health['recent_errors']:
            for err in health['recent_errors'][:5]:
                print(f"  └ {err['msg']}")

        # Tenta auto-fix
        fixed = try_auto_fix(health, state)
        if fixed:
            print(f"[HEALTHCHECK] Auto-fix aplicado: restart do serviço")
            time.sleep(3)  # Espera service subir
            health = check_service()  # Recheck

        # Se ainda não saudável, notifica admin
        if not health['healthy']:
            msg_lines = [
                f"🔴 Vooindo: {health['message']}",
                f"Service: {'ativo' if health['service_active'] else 'parado'}",
                f"Bot: {'vivo' if health['bot_alive'] else 'morto'}",
                f"Scheduler: {'vivo' if health['scheduler_alive'] else 'morto'}",
                f"Workers: {health['workers_alive']}",
                f"Auto-fix: {'✅ aplicado' if fixed else '❌ não aplicado (cooldown)'}",
            ]
            if health['recent_errors']:
                msg_lines.append("\nErros recentes:")
                for err in health['recent_errors'][:5]:
                    msg_lines.append(f"  • {err['msg'][:150]}")
            send_alert('\n'.join(msg_lines), state)
    else:
        # Tudo ok — reseta contagem de falhas
        if state.get('consecutive_failures', 0) > 0:
            state['consecutive_failures'] = 0
            save_state(state)
            send_alert("✅ Vooindo recuperado — serviço estável", state)

    print(f"[HEALTHCHECK] {health['message']} | fix={fixed}")


if __name__ == '__main__':
    main()
