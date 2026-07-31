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


def check_guardian() -> dict:
    """Verifica o Chrome Guardian via HTTP."""
    result = {'healthy': False, 'ready': False, 'session_ok': False, 'message': ''}
    try:
        import urllib.request
        resp = urllib.request.urlopen('http://127.0.0.1:9230/status', timeout=5)
        data = json.loads(resp.read().decode())
        result['ready'] = data.get('ready', 0) == 1
        result['session_ok'] = data.get('session_ok', False)
        result['healthy'] = result['ready'] and result['session_ok']
        if not result['ready']:
            result['message'] = 'Chrome guardian: Chrome não está pronto'
        elif not result['session_ok']:
            result['message'] = 'Chrome guardian: sessão Google inválida'
        else:
            result['message'] = '✅ Guardian OK'
    except Exception as e:
        result['message'] = f'Chrome guardian inacessível: {e}'
    return result


def try_guardian_fix() -> bool:
    """Tenta restartar o guardian se estiver com problemas."""
    try:
        subprocess.run(['systemctl', 'restart', 'chrome-guardian.service'], timeout=30)
        time.sleep(10)
        # Verifica se subiu
        import urllib.request
        resp = urllib.request.urlopen('http://127.0.0.1:9230/status', timeout=5)
        data = json.loads(resp.read().decode())
        return data.get('ready', 0) == 1 and data.get('session_ok', False)
    except Exception:
        return False


def count_active_scan_jobs() -> int:
    """Conta só jobs realmente em execução para evitar matar Chrome no meio de scan.

    Jobs pending/waiting_route_dedupe são backlog de fila; se o Guardian está ruim,
    eles não devem impedir a recuperação automática, senão a fila inteira fica presa.
    """
    try:
        sys.path.insert(0, str(BASE_DIR))
        from db import connect as db_connect, sql
        conn = db_connect()
        cur = conn.cursor()
        cur.execute(sql("""
            SELECT COUNT(*) AS cnt
            FROM scan_jobs
            WHERE status = 'running'
              AND started_at >= NOW() - INTERVAL 3 HOUR
        """))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return int((row or {}).get('cnt') or 0)
    except Exception:
        # Na dúvida, seja conservador: não reinicie o guardian agressivamente.
        return 1


def check_stale_jobs(hours: int = 2) -> dict:
    """Verifica quantos stale_running_recovered nas últimas N horas."""
    result = {'stale_count': 0, 'message': ''}
    try:
        sys.path.insert(0, str(BASE_DIR))
        from db import connect as db_connect, sql
        conn = db_connect()
        cur = conn.cursor()
        cur.execute(sql("""
            SELECT COUNT(*) AS cnt
            FROM scan_jobs
            WHERE error_message = 'stale_running_recovered'
              AND finished_at >= NOW() - INTERVAL %s HOUR
        """), (hours,))
        row = cur.fetchone()
        result['stale_count'] = int(row['cnt'] if isinstance(row, dict) else row[0])
        if result['stale_count'] > 0:
            result['message'] = f"⚠️ {result['stale_count']} stale_running_recovered nas últimas {hours}h"
        conn.close()
    except Exception as e:
        result['message'] = f'Falha ao verificar stales: {e}'
    return result


def check_google_session() -> dict:
    """Verifica sessão Google via Guardian (sem abrir Chrome próprio, evitando conflito de profile lock)."""
    result = {'ok': True, 'score': 3, 'message': ''}
    try:
        import urllib.request as _req
        _resp = _req.urlopen('http://127.0.0.1:9230/status', timeout=5)
        import json as _json
        _status = _json.loads(_resp.read().decode())
        if _status.get('session_ok', False):
            result['score'] = 3
        else:
            result['ok'] = False
            result['score'] = 0
            result['message'] = 'Guardian reporta session_ok=false'
    except Exception as e:
        result['ok'] = True  # Se falhou ao verificar, não alarmar
        result['message'] = f'Verificação de sessão falhou: {e}'
    return result


def show_dashboard():
    """Exibe dashboard com histórico das rodadas."""
    try:
        sys.path.insert(0, str(BASE_DIR))
        from db import connect as db_connect
        conn = db_connect()
        cur = conn.cursor()
        
        # Relatorios do bot_scheduler
        import subprocess
        r = subprocess.run(['journalctl', '-u', 'vooindo.service', '--no-pager', '--since', '12 hours ago'],
            capture_output=True, text=True, timeout=15)
        
        rodadas = []
        for line in r.stdout.splitlines():
            if 'rodada' in line and 'finalizada' in line and 'done=' in line:
                import re
                m = re.search(r'rodada (\S+) finalizada.*?done=(\d+).*?error=(\d+).*?wait_s=([\d.]+)', line)
                if m:
                    rodadas.append({'h': m.group(1)[11:16], 'done': int(m.group(2)), 'err': int(m.group(3)), 'wait_s': float(m.group(4))})

        print('\\n📊 DASHBOARD VOOINDO')
        if rodadas:
            print()
            print(f'{"Hora":<8} {"Rotas":<7} {"✅":<5} {"❌":<5} {"⏱":<10} {"Tend":<12}')
            print('-' * 47)
            prev_s = None
            for rod in rodadas[-7:]:
                total = rod['done'] + rod['err']
                m = int(rod['wait_s'] // 60)
                s = int(rod['wait_s'] % 60)
                tempo = f'{m}m{s:02d}s'
                if prev_s:
                    ratio = rod['wait_s'] / prev_s if prev_s else 1
                    trend = '⬆️ +rápido' if ratio < 0.90 else ('⬇️ +lento' if ratio > 1.10 else '➡️ estável')
                else:
                    trend = '—'
                icon = '✅' if rod['err'] == 0 else '⚠️'
                print(f'{icon} {rod["h"]:<6} {total:<7} {rod["done"]:<5} {rod["err"]:<5} {tempo:<10} {trend}')
                prev_s = rod['wait_s']
            td = sum(r['done'] for r in rodadas)
            te = sum(r['err'] for r in rodadas)
            print('-' * 47)
            print(f'      {td+te:<7} {td:<5} {te:<5}')
            print(f'      {"0 erros" if te == 0 else f"{te} erros"}')
        else:
            print('Nenhuma rodada encontrada nos logs')

        # Usuarios mais lentos
        cur.execute('''SELECT bu.first_name, ROUND(AVG(TIMESTAMPDIFF(SECOND, j.started_at, j.finished_at))) as avg_s, MAX(TIMESTAMPDIFF(SECOND, j.started_at, j.finished_at)) as max_s, COUNT(*) as total FROM scan_jobs j JOIN bot_users bu ON bu.user_id = j.user_id WHERE j.finished_at >= NOW() - INTERVAL 3 HOUR AND j.status = "done" AND j.started_at IS NOT NULL GROUP BY bu.first_name ORDER BY avg_s DESC LIMIT 5''')
        lentos = cur.fetchall()
        if lentos:
            print()
            print('⏱ Mais lentos (média 3h):')
            for l in lentos:
                print(f'  {l["first_name"]:15} ⏱{l["avg_s"]:>4}s | pico {l["max_s"]:>4}s | {l["total"]} scans')

        # Guardian
        import urllib.request
        try:
            g = json.loads(urllib.request.urlopen('http://127.0.0.1:9230/status', timeout=3).read())
            gs = '✅ OK' if g.get('ready') and g.get('session_ok') else '⚠️ PROBLEMA'
            print(f'\\n🛡️ Guardian: {gs}')
        except:
            print('\\n🛡️ Guardian: ❌ inacessível')

        conn.close()
    except Exception as e:
        print(f'Dashboard: {e}', file=sys.stderr)


def main():
    if '--dashboard' in sys.argv or '-d' in sys.argv:
        show_dashboard()
        return

    health = check_service()
    guardian = check_guardian()
    state = load_state()
    fixed = False
    guardian_fixed = False
    
    # Verifica sessão Google
    session = check_google_session()
    if not session['ok']:
        send_alert(session['message'], {})
    
    # Verifica stales (goal = zero)
    stales = check_stale_jobs(hours=2)
    if stales['stale_count'] > 0:
        send_alert(stales['message'], {})
    
    # Auto-fix: guardian
    # Evita falso positivo durante rodada: reiniciar o Guardian mata o Chrome/CDP
    # usado pelos workers e transforma lentidão transitória em timeouts em cascata.
    if guardian['healthy']:
        if state.get('guardian_consecutive_failures'):
            state['guardian_consecutive_failures'] = 0
            save_state(state)
    else:
        state['guardian_consecutive_failures'] = int(state.get('guardian_consecutive_failures', 0)) + 1
        active_jobs = count_active_scan_jobs()
        last_guardian_restart = int(state.get('last_guardian_restart', 0))
        can_restart = (
            state['guardian_consecutive_failures'] >= 3
            and active_jobs == 0
            and now_ts() - last_guardian_restart >= RESTART_COOLDOWN_SECONDS
        )
        if can_restart:
            print(f"[HEALTHCHECK] Guardian: {guardian['message']} — tentando restart após {state['guardian_consecutive_failures']} falhas consecutivas...")
            state['last_guardian_restart'] = now_ts()
            save_state(state)
            if try_guardian_fix():
                guardian = check_guardian()
                if guardian['healthy']:
                    guardian_fixed = True
                    state['guardian_consecutive_failures'] = 0
                    save_state(state)
                    print(f"[HEALTHCHECK] Guardian reiniciado com sucesso ✅")
        else:
            print(
                f"[HEALTHCHECK] Guardian: {guardian['message']} — restart adiado "
                f"(falhas={state['guardian_consecutive_failures']}, active_jobs={active_jobs})"
            )
            save_state(state)
    
    # Auto-fix: vooindo
    if not health['healthy']:
        if try_auto_fix(health, state):
            fixed = True
            time.sleep(3)
            health = check_service()
        
        if not health['healthy']:
            msg_lines = [
                f"🔴 Vooindo: {health['message']}",
                f"Service: {'ativo' if health['service_active'] else 'parado'}",
                f"Bot: {'vivo' if health['bot_alive'] else 'morto'}",
                f"Scheduler: {'vivo' if health['scheduler_alive'] else 'morto'}",
                f"Workers: {health['workers_alive']}",
                f"Guardian: {guardian['message']}",
                f"Stales (2h): {stales['stale_count']}",
                f"Auto-fix vooindo: {'✅' if fixed else '❌ (cooldown)'}",
                f"Auto-fix guardian: {'✅' if guardian_fixed else '—'}",
            ]
            if health['recent_errors']:
                msg_lines.append('\nErros recentes:')
                for err in health['recent_errors'][:5]:
                    msg_lines.append(f"  • {err['msg'][:150]}")
            send_alert('\n'.join(msg_lines), state)
    else:
        state = load_state()
        if state.get('consecutive_failures', 0) > 0:
            state['consecutive_failures'] = 0
            save_state(state)
        if stales['stale_count'] > 0:
            send_alert(stales['message'], state)

    # Resumo da saída
    parts = [f"[HEALTHCHECK] v={health['message'].split('|')[0].strip()}"]
    if not guardian['healthy']:
        parts.append(f"g=⚠️")
    else:
        parts.append(f"g=✅")
    if fixed:
        parts.append("fix_vooindo=✅")
    if guardian_fixed:
        parts.append("fix_guardian=✅")
    if stales['stale_count'] > 0:
        parts.append(f"stales_2h={stales['stale_count']}")
    print(' | '.join(parts))


if __name__ == '__main__':
    main()
