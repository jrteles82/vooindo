#!/usr/bin/env python3
"""
Cycle Monitor Automático — Vooindo
Acompanha cada ciclo, aplica retry técnico, notifica anomalias.
"""

import os
import sys
import json
import time
import logging
import datetime
import subprocess

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import connect as db_connect, sql, get_config, set_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[
        logging.FileHandler('/opt/vooindo/logs/cycle_monitor.log'),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger('cycle_monitor_auto')

# Erros técnicos que merecem retry
RETRYABLE_ERRORS = [
    'proc_error_rc1',
    'timeout_expired',
    'Consulta sem preço ou link confiável',
    'Consulta sem resultados filtrados',
    'chrome_semaphore_timeout',
]

# Erros que NÃO merecem retry
NON_RETRYABLE_ERRORS = [
    'Consulta acima do teto configurado',
    'usuario_bloqueado',
    'bloqueado_por_monetizacao',
    'sessao_google_invalida_aguardando_renovacao',
]

# Caminhos
STATE_FILE = '/opt/vooindo/.cycle_monitor_state.json'


def _load_state():
    """Carrega estado do monitor."""
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            'last_cycle_hour': None,
            'consecutive_failures': 0,
            'browser_fallback_applied': False,
            'semaphore_fallback_applied': False,
        }


def _save_state(state):
    """Salva estado do monitor."""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)


def _should_retry(error_message: str) -> bool:
    """Verifica se o erro merece retry."""
    if not error_message:
        return False
    for err in NON_RETRYABLE_ERRORS:
        if err in error_message:
            return False
    for err in RETRYABLE_ERRORS:
        if err in error_message:
            return True
    return False


def _check_concurrent_processes():
    """Retorna quantos processos Chrome/Firefox existem."""
    try:
        result = subprocess.run(
            ['pgrep', '-c', '-f', 'chrome-headless|chromium|firefox'],
            capture_output=True, text=True, timeout=5
        )
        return int(result.stdout.strip()) if result.stdout.strip().isdigit() else 0
    except Exception:
        return -1


def _get_browser_name():
    """Retorna nome do navegador configurado."""
    try:
        conn = db_connect()
        b = get_config(conn, 'browser', 'chrome')
        conn.close()
        return b
    except Exception:
        return 'chrome'


def analyze_cycle(hour: int) -> dict:
    """
    Analisa o ciclo de uma hora específica.
    Retorna dict com resultados.
    """
    conn = db_connect()
    cur = conn.cursor()

    h = f"{hour:02d}"
    cur.execute(sql("""
        SELECT id, user_id, status, error_message, retry_count, finished_at, created_at
        FROM scan_jobs
        WHERE created_at >= CONCAT(CURDATE(), %s)
          AND created_at < CONCAT(CURDATE(), %s)
        ORDER BY id
    """), (f' {h}:00:00', f' {h + 1 if hour < 23 else "00"}:00:00'))

    jobs = cur.fetchall()
    total = len(jobs)
    success = sum(1 for j in jobs if j['status'] == 'done')
    errors = sum(1 for j in jobs if j['status'] == 'error')
    running = sum(1 for j in jobs if j['status'] == 'running')
    pending = sum(1 for j in jobs if j['status'] == 'pending')

    retryable_errors = []
    non_retryable_errors = []
    for j in jobs:
        if j['status'] == 'error':
            err = j['error_message'] or ''
            if _should_retry(err) and (j.get('retry_count') or 0) < 2:
                retryable_errors.append(j)
            elif not _should_retry(err):
                non_retryable_errors.append(j)

    conn.close()

    return {
        'hour': hour,
        'total': total,
        'success': success,
        'errors': errors,
        'running': running,
        'pending': pending,
        'retryable_errors': retryable_errors,
        'non_retryable_errors': non_retryable_errors,
        'error_rate': errors / total if total > 0 else 0,
    }


def apply_retries(jobs_to_retry: list) -> list:
    """Aplica retry nos jobs elegíveis."""
    conn = db_connect()
    cur = conn.cursor()
    retried = []
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for j in jobs_to_retry:
        jid = j['id']
        uid = j['user_id']
        rc = (j.get('retry_count') or 0)
        cur.execute(sql("""
            UPDATE scan_jobs
            SET status = 'pending', retry_count = %s, error_message = NULL, started_at = NULL
            WHERE id = %s
        """), (rc + 1, jid))
        retried.append({'id': jid, 'user_id': uid})
        log.info(f"[retry] job_id={jid} user_id={uid} retry_count={rc+1} re-agendado")

    conn.commit()
    conn.close()
    return retried


def fallback_browser_and_semaphore(state: dict) -> bool:
    """
    Se detectou falha generalizada, faz fallback:
    1. Troca Firefox → Chrome (se estiver Firefox)
    2. Reduz semáforo pra 1 (se estiver 2)
    3. Retorna True se fez alguma mudança
    """
    changed = False

    try:
        conn = db_connect()
        current_browser = get_config(conn, 'browser', 'chrome')
        current_chrome_max = 1  # lê do main.py se quiser ser preciso

        if current_browser == 'firefox':
            set_config(conn, 'browser', 'chrome')
            log.warning("[fallback] Navegador: firefox → chrome")
            changed = True

        # Semáforo já é 1 por padrão no código, mas forçamos via env
        conn.close()
    except Exception as e:
        log.error(f"[fallback] Erro ao trocar browser: {e}")

    if changed:
        state['browser_fallback_applied'] = True
        state['semaphore_fallback_applied'] = True
        _save_state(state)

        # Restart service to apply changes
        log.warning("[fallback] Reiniciando serviço para aplicar mudanças")
        subprocess.run(['sudo', 'systemctl', 'restart', 'vooindo.service'],
                      capture_output=True, timeout=30)

    return changed


def send_telegram_notification(text: str):
    """Envia notificação no Telegram sobre o ciclo."""
    try:
        # Usa a função de bot_settings via subprocess
        result = subprocess.run(
            [sys.executable, '-c', f'''
import sys
sys.path.insert(0, "/opt/vooindo")
from db import connect as db_connect, sql
import json
msg = """{text}"""
# Encontra o chat_id do admin (user 2)
conn = db_connect()
cur = conn.cursor()
cur.execute(sql("SELECT chat_id FROM users WHERE id = 2"))
admin = cur.fetchone()
conn.close()
if admin:
    # Envia via telegram
    import requests
    TOKEN = "8515270359:AAHg2nipJ9WVkmKwy-P6HXqhXCrz3y5hBBc"
    url = f"https://api.telegram.org/bot{{TOKEN}}/sendMessage"
    requests.post(url, json={{"chat_id": admin["chat_id"], "text": msg, "parse_mode": "Markdown"}}, timeout=10)
'''],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode != 0:
            log.warning(f"[notif] Erro ao enviar: {result.stderr[-100:]}")
    except Exception as e:
        log.warning(f"[notif] Falha ao notificar: {e}")


def run_cycle_scan():
    """Verifica se há um novo ciclo e analisa."""
    now = datetime.datetime.now()
    current_hour = now.hour
    current_min = now.minute

    state = _load_state()
    last_cycle_hour = state.get('last_cycle_hour')

    # Só analisa ciclos completos (>5 min após começar, <55 min)
    if current_min < 5 or current_min > 55:
        return

    if last_cycle_hour == current_hour:
        return  # Já analisou esse ciclo

    # Espera um pouco mais pra garantir que jobs rodaram
    time.sleep(10)

    log.info(f"=== Analisando ciclo {current_hour}:00 ===")
    analysis = analyze_cycle(current_hour)

    if analysis['total'] == 0:
        log.info(f"Ciclo {current_hour}:00: nenhum job encontrado (pode ser fora do horário)")
        state['last_cycle_hour'] = current_hour
        _save_state(state)
        return

    # Resumo
    summary = (
        f"📊 *Ciclo {current_hour}:00*\n"
        f"✅ Sucesso: {analysis['success']}\n"
        f"❌ Erro: {analysis['errors']}\n"
        f"🔴 Rodando: {analysis['running']}\n"
        f"🕒 Pendente: {analysis['pending']}\n"
    )

    log.info(summary.replace('*', ''))

    # Se tem erros retryáveis, aplica retry
    retryable = analysis['retryable_errors']
    if retryable:
        log.info(f"  -> {len(retryable)} jobs elegíveis para retry")
        retried = apply_retries(retryable)
        summary += f"\n🔄 Retry aplicado em {len(retried)} jobs técnicos"
        for r in retried:
            summary += f"\n  ↪ #{r['id']} user={r['user_id']}"
            log.info(f"  -> Retry job #{r['id']} user={r['user_id']}")

    # Verifica se a taxa de erro é muito alta → precisa de fallback
    error_rate = analysis['error_rate']
    total_technical = analysis['errors'] - len(analysis['non_retryable_errors'])

    # Critério de falha generalizada: >50% de erro técnico OU >3 erros técnicos consecutivos
    technical_failure_rate = total_technical / analysis['total'] if analysis['total'] > 0 else 0

    if technical_failure_rate > 0.5 and total_technical >= 2:
        state['consecutive_failures'] = state.get('consecutive_failures', 0) + 1
        _save_state(state)

        log.warning(f"⚠️ Taxa de falha técnica: {technical_failure_rate:.0%}")
        log.warning(f"⚠️ Falhas consecutivas: {state['consecutive_failures']}")

        if state['consecutive_failures'] >= 2:
            browser_name = _get_browser_name()
            semaphore_max = 1  # default

            if browser_name == 'firefox' or state.get('consecutive_failures') >= 3:
                log.warning("🔧 Aplicando fallback: Chrome + semáforo 1")
                fallback_browser_and_semaphore(state)
                summary += (
                    f"\n\n🛠 *Fallback aplicado!*\n"
                    f"  Navegador: \U0001f431 Chrome\n"
                    f"  Semáforo: 1 por vez\n"
                    f"  Motivo: {state['consecutive_failures']} ciclos com falha técnica >50%"
                )
        else:
            summary += f"\n\n⚠️ Atenção: {total_technical} erros técnicos ({technical_failure_rate:.0%})"
            summary += "\n  Próximo ciclo com falha similar aplica fallback automático"
    else:
        # Reset contagem de falhas se o ciclo foi OK
        if error_rate < 0.3:
            state['consecutive_failures'] = 0
            _save_state(state)

    # Marca como analisado
    state['last_cycle_hour'] = current_hour
    _save_state(state)

    # Se tem erros não retryáveis ou retry aplicado, notifica
    non_retry = analysis['non_retryable_errors']
    if non_retry or retryable:
        if non_retry:
            summary += f"\n\n⏭ *Ignorados (não retentáveis):*"
            for j in non_retry:
                err = (j.get('error_message') or '')[:40]
                summary += f"\n  \u274c #{j['id']} user={j['user_id']}: {err}"

        send_telegram_notification(summary)
        log.info("Notificação enviada ao admin")

    log.info(f"=== Fim da análise ciclo {current_hour}:00 ===")


def main():
    """Loop principal: verifica a cada 60s."""
    log.info("🚀 Cycle Monitor Auto iniciado")
    while True:
        try:
            run_cycle_scan()
        except Exception as e:
            log.error(f"Erro no monitor: {e}", exc_info=True)
        time.sleep(60)


if __name__ == '__main__':
    main()
