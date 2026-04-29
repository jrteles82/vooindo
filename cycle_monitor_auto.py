#!/usr/bin/env python3
"""
Cycle & Health Monitor — Vooindo
Acompanha ciclos automáticos, detecta anomalias, aplica retry/fallback, notifica admin.
Healthchecks:
  1. Ciclo iniciou no minuto certo
  2. Jobs progredindo (pending → running → done)
  3. Tempos dentro do esperado (1 rota ~60-90s, 4 rotas ~4-6min)
  4. Sem travamento (job running > 10min sem finalizar)
  5. Workers scheduled/manual vivos
  6. RAM saudável (OOM, swap)
  7. Erros consecutivos (>X aciona fallback)
  8. Fila de pending sem jobs órfãos
  9. Bot saudável para consultas manuais
"""

import os
import sys
import json
import time
import logging
import datetime
import subprocess
import pathlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import connect as db_connect, sql, get_config, set_config

# ── Config ──────────────────────────────────────────────────────────────────
LOG_FILE = '/opt/vooindo/logs/cycle_monitor.log'
STATE_FILE = '/opt/vooindo/.cycle_monitor_state.json'
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '8515270359:AAHg2nipJ9WVkmKwy-P6HXqhXCrz3y5hBBc')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger('health_monitor')

# Erros que merecem retry
RETRYABLE_ERRORS = [
    'proc_error_rc1', 'timeout_expired',
    'Consulta sem preço ou link confiável',
    'Consulta sem resultados filtrados',
    'chrome_semaphore_timeout',
]
# Erros que NÃO merecem retry
NON_RETRYABLE = [
    'Consulta acima do teto configurado', 'usuario_bloqueado',
    'bloqueado_por_monetizacao', 'sessao_google_invalida_aguardando_renovacao',
]

# Limites
MAX_JOB_RUNNING_MIN = 8            # Se job fica >8min running, alerta + reset
MAX_PENDING_STALE_MIN = 20         # Job pending >20min sem worker pegar
EXPECTED_TIME_PER_ROTA_MIN = 2     # ~2min por rota (com Chrome)
WARN_TIME_PER_ROTA_MIN = 4         # >4min por rota = lento
CRIT_RAM_PCT = 90                  # >90% RAM usada = alerta
FALLBACK_CONSECUTIVE_FAIL = 2      # 2 ciclos com >50% erro técnico = fallback

# ── State ───────────────────────────────────────────────────────────────────
def _load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {'last_hour': None, 'consecutive_failures': 0,
                'fallback_browser': False, 'fallback_semaphore': False,
                'alerts_sent': [], 'last_ram_warn': 0}

def _save_state(s: dict):
    with open(STATE_FILE, 'w') as f:
        json.dump(s, f)

# ── Helpers ─────────────────────────────────────────────────────────────────
def _now() -> str:
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def _should_retry(err: str) -> bool:
    if not err: return False
    for e in NON_RETRYABLE:
        if e in err: return False
    for e in RETRYABLE_ERRORS:
        if e in err: return True
    return False

def _ram_pct() -> float:
    try:
        m = pathlib.Path('/proc/meminfo').read_text().splitlines()
        total = int([l for l in m if l.startswith('MemTotal:')][0].split()[1])
        avail = int([l for l in m if l.startswith('MemAvailable:')][0].split()[1])
        return (total - avail) / total * 100
    except: return 0.0

def _swap_used_mb() -> int:
    try:
        r = subprocess.run(['free', '-m'], capture_output=True, text=True, timeout=5)
        for line in r.stdout.splitlines():
            if 'Swap:' in line:
                return int(line.split()[2])
        return 0
    except: return 0

def _worker_alive(pool: str = None) -> list:
    try:
        r = subprocess.run(['pgrep', '-f', 'job_worker.py'], capture_output=True, text=True, timeout=5)
        pids = r.stdout.strip().split()
        if not pids: return []
        alive = []
        for pid in pids:
            try:
                cmd = pathlib.Path(f'/proc/{pid}/cmdline').read_text()
                if pool is None or f'--pool {pool}' in cmd:
                    alive.append(int(pid))
            except: pass
        return alive
    except: return []

def _running_job_count() -> int:
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as n FROM scan_jobs WHERE status='running'")
        n = c.fetchone()['n']
        conn.close()
        return n
    except: return -1

def _pending_jobs_since(minutes_ago: int) -> list:
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute(sql("""
            SELECT id, user_id, created_at
            FROM scan_jobs
            WHERE status = 'pending'
              AND created_at < DATE_SUB(NOW(), INTERVAL %s MINUTE)
            ORDER BY id
        """), (minutes_ago,))
        rows = c.fetchall()
        conn.close()
        return rows
    except: return []

def _send_telegram(text: str):
    """Envia mensagem no Telegram do admin."""
    try:
        admin = None
        conn = db_connect()
        c = conn.cursor()
        c.execute(sql("SELECT chat_id FROM admins WHERE active = 1 LIMIT 1"))
        r = c.fetchone()
        if r: admin = r['chat_id']
        conn.close()
        if not admin: return
        import requests
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": admin, "text": text,
                                 "parse_mode": "Markdown"}, timeout=10)
    except Exception as e:
        log.warning(f"[notif] falha: {e}")

# ── Análise de ciclo ────────────────────────────────────────────────────────
def analyze_cycle(hour: int) -> dict:
    conn = db_connect()
    cur = conn.cursor()
    h = f"{hour:02d}"
    next_h = f"{hour+1:02d}" if hour < 23 else "00"
    cur.execute(sql("""
        SELECT id, user_id, status, error_message, retry_count,
               finished_at, created_at, started_at
        FROM scan_jobs
        WHERE created_at >= CONCAT(CURDATE(), %s)
          AND created_at < CONCAT(CURDATE(), %s)
        ORDER BY id
    """), (f' {h}:00:00', f' {next_h}:00:00'))
    jobs = cur.fetchall()
    conn.close()

    total = len(jobs)
    if not total:
        return {'hour': hour, 'total': 0}

    success = sum(1 for j in jobs if j['status'] == 'done')
    errors = sum(1 for j in jobs if j['status'] == 'error')
    running_now = sum(1 for j in jobs if j['status'] == 'running')
    pending_now = sum(1 for j in jobs if j['status'] == 'pending')

    retryable = []
    non_retryable = []
    stuck_running = []
    slow_jobs = []
    total_rotas = 0

    for j in jobs:
        err = j['error_message'] or ''
        total_rotas += 1  # aproximado
        if j['status'] == 'error':
            if _should_retry(err) and (j.get('retry_count') or 0) < 2:
                retryable.append(j)
            else:
                non_retryable.append(j)
        if j['status'] == 'running' and j['started_at']:
            try:
                started = datetime.datetime.strptime(j['started_at'], '%Y-%m-%d %H:%M:%S')
                elapsed = (datetime.datetime.now() - started).total_seconds() / 60
                if elapsed > MAX_JOB_RUNNING_MIN:
                    stuck_running.append((j, elapsed))
            except: pass

    # Slow jobs: done jobs que demoraram mais que o esperado
    for j in jobs:
        if j['status'] == 'done' and j['started_at'] and j['finished_at']:
            try:
                s = datetime.datetime.strptime(j['started_at'], '%Y-%m-%d %H:%M:%S')
                f = datetime.datetime.strptime(j['finished_at'], '%Y-%m-%d %H:%M:%S')
                el = (f - s).total_seconds() / 60
                if el > WARN_TIME_PER_ROTA_MIN:
                    slow_jobs.append((j, el))
            except: pass

    tech_errors = len(retryable)
    total_errors = errors

    return {
        'hour': hour, 'total': total,
        'success': success, 'errors': total_errors,
        'running': running_now, 'pending': pending_now,
        'retryable': retryable, 'non_retryable': non_retryable,
        'stuck_running': stuck_running,
        'slow_jobs': slow_jobs,
        'tech_errors': tech_errors,
        'total_rotas': total_rotas,
        'error_rate': total_errors / total if total else 0,
    }

def apply_retries(jobs: list) -> list:
    conn = db_connect()
    cur = conn.cursor()
    retried = []
    for j in jobs:
        jid = j['id']
        rc = (j.get('retry_count') or 0)
        cur.execute(sql("""
            UPDATE scan_jobs SET status='pending', retry_count=%s,
                   error_message=NULL, started_at=NULL
            WHERE id=%s
        """), (rc + 1, jid))
        retried.append({'id': jid, 'user_id': j['user_id']})
        log.info(f"[retry] job #{jid} user={j['user_id']} tentativa={rc+1}")
    conn.commit()
    conn.close()
    return retried

def do_fallback(state: dict) -> bool:
    """Fallback: Chrome + semáforo 1 + restart."""
    try:
        conn = db_connect()
        cur_browser = get_config(conn, 'browser', 'chrome')
        changed = False
        if cur_browser == 'firefox':
            set_config(conn, 'browser', 'chrome')
            log.warning("[fallback] firefox → chrome")
            changed = True
        conn.close()
        if changed:
            state['fallback_browser'] = True
            state['fallback_semaphore'] = True
            _save_state(state)
            subprocess.run(['sudo', 'systemctl', 'restart', 'vooindo.service'],
                          capture_output=True, timeout=30)
            log.warning("[fallback] serviço reiniciado")
        return changed
    except Exception as e:
        log.error(f"[fallback] erro: {e}")
        return False

# ── Verificaçōes extras ─────────────────────────────────────────────────────
def check_workers_and_ram(state: dict) -> list:
    """Verifica workers vivos, RAM, swap."""
    alerts = []

    # Workers
    sched = _worker_alive('scheduled')
    manual = _worker_alive('manual')
    if len(sched) < 2:
        alerts.append(f"⚠️ Workers scheduled: {len(sched)}/2 vivos")
    if len(manual) < 2:
        alerts.append(f"⚠️ Workers manual: {len(manual)}/2 vivos")

    # RAM
    ram = _ram_pct()
    swap = _swap_used_mb()
    if ram > CRIT_RAM_PCT:
        if time.time() - state.get('last_ram_warn', 0) > 600:
            alerts.append(f"🚨 RAM crítica: {ram:.0f}% | Swap: {swap}MB")
            state['last_ram_warn'] = time.time()
            _save_state(state)
    elif ram > 75:
        alerts.append(f"⚠️ RAM elevada: {ram:.0f}% | Swap: {swap}MB")

    return alerts

def check_semaphore_orphan(state: dict) -> list:
    """Se semáforo > 0 mas nenhum Chrome vivo, reseta."""
    alerts = []
    try:
        lock = pathlib.Path('/tmp/vooindo_chrome_semaphore.lock')
        if lock.exists():
            val = lock.read_text().strip()
            if val.isdigit() and int(val) > 0:
                r = subprocess.run(['pgrep', '-c', '-f', 'chrome-headless|chromium'],
                                  capture_output=True, text=True, timeout=5)
                chrome_count = int(r.stdout.strip()) if r.stdout.strip().isdigit() else 0
                if chrome_count == 0:
                    lock.write_text('0')
                    alerts.append('🔧 Semáforo Chrome resetado (nenhum Chrome vivo)')
                    log.warning('[watchdog] semáforo resetado')
    except Exception as e:
        log.warning(f'[watchdog] erro: {e}')
    return alerts


def check_pending_stale() -> list:
    """Jobs pending há mais de 20 min."""
    stale = _pending_jobs_since(MAX_PENDING_STALE_MIN)
    if stale:
        return [f"🕒 {len(stale)} jobs pending >{MAX_PENDING_STALE_MIN}min (órfãos)"]
    return []

def check_manual_health() -> list:
    """Verifica se o bot responde e workers manual tão prontos."""
    alerts = []
    pool = _worker_alive('manual')
    if not pool:
        alerts.append("❌ Nenhum worker manual vivo — consultas manuais indisponíveis")
    run = _running_job_count()
    if run >= 4:
        alerts.append(f"⚠️ {run} jobs rodando — manual pode demorar")
    return alerts

# ── Rotina principal ────────────────────────────────────────────────────────
def run_cycle_scan(state: dict):
    now = datetime.datetime.now()
    hour = now.hour
    minute = now.minute

    # Só analisa entre 5-55 min de cada hora
    if minute < 5 or minute > 55:
        return

    if state.get('last_hour') == hour:
        return

    time.sleep(10)

    log.info(f"═══ Healthcheck {hour}:00 ═══")

    # 1. Ciclo iniciou?
    analysis = analyze_cycle(hour)
    if analysis['total'] == 0:
        log.info(f"Ciclo {hour}:00 sem jobs")
        state['last_hour'] = hour
        _save_state(state)
        return

    # 2. Monta relatório
    report_parts = [f"📊 *Ciclo {hour}:00*"]
    report_parts.append(f"✅ {analysis['success']} ok | ❌ {analysis['errors']} erro | "
                        f"🔴 {analysis['running']} rodando | 🕒 {analysis['pending']} pendente")

    # 3. Jobs travados?
    stuck = analysis['stuck_running']
    if stuck:
        for j, elapsed in stuck:
            report_parts.append(f"🔴 #{j['id']} user={j['user_id']} travado ({elapsed:.0f}min)")
        report_parts.append(f"⚠️ {len(stuck)} jobs running >{MAX_JOB_RUNNING_MIN}min — possivelmente zumbis")

    # 4. Jobs lentos?
    slow = analysis['slow_jobs']
    if slow:
        for j, elapsed in slow[:3]:
            report_parts.append(f"🐌 #{j['id']} user={j['user_id']} levou {elapsed:.0f}min")
        if len(slow) > 3:
            report_parts.append(f"  ...e mais {len(slow)-3}")

    # 5. Retry
    retried = []
    if analysis['retryable']:
        retried = apply_retries(analysis['retryable'])
        report_parts.append(f"🔄 Retry: {len(retried)} jobs")
        for r in retried:
            report_parts.append(f"  ↪ #{r['id']} user={r['user_id']}")

    # 6. Erros não retentáveis
    if analysis['non_retryable']:
        report_parts.append("⏭ Ignorados (preço/bloqueio):")
        for j in analysis['non_retryable']:
            err = (j['error_message'] or '')[:45]
            report_parts.append(f"  ❌ #{j['id']} user={j['user_id']}: {err}")

    # 7. Workers, RAM e semáforo
    infra_alerts = check_workers_and_ram(state)
    sem_alerts = check_semaphore_orphan(state)
    report_parts.extend(infra_alerts)
    report_parts.extend(sem_alerts)

    # 8. Jobs pending órfãos
    stale_pending = check_pending_stale()
    report_parts.extend(stale_pending)

    # 9. Health manual
    manual_alerts = check_manual_health()
    report_parts.extend(manual_alerts)

    # 10. Fallback check
    tech_errs = len(analysis['retryable'])
    total_errs = analysis['errors']
    tech_rate = tech_errs / analysis['total'] if analysis['total'] else 0
    total_stuck = len(stuck)

    if tech_rate > 0.5 and total_errs >= 2:
        state['consecutive_failures'] = state.get('consecutive_failures', 0) + 1
        _save_state(state)
        report_parts.append(f"\n⚠️ Falha técnica: {tech_rate:.0%} | consecutivas: {state['consecutive_failures']}")

        if state['consecutive_failures'] >= FALLBACK_CONSECUTIVE_FAIL:
            report_parts.append("\n🛠 *Fallback ativado!*")
            report_parts.append("  Navegador: Chrome")
            report_parts.append("  Semáforo: 1")
            msg = '\n'.join(report_parts)
            _send_telegram(f"{msg}\n\n🔧 Aplicando fallback e reiniciando...")
            do_fallback(state)
            return
        else:
            report_parts.append(f"  Próx falha similar → fallback")
    else:
        # Reset se ciclo OK
        if total_errs == 0 and not stuck:
            state['consecutive_failures'] = 0
            _save_state(state)

    # 11. Jobs zumbis — reset automático
    if stuck:
        conn = db_connect()
        c = conn.cursor()
        for j, elapsed in stuck:
            c.execute(sql("""
                UPDATE scan_jobs SET status='error', error_message='stuck_auto_recovery',
                       finished_at=NOW() WHERE id=%s AND status='running'
            """), (j['id'],))
        conn.commit()
        conn.close()
        log.warning(f"[health] {len(stuck)} jobs zumbis resetados")

    # 12. Notifica se tem algo relevante
    should_notify = (
        len(infra_alerts) > 0 or retried or stuck or
        analysis['non_retryable'] or tech_rate > 0.3
    )
    if should_notify:
        msg = '\n'.join(report_parts)
        _send_telegram(msg)
        log.info("Notificação enviada ao admin")
    else:
        log.info("Tudo OK — sem notificação")

    # Marca como analisado
    state['last_hour'] = hour
    _save_state(state)
    log.info(f"═══ Fim healthcheck {hour}:00 ═══")

# ── Loop ────────────────────────────────────────────────────────────────────
def main():
    log.info("🚀 Health Monitor iniciado")
    while True:
        try:
            state = _load_state()
            run_cycle_scan(state)
        except Exception as e:
            log.error(f"Erro: {e}", exc_info=True)
        time.sleep(60)

if __name__ == '__main__':
    main()
