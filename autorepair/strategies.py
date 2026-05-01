"""
AutoRepair — Estratégias de reparo

Cada estratégia é uma função que:
- Recebe o contexto do erro (job_id, erro, worker, etc)
- Tenta corrigir
- Retorna True se corrigiu, False se não conseguiu
"""

import logging, os, subprocess, time, json
from pathlib import Path

logger = logging.getLogger('autorepair')

REPAIR_STATE = Path('/tmp/vooindo_repair_state.json')

def _load_state():
    try:
        if REPAIR_STATE.exists():
            return json.loads(REPAIR_STATE.read_text())
    except: pass
    return {"applied": [], "attempts": {}}

def _save_state(state):
    try:
        REPAIR_STATE.write_text(json.dumps(state, indent=2))
    except: pass

# ─── Estratégias individuais ─────────────────────────────────────────

def repair_deadlock_semaphore(ctx: dict) -> bool:
    """Corrige deadlock do semáforo Chrome: reseta lockfile e mata processos órfãos."""
    lockfile = '/tmp/vooindo_chrome_semaphore.lock'
    try:
        # Mata processos Chrome órfãos
        subprocess.run(['pkill', '-9', '-f', 'chrome-headless'], capture_output=True, timeout=5)
        subprocess.run(['pkill', '-9', '-f', 'chromium'], capture_output=True, timeout=5)
        time.sleep(1)
        # Reseta semáforo
        Path(lockfile).write_text('0')
        logger.warning('[repair] semáforo resetado + Chrome kill')
        return True
    except Exception as e:
        logger.error(f'[repair] falha ao resetar semáforo: {e}')
        return False

def repair_oom(ctx: dict) -> bool:
    """OOM: aumenta swap se possível, limpa cache, mata processos pesados."""
    try:
        # Limpa cache de página
        subprocess.run(['sync'], timeout=5)
        with open('/proc/sys/vm/drop_caches', 'w') as f:
            f.write('3')
        # Mata Chrome órfão
        subprocess.run(['pkill', '-9', '-f', 'chrome-headless'], capture_output=True, timeout=5)
        time.sleep(2)
        logger.warning('[repair] OOM: cache limpo + Chrome kill')
        return True
    except Exception as e:
        logger.error(f'[repair] falha OOM: {e}')
        return False

def repair_stale_workers(ctx: dict) -> bool:
    """Workers zumbis: mata e reinicia."""
    try:
        subprocess.run(['pkill', '-9', '-f', 'job_worker.py'], capture_output=True, timeout=5)
        time.sleep(2)
        subprocess.run(['sudo', 'systemctl', 'start', 'vooindo.service'], capture_output=True, timeout=10)
        time.sleep(15)
        logger.warning('[repair] workers restartados')
        return True
    except Exception as e:
        logger.error(f'[repair] falha restart workers: {e}')
        return False

def repair_mysql_timeout(ctx: dict) -> bool:
    """Timeout MySQL: reconecta, sem restart agressivo."""
    # Só loga, o próprio worker já reconecta
    logger.warning('[repair] mysql timeout detectado — reconexão automática dos workers')
    return True

def repair_parse_zero(ctx: dict) -> bool:
    """parsed=0: força retry com semáforo resetado."""
    return repair_deadlock_semaphore(ctx)

def repair_chrome_crash(ctx: dict) -> bool:
    """Chrome crashou (rc=1 no_stderr): mata órfãos e limpa cache."""
    return repair_oom(ctx)


def repair_requeue_job(ctx: dict) -> bool:
    """Re-cria job como pending tentando novamente (até 3x)."""
    import sys
    from db import connect as _db, sql as _sql
    
    job_id = ctx.get('job_id')
    if not job_id:
        return False
    
    try:
        conn = _db()
        # Pega info do job original
        row = conn.execute(_sql('''
            SELECT user_id, chat_id, job_type, created_at
            FROM scan_jobs WHERE id = %s
        '''), (job_id,)).fetchone()
        if not row:
            conn.close()
            return False
        
        user_id = row['user_id']
        chat_id = row['chat_id']
        job_type = row['job_type']
        
        # Verifica quantas tentativas já teve
        prev_tries = conn.execute(_sql('''
            SELECT COUNT(*) as cnt FROM scan_jobs
            WHERE user_id = %s AND DATE(created_at) = DATE(%s)
              AND status = 'error'
        '''), (user_id, row['created_at'])).fetchone()
        tries = prev_tries['cnt'] if isinstance(prev_tries, dict) else prev_tries[0]
        
        if tries >= 3:
            conn.close()
            logger.warning(f'[repair] job={job_id} max retries={tries} para user={user_id}')
            return False
        
        # Cria novo job pending
        conn.execute(_sql('''
            INSERT INTO scan_jobs (user_id, chat_id, job_type, status, created_at, retry_count)
            VALUES (?, ?, ?, 'pending', NOW(), COALESCE((SELECT retry_count FROM scan_jobs WHERE id = ?), 0) + 1)
        '''), (user_id, chat_id, job_type, job_id))
        conn.commit()
        new_id = conn.execute(_sql('SELECT LAST_INSERT_ID() as id')).fetchone()
        if isinstance(new_id, dict):
            new_id = new_id['id']
        conn.close()
        logger.warning(f'[repair] job={job_id} → novo job={new_id} user={user_id} (retry #{tries+1})')
        return True
    except Exception as e:
        logger.error(f'[repair] falha requeue job={job_id}: {e}')
        return False

# ─── Mapa erro → estratégia ─────────────────────────────────────────

ERROR_STRATEGIES = {
    'parsed=0': [repair_parse_zero, repair_requeue_job],
    'proc_error_rc1': [repair_chrome_crash, repair_requeue_job],
    'chrome_semaphore_timeout': [repair_deadlock_semaphore, repair_requeue_job],
    'deadlock': [repair_deadlock_semaphore, repair_stale_workers, repair_requeue_job],
    'mysql_timeout': [repair_mysql_timeout, repair_requeue_job],
    'OOM': [repair_oom, repair_stale_workers, repair_requeue_job],
    'job_timeout_300s': [repair_chrome_crash, repair_requeue_job],  # watchdog matou, limpa Chrome + re-tenta
    'stale_running_recovered': [],  # job já foi recuperado pelo recovery system, sem ação
    'cancelled_by_new_request': [],  # não é erro técnico (ignora)
    'usuario_bloqueado': [],  # não é erro técnico (ignora: bloqueado, teto, sem preço)
    'process_killed': [repair_requeue_job],  # SIGTERM/SIGKILL, re-tenta
}

def classify_error(error_message: str) -> list:
    """Classifica um erro em categorias."""
    if not error_message:
        return []
    error_lower = error_message.lower()
    categories = []
    if 'parsed=0' in error_lower or 'sem resultados' in error_lower:
        categories.append('parsed=0')
    if 'proc_error_rc1' in error_lower or 'no_stderr' in error_lower:
        categories.append('proc_error_rc1')
    if 'semaphore' in error_lower or 'timeout' in error_lower and 'chrome' in error_lower:
        categories.append('chrome_semaphore_timeout')
    if 'mysql' in error_lower or 'lock wait' in error_lower or 'gone away' in error_lower:
        categories.append('mysql_timeout')
    if 'killed' in error_lower or 'oom' in error_lower or 'mem' in error_lower:
        categories.append('OOM')
    if 'deadlock' in error_lower:
        categories.append('deadlock')
    if 'job_timeout' in error_lower:
        categories.append('job_timeout_300s')
    if 'stale_running' in error_lower:
        categories.append('stale_running_recovered')
    if 'cancelled' in error_lower:
        categories.append('cancelled_by_new_request')
    if 'bloqueado' in error_lower:
        categories.append('usuario_bloqueado')
    if error_message.strip() in ('143',):
        categories.append('process_killed')
    if 'sem preço' in error_lower or 'sem_preco' in error_lower or 'sem preco' in error_lower or 'sem pre' in error_lower:
        categories.append('usuario_bloqueado')  # trata como não-técnico
    if 'acima do teto' in error_lower or 'acima do limite' in error_lower:
        categories.append('usuario_bloqueado')  # trata como não-técnico
    return categories

def run_repair(job_id: int, error_message: str) -> dict:
    """Tenta reparar um erro automaticamente.
    
    Returns: dict com resultado do reparo
    """
    categories = classify_error(error_message)
    if not categories:
        return {'repaired': False, 'action': 'unknown_error', 'notify': True}
    
    # Se só tem erros não-técnicos, não repara
    nonttechnical = {'cancelled_by_new_request', 'usuario_bloqueado', 'stale_running_recovered', 'job_timeout_300s', 'process_killed'}
    if all(c in nonttechnical for c in categories):
        return {'repaired': False, 'action': 'not_technical', 'notify': False}
    
    state = _load_state()
    key = f'job_{job_id}'
    state.setdefault('attempts', {})
    state['attempts'].setdefault(key, 0)
    state['attempts'][key] += 1
    attempt = state['attempts'][key]
    
    if attempt > 3:
        _save_state(state)
        return {'repaired': False, 'action': 'max_attempts', 'notify': True}
    
    # Tenta cada estratégia
    strategies_to_try = []
    for cat in categories:
        strategies_to_try.extend(ERROR_STRATEGIES.get(cat, []))
    
    # Remove duplicatas mantendo ordem
    seen = set()
    unique_strategies = []
    for s in strategies_to_try:
        name = s.__name__
        if name not in seen:
            seen.add(name)
            unique_strategies.append(s)
    
    ctx = {'job_id': job_id, 'error': error_message, 'attempt': attempt}
    
    for strategy in unique_strategies:
        logger.info(f'[repair] job={job_id} tentando {strategy.__name__} (attempt {attempt})')
        ok = strategy(ctx)
        if ok:
            state['applied'].append({
                'job_id': job_id,
                'strategy': strategy.__name__,
                'error': error_message[:100],
                'attempt': attempt,
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            })
            _save_state(state)
            return {'repaired': True, 'action': strategy.__name__, 'notify': False}
    
    _save_state(state)
    return {'repaired': False, 'action': 'all_strategies_failed', 'notify': True}
