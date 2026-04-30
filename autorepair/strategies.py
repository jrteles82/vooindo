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

# ─── Mapa erro → estratégia ─────────────────────────────────────────

ERROR_STRATEGIES = {
    'parsed=0': [repair_parse_zero],
    'proc_error_rc1': [repair_chrome_crash],
    'chrome_semaphore_timeout': [repair_deadlock_semaphore],
    'deadlock': [repair_deadlock_semaphore, repair_stale_workers],
    'mysql_timeout': [repair_mysql_timeout],
    'OOM': [repair_oom, repair_stale_workers],
    'job_timeout_300s': [repair_chrome_crash],  # watchdog matou, limpa Chrome e semáforo
    'stale_running_recovered': [],  # job já foi recuperado pelo recovery system, sem ação
    'cancelled_by_new_request': [],  # não é erro técnico (ignora)
    'usuario_bloqueado': [],  # não é erro técnico (ignora: bloqueado, teto, sem preço)
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
    nonttechnical = {'cancelled_by_new_request', 'usuario_bloqueado', 'stale_running_recovered', 'job_timeout_300s'}
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
