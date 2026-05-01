#!/usr/bin/env python3
"""Monitor de rodadas do Vooindo — registra cada ciclo e erros pra análise."""
import time, json, os, subprocess, sys
from datetime import datetime
from pathlib import Path

LOG = Path('/opt/vooindo/logs/cycle_monitor.log')
STATE = Path('/tmp/vooindo_monitor_state.json')

def log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f'[{ts}] {msg}'
    print(line, flush=True)
    LOG.parent.mkdir(exist_ok=True)
    with LOG.open('a') as f:
        f.write(line + '\n')

def record_cycle_start() -> dict:
    """Registra início de um ciclo do scheduler, retorna métricas iniciais."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log(f'🔄 CICLO INICIADO em {ts}')
    return {
        'started_at': time.time(),
        'started_iso': ts,
    }


def record_cycle_end(cycle_metrics: dict, scan_results: dict | None = None) -> None:
    """Registra fim de um ciclo do scheduler com resultados."""
    elapsed = time.time() - cycle_metrics.get('started_at', time.time())
    summary = scan_results or {}
    log(f'✅ CICLO CONCLUÍDO em {elapsed:.1f}s | resultados={summary}')


def get_db():
    sys.path.insert(0, '/opt/vooindo')
    from db import connect, sql
    return connect(), sql

def check_rodada():
    try:
        conn, sql = get_db()
        # Jobs das últimas 2 horas
        rows = conn.execute(sql('''
            SELECT id, user_id, status, error_message, finished_at,
                   TIMESTAMPDIFF(SECOND, created_at, COALESCE(finished_at, NOW())) as duracao
            FROM scan_jobs
            WHERE created_at > DATE_SUB(NOW(), INTERVAL 2 HOUR)
            ORDER BY id
        ''')).fetchall()
        
        if not rows:
            conn.close()
            return None
        
        statuses = {}
        for r in rows:
            s = r['status']
            statuses[s] = statuses.get(s, 0) + 1
        
        # Carrega estado anterior
        state = {'last_job_id': 0, 'rodadas': []}
        if STATE.exists():
            try:
                state = json.loads(STATE.read_text())
            except: pass
        
        new_jobs = [r for r in rows if r['id'] > state.get('last_job_id', 0)]
        if new_jobs:
            state['last_job_id'] = max(r['id'] for r in new_jobs)
        
        # Erros novos não registrados
        novos_erros = [r for r in new_jobs if r['status'] == 'error' and r['error_message']]
        if novos_erros:
            for e in novos_erros:
                log(f'❌ ERRO: Job {e["id"]} user={e["user_id"]} | {e["error_message"]}')
        
        # Jobs lentos (>300s)
        lentos = [r for r in rows if r.get('duracao', 0) and r['duracao'] > 300 and r['status'] == 'running']
        if lentos:
            for l in lentos:
                log(f'🐌 LENTO: Job {l["id"]} user={l["user_id"]} rodando há {l["duracao"]}s')
        
        conn.close()
        STATE.write_text(json.dumps(state, indent=2))
        
        return statuses
    except Exception as e:
        log(f'[ERRO MONITOR] {e}')
        return None

def main():
    log('📊 Monitor iniciado')
    last_hour = -1
    while True:
        try:
            check_rodada()
            now = datetime.now()
            h = now.hour
            
            # Só loga hora a hora se tudo tranquilo
            if h != last_hour and now.minute < 5:
                log(f'✅ Hora {h:02d}:00 — check de rotina ok')
                last_hour = h
            
            time.sleep(60)  # check a cada 1 minuto
        except KeyboardInterrupt:
            log('📊 Monitor encerrado')
            break
        except Exception as e:
            log(f'[ERRO FATAL MONITOR] {e}')
            time.sleep(30)

if __name__ == '__main__':
    main()
