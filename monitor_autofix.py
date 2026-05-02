#!/usr/bin/env python3
"""
Monitor automático com auto-fix.
- Acompanha rodadas programadas até 12:00
- Detecta jobs travados (>10 min) e mata automaticamente
- Reseta semáforo Chrome se necessário
- Só reporta no log (sem notificação pro Teles)
"""
import sys, time, os, subprocess
from datetime import datetime
sys.path.insert(0, '/opt/vooindo')
from db import connect as db_connect, sql

LAST_ID = 2852
END_AT = datetime.now().replace(hour=12, minute=0, second=0, microsecond=0)
CHECK_INTERVAL = 30  # segundos

def auto_fix_stuck():
    """Mata jobs travados e reseta Chrome/semáforo."""
    conn = db_connect()
    try:
        # Jobs rodando há mais de 10 min
        stuck = conn.execute(sql("""
            SELECT sj.id, bu.first_name, TIMESTAMPDIFF(SECOND, sj.started_at, NOW()) AS seg
            FROM scan_jobs sj JOIN bot_users bu ON bu.user_id=sj.user_id
            WHERE sj.status = 'running' AND sj.started_at IS NOT NULL
            AND sj.started_at < NOW() - INTERVAL 10 MINUTE
            AND sj.job_type = 'scheduled'
        """)).fetchall()
        
        if stuck:
            for s in stuck:
                print(f'         🔧 Auto-fix: matando {s["first_name"]} (job #{s["id"]}, {s["seg"]//60}min)', flush=True)
                conn.execute(sql("UPDATE scan_jobs SET status='error', finished_at=NOW(), error_message='auto_fix_stuck' WHERE id=%s AND status='running'"), (s['id'],))
            conn.commit()
            
            # Mata todos Chromes
            subprocess.run(['pkill', '-9', '-f', 'chrome-headless-shell'], capture_output=True, timeout=5)
            
            # Reseta semáforo
            with open('/tmp/vooindo_chrome_semaphore.lock', 'w') as f:
                f.write('0')
            
            print('         🔧 Semáforo resetado + Chromes mortos', flush=True)
    except Exception as e:
        print(f'[ERR auto_fix] {e}', flush=True)
    finally:
        conn.close()

def check_round():
    global LAST_ID
    conn = db_connect()
    try:
        r = conn.execute(sql('SELECT MAX(id) AS mid FROM scan_jobs WHERE job_type="scheduled" AND id > %s'), (LAST_ID,)).fetchone()
        if r and r['mid']:
            LAST_ID = r['mid']
            entries = conn.execute(sql('''SELECT bu.first_name, sj.status, sj.started_at,
                ROUND(TIME_TO_SEC(TIMEDIFF(sj.finished_at, sj.started_at))/60, 1) AS d,
                sj.error_message
            FROM scan_jobs sj JOIN bot_users bu ON bu.user_id=sj.user_id
            WHERE sj.id >= %s AND sj.job_type="scheduled"
            ORDER BY sj.started_at'''), (LAST_ID,)).fetchall()
            
            now = datetime.now().strftime('%H:%M')
            done = sum(1 for e in entries if e['status'] == 'done')
            err = sum(1 for e in entries if e['status'] == 'error')
            running = sum(1 for e in entries if e['status'] == 'running')
            er143 = sum(1 for e in entries if e['error_message'] == '143')
            times = [e['d'] for e in entries if e['d'] and e['status'] == 'done']
            avg = sum(times)/len(times) if times else 0
            
            print(f'[{now}] #{LAST_ID}: {done} done, {err} err, {running} run | avg={avg:.1f}min | 143={er143}', flush=True)
            
            sam = [e for e in entries if 'Samily' in str(e['first_name']) and e['status'] == 'done']
            if sam:
                print(f'         Samily: {sam[0]["d"]} min', flush=True)
            
            # Auto-fix se houver travados
            if running > 0:
                auto_fix_stuck()
    except Exception as exc:
        print(f'[ERR] {exc}', flush=True)
    finally:
        conn.close()

print(f'📊 Monitor+AutoFix iniciado as {datetime.now().strftime("%H:%M")} ate 12:00', flush=True)
print(f'   Jobs a partir de #{LAST_ID}', flush=True)
print(flush=True)

while datetime.now() < END_AT:
    check_round()
    time.sleep(CHECK_INTERVAL)

print(f'✅ Monitor encerrado as {datetime.now().strftime("%H:%M")}', flush=True)
