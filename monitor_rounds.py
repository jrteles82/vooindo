#!/usr/bin/env python3
"""Monitor das rodadas programadas até 12:00."""
import time, sys, os
from datetime import datetime
sys.path.insert(0, '/opt/vooindo')
from db import connect as db_connect, sql

LAST_ID = 2852  # último job conhecido
END_AT = datetime.now().replace(hour=12, minute=0, second=0, microsecond=0)
REPORT_EVERY = 10  # segundos entre checagens

def check():
    global LAST_ID
    conn = db_connect()
    try:
        r = conn.execute(sql('SELECT MAX(id) AS mid FROM scan_jobs WHERE job_type="scheduled" AND id > %s'), (LAST_ID,)).fetchone()
        if r and r['mid']:
            LAST_ID = r['mid']
            entries = conn.execute(sql('''SELECT bu.first_name, sj.status, sj.started_at, sj.finished_at,
                ROUND(TIME_TO_SEC(TIMEDIFF(sj.finished_at, sj.started_at))/60, 1) AS d,
                sj.error_message
            FROM scan_jobs sj JOIN bot_users bu ON bu.user_id=sj.user_id
            WHERE sj.id >= %s AND sj.job_type='scheduled'
            ORDER BY sj.started_at'''), (LAST_ID,)).fetchall()
            
            now = datetime.now().strftime('%H:%M')
            done = sum(1 for e in entries if e['status'] == 'done')
            err = sum(1 for e in entries if e['status'] == 'error')
            running = sum(1 for e in entries if e['status'] == 'running')
            pend = sum(1 for e in entries if e['status'] == 'pending')
            er143 = sum(1 for e in entries if e['error_message'] == '143')
            avg = 0
            times = [e['d'] for e in entries if e['d'] and e['status'] == 'done']
            if times:
                avg = sum(times)/len(times)
            
            print(f'[{now}] Rodada #{LAST_ID}: {done} done, {err} err, {running} run, {pend} pend | avg={avg:.1f}min | er143={er143}')
            
            # Find Samily
            sam = [e for e in entries if 'Samily' in str(e['first_name']) and e['status'] == 'done']
            if sam:
                print(f'         Samily: {sam[0]["d"]} min')
            
            # Stuck?
            if running > 0:
                old = [e for e in entries if e['status'] == 'running' and e['started_at']]
                for e in old:
                    secs = (datetime.now() - e['started_at']).total_seconds()
                    if secs > 600:  # 10 min
                        print(f'         ⚠️  {e["first_name"]} rodando ha {secs//60:.0f} min')
    except Exception as exc:
        print(f'[ERR] {exc}')
    finally:
        conn.close()

print(f'📊 Monitor iniciado às {datetime.now().strftime("%H:%M")} até 12:00')
print(f'   Acompanhando jobs a partir de #{LAST_ID}')
print()

while datetime.now() < END_AT:
    check()
    time.sleep(REPORT_EVERY)
