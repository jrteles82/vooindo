from db import connect
from datetime import datetime
import json

conn = connect()
cur = conn.cursor()
now_iso = datetime.now().isoformat()
total = 0

rows = cur.execute("SELECT user_id, chat_id, first_name FROM bot_users WHERE blocked IS NULL OR blocked != 1 ORDER BY user_id").fetchall()
for user in rows:
    routes = cur.execute("SELECT id, origin, destination, outbound_date, inbound_date FROM user_routes WHERE user_id = %s AND active = 1", (user['user_id'],)).fetchall()
    if not routes: continue
    num = len(routes)
    gk = f"forced_round_{now_iso}"
    for route in routes:
        p = json.dumps({
            'round_started_at': now_iso,
            'route': {'id': route['id'], 'origin': route['origin'], 'destination': route['destination'], 'outbound_date': route['outbound_date'], 'inbound_date': route['inbound_date'] or ''},
            'group_info': {'total_routes': num, 'label': user['first_name']},
            'executor_timeout': 300,
        }, ensure_ascii=False)
        cur.execute("INSERT INTO scan_jobs (user_id, chat_id, job_type, status, payload, cost_score, group_key) VALUES (%s,%s,%s,%s,%s,%s,%s)", (user['user_id'], user['chat_id'], 'scheduled', 'pending', p, 1, gk))
        total += 1
conn.commit()
print(f"✓ {total} jobs criados")
