"""
Force a full round for all active users with active routes.
Run this before restarting so workers pick up jobs immediately.
"""
from db import connect
from datetime import datetime
import json

conn = connect()
cur = conn.cursor()

# All active users (not test, not blocked)
cur.execute("""
    SELECT user_id, chat_id, first_name 
    FROM bot_users 
    WHERE blocked IS NULL OR blocked != 1
    ORDER BY user_id
""")
users = cur.fetchall()

now_iso = datetime.now().isoformat()
total_created = 0

for user in users:
    user_id = user['user_id']
    chat_id = user['chat_id']
    name = user['first_name']

    cur.execute(
        "SELECT id, origin, destination, outbound_date, inbound_date FROM user_routes WHERE user_id = %s AND active = 1",
        (user_id,)
    )
    routes = cur.fetchall()
    if not routes:
        continue

    num_routes = len(routes)
    group_key = f"round_manual_{now_iso}"

    for route in routes:
        payload = json.dumps({
            'round_started_at': now_iso,
            'route': {
                'id': route['id'],
                'origin': route['origin'],
                'destination': route['destination'],
                'outbound_date': route['outbound_date'],
                'inbound_date': route['inbound_date'] or '',
            },
            'group_info': {
                'total_routes': num_routes,
                'label': name,
            },
            'executor_timeout': 300,
        }, ensure_ascii=False)

        cur.execute(
            "INSERT INTO scan_jobs (user_id, chat_id, job_type, status, payload, cost_score, group_key) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (user_id, chat_id, 'scheduled', 'pending', payload, 1, group_key)
        )
        total_created += 1

conn.commit()
print(f"✅ {total_created} jobs criados para {len(users)} usuarios")
