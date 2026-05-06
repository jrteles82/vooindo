from db import connect as db_connect
conn = db_connect()
cur = conn.cursor()

cur.execute("""
SELECT 
    bu.first_name,
    COUNT(*) AS total_jobs,
    ROUND(AVG(TIMESTAMPDIFF(SECOND, js.started_at, js.finished_at))) AS avg_seconds,
    ROUND(AVG(CASE WHEN js.status = 'completed' THEN TIMESTAMPDIFF(SECOND, js.started_at, js.finished_at) ELSE NULL END)) AS avg_completed_seconds,
    SUM(CASE WHEN js.status = 'completed' THEN 1 ELSE 0 END) AS completed,
    SUM(CASE WHEN js.status = 'error' THEN 1 ELSE 0 END) AS errors
FROM scan_jobs js 
JOIN bot_users bu ON bu.user_id = js.user_id
WHERE js.started_at >= CURDATE() AND js.started_at < CURDATE() + INTERVAL 6 HOUR
GROUP BY js.user_id
ORDER BY avg_seconds DESC
""")

for r in cur.fetchall():
    print(f"{r['first_name']}: {r['total_jobs']} jobs | media {r['avg_seconds'] or 0}s | completed {r['avg_completed_seconds'] or 0}s | {r['completed']} ok / {r['errors']} erro")

print()

cur.execute("""
SELECT 
    COUNT(*) AS total,
    ROUND(AVG(TIMESTAMPDIFF(SECOND, started_at, finished_at))),
    ROUND(AVG(CASE WHEN status = 'completed' THEN TIMESTAMPDIFF(SECOND, started_at, finished_at) ELSE NULL END)),
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END),
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END)
FROM scan_jobs
WHERE started_at >= CURDATE() AND started_at < CURDATE() + INTERVAL 6 HOUR
""")

r = cur.fetchone()
print(f"GERAL: {r['total']} jobs | total: {r[1] or 0}s | completed: {r[2] or 0}s | {r[3]} ok / {r[4]} erro")
