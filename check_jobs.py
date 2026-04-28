
import os
import pymysql
from urllib.parse import urlparse

# Carregar .env manualmente para simplificar
with open('.env') as f:
    for line in f:
        if '=' in line and not line.startswith('#'):
            k, v = line.strip().split('=', 1)
            os.environ[k] = v.strip('"\'')

url = urlparse(os.getenv('MYSQL_URL'))
conn = pymysql.connect(
    host=url.hostname,
    user=url.username,
    password=url.password,
    database=url.path.lstrip('/'),
    port=url.port or 3306,
    cursorclass=pymysql.cursors.DictCursor
)

try:
    with conn.cursor() as cursor:
        cursor.execute("SELECT status, job_type, COUNT(*) as count FROM scan_jobs GROUP BY status, job_type")
        results = cursor.fetchall()
        print("--- Relatório de Jobs ---")
        for row in results:
            print(f"Status: {row['status']} | Tipo: {row['job_type']} | Qtd: {row['count']}")
finally:
    conn.close()
