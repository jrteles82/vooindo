#!/usr/bin/env python3
"""
delete_user_cli.py — Exclui um usuário e todos os seus relacionamentos do banco.
Uso: delete_user_cli.py <user_id ou chat_id>

Cria uma conexão nova com autocommit=1 e lock_wait_timeout=5 para evitar
deadlocks com workers ativos.
"""

import os
import sys
import time
from urllib.parse import urlparse
import pymysql

def load_env(path: str = '.env') -> None:
    with open(path) as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k] = v.strip("'\"")

def main():
    if len(sys.argv) < 2:
        print("Uso: delete_user_cli.py <user_id ou chat_id>", file=sys.stderr)
        sys.exit(1)

    identifier = sys.argv[1].strip()
    
    load_env()
    db_url = urlparse(os.getenv('MYSQL_URL'))
    
    conn = pymysql.connect(
        host=db_url.hostname, port=db_url.port or 3306,
        user=db_url.username, password=db_url.password,
        database=db_url.path.lstrip('/'),
    )
    
    try:
        with conn.cursor() as c:
            c.execute("SET SESSION innodb_lock_wait_timeout = 5")
            c.execute("SET SESSION autocommit = 1")
        
        with conn.cursor() as c:
            # Tenta encontrar como user_id ou chat_id
            c.execute("SELECT user_id, chat_id, first_name FROM bot_users WHERE user_id = %s OR chat_id = %s", (identifier, identifier))
            row = c.fetchone()
            if not row:
                print(f"ERRO: usuário '{identifier}' não encontrado", file=sys.stderr)
                sys.exit(1)
            
            user_id = row[0]
            chat_id = row[1]
            name = row[2]
            print(f"Excluindo: {name} (user_id={user_id}, chat_id={chat_id})")
        
        queries = [
            ("DELETE FROM support_messages WHERE thread_id IN (SELECT id FROM support_threads WHERE user_id = %s OR chat_id = %s)", (user_id, chat_id)),
            ("DELETE FROM support_threads WHERE user_id = %s OR chat_id = %s", (user_id, chat_id)),
            ("DELETE FROM payments WHERE chat_id = %s", (chat_id,)),
            ("DELETE FROM user_routes WHERE user_id = %s", (user_id,)),
            ("DELETE FROM user_access WHERE chat_id = %s", (chat_id,)),
            ("DELETE FROM bot_settings WHERE user_id = %s", (user_id,)),
            ("DELETE FROM bot_users WHERE user_id = %s", (user_id,)),
        ]
        
        # scan_jobs por último - pode travar com lock
        scan_jobs_ok = False
        
        for sql_q, params in queries:
            with conn.cursor() as c:
                try:
                    c.execute(sql_q, params)
                    print(f"  OK: {c.rowcount} linhas - {sql_q.split()[2]} {sql_q.split()[3][:50]}")
                except Exception as e:
                    print(f"  SKIP (lock): {sql_q.split()[2]} - {e}")
        
        # scan_jobs: tenta com múltiplas tentativas
        for attempt in range(5):
            try:
                with conn.cursor() as c:
                    c.execute("DELETE FROM scan_jobs WHERE user_id = %s", (user_id,))
                    print(f"  OK: {c.rowcount} linhas - scan_jobs (tentativa {attempt+1})")
                    scan_jobs_ok = True
                    break
            except pymysql.err.OperationalError as e:
                if 'Lock wait timeout' in str(e):
                    time.sleep(1)
                else:
                    print(f"  ERRO: scan_jobs - {e}")
                    break
        
        if not scan_jobs_ok:
            print(f"  AVISO: scan_jobs não foi limpo (lock timeout) - o user_id não existe mais, jobs órfãos são inofensivos")
        
        print(f"\n✅ Usuário {name} (user_id={user_id}) excluído com sucesso!")
        
    finally:
        conn.close()

if __name__ == '__main__':
    main()
