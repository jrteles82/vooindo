#!/usr/bin/env python3
"""Renova sessão Google com 2FA via stdin interativo."""
import sys, os, subprocess, select, time

os.chdir('/opt/vooindo')

email = 'vooindo.bot@gmail.com'
password = 'Vooindo#8212'

# Limpar sessões antigas
# for d in ['google_session', 'google_session_1', 'google_session_2', 'google_session_3', 'google_session_4']:
#     import shutil
#     path = os.path.join('/opt/vooindo', d)
#     if os.path.exists(path):
#         shutil.rmtree(path)
for l in ['google_session.lock', 'google_session_3.lock']:
    lpath = os.path.join('/opt/vooindo', l)
    if os.path.exists(lpath):
        os.remove(lpath)

print("Sessões antigas limpas. Iniciando login...")

proc = subprocess.Popen(
    [sys.executable, 'google_login_stdin.py', '--email', email, '--force'],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    bufsize=1,
)

# Manda senha
proc.stdin.write(password + '\n')
proc.stdin.flush()
print("Senha enviada, aguardando...")

# Lê output até NEED_2FA ou final
while True:
    line = proc.stdout.readline()
    if not line:
        break
    print(line, end='', flush=True)
    if 'NEED_2FA' in line:
        print('\n🔄 Detectado NEED_2FA. Aguardando código (30s)...')
        # Espera até 30s pelo código via argumento ou stdin
        code = sys.argv[1] if len(sys.argv) > 1 else input("Código 2FA: ")
        proc.stdin.write(code + '\n')
        proc.stdin.flush()
        print(f"Código '{code}' enviado.")
    if 'AUTH_SCORE' in line:
        break

proc.stdin.close()
proc.wait()

# Sincronizar workers
print("\nSincronizando workers...")
from google_session_sync import sync_base_session_to_worker_profiles
result = sync_base_session_to_worker_profiles(force=True)
print(f"Workers: {result}")

# Desativar manutenção
print("\nDesativando manutenção...")
from db import connect as db_connect
from access_policy import set_maintenance_mode
conn = db_connect()
set_maintenance_mode(conn, False)
conn.close()
print("Manutenção desativada ✅")
