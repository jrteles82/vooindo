#!/usr/bin/env python3
"""Relatório de fim de rodada — enviado via Telegram ao admin quando uma rodada completa."""
import json, os, sys, time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, '/opt/vooindo')
from db import connect, sql

TOKEN = os.getenv('BOT_TOKEN', '8515276403:AAGj7Xy36v1gWO0d2t43h5P7nK87JhBBc')
ADMIN_CHAT = '1748352987'
LAST_RUN = Path('/tmp/vooindo_last_rodada_report.txt')

def get_users():
    conn = connect()
    rows = conn.execute(sql('''
        SELECT t.user_id, t.first_name, t.username, t.chat_id,
               GROUP_CONCAT(DISTINCT r.origin ORDER BY r.id) as origens
        FROM users_transactions t
        LEFT JOIN user_routes r ON r.user_id = t.user_id
        WHERE t.active = 1
        GROUP BY t.user_id
        ORDER BY t.user_id
    ''')).fetchall()
    conn.close()
    return {r['user_id']: r for r in rows}

def report():
    conn, sql = get_db() if False else (connect(), __import__('db', fromlist=['sql']).sql)
    
    # Última rodada agendada
    last_round = conn.execute(sql('''
        SELECT MIN(created_at) as inicio, MAX(finished_at) as fim
        FROM scan_jobs
        WHERE job_type = 'scheduled'
          AND created_at >= DATE_SUB(NOW(), INTERVAL 2 HOUR)
    ''')).fetchone()
    
    if not last_round or not last_round['inicio']:
        conn.close()
        return None
    
    inicio = last_round['inicio']
    fim = last_round['fim'] or datetime.now()
    duracao_total = (fim - inicio).total_seconds()
    
    jobs = conn.execute(sql('''
        SELECT user_id, status, error_message,
               TIMESTAMPDIFF(SECOND, created_at, COALESCE(finished_at, NOW())) as duracao
        FROM scan_jobs
        WHERE job_type = 'scheduled'
          AND created_at >= DATE_SUB(NOW(), INTERVAL 2 HOUR)
        ORDER BY created_at
    ''')).fetchall()
    
    conn.close()
    
    total = len(jobs)
    done = sum(1 for j in jobs if j['status'] == 'done')
    errs = sum(1 for j in jobs if j['status'] == 'error')
    users_info = get_users()
    
    # Linha do tempo
    tempos = sorted(jobs, key=lambda j: j['duracao'], reverse=True)
    
    # Monta relatório
    topo = f"📊 RELATÓRIO DA RODADA — {inicio.strftime('%Y-%m-%dT%H:%M')}"
    sep = "═" * 40
    
    corpo = f"{topo}\n\n⚙️ DESEMPENHO\n"
    corpo += f" ⏱ Rodada completa: {duracao_total:.1f}s\n"
    corpo += f" ⏱ Janela: {inicio.strftime('%H:%M')} → {fim.strftime('%H:%M')}\n"
    corpo += f" 📦 Jobs: {total} | ✅ {done} | ❌ {errs}"
    if done:
        med = sum(j['duracao'] for j in jobs if j['status'] == 'done') / done
        corpo += f"\n ⏲ Média/job: {med:.1f}s"
    
    corpo += f"\n\n✅ RECEBERAM ({done})\n"
    receberam = []
    for j in sorted(jobs, key=lambda x: x['user_id']):
        if j['status'] == 'done':
            u = users_info.get(j['user_id'], {})
            nome = u.get('first_name', f'user_{j["user_id"]}')
            receberam.append(f" {nome}")
    corpo += "\n".join(receberam) if receberam else " (nenhum)"
    
    if errs:
        corpo += f"\n\n❌ NÃO RECEBERAM ({errs})\n"
        for j in jobs:
            if j['status'] == 'error':
                u = users_info.get(j['user_id'], {})
                nome = u.get('first_name', f'user_{j["user_id"]}')
                corpo += f" {nome}: {j['error_message']}\n"
    
    corpo += f"\n\n🐌 TEMPOS\n"
    for j in tempos[:5]:
        u = users_info.get(j['user_id'], {})
        nome = u.get('first_name', f'user_{j["user_id"]}')
        status = "❌" if j['status'] == 'error' else "✅"
        err = f" | {j['error_message']}" if j.get('error_message') else ""
        corpo += f" {nome}: {j['duracao']}s | {status}{err}\n"
    
    return corpo

def send_report(text):
    import requests
    url = f'https://api.telegram.org/bot{TOKEN}/sendMessage'
    try:
        r = requests.post(url, json={
            'chat_id': ADMIN_CHAT,
            'text': text,
            'parse_mode': 'HTML',
            'disable_notification': False
        }, timeout=15)
        return r.ok
    except Exception as e:
        print(f'Erro envio: {e}')
        return False

if __name__ == '__main__':
    delay = 600  # espera 10 min após rodada pra garantir que todos terminaram
    if len(sys.argv) > 1:
        delay = int(sys.argv[1])
    
    # Marca último envio
    last_id = ''
    if LAST_RUN.exists():
        last_id = LAST_RUN.read_text().strip()
    
    now = datetime.now()
    current_id = f'{now.hour:02d}:{now.minute:02d}'
    
    # Só envia se for horário de rodada (00, 01, 02, 03...) e passou delay
    if now.minute < 5:
        # Muito cedo, acabou de começar
        sys.exit(0)
    
    if current_id == last_id:
        sys.exit(0)  # já enviou pra essa rodada
    
    print(f'Gerando relatório...')
    text = report()
    if text:
        ok = send_report(text)
        if ok:
            LAST_RUN.write_text(current_id)
            print(f'Relatório enviado ✅ ({current_id})')
        else:
            print('Falha ao enviar')
    else:
        print('Nenhuma rodada encontrada')
