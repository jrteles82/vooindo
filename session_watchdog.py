"""Monitora expiração da sessão Google e avisa admin com antecedência."""
import sqlite3
import os
import sys
import json
import subprocess
import urllib.request

SESSION_DIR = '/opt/vooindo/google_session'
COOKIE_DB = os.path.join(SESSION_DIR, 'Default', 'Cookies')

# Telegram config via env
BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN') or os.environ.get('BOT_TOKEN')
ADMIN_CHAT_ID = os.environ.get('TELEGRAM_ADMIN_CHAT_ID') or os.environ.get('ADMIN_CHAT_ID')

# Limiares (em horas antes da expiração)
WARN_HOURS = [72, 24, 6]  # avisa com 3d, 1d, 6h
CRITICAL_HOURS = 1  # 1h = crítico

# Cookie names que indicam sessão ativa (excluir transitórios como STRP)
# STRP = Security Token for Redirect Protection, expira em horas, não indica fim de sessão
AUTH_COOKIES_PREFIX = ['SID', 'SSID', 'SAPISID', 'SIDCC', 'AEC']
IGNORE_COOKIES = ['__Secure-STRP']

STATE_FILE = '/opt/vooindo/.session_watchdog_state.json'

def get_earliest_auth_expiry():
    """Retorna o timestamp UNIX do cookie auth que expira primeiro."""
    if not os.path.exists(COOKIE_DB):
        return None
    
    try:
        conn = sqlite3.connect(f'file:{COOKIE_DB}%smode=ro', uri=True)
        cursor = conn.cursor()
        rows = cursor.execute("""
            SELECT name, (expires_utc / 1000000 - 11644473600) as expires_unix
            FROM cookies 
            WHERE host_key LIKE '%google%' 
              AND expires_utc > 0
            ORDER BY expires_utc ASC
            LIMIT 20
        """).fetchall()
        conn.close()
        
        if not rows:
            return None
        
        # Ignorar cookies transitórios como STRP
        # Filtrar apenas cookies de auth permanentes
        auth_rows = []
        for name, ts in rows:
            if ts is None or ts <= 0:
                continue
            if name in IGNORE_COOKIES:
                continue
            # Focar em cookies de sessão real
            if any(name.startswith(p) for p in AUTH_COOKIES_PREFIX):
                auth_rows.append((name, ts))
        
        # Se não achou auth cookies, usar TODOS exceto ignore
        if not auth_rows:
            for name, ts in rows:
                if ts is not None and ts > 0 and name not in IGNORE_COOKIES:
                    auth_rows.append((name, ts))
        
        if not auth_rows:
            return None
        
        earliest_name, earliest_ts = min(auth_rows, key=lambda x: x[1])
        
        return earliest_ts, earliest_name
    except Exception:
        return None


def send_alert(msg, level='⚠️'):
    if not BOT_TOKEN or not ADMIN_CHAT_ID:
        print(f"[watchdog] Sem credenciais para alerta: BOT_TOKEN={bool(BOT_TOKEN)}, ADMIN_CHAT_ID={bool(ADMIN_CHAT_ID)}")
        return
    
    full_msg = f"{level} *Watchdog Sessão Google*\n\n{msg}"
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = json.dumps({
        'chat_id': ADMIN_CHAT_ID,
        'text': full_msg,
        'parse_mode': 'Markdown',
        'reply_markup': json.dumps({
            'inline_keyboard': [[
                {'text': '🔐 Renovar Sessão', 'callback_data': 'painel:renovar_sessao'}
            ]]
        })
    }).encode()
    
    try:
        req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
        resp = urllib.request.urlopen(req, timeout=10)
        print(f"[watchdog] Alerta enviado: {resp.status}")
    except Exception as e:
        print(f"[watchdog] Erro ao enviar alerta: {e}")


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {'last_warned_hours': []}


def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)


def main():
    import time
    from datetime import datetime, timezone
    
    state = load_state()
    
    result = get_earliest_auth_expiry()
    if result is None:
        print(f"[watchdog] Não foi possível ler cookies. DB existe%s {os.path.exists(COOKIE_DB)}")
        return
    
    earliest_ts, earliest_name = result
    now_ts = time.time()
    
    if earliest_ts <= now_ts:
        print(f"[watchdog] Cookie '{earliest_name}' JÁ expirou ({datetime.fromtimestamp(earliest_ts, tz=timezone.utc).isoformat()})")
        send_alert(
            f"Cookie *{earliest_name}* expirou!\n\n"
            f"Expirou em: {datetime.fromtimestamp(earliest_ts).strftime('%d/%m %H:%M')}\n"
            f"Renove a sessão imediatamente.",
            level='🚨'
        )
        state['last_warned_hours'] = []  # Reset após expirar
        save_state(state)
        return
    
    hours_left = (earliest_ts - now_ts) / 3600
    
    print(f"[watchdog] Cookie mais crítico: {earliest_name}")
    print(f"[watchdog] Expira em: {datetime.fromtimestamp(earliest_ts).strftime('%d/%m %H:%M')} ({hours_left:.1f}h restantes)")
    
    # Verificar se algum limiar foi atingido
    warned = set(state.get('last_warned_hours', []))
    
    if hours_left <= CRITICAL_HOURS and CRITICAL_HOURS not in warned:
        send_alert(
            f"🚨 Sessão Google expira *logo*!\n\n"
            f"Cookie: {earliest_name}\n"
            f"Expira em: {datetime.fromtimestamp(earliest_ts).strftime('%d/%m %H:%M')}\n"
            f"Restam: {hours_left:.1f}h\n\n"
            f"*Melhor renovar AGORA* para evitar interrupção.",
            level='🚨'
        )
        warned.add(CRITICAL_HOURS)
    
    for h in WARN_HOURS:
        if hours_left <= h and h not in warned:
            send_alert(
                f"Sessão Google expira em ~{h}h\n\n"
                f"Cookie: {earliest_name}\n"
                f"Expira em: {datetime.fromtimestamp(earliest_ts).strftime('%d/%m %H:%M')}\n"
                f"Restam: {hours_left:.1f}h\n\n"
                f"Renove quando puder para evitar interrupção.",
                level='⚠️'
            )
            warned.add(h)
            break  # Só avisa o mais próximo
    
    # Se passou de todos os limiares, reset
    if hours_left > max(WARN_HOURS):
        warned = set()
    
    state['last_warned_hours'] = list(warned)
    save_state(state)


if __name__ == '__main__':
    main()
