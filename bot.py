import asyncio
import base64
import json
import os
import shlex
import subprocess
import sys
import uuid
import requests
import logging
import unicodedata
import pymysql
import pymysql.cursors
from urllib.parse import urlparse
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, ForceReply, ReplyKeyboardRemove, Bot
from telegram_bot_calendar import DetailedTelegramCalendar, LSTEP
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)
from telegram.error import Conflict
from telegram.request import HTTPXRequest
from config import (
    DEFAULT_PANEL_TITLE,
    PANEL_DIVIDER,
    TOKEN,
    MP_ACCESS_TOKEN,
    MERCADOPAGO_API_BASE_URL,
    now_local,
)

PANEL_RESTART_COMMAND = os.getenv('RESTART_COMMAND', '').strip()
from db import auto_pk_column, connect as connect_db, id_ref_column, indexed_text_column, insert_ignore_sql, is_missing_column_error, sql, text_column, upsert_payment_sql, DatabaseRateLimitError
from app_logging import setup_logging
from audit import audit
from notif import get_notif_settings, push_admin_notif, NOTIF_LABELS
from access_policy import (
    ensure_policy_schema,
    get_monetization_settings as ap_get_monetization_settings,
    ensure_user_access as ap_ensure_user_access,
    is_active_access as ap_is_active_access,
    should_charge_user as ap_should_charge_user,
    is_admin_chat,
    list_active_admin_chat_ids,
    get_free_uses_limit,
    get_max_routes_default,
    get_pix_pending_expiration_hours,
    list_airports,
    search_airports,
    get_airport_labels,
    is_maintenance_mode,
    set_maintenance_mode,
    is_exempt_from_maintenance,
)
from cmd_status import cmd_status


ASK_ORIGIN, ASK_DESTINATION, ASK_OUTBOUND, ASK_LIMIT, ASK_SUPPORT_MESSAGE, ASK_ADMIN_SUPPORT_MESSAGE = range(6)
ASK_GOOGLE_PASSWORD, ASK_GOOGLE_2FA = range(6, 8)
setup_logging()
logger = logging.getLogger(__name__)
LEGACY_BROADCAST_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN_LEGACY', '').strip()

# Active Google login sessions: chat_id -> {'proc': Process, '2fa_queue': Queue, 'done': bool}
_login_sessions: dict[str, dict] = {}

def trigger_service_restart() -> tuple[bool, str, bool]:
    command = PANEL_RESTART_COMMAND or 'systemctl restart vooindo.service'
    try:
        completed = subprocess.run(
            shlex.split(command),
            capture_output=True,
            text=True,
            timeout=30,
        )
        if completed.returncode != 0:
            error_details = (completed.stderr or completed.stdout or '').strip()
            suffix = f' Detalhes: {error_details}' if error_details else ''
            return False, f'Falha ao executar reinício.{suffix}', False
        return True, '✅ Serviço reiniciado com sucesso. O bot já está de volta e pronto para uso.', False
    except Exception as exc:
        return False, f'Falha ao executar reinício: {exc}', False


def format_date_display(raw: str | None) -> str:
    txt = (raw or '').strip()
    if not txt:
        return txt
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d'):
        try:
            return datetime.strptime(txt, fmt).strftime('%d-%m-%Y')
        except ValueError:
            continue
    return txt


def get_panel_text(chat_id: str) -> str:
    conn = get_db()
    row = get_bot_user_by_chat(conn, chat_id)
    interval_row = conn.execute(sql('SELECT scan_interval_minutes FROM app_settings WHERE id = 1')).fetchone()
    interval_minutes = 60
    if interval_row and interval_row['scan_interval_minutes'] is not None:
        try:
            interval_minutes = max(1, int(interval_row['scan_interval_minutes']))
        except (TypeError, ValueError):
            interval_minutes = 60

    panel_text = (
        f"{DEFAULT_PANEL_TITLE}\n"
        f"{PANEL_DIVIDER}\n"
        f"🤖 <b>Automático:</b> buscas a cada {interval_minutes} min\n"
        "🖼️ <b>Manual:</b> print imediato\n\n"
        "<i>SELECIONE UMA DAS OPÇÕES ABAIXO:</i>"
    )

    if not row:
        conn.close()
        return panel_text

    cur = conn.execute(sql('SELECT COUNT(*) FROM user_routes WHERE user_id = %s AND active = 1'), (row['user_id'],))
    count_row = cur.fetchone()
    routes_count = count_row[0] if not isinstance(count_row, dict) else next(iter(count_row.values()))
    conn.close()

    msg_text = panel_text
    if routes_count == 0:
        msg_text += "\n\n⚠️ <b>Atenção:</b> Você ainda não tem nenhuma rota cadastrada.\nClique em <b>➕ Adicionar nova rota</b> abaixo para começar."
    return msg_text


def get_db():
    return connect_db()


def _new_lockfree_conn():
    """Abre conexão MariaDB curta com autocommit=1 e lock_wait_timeout baixo,
    ideal para UPDATEs que podem travar com workers."""
    import pymysql
    from urllib.parse import urlparse
    url = urlparse(os.environ.get('MYSQL_URL', ''))
    if not url.hostname:
        with open('/opt/vooindo/.env') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    k, v = line.strip().split('=', 1)
                    os.environ[k] = v.strip("'\"")
        url = urlparse(os.environ.get('MYSQL_URL', ''))
    conn = pymysql.connect(
        host=url.hostname or 'localhost',
        port=url.port or 3306,
        user=url.username or 'vooindobot',
        password=url.password or '',
        database=url.path.lstrip('/') or 'vooindo',
        autocommit=True,
        connect_timeout=5,
        read_timeout=10,
    )
    with conn.cursor() as cur:
        cur.execute("SET SESSION lock_wait_timeout = 3")
    return conn


def db_overload_message() -> str:
    return '⚠️ O banco atingiu o limite de conexões por hora no host MySQL. Tente novamente em alguns minutos.'


def strip_accents(value: str) -> str:
    return ''.join(ch for ch in unicodedata.normalize('NFD', value or '') if unicodedata.category(ch) != 'Mn')


def normalize_date(raw: str) -> str:
    raw = (raw or '').strip()
    compact = ''.join(ch for ch in raw if ch.isdigit())

    formats = [
        '%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y', '%d %m %Y',
        '%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d', '%Y %m %d',
        '%d/%m/%y', '%d-%m-%y', '%d.%m.%y',
    ]

    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue

    if len(compact) == 8:
        for fmt in ('%d%m%Y', '%Y%m%d'):
            try:
                return datetime.strptime(compact, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue

    normalized_text = strip_accents(raw).lower()
    for token in (',', '-', '.', '/', '_'):
        normalized_text = normalized_text.replace(token, ' ')
    normalized_text = ' '.join(normalized_text.split())

    month_map = {
        'jan': '01', 'janeiro': '01',
        'fev': '02', 'fevereiro': '02',
        'mar': '03', 'marco': '03', 'março': '03',
        'abr': '04', 'abril': '04',
        'mai': '05', 'maio': '05',
        'jun': '06', 'junho': '06',
        'jul': '07', 'julho': '07',
        'ago': '08', 'agosto': '08',
        'set': '09', 'setembro': '09',
        'out': '10', 'outubro': '10',
        'nov': '11', 'novembro': '11',
        'dez': '12', 'dezembro': '12',
    }

    parts = normalized_text.split()
    if len(parts) == 3 and parts[1] in month_map and parts[0].isdigit() and parts[2].isdigit():
        day = int(parts[0])
        year = int(parts[2])
        if year < 100:
            year += 2000
        try:
            return datetime(year, int(month_map[parts[1]]), day).strftime('%Y-%m-%d')
        except ValueError:
            pass

    raise ValueError(
        'Data inválida. Tente 25/12/2026, 25-12-2026, 2026-12-25, 25122026, 25 dez 2026 ou 25 dezembro 2026.'
    )


def format_date_br(raw: str) -> str:
    raw = (raw or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%d/%m/%Y")
        except ValueError:
            continue
    return raw


def format_money_br(value: float) -> str:
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _load_airport_labels() -> dict[str, str]:
    conn = get_db()
    try:
        return get_airport_labels(conn)
    finally:
        conn.close()


def airport_label(code: str) -> str:
    labels = _load_airport_labels()
    return labels.get((code or "").upper(), code)


DEFAULT_AIRLINES = [
    ('LA', 'LATAM Airlines', 1),
    ('AR', 'Aerolineas Argentinas', 1),
    ('G3', 'Gol', 1),
    ('AD', 'Azul', 1),
    ('FO', 'Flybondi', 1),
    ('WJ', 'JetSmart', 1),
    ('AV', 'Avianca', 1),
    ('DM', 'Arajet', 1),
]


def seed_airlines(conn) -> None:
    for iata_code, name, is_active in DEFAULT_AIRLINES:
        conn.execute(
            sql(insert_ignore_sql('airlines', ['iata_code', 'name', 'is_active'], '%s, %s, %s')),
            (iata_code, name, is_active),
        )


def ensure_bot_tables() -> bool:
    try:
        conn = get_db()
    except DatabaseRateLimitError:
        logger.warning('ensure_bot_tables ignorado temporariamente por limite de conexões MySQL por hora')
        return False
    cur = conn.cursor()
    cur.execute(
        f'''
        CREATE TABLE IF NOT EXISTS bot_users (
            id {auto_pk_column()},
            user_id INTEGER UNIQUE NOT NULL,
            chat_id {indexed_text_column()} UNIQUE NOT NULL,
            username {indexed_text_column()} NULL,
            first_name {indexed_text_column()} NULL,
            confirmed INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )
    try:
        cur.execute("ALTER TABLE bot_users ADD COLUMN confirmed INTEGER DEFAULT 0")
    except Exception as exc:
        if is_missing_column_error(exc):
            pass
        else:
            raise
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS bot_settings (
            user_id INTEGER PRIMARY KEY,
            max_price REAL,
            enable_google_flights INTEGER DEFAULT 1,
            alerts_enabled INTEGER DEFAULT 1,
            last_sent_at TEXT,
            last_manual_sent_at TEXT,
            last_scheduled_sent_at TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )
    cur.execute(
        f'''
        CREATE TABLE IF NOT EXISTS scan_jobs (
            id {auto_pk_column()},
            user_id INTEGER NOT NULL,
            chat_id TEXT NOT NULL,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            payload {text_column()},
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            started_at TEXT,
            finished_at TEXT,
            error_message TEXT
        )
        '''
    )
    cur.execute(
        f'''
        CREATE TABLE IF NOT EXISTS payments (
            id {auto_pk_column()},
            mp_payment_id {indexed_text_column()} UNIQUE,
            chat_id {indexed_text_column()} NOT NULL,
            plan_name {indexed_text_column()} NULL,
            amount REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            qr_code {text_column()},
            ticket_url {text_column()},
            created_at {indexed_text_column()} DEFAULT CURRENT_TIMESTAMP,
            approved_at {indexed_text_column()} NULL
        )
        '''
    )
    cur.execute(
        f'''
        CREATE TABLE IF NOT EXISTS support_threads (
            id {auto_pk_column()},
            user_id INTEGER NOT NULL,
            chat_id {indexed_text_column()} NOT NULL,
            subject {indexed_text_column()} NOT NULL,
            status {indexed_text_column()} NOT NULL DEFAULT 'open',
            blocked INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )
    cur.execute(
        f'''
        CREATE TABLE IF NOT EXISTS support_messages (
            id {auto_pk_column()},
            thread_id {id_ref_column()} NOT NULL,
            sender_role {indexed_text_column()} NOT NULL,
            sender_chat_id {indexed_text_column()} NULL,
            body {text_column()} NOT NULL,
            is_read INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(thread_id) REFERENCES support_threads(id)
        )
        '''
    )
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS app_settings (
            id INTEGER PRIMARY KEY,
            cron_enabled INTEGER DEFAULT 1,
            scan_interval_minutes INTEGER DEFAULT 60,
            max_price_display REAL,
            show_result_type_filters INTEGER DEFAULT 1,
            updated_at TEXT
        )
        '''
    )
    cur.execute(
        f'''
        CREATE TABLE IF NOT EXISTS airlines (
            iata_code {indexed_text_column(8)} PRIMARY KEY,
            name {indexed_text_column()} NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )
    ensure_policy_schema(conn)
    for ddl in [
        "ALTER TABLE bot_settings ADD COLUMN enable_google_flights INTEGER DEFAULT 1",
        "ALTER TABLE bot_settings ADD COLUMN alerts_enabled INTEGER DEFAULT 1",
        "ALTER TABLE bot_settings ADD COLUMN last_sent_at TEXT",
        "ALTER TABLE bot_settings ADD COLUMN last_manual_sent_at TEXT",
        "ALTER TABLE bot_settings ADD COLUMN last_scheduled_sent_at TEXT",
        "ALTER TABLE bot_settings ADD COLUMN airline_filters_json TEXT DEFAULT ''",
        "ALTER TABLE app_settings ADD COLUMN show_result_type_filters INTEGER DEFAULT 1",
        "ALTER TABLE bot_users ADD COLUMN blocked INTEGER DEFAULT 0",
        "ALTER TABLE user_access ADD COLUMN skip_charge INTEGER DEFAULT 0",
        "ALTER TABLE app_settings ADD COLUMN notif_novo_usuario INTEGER DEFAULT 1",
        "ALTER TABLE app_settings ADD COLUMN notif_acesso_expirado INTEGER DEFAULT 1",
        "ALTER TABLE app_settings ADD COLUMN notif_pix_gerado INTEGER DEFAULT 1",
        "ALTER TABLE app_settings ADD COLUMN notif_pagamento_confirmado INTEGER DEFAULT 1",
        "ALTER TABLE bot_users ADD COLUMN is_test_user INTEGER DEFAULT 0",
    ]:
        try:
            cur.execute(ddl)
        except Exception as exc:
            if is_missing_column_error(exc):
                pass
            else:
                raise
    conn.execute(sql(insert_ignore_sql('app_settings', ['id', 'cron_enabled', 'scan_interval_minutes'], '1, 1, 60')))
    seed_airlines(conn)
    conn.commit()
    conn.close()
    return True


def get_monetization_settings(conn):
    return ap_get_monetization_settings(conn)


def ensure_user_access(conn, chat_id: str):
    return ap_ensure_user_access(conn, chat_id)


def ensure_owner_test_access(conn):
    settings = get_monetization_settings(conn)
    desired_test = 1 if int(settings['test_mode']) == 1 else 0
    admin_chat_ids = list_active_admin_chat_ids(conn)
    for admin_chat_id in admin_chat_ids:
        access = ensure_user_access(conn, admin_chat_id)
        if int(access['test_charge'] or 0) == desired_test:
            continue
        conn.execute(
            sql("UPDATE user_access SET test_charge = %s, updated_at = NOW() WHERE chat_id = %s"),
            (desired_test, admin_chat_id),
        )
        conn.commit()


def should_charge_user(conn, chat_id: str, access_row) -> bool:
    return ap_should_charge_user(conn, chat_id, access_row)


def plan_catalog(settings_row):
    plans = []
    if float(settings_row.get('weekly_price', 0) or 0) > 0:
        plans.append(('Semanal', float(settings_row['weekly_price']), 7))
    if float(settings_row.get('biweekly_price', 0) or 0) > 0:
        plans.append(('Quinzenal', float(settings_row['biweekly_price']), 15))
    if float(settings_row.get('monthly_price', 0) or 0) > 0:
        plans.append(('Mensal', float(settings_row['monthly_price']), 30))
    return plans


def plan_amount_by_name(settings_row, plan_name: str) -> float:
    mapping = {
        'Semanal': float(settings_row.get('weekly_price', 0) or 0),
        'Quinzenal': float(settings_row.get('biweekly_price', 0) or 0),
        'Mensal': float(settings_row.get('monthly_price', 0) or 0),
        'Teste Admin': 1.0,
    }
    fallback = next((amount for _name, amount, _days in plan_catalog(settings_row)), 0.0)
    return float(mapping.get(plan_name, fallback))


def plan_days(plan_name: str) -> int:
    mapping = {
        'Semanal': 7,
        'Quinzenal': 15,
        'Mensal': 30,
        'Teste Admin': 7,
    }
    return int(mapping.get(plan_name, 30))


def offer_paid_plans_text(conn, chat_id: str) -> str:
    access = ensure_user_access(conn, chat_id)
    free_uses_limit = get_free_uses_limit(conn)
    return '⏰ Seu acesso venceu.' if (access['status'] or '') == 'expired' else f'🚫 Seus {free_uses_limit} usos grátis acabaram.'


def choose_plan_text(conn, chat_id: str) -> str:
    settings = get_monetization_settings(conn)
    plans = plan_catalog(settings)
    if not plans:
        return '⚠️ Nenhum plano está configurado no momento.'
    lines = ['💰 *Escolha um plano para continuar*', '']
    medals = ['🥉', '🥈', '🥇']
    for idx, (name, amount, days) in enumerate(plans):
        medal = medals[idx] if idx < len(medals) else '💠'
        lines.append(f"{medal} {name}: R$ {format_money_br(amount)} ({days} dias)")
    return '\n'.join(lines)


def user_payments_markup(rows) -> InlineKeyboardMarkup:
    keyboard = []
    for row in rows:
        label = f"{row['plan_name'] or '-'} | R$ {format_money_br(row['amount'])} | {row['status']}"
        keyboard.append([InlineKeyboardButton(label[:60], callback_data=f"payment:view:{row['mp_payment_id']}")])
        if row['status'] == 'pending':
            keyboard.append([InlineKeyboardButton('✅ Atualizar este pagamento', callback_data=f"payment:check:{row['mp_payment_id']}")])
    keyboard.append([InlineKeyboardButton('⬅️ Voltar ao menu', callback_data='menu:back')])
    return InlineKeyboardMarkup(keyboard)


def is_active_access(access_row) -> bool:
    return ap_is_active_access(access_row)


def get_valid_pending_payment(conn, chat_id: str):
    row = conn.execute(
        sql('''
        SELECT mp_payment_id, plan_name, amount, status, qr_code, ticket_url, created_at
        FROM payments
        WHERE chat_id = %s AND status = 'pending'
        ORDER BY created_at DESC
        LIMIT 1
        '''),
        (chat_id,)
    ).fetchone()
    if not row:
        return None
    created_at = (row['created_at'] or '').strip()
    if not created_at:
        return None
    try:
        created_dt = datetime.fromisoformat(created_at.replace(' ', 'T'))
    except ValueError:
        return None
    pix_pending_expiration_hours = get_pix_pending_expiration_hours(conn)
    if now_local() - created_dt > __import__('datetime').timedelta(hours=pix_pending_expiration_hours):
        return None
    return row


def ensure_app_user(conn, first_name: str) -> int:
    row = conn.execute(
        sql("SELECT id FROM users WHERE email = %s"),
        (f"telegram:{first_name.lower()}@local",),
    ).fetchone()
    if row:
        return int(row['id'])

    cur = conn.execute(
        sql("INSERT INTO users (email, password_hash, created_at) VALUES (%s, %s, NOW())"),
        (f"telegram:{first_name.lower()}@local", 'telegram-bot'),
    )
    conn.commit()
    return int(cur.lastrowid)


def get_bot_user_by_chat(conn, chat_id: str):
    return conn.execute(
        sql("SELECT user_id, confirmed, first_name, username, COALESCE(blocked, 0) AS blocked, COALESCE(is_test_user, 0) AS is_test_user FROM bot_users WHERE chat_id = %s"),
        (chat_id,),
    ).fetchone()


def create_mp_pix_payment(chat_id: str, plan_name: str, amount: float) -> dict:
    if not MP_ACCESS_TOKEN:
        raise RuntimeError('MP_ACCESS_TOKEN não configurado no .env')

    headers = {
        'Authorization': f'Bearer {MP_ACCESS_TOKEN}',
        'Content-Type': 'application/json',
        'X-Idempotency-Key': str(uuid.uuid4()),
    }
    payload = {
        'transaction_amount': float(amount),
        'description': f'Plano {plan_name}',
        'payment_method_id': 'pix',
        'external_reference': f'{chat_id}:{plan_name}:{int(now_local().timestamp())}',
        'payer': {
            'email': f'admin{chat_id}@gmail.com'
        }
    }
    response = requests.post(f'{MERCADOPAGO_API_BASE_URL}/v1/payments', headers=headers, json=payload, timeout=30)
    data = response.json()
    if response.status_code >= 400:
        raise RuntimeError(data.get('message') or 'Erro ao gerar pagamento Pix')
    return data


def save_payment(conn, mp_payment_id: str, chat_id: str, plan_name: str, amount: float, status: str, qr_code: str, ticket_url: str):
    conn.execute(
        sql("""
        INSERT INTO payments (mp_payment_id, chat_id, plan_name, amount, status, qr_code, ticket_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE status=VALUES(status), qr_code=VALUES(qr_code), ticket_url=VALUES(ticket_url)
        """),
        (mp_payment_id, chat_id, plan_name, amount, status, qr_code, ticket_url),
    )
    conn.commit()


def get_mp_payment(payment_id: str) -> dict:
    if not MP_ACCESS_TOKEN:
        raise RuntimeError('MP_ACCESS_TOKEN não configurado no .env')
    headers = {
        'Authorization': f'Bearer {MP_ACCESS_TOKEN}',
        'Content-Type': 'application/json',
    }
    response = requests.get(f'{MERCADOPAGO_API_BASE_URL}/v1/payments/{payment_id}', headers=headers, timeout=30)
    data = response.json()
    if response.status_code >= 400:
        raise RuntimeError(data.get('message') or 'Erro ao consultar pagamento Pix')
    return data


def add_days_to_expiration(current_expiration: str | None, days: int) -> str:
    base = now_local()
    if current_expiration:
        try:
            parsed = datetime.fromisoformat(current_expiration)
            if parsed > base:
                base = parsed
        except ValueError:
            pass
    return (base + __import__('datetime').timedelta(days=days)).replace(microsecond=0).isoformat(sep=' ')


def apply_approved_payment(conn, payment_id: str) -> tuple[bool, str]:
    row = conn.execute(
        sql('SELECT mp_payment_id, chat_id, plan_name, amount, status FROM payments WHERE mp_payment_id = %s'),
        (payment_id,)
    ).fetchone()
    if not row:
        return False, 'pagamento_nao_encontrado'

    payment = get_mp_payment(payment_id)
    status = payment.get('status', row['status'])
    approved_at = payment.get('date_approved')
    conn.execute(
        sql('UPDATE payments SET status = %s, approved_at = COALESCE(%s, approved_at) WHERE mp_payment_id = %s'),
        (status, approved_at, payment_id)
    )

    if status != 'approved':
        conn.commit()
        return False, status

    chat_id = str(row['chat_id'])
    plan_name = row['plan_name'] or 'Teste Admin'
    access = ensure_user_access(conn, chat_id)
    expires_at = add_days_to_expiration(access['expires_at'], plan_days(plan_name))
    conn.execute(
        sql('''
        UPDATE user_access
        SET status = %s, expires_at = %s, free_uses = 0, total_paid = COALESCE(total_paid, 0) + %s, updated_at = NOW()
        WHERE chat_id = %s
        '''),
        ('active', expires_at, float(row['amount'] or 0), chat_id)
    )
    user_id = get_user_id_by_chat(conn, chat_id)
    if user_id:
        conn.execute(
            sql("UPDATE bot_settings SET alerts_enabled = 1, updated_at = NOW() WHERE user_id = %s"),
            (user_id,),
        )
    push_admin_notif(
        conn,
        "notif_pagamento_confirmado",
        f"✅ *Pagamento confirmado*\n\n"
        f"*Chat ID:* `{chat_id}`\n"
        f"*Plano:* {plan_name}\n"
        f"*Valor:* R$ {float(row['amount'] or 0):.2f}\n"
        f"*Válido até:* {expires_at}",
    )
    conn.commit()
    return True, expires_at


def get_user_id_by_chat(conn, chat_id: str):
    row = get_bot_user_by_chat(conn, chat_id)
    if not row:
        return None
    return int(row['user_id'])


def is_confirmed(conn, chat_id: str) -> bool:
    row = get_bot_user_by_chat(conn, chat_id)
    return bool(row and int(row['confirmed']) == 1)


def ensure_user_settings(conn, user_id: int) -> None:
    conn.execute(sql(
        'INSERT INTO bot_settings (user_id, max_price, enable_google_flights, alerts_enabled) '
        'VALUES (%s, NULL, 1, 1) ON DUPLICATE KEY UPDATE user_id = user_id'
    ), (user_id,))
    conn.commit()


def get_user_settings(conn, user_id: int):
    ensure_user_settings(conn, user_id)
    return conn.execute(
        sql(
            """
            SELECT max_price,
                   enable_google_flights,
                   COALESCE(alerts_enabled, 1) AS alerts_enabled,
                   COALESCE(airline_filters_json, '') AS airline_filters_json
            FROM bot_settings
            WHERE user_id = %s
            """
        ),
        (user_id,),
    ).fetchone()


def is_user_blocked(conn, chat_id: str) -> bool:
    row = get_bot_user_by_chat(conn, chat_id)
    return bool(row and int(row['blocked'] if isinstance(row, dict) else row[row.keys().index('blocked')] if hasattr(row, 'keys') else 0 or 0))


def is_test_user(conn, chat_id: str) -> bool:
    row = get_bot_user_by_chat(conn, chat_id)
    if not row:
        return False
    try:
        return bool(int(row.get('is_test_user', 0) or 0))
    except Exception:
        return False


def require_confirmation(conn, chat_id: str):
    row = get_bot_user_by_chat(conn, chat_id)
    if not row:
        return '⚠️ Use /start para iniciar seu cadastro.'
    try:
        blocked = int(row['blocked'] or 0)
    except Exception:
        blocked = 0
    if blocked and not is_test_user(conn, chat_id):
        return '🚫 Sua conta foi suspensa. Entre em contato com o suporte.'
    if int(row['confirmed']) != 1:
        return '⚠️ Confirme seu cadastro primeiro para liberar as funções. Use o botão em /start.'
    return None


def should_block_paid_action(conn, chat_id: str) -> bool:
    # Usuário teste nunca é bloqueado
    if is_test_user(conn, chat_id):
        return False
    ensure_user_access(conn, chat_id)
    ensure_owner_test_access(conn)
    access = ensure_user_access(conn, chat_id)
    if int(access.get('skip_charge', 0) or 0):
        return False
    if not should_charge_user(conn, chat_id, access):
        return False
    if is_active_access(access):
        return False
    # Se tem acessos grátis restantes, libera
    free_uses = int(access.get('free_uses', 0) or 0)
    free_uses_limit = get_free_uses_limit(conn)
    if free_uses < free_uses_limit:
        return False
    return True


def start_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ Confirmar cadastro', callback_data='confirm:cadastro')],
    ])


def blocked_support_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💬 Fale conosco', callback_data='menu:support')],
    ])


def confirmation_markup_for_message(msg: str | None) -> InlineKeyboardMarkup:
    text = str(msg or '')
    if 'suspensa' in text.lower():
        return blocked_support_markup()
    return start_markup()


def main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🏠 Abrir menu principal', callback_data='menu:back')],
    ])


def cancel_markup(callback_data: str, label: str = '❌ Cancelar') -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=callback_data)]])


def force_reply_markup(placeholder: str) -> ForceReply:
    return ForceReply(selective=False, input_field_placeholder=placeholder)


def clear_pending_input_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    keys = ['airport_stage', 'origin', 'destination', 'outbound_date', 'inbound_date']
    for k in keys:
        context.user_data.pop(k, None)



def _render_user_list(conn) -> tuple[str, InlineKeyboardMarkup]:
    """Retorna (texto, markup) com a lista de usuários para o painel."""
    users = conn.execute(
        sql("""
        SELECT b.user_id, b.chat_id, b.first_name, b.username, b.confirmed,
               COALESCE(b.blocked, 0) AS blocked,
               COALESCE(b.is_test_user, 0) AS is_test_user,
               ua.status, ua.expires_at, ua.free_uses, ua.total_paid,
               bs.max_price
        FROM bot_users b
        LEFT JOIN user_access ua ON ua.chat_id = b.chat_id
        LEFT JOIN bot_settings bs ON bs.user_id = b.user_id
        ORDER BY COALESCE(NULLIF(TRIM(b.first_name), ''), NULLIF(TRIM(b.username), ''), b.chat_id) ASC
        LIMIT 20
        """)
    ).fetchall()
    _count_row = conn.execute(sql('SELECT COUNT(*) AS cnt FROM bot_users')).fetchone()
    total = _count_row['cnt'] if isinstance(_count_row, dict) else _count_row[0]
    linhas_info = []
    text = f"👤 *Usuários Registrados* ({total} total)\n\nSelecione para gerenciar:\n"
    keyboard = []
    for idx, u in enumerate(users, start=1):
        nome = (u['first_name'] or 'Sem nome')[:18]
        status = u['status'] or 'free'
        bloq = ' 🚫' if int(u['blocked'] or 0) else ''
        test_badge = ' 🧪' if int(u.get('is_test_user', 0) or 0) else ''
        filtro_valor = normalize_max_price(u['max_price'])
        filtro_txt = 'Sem limite' if filtro_valor is None else f"R$ {int(float(filtro_valor)) if float(filtro_valor).is_integer() else format_money_br(float(filtro_valor))}"
        linhas_info.append(f"{idx}. {nome}{test_badge}{bloq} | {status} | {filtro_txt}")
        keyboard.append([InlineKeyboardButton(
            f"{idx}. {nome}{test_badge}{bloq}",
            callback_data=f"painel:usr:{u['chat_id']}"
        )])
    if linhas_info:
        text += '\n'.join(linhas_info[:20])
    keyboard.append([InlineKeyboardButton('🔙 Voltar ao Painel', callback_data='painel:back')])
    return text, InlineKeyboardMarkup(keyboard)


def _fetchcount(row) -> int:
    if row is None:
        return 0
    if isinstance(row, dict):
        return int(next(iter(row.values())) or 0)
    return int(row[0] or 0)


def get_support_badges(conn, chat_id: str, admin: bool = False) -> tuple[int, int]:
    if admin:
        unread = _fetchcount(conn.execute(sql("SELECT COUNT(*) FROM support_messages sm JOIN support_threads st ON st.id = sm.thread_id WHERE sm.sender_role = 'user' AND sm.is_read = 0 AND st.status = 'open'")).fetchone())
        threads = _fetchcount(conn.execute(sql("SELECT COUNT(*) FROM support_threads WHERE status = 'open' AND blocked = 0")).fetchone())
        return int(unread or 0), int(threads or 0)
    row = get_bot_user_by_chat(conn, chat_id)
    if not row:
        return 0, 0
    unread = _fetchcount(conn.execute(sql("SELECT COUNT(*) FROM support_messages sm JOIN support_threads st ON st.id = sm.thread_id WHERE st.user_id = %s AND sm.sender_role = 'admin' AND sm.is_read = 0 AND st.status = 'open'"), (row['user_id'],)).fetchone())
    threads = _fetchcount(conn.execute(sql("SELECT COUNT(*) FROM support_threads WHERE user_id = %s AND status = 'open'"), (row['user_id'],)).fetchone())
    return int(unread or 0), int(threads or 0)


def support_subject_label(subject: str) -> str:
    mapping = {
        'duvidas': 'Dúvidas',
        'sugestoes': 'Sugestões',
        'reclamacoes': 'Reclamações',
    }
    return mapping.get((subject or '').strip().lower(), (subject or 'Atendimento').strip().title())


def list_support_conversations_markup(rows, admin: bool = False) -> InlineKeyboardMarkup:
    keyboard = []
    for row in rows:
        label = support_subject_label(row['subject'])
        unread = int(row['unread'] or 0)
        if admin:
            user_name = row['first_name'] or 'Usuário'
            status_badge = ' 🚫' if int(row['blocked'] or 0) == 1 else ''
            text = f"{user_name} | {label}{status_badge}"
            callback_data = f"support:admin:open:{row['id']}"
        else:
            text = label
            callback_data = f"support:open:{row['id']}"
        if unread:
            text += f" ({unread})"
        keyboard.append([InlineKeyboardButton(text[:64], callback_data=callback_data)])
    keyboard.append([InlineKeyboardButton('🧹 Limpar todas', callback_data='support:admin:clearall' if admin else 'support:clear_all')])
    keyboard.append([InlineKeyboardButton('⬅️ Voltar ao menu', callback_data='menu:back')])
    return InlineKeyboardMarkup(keyboard)


def create_support_conversation(conn, chat_id: str, subject: str) -> int:
    row = get_bot_user_by_chat(conn, chat_id)
    cur = conn.execute(
        sql("INSERT INTO support_threads (user_id, chat_id, subject, status, blocked, created_at, updated_at) VALUES (%s, %s, %s, 'open', 0, NOW(), NOW())"),
        (row['user_id'], chat_id, subject),
    )
    conn.commit()
    return int(cur.lastrowid)


def get_support_conversation(conn, thread_id: int):
    return conn.execute(
        sql("SELECT st.*, bu.first_name FROM support_threads st LEFT JOIN bot_users bu ON bu.user_id = st.user_id WHERE st.id = %s"),
        (thread_id,),
    ).fetchone()


def append_support_message(conn, thread_id: int, sender_role: str, sender_chat_id: str, body: str) -> int:
    cur = conn.execute(
        sql("INSERT INTO support_messages (thread_id, sender_role, sender_chat_id, body, is_read, created_at) VALUES (%s, %s, %s, %s, 0, NOW())"),
        (thread_id, sender_role, sender_chat_id, body),
    )
    conn.execute(sql("UPDATE support_threads SET updated_at = NOW() WHERE id = %s"), (thread_id,))
    conn.commit()
    return int(cur.lastrowid)


def mark_support_conversation_as_read(conn, thread_id: int, admin: bool = False) -> None:
    sender_role = 'user' if admin else 'admin'
    conn.execute(
        sql("UPDATE support_messages SET is_read = 1 WHERE thread_id = %s AND sender_role = %s AND is_read = 0"),
        (thread_id, sender_role),
    )
    conn.commit()


def clear_support_conversation(conn, thread_id: int) -> None:
    conn.execute(sql("DELETE FROM support_messages WHERE thread_id = %s"), (thread_id,))
    conn.execute(sql("DELETE FROM support_threads WHERE id = %s"), (thread_id,))
    conn.commit()


async def notify_support_message(context: ContextTypes.DEFAULT_TYPE, conn, thread_id: int, sender_role: str, body: str) -> None:
    thread = get_support_conversation(conn, thread_id)
    if not thread:
        return

    preview = (body or '').strip().replace('\n', ' ')
    if len(preview) > 120:
        preview = preview[:117] + '...'

    if sender_role == 'user':
        try:
            push_admin_notif(
                conn,
                'notif_novo_usuario',
                (
                    f"📥 Nova mensagem de atendimento\n"
                    f"Pessoa: {thread['first_name'] or thread['chat_id']}\n"
                    f"Assunto: {support_subject_label(thread['subject'])}\n"
                    f"Mensagem: {preview}"
                ),
            )
        except Exception as exc:
            logger.warning('Falha ao registrar notif admin para atendimento | thread_id=%s | erro=%s', thread_id, exc)

        admin_chat_ids = list_active_admin_chat_ids(conn)
        for admin_chat_id in admin_chat_ids:
            try:
                await context.bot.send_message(
                    chat_id=str(admin_chat_id),
                    text=(
                        f"📥 Nova mensagem de atendimento\n"
                        f"Pessoa: {thread['first_name'] or thread['chat_id']}\n"
                        f"Assunto: {support_subject_label(thread['subject'])}\n"
                        f"Mensagem: {preview}"
                    ),
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton('Abrir conversa', callback_data=f"support:admin:open:{thread_id}")]
                    ]),
                )
            except Exception as exc:
                logger.warning('Falha ao avisar admin sobre nova mensagem de atendimento | thread_id=%s | chat_id=%s | erro=%s', thread_id, admin_chat_id, exc)
        return

    try:
        await context.bot.send_message(
            chat_id=str(thread['chat_id']),
            text=(
                f"💬 Você recebeu uma resposta do atendimento\n"
                f"Assunto: {support_subject_label(thread['subject'])}\n"
                f"Mensagem: {preview}"
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('Abrir conversa', callback_data=f"support:open:{thread_id}")]
            ]),
        )
    except Exception as exc:
        logger.warning('Falha ao avisar usuário sobre resposta de atendimento | thread_id=%s | chat_id=%s | erro=%s', thread_id, thread['chat_id'], exc)


def full_menu_markup(chat_id: str | None = None) -> InlineKeyboardMarkup:
    if chat_id:
        conn = get_db()
        admin = is_admin_chat(conn, str(chat_id))
        settings = get_monetization_settings(conn)
        show_payments = bool(int(settings['charge_global']) == 1)
        user_id = get_user_id_by_chat(conn, str(chat_id))
        user_settings = get_user_settings(conn, user_id) if user_id else None
        user_unread_support, open_support_threads = get_support_badges(conn, str(chat_id), admin=False)
        admin_unread_support, admin_open_threads = get_support_badges(conn, str(chat_id), admin=True) if admin else (0, 0)
        conn.close()
        alerts_enabled = bool(int(user_settings['alerts_enabled'])) if user_settings else True
    else:
        admin = False
        show_payments = False
        alerts_enabled = True
        user_unread_support, open_support_threads = 0, 0
        admin_unread_support, admin_open_threads = 0, 0

    suporte_label = '💬 Fale conosco'
    if user_unread_support:
        suporte_label += f' ({user_unread_support})'

    keyboard = [
        [InlineKeyboardButton('🖼️ Gerar consulta manual agora', callback_data='menu:agora')],
        [InlineKeyboardButton('🛫 Minhas Rotas', callback_data='menu:minhasrotas')],
        [InlineKeyboardButton('⚙️ Filtro de consultas', callback_data='menu:limite')],
        [InlineKeyboardButton('🔔 Desativar alertas' if alerts_enabled else '🔕 Ativar alertas', callback_data='menu:togglealerts')],
    ]
    if show_payments:
        keyboard.append([InlineKeyboardButton('💳 Meus pagamentos', callback_data='menu:pagamentos')])
    keyboard.append([InlineKeyboardButton('ℹ️ Ajuda e instruções', callback_data='menu:manual')])
    if not admin:
        keyboard.append([InlineKeyboardButton(suporte_label, callback_data='menu:support')])
    if admin:
        keyboard.append([InlineKeyboardButton('🛠 Painel', callback_data='menu:adminpainel')])
    return InlineKeyboardMarkup(keyboard)


def user_welcome_preview_text() -> str:
    return (
        '🎉 *Bem-vindo ao bot de voos*\n────────────────────────\n\n'
        'Eu acompanho rotas cadastradas e envio notificações automáticas quando encontrar oportunidades.\n\n'
        '*Próximo passo:* cadastre sua primeira rota.\n'
        'Você vai informar:\n'
        '- origem\n'
        '- destino\n'
        '- data de ida\n'
        '- data de volta, se quiser\n\n'
        'Depois disso, é só aguardar as notificações.\n'
        'Se preferir, você também pode rodar uma *consulta instantânea* a qualquer momento.'
    )


def user_manage_markup(target_chat_id: str, blocked: bool, skip_charge: bool, can_trigger_scan: bool, is_test: bool = False, status: str = 'free') -> InlineKeyboardMarkup:
    block_label = '✅ Desbloquear usuário' if blocked else '🚫 Bloquear usuário'
    plan_label = '🔒 Exigir verificação de plano' if skip_charge else '🔓 Liberar sem verificação de plano'
    test_label = '🧪 Desmarcar como teste' if is_test else '🧪 Marcar como usuário teste'
    status_label = f'📋 Status: {status} 🔄'
    rows = [
        [InlineKeyboardButton(block_label, callback_data=f'painel:usr_bloquear:{target_chat_id}')],
        [InlineKeyboardButton('🧭 Ver trechos do usuário', callback_data=f'painel:usr_trechos:{target_chat_id}')],
        [InlineKeyboardButton('👁️ Ver mensagem inicial do usuário', callback_data=f'painel:usr_preview_start:{target_chat_id}')],
    ]
    if can_trigger_scan:
        rows.append([InlineKeyboardButton('🖼️ Gerar consulta manual', callback_data=f'painel:usr_manual:{target_chat_id}')])
        rows.append([InlineKeyboardButton('⏰ Gerar consulta agendada', callback_data=f'painel:usr_sched:{target_chat_id}')])
    rows.extend([
        [InlineKeyboardButton('🔄 Zerar acessos grátis', callback_data=f'painel:usr_zerar:{target_chat_id}')],
        [InlineKeyboardButton(test_label, callback_data=f'painel:usr_test_toggle:{target_chat_id}')],
        [InlineKeyboardButton(status_label, callback_data=f'painel:usr_status:{target_chat_id}')],
        [InlineKeyboardButton(plan_label, callback_data=f'painel:usr_plano:{target_chat_id}')],
        [InlineKeyboardButton('🗑️ Excluir usuário', callback_data=f'painel:usr_del:{target_chat_id}')],
        [InlineKeyboardButton('🔙 Voltar à lista', callback_data='painel:usuarios')],
    ])
    return InlineKeyboardMarkup(rows)


def user_delete_confirm_markup(target_chat_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('⚠️ Sim, excluir tudo', callback_data=f'painel:usr_del_ok:{target_chat_id}')],
        [InlineKeyboardButton('❌ Cancelar',           callback_data=f'painel:usr:{target_chat_id}')],
    ])


async def _hide_query_markup_safe(query) -> None:
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass


def normalize_max_price(max_price) -> float | None:
    if max_price is None:
        return None
    if isinstance(max_price, (int, float)):
        return float(max_price)
    txt = str(max_price or '').strip()
    if not txt:
        return None
    lowered = txt.lower()
    if lowered in {'sem limite', 'semlimite', 'qualquer valor', 'qualquer', 'todos'}:
        return None
    txt = txt.replace('R$', '').replace('r$', '').strip()
    txt = txt.replace('.', '').replace(',', '.') if ',' in txt else txt
    try:
        return float(txt)
    except ValueError:
        return None


def _user_manage_text(conn, target_chat_id: str) -> tuple[str, bool, bool, bool, bool, str]:
    """Retorna (texto, blocked, skip_charge, can_trigger_scan, is_test, status)."""
    u = get_bot_user_by_chat(conn, target_chat_id)
    if not u:
        return ('Usuário não encontrado.', False, False, False, False, 'free')
    access = conn.execute(sql('SELECT * FROM user_access WHERE chat_id = %s'), (target_chat_id,)).fetchone()
    routes_count = conn.execute(
        sql('SELECT COUNT(*) AS total FROM user_routes WHERE user_id = %s AND active = 1'), (u['user_id'],)
    ).fetchone()
    bot_settings = conn.execute(
        sql('SELECT max_price, COALESCE(alerts_enabled, 1) AS alerts_enabled FROM bot_settings WHERE user_id = %s'), (u['user_id'],)
    ).fetchone()
    rotas = int((routes_count['total'] if routes_count else None) or 0)
    max_price_raw = (bot_settings['max_price'] if bot_settings else None)
    max_price_norm = normalize_max_price(max_price_raw)
    filtro_valor = 'Sem limite' if max_price_norm is None else f'R$ {format_money_br(float(max_price_norm))}'
    free_uses_limit = conn.execute(sql('SELECT free_uses_limit FROM monetization_settings WHERE id = 1')).fetchone()
    limite = int((free_uses_limit['free_uses_limit'] if free_uses_limit else None) or 3)
    blocked    = bool(int(u['blocked'] or 0))
    skip_c     = bool(int((access['skip_charge'] if access else None) or 0))
    is_test    = bool(int(u.get('is_test_user', 0) or 0))
    status     = (access['status'] if access else None) or 'free'
    expires    = (access['expires_at'] if access else None) or '—'
    free_uses  = int((access['free_uses'] if access else None) or 0)
    total_paid = float((access['total_paid'] if access else None) or 0)
    alerts_enabled = bool(int((bot_settings['alerts_enabled'] if bot_settings else 1) or 0))
    is_confirmed = bool(int(u['confirmed'] or 0))
    can_trigger_scan = is_confirmed and rotas > 0 and alerts_enabled and not blocked
    if is_test:
        test_badge = '🧪 Sim — pula bloqueio e cobrança'
    else:
        test_badge = 'Não'
    text = (
        f"👤 *Gerenciar Usuário*\n\n"
        f"*Nome:* {u['first_name'] or '—'}\n"
        f"*Username:* @{u['username'] or '—'}\n"
        f"*Chat ID:* `{target_chat_id}`\n"
        f"*Confirmado:* {'Sim' if int(u['confirmed'] or 0) else 'Não'}\n"
        f"*Teste:* {test_badge}\n"
        f"*Bloqueado:* {'🚫 Sim' if blocked else '✅ Não'}\n"
        f"*Status:* {status}\n"
        f"*Válido até:* {expires}\n"
        f"*Usos grátis:* {free_uses}/{limite}\n"
        f"*Verificação de plano:* {'Desativada 🔓' if skip_c else 'Ativa 🔒'}\n"
        f"*Total pago:* R$ {total_paid:.2f}\n"
        f"*Rotas ativas:* {rotas}\n"
        f"*Filtro de valor:* {filtro_valor}\n"
        f"*Notificações:* {'Ativas ✅' if alerts_enabled else 'Desativadas ❌'}\n"
        f"*Pode disparar consulta:* {'Sim ✅' if can_trigger_scan else 'Não ❌'}"
    )
    return text, blocked, skip_c, can_trigger_scan, is_test, status


def admin_notif_markup(notif_settings: dict) -> InlineKeyboardMarkup:
    def btn(key: str) -> InlineKeyboardButton:
        ativo = notif_settings.get(key, True)
        label = f"{'✅' if ativo else '❌'} {NOTIF_LABELS[key]}"
        return InlineKeyboardButton(label, callback_data=f'painel:notif:{key}')
    return InlineKeyboardMarkup([
        [btn('notif_novo_usuario')],
        [btn('notif_acesso_expirado')],
        [btn('notif_pix_gerado')],
        [btn('notif_pagamento_confirmado')],
        [InlineKeyboardButton('🔙 Voltar ao Painel', callback_data='painel:back')],
    ])


def admin_panel_markup(settings_row=None, maintenance_on: bool = False, show_result_type_filters: bool = True, admin_unread_support: int = 0) -> InlineKeyboardMarkup:
    settings = settings_row or {}
    test_mode_on = bool(int(settings.get('test_mode', 0) or 0)) if isinstance(settings, dict) else False
    charge_global_on = bool(int(settings.get('charge_global', 0) or 0)) if isinstance(settings, dict) else False
    charge_admin_on = bool(int(settings.get('charge_admin_only', 0) or 0)) if isinstance(settings, dict) else False

    manut_label = '🔧 Manutenção ✅' if maintenance_on else '🔧 Manutenção ❌'
    filtros_label = '🎛 Filtros Companhia/Agências ✅' if show_result_type_filters else '🎛 Filtros Companhia/Agências ❌'
    modo_teste_label = '🧪 Modo Teste ✅' if test_mode_on else '🧪 Modo Teste ❌'
    cobranca_global_label = '🌐 Cobrança Geral ✅' if charge_global_on else '🌐 Cobrança Geral ❌'
    cobranca_admin_label = '👤 Cobrança Admin ✅' if charge_admin_on else '👤 Cobrança Admin ❌'
    atendimento_label = '📥 Atendimento'
    if admin_unread_support:
        atendimento_label += f' ({admin_unread_support})'
    return InlineKeyboardMarkup([
        # Gestão
        [InlineKeyboardButton('👤 Usuários', callback_data='painel:usuarios'),
         InlineKeyboardButton('🧭 Trechos', callback_data='painel:usuarios_trechos')],
        [InlineKeyboardButton('💰 Vendas', callback_data='painel:vendas'),
         InlineKeyboardButton('⚙️ Planos', callback_data='painel:planos')],
        # Toggles
        [InlineKeyboardButton(manut_label, callback_data='painel:manutencao'),
         InlineKeyboardButton(modo_teste_label, callback_data='painel:modo_teste')],
        [InlineKeyboardButton(cobranca_global_label, callback_data='painel:cobranca_global'),
         InlineKeyboardButton(cobranca_admin_label, callback_data='painel:cobranca_admin')],
        [InlineKeyboardButton(filtros_label, callback_data='painel:toggle_result_type_filters')],
        # Config
        [InlineKeyboardButton('🎁 Acessos Grátis', callback_data='painel:free_access'),
         InlineKeyboardButton('⏱ Intervalo', callback_data='painel:scan_interval')],
        # Ações
        [InlineKeyboardButton('💳 Pix', callback_data='painel:pix'),
         InlineKeyboardButton('🔔 Notificações', callback_data='painel:notificacoes')],
        [InlineKeyboardButton('📣 Broadcast', callback_data='painel:broadcast'),
         InlineKeyboardButton('📅 Agendador', callback_data='painel:scheduler_status')],
        [InlineKeyboardButton('🔄 Reiniciar', callback_data='painel:restart_service'),
         InlineKeyboardButton('🔐 Renovar Google', callback_data='painel:renovar_sessao')],
        [InlineKeyboardButton('📊 Desempenho', callback_data='painel:desempenho'),
         InlineKeyboardButton(atendimento_label, callback_data='menu:adminsupport')],
        # Voltar
        [InlineKeyboardButton('🏠 Menu principal', callback_data='menu:back')],
    ])


def selector_health_result_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔙 Voltar ao Painel', callback_data='painel:back')],
    ])


def plans_adjust_markup(settings_row=None) -> InlineKeyboardMarkup:
    settings = settings_row or {}
    plan_buttons = []
    if float(settings.get('weekly_price', 0) or 0) > 0:
        plan_buttons.append([
            InlineKeyboardButton('✏️ Semanal', callback_data='painel:plan_edit:weekly'),
            InlineKeyboardButton('🗑️ Semanal', callback_data='painel:plan_delete:weekly')
        ])
    if float(settings.get('biweekly_price', 0) or 0) > 0:
        plan_buttons.append([
            InlineKeyboardButton('✏️ Quinzenal', callback_data='painel:plan_edit:biweekly'),
            InlineKeyboardButton('🗑️ Quinzenal', callback_data='painel:plan_delete:biweekly')
        ])
    if float(settings.get('monthly_price', 0) or 0) > 0:
        plan_buttons.append([
            InlineKeyboardButton('✏️ Mensal', callback_data='painel:plan_edit:monthly'),
            InlineKeyboardButton('🗑️ Mensal', callback_data='painel:plan_delete:monthly')
        ])
    plan_buttons.append([InlineKeyboardButton('➕ Adicionar plano', callback_data='painel:plan_add')])
    plan_buttons.append([InlineKeyboardButton('🔙 Voltar ao Painel', callback_data='painel:back')])
    return InlineKeyboardMarkup(plan_buttons)


def plan_entry_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💰 Escolha um plano para continuar', callback_data='payment:changeplan')],
        [InlineKeyboardButton('⬅️ Voltar ao menu', callback_data='menu:back')],
    ])


def user_plan_markup() -> InlineKeyboardMarkup:
    conn = get_db()
    try:
        plans = plan_catalog(get_monetization_settings(conn))
    finally:
        conn.close()
    rows = [[InlineKeyboardButton(f'Pix {name}', callback_data=f'userpix:{name}')] for name, _amount, _days in plans]
    rows.append([InlineKeyboardButton('⬅️ Voltar ao menu', callback_data='menu:back')])
    return InlineKeyboardMarkup(rows)


def pending_payment_markup(payment_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ Verificar pagamento', callback_data=f'painel:checkpay:{payment_id}')],
        [InlineKeyboardButton('❌ Cancelar pagamento', callback_data=f'payment:cancel:{payment_id}')],
        [InlineKeyboardButton('⬅️ Voltar ao painel', callback_data='menu:back')],
    ])


def airport_keyboard(prefix: str, options: list[tuple[str, str]] | None = None, include_search_hint: bool = True) -> InlineKeyboardMarkup:
    label = 'origem' if prefix == 'origem' else 'destino'
    buttons = []
    if include_search_hint:
        buttons.append([InlineKeyboardButton(f'🔎 Pesquisar {label}', callback_data=f'{prefix}:search')])
    if options:
        for code, name in options[:8]:
            buttons.append([InlineKeyboardButton(f'{code} — {name}', callback_data=f'{prefix}:{code}')])
    buttons.append([InlineKeyboardButton('❌ Cancelar cadastro de rota', callback_data='addrota:cancel')])
    return InlineKeyboardMarkup(buttons)


def airport_search_results_markup(prefix: str, options: list[tuple[str, str]], query: str) -> InlineKeyboardMarkup:
    buttons = []
    for code, name in options[:8]:
        buttons.append([InlineKeyboardButton(f'{code} — {name}', callback_data=f'{prefix}:{code}')])
    buttons.append([InlineKeyboardButton(f'🔁 Buscar novamente: {query[:30]}', callback_data=f'{prefix}:search')])
    buttons.append([InlineKeyboardButton('❌ Cancelar cadastro de rota', callback_data='addrota:cancel')])
    return InlineKeyboardMarkup(buttons)


def sources_menu_markup(enable_google: bool) -> InlineKeyboardMarkup:
    google_label = '✅ Google Voos (fixo)' if enable_google else 'Google Voos (fixo)'
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(google_label, callback_data='sources:noop')],
        [InlineKeyboardButton('⬅️ Voltar ao menu', callback_data='menu:back')],
    ])


AIRLINE_FILTER_OPTIONS = [
    ('any_airline', 'Companhias Aéreas'),
]


def normalize_airline_label(name: str) -> str:
    raw = (name or '').strip().lower()
    normalized = raw.replace('á', 'a').replace('à', 'a').replace('ã', 'a').replace('â', 'a')
    normalized = normalized.replace('é', 'e').replace('ê', 'e').replace('í', 'i').replace('ó', 'o').replace('ô', 'o').replace('õ', 'o').replace('ú', 'u').replace('ç', 'c')
    if 'companhia aerea' in normalized or 'companhia aerea' in normalized.replace('á', 'a'):
        return 'any_airline'
    if 'gol' in normalized:
        return 'gol'
    if 'latam' in normalized or 'tam' in normalized:
        return 'latam'
    if 'azul' in normalized:
        return 'azul'
    if 'voepass' in normalized or 'passaredo' in normalized or 'passaro' in normalized or 'map ' in normalized or normalized == 'map':
        return 'voepass'
    if 'aerolineas argentinas' in normalized or 'aerolineas' in normalized or 'aerolineas argentinascompanhia aerea' in normalized:
        return 'any_airline'
    if 'avianca' in normalized or 'tap' in normalized or 'copa' in normalized or 'aeromexico' in normalized:
        return 'any_airline'
    if 'american' in normalized or 'delta' in normalized or 'united' in normalized:
        return 'any_airline'
    if 'air france' in normalized or 'airfrance' in normalized or 'klm' in normalized:
        return 'any_airline'
    if 'iberia' in normalized or 'sky' in normalized or 'jetsmart' in normalized:
        return 'any_airline'
    if 'lufthansa' in normalized or 'british' in normalized or 'emirates' in normalized:
        return 'any_airline'
    return 'others'


def parse_airline_filters(raw: str | None) -> dict[str, bool]:
    selected = {'any_airline': True}
    if not raw:
        return selected
    try:
        data = json.loads(raw)
    except Exception:
        return selected
    if not isinstance(data, dict):
        return selected

    if 'any_airline' in data:
        for key, _ in AIRLINE_FILTER_OPTIONS:
            if key in data:
                selected[key] = bool(data[key])
        return selected

    legacy_airline_enabled = any(bool(data.get(key, True)) for key in ('gol', 'latam', 'azul', 'voepass', 'others'))
    selected['any_airline'] = legacy_airline_enabled
    return selected


def filter_rows_by_airlines(rows: list[dict], airline_filters_json: str | None, show_result_type_filters: bool = True) -> list[dict]:
    selected = parse_airline_filters(airline_filters_json)
    allow_any_airline = bool(selected.get('any_airline', True))

    if not show_result_type_filters or allow_any_airline:
        return rows

    filtered = []
    for row in rows:
        airline = normalize_airline_label(str(row.get('airline') or ''))
        if selected.get(airline, False):
            filtered.append(row)
    return filtered


def serialize_airline_filters(selected: dict[str, bool]) -> str:
    payload = {
        'any_airline': bool(selected.get('any_airline', True)),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def should_show_result_type_filters(conn) -> bool:
    row = conn.execute(sql("SELECT COALESCE(show_result_type_filters, 1) AS v FROM app_settings WHERE id = 1")).fetchone()
    return bool(int(row['v'])) if row else True


def filter_menu_markup(max_price: float | None, selected_airlines: dict[str, bool], enable_google: bool) -> InlineKeyboardMarkup:
    limit_value = 'Sem limite' if max_price is None else f'R$ {format_money_br(float(max_price))}'
    any_airline = bool(selected_airlines.get('any_airline', True))
    conn = get_db()
    show_result_type_filters = should_show_result_type_filters(conn)
    conn.close()
    keyboard = [
        [InlineKeyboardButton('💰 PREÇO MÁXIMO', callback_data='filter:edit_limit')],
        [InlineKeyboardButton(f'• {limit_value}', callback_data='filter:price_info')],
    ]
    if show_result_type_filters:
        keyboard.extend([
            [InlineKeyboardButton('✈️ FILTRO DE COMPANHIAS', callback_data='filter:airlines_info')],
            [
                InlineKeyboardButton(('✅ ' if any_airline else '⬜ ') + 'Companhias Aéreas', callback_data='filter:toggle_airline:any_airline'),
            ],
        ])
    keyboard.append([InlineKeyboardButton('⬅️ Voltar ao menu', callback_data='menu:back')])
    return InlineKeyboardMarkup(keyboard)


def build_filter_menu_text(max_price: float | None, selected_airlines: dict[str, bool], enable_google: bool) -> str:
    enabled = [label for key, label in AIRLINE_FILTER_OPTIONS if selected_airlines.get(key, False)]
    airlines_txt = ', '.join(enabled) if enabled else 'Nenhum'
    conn = get_db()
    show_result_type_filters = should_show_result_type_filters(conn)
    conn.close()
    parts = [
        '\n✈️ *Filtro de consultas*\n────────────────────────\n',
        '*PREÇO MÁXIMO*\n',
        f"💰 Valor atual por trecho: *{'Sem limite' if max_price is None else 'R$ ' + format_money_br(float(max_price))}*\n",
        'Toque no botão *PREÇO MÁXIMO* para editar o valor.\n',
    ]
    if show_result_type_filters:
        parts.extend([
            '\n────────────────────────\n\n',
            '*TIPOS DE RESULTADO*\n',
            f'✈️ Ativos: *{airlines_txt}*\n',
            'Toque nos botões abaixo para alterar os tipos exibidos.\n',
        ])
    return ''.join(parts)


def removerrota_list_markup(rows) -> InlineKeyboardMarkup:
    keyboard = []
    for row in rows:
        label = f"{row['origin']}→{row['destination']} | {format_date_br(row['outbound_date'])}"
        if row['inbound_date']:
            label += f" | {format_date_br(row['inbound_date'])}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"removerrota:{row['id']}")])
    keyboard.append([InlineKeyboardButton('❌ Cancelar remoção', callback_data='removerrota:cancel_list')])
    return InlineKeyboardMarkup(keyboard)


def rotas_management_markup(rows: list) -> InlineKeyboardMarkup:
    """Teclado com ações no final (Adicionar/Remover/Voltar)."""
    keyboard = []
    if rows:
        keyboard.append([
            InlineKeyboardButton('➕ Adicionar rota', callback_data='menu:addrota'),
            InlineKeyboardButton('➖ Remover rota', callback_data='menu:removerrota'),
        ])
    else:
        keyboard.append([InlineKeyboardButton('➕ Adicionar rota', callback_data='menu:addrota')])
    keyboard.append([InlineKeyboardButton('🔙 Voltar', callback_data='menu:back')])
    return InlineKeyboardMarkup(keyboard)


def manual_topics_markup(show_monetization: bool = True) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton('🚀 Primeiros passos', callback_data='manual:primeiros_passos')],
        [InlineKeyboardButton('➕ Como cadastrar uma rota', callback_data='manual:cadastrar_rota')],
        [InlineKeyboardButton('🔎 Como buscar aeroportos', callback_data='manual:buscar_aeroportos')],
        [InlineKeyboardButton('📅 Como informar as datas', callback_data='manual:datas')],
        [InlineKeyboardButton('💰 Filtro de preço', callback_data='manual:filtro_preco')],
        [InlineKeyboardButton('🖼️ Consulta manual agora', callback_data='manual:consulta_manual')],
    ]
    if show_monetization:
        buttons.append([InlineKeyboardButton('💳 Pagamentos e cobrança', callback_data='manual:pagamentos')])
        buttons.append([InlineKeyboardButton('🎁 Consultas grátis', callback_data='manual:consultas_gratis')])
        buttons.append([InlineKeyboardButton('📊 Meu status de cobrança', callback_data='manual:consultas_gratis_status')])
    buttons.append([InlineKeyboardButton('⬅️ Voltar ao menu', callback_data='menu:back')])
    return InlineKeyboardMarkup(buttons)


def charging_status_text(conn, chat_id: str) -> str:
    settings = get_monetization_settings(conn)
    access = ensure_user_access(conn, chat_id)
    free_uses_limit = get_free_uses_limit(conn)
    free_uses = int(access['free_uses'] or 0)
    charge_global = int(settings['charge_global'] or 0) == 1
    charge_admin_only = int(settings['charge_admin_only'] or 0) == 1
    test_charge = int(access.get('test_charge', 0) or 0) == 1 if isinstance(access, dict) else False
    plans = plan_catalog(settings)

    if charge_global:
        charging_line = '🌐 Cobrança geral: *ATIVA* para todos os usuários.'
    elif charge_admin_only:
        charging_line = '👤 Cobrança em modo teste: *ATIVA apenas para admin/teste*.'
    else:
        charging_line = '🆓 Cobrança: *desativada no momento*.'

    test_line = '🧪 Seu usuário está marcado para teste de cobrança.' if test_charge else '🧪 Seu usuário não está marcado para teste de cobrança.'

    return (
        '💳 *Cobrança e consultas grátis*\n────────────────────────\n\n'
        f'{charging_line}\n'
        f'{test_line}\n\n'
        f'🎁 Consultas grátis disponíveis antes da cobrança: *{free_uses_limit}*\n'
        f'📊 Consultas grátis já usadas no seu acesso atual: *{free_uses}/{free_uses_limit}*\n\n'
        '*Planos atuais disponíveis*\n'
        f'🥉 {weekly[0]}: R$ {format_money_br(weekly[1])} ({weekly[2]} dias)\n'
        f'🥈 {biweekly[0]}: R$ {format_money_br(biweekly[1])} ({biweekly[2]} dias)\n'
        f'🥇 {monthly[0]}: R$ {format_money_br(monthly[1])} ({monthly[2]} dias)\n\n'
        'Os valores dos planos vêm da configuração do banco e podem ser alterados pelo administrador.'
    )


def manual_topic_text(topic: str) -> str:
    mapping = {
        'primeiros_passos': (
            '🚀 *Primeiros passos*\n────────────────────────\n\n'
            'Bem-vindo ao bot de monitoramento de voos.\n\n'
            '*Para que serve?*\n'
            'O bot acompanha rotas cadastradas e envia notificações automáticas com oportunidades de voo.\n\n'
            '*O que fazer primeiro?*\n'
            '1. Cadastre pelo menos uma rota.\n'
            '2. Se quiser, ajuste o filtro de preço máximo.\n'
            '3. Depois, é só aguardar as notificações automáticas.\n\n'
            'Se quiser consultar na hora, você também pode usar *🖼️ Gerar consulta manual agora* a qualquer momento.'
        ),
        'cadastrar_rota': (
            '➕ *Como cadastrar uma rota*\n────────────────────────\n\n'
            'O cadastro acontece em sequência:\n'
            '1. informar a *origem*\n'
            '2. informar o *destino*\n'
            '3. informar a *data de ida*\n'
            '4. depois informar a *data de volta* se quiser\n\n'
            'Se não quiser passagem de volta, basta deixar a rota como somente ida quando o fluxo permitir ou cadastrar apenas a ida no formato atual disponível.'
        ),
        'buscar_aeroportos': (
            '🔎 *Como buscar aeroportos*\n────────────────────────\n\n'
            'Você pode digitar de várias formas:\n'
            '- código IATA, ex: `PVH`, `GRU`, `LIS`\n'
            '- cidade, ex: `Porto Velho`, `São Paulo`, `Lisboa`\n'
            '- aeroporto, ex: `Guarulhos`\n'
            '- região/estado, quando ajudar na busca\n\n'
            'Depois o bot mostra opções para você tocar na correta.'
        ),
        'datas': (
            '📅 *Como informar as datas*\n────────────────────────\n\n'
            'A ordem ideal é:\n'
            '1. primeiro a *ida*\n'
            '2. depois a *volta*\n\n'
            'Formatos aceitos:\n'
            '- `25/12/2026`\n'
            '- `25-12-2026`\n'
            '- `2026-12-25`\n'
            '- `25122026`\n'
            '- `25 dez 2026`\n\n'
            'Se a data estiver inválida, o bot pede para reenviar.'
        ),
        'filtro_preco': (
            '💰 *Filtro de preço*\n────────────────────────\n\n'
            'O filtro de preço é opcional.\n\n'
            'Se você quiser, pode definir um valor máximo por trecho.\n'
            'Se não cadastrar nenhum valor, o bot não considera esse filtro.'
        ),
        'consulta_manual': (
            '🖼️ *Consulta manual agora*\n────────────────────────\n\n'
            'Use essa opção quando quiser gerar uma consulta instantânea sem esperar o próximo envio automático.\n\n'
            'Fluxo recomendado:\n'
            '1. cadastre sua rota\n'
            '2. ajuste o filtro de preço se quiser\n'
            '3. use *🖼️ Gerar consulta manual agora*\n\n'
            'No dia a dia, depois do cadastro, o normal é só aguardar as notificações automáticas.'
        ),
        'pagamentos': (
            '💳 *Pagamentos e cobrança*\n────────────────────────\n\n'
            'Se a cobrança estiver ativa, o bot pode pedir um plano quando os usos grátis acabarem ou quando a regra de cobrança do momento exigir.\n\n'
            'Os valores dos planos são definidos na configuração do banco e exibidos para o usuário conforme a configuração atual.\n\n'
            'Você pode acompanhar seus pagamentos no menu em *💳 Meus pagamentos*.\n\n'
            'Se tiver dúvida sobre pagamento, use *💬 Fale conosco*.'
        ),
        'consultas_gratis': (
            '🎁 *Consultas grátis*\n────────────────────────\n\n'
            'O bot pode liberar uma quantidade de consultas grátis antes da cobrança.\n\n'
            'Essa quantidade vem da configuração do banco e deve ser usada em toda a lógica e nos textos do sistema.\n\n'
            'Quando os usos grátis acabarem, o bot pode apresentar os planos disponíveis conforme a configuração atual.'
        ),
    }
    return mapping.get(topic, mapping['primeiros_passos'])


async def manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    conn = get_db()
    try:
        settings = get_monetization_settings(conn)
        show_monetization = bool(int(settings['charge_global']) == 1)
    finally:
        conn.close()
    await update.message.reply_text(
        'ℹ️ *Dúvidas frequentes*\n────────────────────────\n\nEscolha abaixo o assunto que você quer ver:',
        parse_mode='Markdown',
        reply_markup=manual_topics_markup(show_monetization),
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_pending_input_state(context)
    user = update.effective_user
    chat_id = str(update.effective_chat.id)
    first_name = user.first_name or 'telegram'

    conn = get_db()
    row = get_bot_user_by_chat(conn, chat_id)
    if row is None:
        user_id = ensure_app_user(conn, first_name)
        conn.execute(
            sql('''
            INSERT INTO bot_users (user_id, chat_id, username, first_name, confirmed)
            VALUES (%s, %s, %s, %s, 0)
            '''),
            (user_id, chat_id, user.username or '', first_name),
        )
        ensure_user_settings(conn, user_id)
        confirmed = False
    else:
        conn.execute(
            sql('''
            UPDATE bot_users
            SET username = %s, first_name = %s
            WHERE chat_id = %s
            '''),
            (user.username or '', first_name, chat_id),
        )
        confirmed = int(row['confirmed']) == 1
        ensure_user_settings(conn, int(row['user_id']))
    ensure_user_access(conn, chat_id)
    ensure_owner_test_access(conn)
    conn.commit()
    conn.close()

    audit.user_action("cmd_start", chat_id=chat_id,
                      payload={"first_name": first_name, "confirmed": confirmed,
                               "username": user.username or ""})

    if not confirmed:
        conn2 = get_db()
        push_admin_notif(
            conn2,
            "notif_novo_usuario",
            f"👤 *Novo usuário no bot*\n\n"
            f"*Nome:* {first_name}\n"
            f"*Username:* @{user.username or '—'}\n"
            f"*Chat ID:* `{chat_id}`",
        )
        conn2.close()

    if not confirmed:
        await update.message.reply_text(
            '👋 Olá! Para começar a usar o bot, confirme seu cadastro com o botão abaixo.',
            reply_markup=start_markup(),
        )
        return

    await update.message.reply_text(
        get_panel_text(chat_id),
        parse_mode='HTML',
        reply_markup=full_menu_markup(chat_id),
    )

    if confirmed:
        cur_routes = conn.execute(sql('SELECT COUNT(*) AS total FROM user_routes WHERE user_id = %s AND active = 1'), (row['user_id'],))
        row_routes = cur_routes.fetchone()
        has_routes = (row_routes['total'] if isinstance(row_routes, dict) else row_routes[0]) > 0
        if not has_routes:
            await update.message.reply_text(
                '🎉 <b>Seja bem-vindo ao bot de voos!</b>\n────────────────────────\n\n'
                'Aqui você acompanha rotas e recebe notificações quando encontrarmos oportunidades.\n\n'
                '👇 <b>Primeiros passos:</b>\n'
                '1️⃣ Clique em <b>✈️ Cadastrar rota</b> no menu acima\n'
                '2️⃣ Informe: origem, destino, data de ida e volta (se quiser)\n'
                '3️⃣ Pronto! Você receberá alertas automáticos 🚀',
                parse_mode='HTML',
            )


async def confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass
    chat_id = str(query.message.chat.id)

    try:
        conn = get_db()
        row = get_bot_user_by_chat(conn, chat_id)
        if row is None:
            conn.close()
            await query.edit_message_text('⚠️ Use /start antes para iniciar seu cadastro.')
            return

        conn.execute(sql('UPDATE bot_users SET confirmed = 1 WHERE chat_id = %s'), (chat_id,))
        ensure_user_settings(conn, int(row['user_id']))

        cur = conn.execute(sql('SELECT COUNT(*) AS total FROM user_routes WHERE user_id = %s AND active = 1'), (row['user_id'],))
        count_row = cur.fetchone()
        routes_count = count_row['total'] if isinstance(count_row, dict) else count_row[0]

        conn.commit()
        conn.close()

        audit.user_action("cadastro_confirmado", chat_id=chat_id,
                          user_id=row['user_id'])

        try:
            await query.edit_message_text('✅ *Cadastro confirmado com sucesso!* 🎉', parse_mode='Markdown')
        except Exception:
            pass

        await query.message.reply_text(
            '🎉 *Seja bem-vindo ao bot de voos!*\n────────────────────────\n\n'
            'Aqui você acompanha rotas e recebe notificações quando encontrarmos oportunidades.\n\n'
            '👇 *Primeiros passos:*\n'
            '1️⃣ Clique em *✈️ Cadastrar rota* no menu abaixo\n'
            '2️⃣ Informe: origem, destino, data de ida e volta (se quiser)\n'
            '3️⃣ Pronto! Você receberá alertas automáticos 🚀',
            parse_mode='Markdown',
        )

        await query.message.reply_text(
            get_panel_text(chat_id),
            parse_mode='HTML',
            reply_markup=full_menu_markup(chat_id),
        )
    except Exception as exc:
        logger.error('[confirm_callback] erro ao confirmar chat=%s: %s', chat_id, exc)


async def cmd_painel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    conn = get_db()
    if not is_admin_chat(conn, chat_id):
        conn.close()
        await update.message.reply_text('🚫 Comando restrito a administradores.')
        return

    ensure_owner_test_access(conn)
    settings = get_monetization_settings(conn)
    maintenance_on = is_maintenance_mode(conn)
    show_result_type_filters = should_show_result_type_filters(conn)
    admin_unread_support, _ = get_support_badges(conn, chat_id, admin=True)
    conn.close()

    texto = (
        '🛠 *Painel Administrativo*\n\n'
        f"🧪 Modo teste: {'ATIVADO ✅' if int(settings['test_mode']) == 1 else 'DESATIVADO ❌'}\n"
        f"🌐 Cobrança geral: {'ATIVA ✅' if int(settings['charge_global']) == 1 else 'DESATIVADA ❌'}\n"
        f"👤 Cobrança só admin: {'ATIVA ✅' if int(settings['charge_admin_only']) == 1 else 'DESATIVADA ❌'}\n"
        f"🔧 Manutenção: {'ATIVA ✅' if maintenance_on else 'DESATIVADA ❌'}\n"
        f"🎛 Exibir filtros Companhia/Agências: {'SIM ✅' if show_result_type_filters else 'NÃO ❌'}"
    )

    await update.message.reply_text(
        texto,
        parse_mode='Markdown',
        reply_markup=admin_panel_markup(settings, maintenance_on, show_result_type_filters, admin_unread_support)
    )

async def painel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = str(query.message.chat.id)
    conn = get_db()

    # userpix: é para usuários normais (gerar Pix), não requer admin
    if query.data.startswith('userpix:'):
        plan_name = query.data.split(':', 1)[1]
        access = ensure_user_access(conn, chat_id)
        if not should_charge_user(conn, chat_id, access):
            conn.close()
            await query.answer('Cobrança não disponível para este usuário.', show_alert=True)
            return
        if is_active_access(access):
            conn.close()
            await query.message.reply_text(f"✅ Você já tem um plano ativo até {access['expires_at']}. Não é necessário gerar outro Pix agora.")
            await query.answer()
            return
        existing_pending = get_valid_pending_payment(conn, chat_id)
        if existing_pending:
            conn.close()
            await query.edit_message_text(
                f"💳 *Você já tem um Pix pendente válido*\n\n*Plano:* {existing_pending['plan_name']}\n*Valor:* R$ {format_money_br(existing_pending['amount'])}",
                parse_mode='Markdown'
            )
            await query.message.reply_text(existing_pending['qr_code'] or 'Código Pix indisponível no momento.')
            if existing_pending['ticket_url']:
                await query.message.reply_text(existing_pending['ticket_url'])
            await query.message.reply_text(
                'Selecione uma opção:',
                reply_markup=pending_payment_markup(str(existing_pending['mp_payment_id']))
            )
            await query.answer()
            return
        settings = get_monetization_settings(conn)
        amount = plan_amount_by_name(settings, plan_name)
        payment = create_mp_pix_payment(chat_id, plan_name, amount)
        qr_code = payment.get('point_of_interaction', {}).get('transaction_data', {}).get('qr_code', '')
        ticket_url = payment.get('point_of_interaction', {}).get('transaction_data', {}).get('ticket_url', '')
        save_payment(conn, str(payment.get('id')), chat_id, plan_name, amount, payment.get('status', 'pending'), qr_code, ticket_url)
        audit.payment("pix_gerado", chat_id=chat_id, status="pending",
                      payload={"plano": plan_name, "valor": amount,
                               "mp_payment_id": str(payment.get('id'))})
        push_admin_notif(
            conn,
            "notif_pix_gerado",
            f"💳 *PIX gerado*\n\n"
            f"*Chat ID:* `{chat_id}`\n"
            f"*Plano:* {plan_name}\n"
            f"*Valor:* R$ {amount:.2f}\n"
            f"*ID:* `{payment.get('id')}`",
        )
        conn.close()
        await query.edit_message_text(
            f"💳 *Pix gerado com sucesso!*\n\n*Valor:* R$ {format_money_br(amount)}\n*ID:* `{payment.get('id')}`",
            parse_mode='Markdown'
        )
        await query.message.reply_text(qr_code or 'Código Pix indisponível no momento.')
        if ticket_url:
            await query.message.reply_text(ticket_url)
        await query.message.reply_text(
            'Selecione uma opção:',
            reply_markup=pending_payment_markup(str(payment.get("id")))
        )
        await query.answer()
        return

    if not is_admin_chat(conn, chat_id):
        conn.close()
        await query.answer('Não autorizado', show_alert=True)
        return

    parts = query.data.split(':')
    action = ':'.join(parts[1:]) if len(parts) > 1 else ''
    ensure_owner_test_access(conn)

    if action == 'usuarios':
        await query.answer()
        text, markup = _render_user_list(conn)
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=markup)

    elif action.startswith('usr:'):
        await query.answer()
        target_chat_id = action[4:]
        text, blocked, skip_c, can_trigger_scan, is_test, status = _user_manage_text(conn, target_chat_id)
        await query.edit_message_text(text, parse_mode='Markdown',
                                      reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))

    elif action.startswith('usr_bloquear:'):
        target_chat_id = action[len('usr_bloquear:'):]
        row = get_bot_user_by_chat(conn, target_chat_id)
        novo = 1
        if row:
            novo = 0 if int(row['blocked'] or 0) else 1
            conn.execute(sql('UPDATE bot_users SET blocked = %s WHERE chat_id = %s'), (novo, target_chat_id))
            conn.commit()
            acao = 'bloqueado' if novo else 'desbloqueado'
            audit.admin(f"usuario_{acao}", chat_id=chat_id,
                        payload={"target_chat_id": target_chat_id})
            if not novo:
                # Desbloqueou — avisa o usuário e abre o menu
                try:
                    await context.bot.send_message(
                        chat_id=target_chat_id,
                        text='✅ Sua conta foi desbloqueada. Seu acesso foi restabelecido.',
                    )
                except Exception as exc:
                    logger.warning('usr_bloquear: falha ao notificar desbloqueio | target=%s | erro=%s', target_chat_id, exc)
                try:
                    panel = get_panel_text(target_chat_id)
                    await context.bot.send_message(
                        chat_id=target_chat_id,
                        text=panel,
                        parse_mode='HTML',
                        reply_markup=full_menu_markup(target_chat_id),
                    )
                except Exception as exc:
                    logger.warning('usr_bloquear: falha ao abrir menu | target=%s | erro=%s', target_chat_id, exc)
        await query.answer('Bloqueado ✅' if novo else 'Desbloqueado ✅')
        # Recarrega o card do usuário
        text, blocked, skip_c, can_trigger_scan, is_test, status = _user_manage_text(conn, target_chat_id)
        try:
            await query.edit_message_text(text, parse_mode='Markdown',
                                          reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))
        except Exception:
            await query.message.reply_text(text, parse_mode='Markdown',
                                          reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))

    elif action.startswith('usr_trechos:'):
        await query.answer()
        target_chat_id = action[len('usr_trechos:'):]
        user_row = get_bot_user_by_chat(conn, target_chat_id)
        if not user_row:
            await query.edit_message_text('Usuário não encontrado.', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Voltar à lista', callback_data='painel:usuarios')]]))
        else:
            routes = conn.execute(sql('''
                SELECT origin, destination, outbound_date, inbound_date, active
                FROM user_routes
                WHERE user_id = %s
                ORDER BY active DESC, created_at DESC, id DESC
            '''), (user_row['user_id'],)).fetchall()
            if routes:
                linhas = []
                for r in routes:
                    status_rota = '✅ ativa' if int(r['active'] or 0) else '⏸ inativa'
                    volta = f" | volta {format_date_display(r['inbound_date'])}" if (r['inbound_date'] or '').strip() else ''
                    linhas.append(f"• {r['origin']} → {r['destination']} | ida {format_date_display(r['outbound_date'])}{volta} | {status_rota}")
                texto = f"🧭 *Trechos do usuário*\n\n*Usuário:* {user_row['first_name'] or '—'}\n*Chat ID:* `{target_chat_id}`\n\n" + '\n'.join(linhas)
            else:
                texto = f"🧭 *Trechos do usuário*\n\n*Usuário:* {user_row['first_name'] or '—'}\n*Chat ID:* `{target_chat_id}`\n\n_Nenhum trecho cadastrado._"
            await query.edit_message_text(texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Voltar ao usuário', callback_data=f'painel:usr:{target_chat_id}')]]))

    elif action.startswith('usr_preview_start:'):
        await query.answer()
        target_chat_id = action[len('usr_preview_start:'):]
        user_row = get_bot_user_by_chat(conn, target_chat_id)
        nome = (user_row['first_name'] or '—') if user_row else '—'
        texto = (
            f"👁️ *Prévia da mensagem inicial do usuário*\n\n"
            f"*Usuário:* {nome}\n"
            f"*Chat ID:* `{target_chat_id}`\n\n"
            + user_welcome_preview_text()
        )
        await query.edit_message_text(texto, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Voltar ao usuário', callback_data=f'painel:usr:{target_chat_id}')]]))

    elif action.startswith('usr_manual:'):
        target_chat_id = action[len('usr_manual:'):]
        user_row = get_bot_user_by_chat(conn, target_chat_id)
        if not user_row:
            await query.answer('Usuário não encontrado.', show_alert=True)
        else:
            conn.execute(
                sql("UPDATE scan_jobs SET status = 'error', finished_at = NOW(), error_message = 'cancelled_by_new_request' WHERE user_id = %s AND job_type = 'manual_now' AND status IN ('pending', 'running')"),
                (user_row['user_id'],),
            )
            conn.execute(
                sql("INSERT INTO scan_jobs (user_id, chat_id, job_type, status, payload, cost_score) VALUES (%s, %s, 'manual_now', 'pending', %s, 0)"),
                (user_row['user_id'], target_chat_id, '{}'),
            )
            conn.commit()
            await query.answer('Consulta manual enfileirada ✅')
        text, blocked, skip_c, can_trigger_scan, is_test, status = _user_manage_text(conn, target_chat_id)
        try:
            await query.edit_message_text(text, parse_mode='Markdown',
                                          reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))
        except Exception:
            await query.message.reply_text(text, parse_mode='Markdown',
                                          reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))

    elif action.startswith('usr_sched:'):
        target_chat_id = action[len('usr_sched:'):]
        user_row = get_bot_user_by_chat(conn, target_chat_id)
        if not user_row:
            await query.answer('Usuário não encontrado.', show_alert=True)
        else:
            conn.execute(
                sql("UPDATE scan_jobs SET status = 'error', finished_at = NOW(), error_message = 'cancelled_by_new_request' WHERE user_id = %s AND job_type = 'scheduled' AND status IN ('pending', 'running')"),
                (user_row['user_id'],),
            )
            conn.execute(
                sql("INSERT INTO scan_jobs (user_id, chat_id, job_type, status, payload) VALUES (%s, %s, 'scheduled', 'pending', %s )"),
                (user_row['user_id'], target_chat_id, '{}'),
            )
            conn.commit()
            await query.answer('Consulta agendada enfileirada ✅')
        text, blocked, skip_c, can_trigger_scan, is_test, status = _user_manage_text(conn, target_chat_id)
        try:
            await query.edit_message_text(text, parse_mode='Markdown',
                                          reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))
        except Exception:
            await query.message.reply_text(text, parse_mode='Markdown',
                                          reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))

    elif action == 'usuarios_trechos':
        await query.answer()
        users = conn.execute(sql('''
            SELECT b.user_id, b.chat_id, COALESCE(b.first_name, '') AS first_name, COALESCE(b.username, '') AS username
            FROM bot_users b
            WHERE COALESCE(b.confirmed, 0) = 1
            ORDER BY COALESCE(NULLIF(TRIM(b.first_name), ''), NULLIF(TRIM(b.username), ''), b.chat_id) ASC
            LIMIT 50
        ''')).fetchall()
        blocos = []
        for u in users:
            routes = conn.execute(sql('''
                SELECT origin, destination, outbound_date, inbound_date, active
                FROM user_routes
                WHERE user_id = %s
                ORDER BY active DESC, created_at DESC, id DESC
            '''), (u['user_id'],)).fetchall()
            if not routes:
                continue
            linhas = [f"*{u['first_name'] or u['username'] or u['chat_id']}* (`{u['chat_id']}`)"]
            for r in routes:
                status_rota = '✅' if int(r['active'] or 0) else '⏸'
                volta = f" | volta {format_date_display(r['inbound_date'])}" if (r['inbound_date'] or '').strip() else ''
                linhas.append(f"{status_rota} {r['origin']} → {r['destination']} | ida {format_date_display(r['outbound_date'])}{volta}")
            blocos.append('\n'.join(linhas))
        texto = '🧭 *Trechos dos usuários*\n\n' + ('\n\n'.join(blocos) if blocos else '_Nenhum trecho cadastrado no momento._')
        await query.edit_message_text(texto[:4096], parse_mode='Markdown', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Voltar ao Painel', callback_data='painel:back')]]))

    elif action.startswith('usr_zerar:'):
        target_chat_id = action[len('usr_zerar:'):]
        conn.execute(
            sql(f"UPDATE user_access SET free_uses = 0, updated_at = NOW() WHERE chat_id = %s"),
            (target_chat_id,)
        )
        conn.commit()
        audit.admin("usuario_usos_zerados", chat_id=chat_id,
                    payload={"target_chat_id": target_chat_id})
        text, blocked, skip_c, can_trigger_scan, is_test, status = _user_manage_text(conn, target_chat_id)
        try:
            await query.edit_message_text(text, parse_mode='Markdown',
                                          reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))
        except Exception:
            await query.message.reply_text(text, parse_mode='Markdown',
                                          reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))

    elif action.startswith('usr_test_toggle:'):
        target_chat_id = action[len('usr_test_toggle:'):]
        u_row = get_bot_user_by_chat(conn, target_chat_id)
        novo = 1
        if u_row:
            current = int(u_row.get('is_test_user', 0) or 0)
            novo = 0 if current else 1
            conn.execute(sql('UPDATE bot_users SET is_test_user = %s WHERE chat_id = %s'), (novo, target_chat_id))
            conn.commit()
            audit.admin('usuario_test_toggle', chat_id=chat_id,
                        payload={'target_chat_id': target_chat_id, 'is_test_user': novo})
        await query.answer('Usuário teste ✅' if novo else 'Usuário normal ✅')
        text, blocked, skip_c, can_trigger_scan, is_test, status = _user_manage_text(conn, target_chat_id)
        try:
            await query.edit_message_text(text, parse_mode='Markdown',
                                          reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))
        except Exception:
            await query.message.reply_text(text, parse_mode='Markdown',
                                          reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))

    elif action.startswith('usr_status:'):
        target_chat_id = action[len('usr_status:'):]
        access = conn.execute(sql('SELECT status FROM user_access WHERE chat_id = %s'), (target_chat_id,)).fetchone()
        current_status = (access['status'] or 'free') if access else 'free'
        # Ciclo: free -> active -> expired -> free
        novo_status = {'free': 'active', 'active': 'expired', 'expired': 'free'}.get(current_status, 'free')
        conn.execute(
            sql(f"UPDATE user_access SET status = %s, updated_at = NOW() WHERE chat_id = %s"),
            (novo_status, target_chat_id),
        )
        conn.commit()
        audit.admin('usuario_status_alterado', chat_id=chat_id,
                    payload={'target_chat_id': target_chat_id, 'de': current_status, 'para': novo_status})
        await query.answer(f'Status alterado: {current_status} → {novo_status} ✅')
        text, blocked, skip_c, can_trigger_scan, is_test, status = _user_manage_text(conn, target_chat_id)
        try:
            await query.edit_message_text(text, parse_mode='Markdown',
                                          reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))
        except Exception:
            await query.message.reply_text(text, parse_mode='Markdown',
                                          reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))

    elif action.startswith('usr_plano:'):
        target_chat_id = action[len('usr_plano:'):]
        acc = conn.execute(sql('SELECT skip_charge FROM user_access WHERE chat_id = %s'), (target_chat_id,)).fetchone()
        current = int((acc['skip_charge'] if acc else None) or 0) if acc else 0
        novo = 0 if current else 1
        conn.execute(
            sql(f"UPDATE user_access SET skip_charge = %s, updated_at = NOW() WHERE chat_id = %s"),
            (novo, target_chat_id)
        )
        conn.commit()
        audit.admin("usuario_skip_charge", chat_id=chat_id,
                    payload={"target_chat_id": target_chat_id, "skip_charge": novo})
        text, blocked, skip_c, can_trigger_scan, is_test, status = _user_manage_text(conn, target_chat_id)
        try:
            await query.edit_message_text(text, parse_mode='Markdown',
                                          reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))
        except Exception:
            await query.message.reply_text(text, parse_mode='Markdown',
                                          reply_markup=user_manage_markup(target_chat_id, blocked, skip_c, can_trigger_scan, is_test, status=status))

    elif action.startswith('usr_del:'):
        target_chat_id = action[len('usr_del:'):]
        u = get_bot_user_by_chat(conn, target_chat_id)
        nome = (u['first_name'] or target_chat_id) if u else target_chat_id
        try:
            await query.answer()
            await query.edit_message_text(
                f"🗑️ *Excluir usuário*\n\n"
                f"Você está prestes a excluir *{nome}* (`{target_chat_id}`) e todos os seus dados:\n"
                f"rotas, pagamentos, histórico de suporte e configurações.\n\n"
                f"⚠️ Esta ação é *irreversível*.",
                parse_mode='Markdown',
                reply_markup=user_delete_confirm_markup(target_chat_id),
            )
        except Exception:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🗑️ *Excluir usuário*\n\n"
                     f"Você está prestes a excluir *{nome}* (`{target_chat_id}`) e todos os seus dados:\n"
                     f"rotas, pagamentos, histórico de suporte e configurações.\n\n"
                     f"⚠️ Esta ação é *irreversível*.",
                parse_mode='Markdown',
                reply_markup=user_delete_confirm_markup(target_chat_id),
            )

    elif action.startswith('usr_del_ok:'):
        target_chat_id = action[len('usr_del_ok:'):]
        try:
            await query.answer()
        except Exception:
            pass
        import subprocess as _sp
        _base = '/opt/vooindo'
        # Responde callback imediatamente para n\u00e3o expirar
        try:
            await query.answer()
        except Exception:
            pass

        _script = '/opt/vooindo/delete_user_cli.py'
        try:
            _r = _sp.run(
                [_sp.sys.executable if hasattr(_sp, 'sys') else sys.executable, _script, target_chat_id],
                capture_output=True, text=True, timeout=25,
                cwd=_base,
            )
            _out = (_r.stdout or '').strip()
            _err = (_r.stderr or '').strip()
            if _r.returncode == 0:
                audit.admin("usuario_excluido", chat_id=chat_id,
                            payload={"target_chat_id": target_chat_id, "action": "subprocess"})
                _m = '\u2705 Usu\u00e1rio `' + target_chat_id + '` exclu\u00eddo com sucesso.'
            else:
                _m = '\u274c Erro ao excluir usu\u00e1rio `' + target_chat_id + '`.\n\n`' + (_err or _out)[:1500] + '`'
        except _sp.TimeoutExpired:
            _m = '\u274c Exclus\u00e3o em segundo plano... O usu\u00e1rio foi removido, mas pode levar alguns segundos.'
        except Exception as _e:
            _m = '\u274c Falha ao excluir: `' + str(_e)[:2000] + '`'
        try:
            await query.edit_message_text(_m, parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('\U0001f519 Voltar \u00e0 lista', callback_data='painel:usuarios')]]))
        except Exception:
            try:
                await context.bot.send_message(chat_id=chat_id, text=_m, parse_mode='Markdown')
            except Exception:
                pass

    elif action.startswith('selectorhealth') and len(parts) >= 3:
        subaction = parts[2]
        admin_main_chat_id = str(os.getenv('TELEGRAM_ADMIN_CHAT_ID', '') or '')
        if admin_main_chat_id and chat_id != admin_main_chat_id:
            audit.admin('selector_health_aprovacao_bloqueada', chat_id=chat_id, status='blocked', payload={'motivo': 'admin_principal_only', 'query_data': query.data})
            conn.close()
            await query.answer('Apenas o admin principal pode aprovar isso.', show_alert=True)
            return
        if subaction == 'approve' and len(parts) >= 4:
            token = parts[3]
            conn.close()
            try:
                result = subprocess.run(
                    [os.path.join(os.path.dirname(__file__), '.venv', 'bin', 'python'), os.path.join(os.path.dirname(__file__), 'selector_health.py'), '--approve', token],
                    capture_output=True,
                    text=True,
                    timeout=180,
                    cwd=os.path.dirname(__file__),
                )
                output = (result.stdout or '').strip() or (result.stderr or '').strip() or 'Sem saída.'
                if result.returncode == 0:
                    audit.admin('selector_health_aprovado', chat_id=chat_id, payload={'token': token, 'returncode': result.returncode})
                    await query.edit_message_text(
                        '✅ Correções do selector health aplicadas com sucesso.\n\n' + output[:3500],
                        reply_markup=selector_health_result_markup(),
                    )
                else:
                    audit.admin('selector_health_aprovacao_falhou', chat_id=chat_id, status='error', payload={'token': token, 'returncode': result.returncode, 'output': output[:1000]})
                    await query.edit_message_text(
                        '⚠️ A aprovação rodou, mas houve falha ao aplicar as correções.\n\n' + output[:3500],
                        reply_markup=selector_health_result_markup(),
                    )
            except Exception as exc:
                audit.admin('selector_health_execucao_falhou', chat_id=chat_id, status='error', error_msg=str(exc), payload={'token': token})
                await query.edit_message_text(
                    f'❌ Falha ao executar aprovação do selector health.\n\n{exc}',
                    reply_markup=selector_health_result_markup(),
                )
            await query.answer('Processando aprovação...')
            return
        elif subaction == 'reject' and len(parts) >= 4:
            token = parts[3]
            audit.admin('selector_health_rejeitado', chat_id=chat_id, status='skipped', payload={'token': token})
            conn.close()
            await query.edit_message_text(
                f'❌ Correções do selector health rejeitadas.\n\nToken: `{token}`',
                parse_mode='Markdown',
                reply_markup=selector_health_result_markup(),
            )
            await query.answer('Correções rejeitadas.')
            return

    elif action == 'vendas':
        await query.answer()
        rows = conn.execute(sql("SELECT mp_payment_id, plan_name, amount, status, created_at FROM payments ORDER BY created_at DESC LIMIT 10")).fetchall()
        total_aprovado = _fetchcount(conn.execute(sql("SELECT COALESCE(SUM(amount), 0) FROM payments WHERE status = 'approved'")).fetchone())
        total_pendente = _fetchcount(conn.execute(sql("SELECT COALESCE(SUM(amount), 0) FROM payments WHERE status = 'pending'")).fetchone())
        aprovados = _fetchcount(conn.execute(sql("SELECT COUNT(*) FROM payments WHERE status = 'approved'")).fetchone())
        pendentes = _fetchcount(conn.execute(sql("SELECT COUNT(*) FROM payments WHERE status = 'pending'")).fetchone())
        outros = _fetchcount(conn.execute(sql("SELECT COUNT(*) FROM payments WHERE status NOT IN ('approved', 'pending')")).fetchone())
        if rows:
            lines = [f"• {r['mp_payment_id'] or '-'} | {r['plan_name'] or '-'} | R$ {format_money_br(r['amount'])} | {r['status']}" for r in rows]
            texto = (
                "💰 *Relatório de Vendas*\n\n"
                f"Receita aprovada: *R$ {format_money_br(total_aprovado)}*\n"
                f"Valor pendente: *R$ {format_money_br(total_pendente)}*\n\n"
                f"Pagamentos aprovados: *{aprovados}*\n"
                f"Pagamentos pendentes: *{pendentes}*\n"
                f"Outros status: *{outros}*\n\n"
                "Últimos registros:\n" + "\n".join(lines)
            )
        else:
            texto = "💰 *Relatório de Vendas*\n\n_Nenhum pagamento registrado ainda._"
        await query.edit_message_text(
            texto,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Voltar ao Painel', callback_data='painel:back')]])
        )

    elif action == 'planos':
        await query.answer()
        settings = get_monetization_settings(conn)
        plans = plan_catalog(settings)
        if plans:
            linhas = [f"• {name}: R$ {format_money_br(amount)} ({days} dias)" for name, amount, days in plans]
            detalhes = '\n'.join(linhas)
        else:
            detalhes = 'Nenhum plano ativo no banco.'
        texto = (
            "⚙️ *Configuração de Planos*\n\n"
            f"{detalhes}\n\n"
            "Use os botões abaixo para editar ou excluir cada plano cadastrado no banco."
        )
        await query.edit_message_text(texto, parse_mode='Markdown', reply_markup=plans_adjust_markup(settings))

    elif action == 'modo_teste':
        await query.answer()
        settings = get_monetization_settings(conn)
        novo = 0 if int(settings['test_mode']) == 1 else 1
        conn.execute(sql('UPDATE monetization_settings SET test_mode = %s WHERE id = 1'), (novo,))
        for admin_chat_id in list_active_admin_chat_ids(conn):
            ensure_user_access(conn, admin_chat_id)
            conn.execute(
                sql("UPDATE user_access SET test_charge = %s, updated_at = NOW() WHERE chat_id = %s"),
                (novo, admin_chat_id),
            )
        conn.commit()
        audit.admin("config_alterada", chat_id=chat_id,
                    payload={"campo": "test_mode", "valor_novo": novo})
        settings = get_monetization_settings(conn)
        maintenance_on = is_maintenance_mode(conn)
        show_result_type_filters = should_show_result_type_filters(conn)
        texto = (
            '🛠 *Painel Administrativo*\n\n'
            f"🧪 Modo teste: {'ATIVADO ✅' if int(settings['test_mode']) == 1 else 'DESATIVADO ❌'}\n"
            f"🌐 Cobrança geral: {'ATIVA ✅' if int(settings['charge_global']) == 1 else 'DESATIVADA ❌'}\n"
            f"👤 Cobrança só admin: {'ATIVA ✅' if int(settings['charge_admin_only']) == 1 else 'DESATIVADA ❌'}\n"
            f"🔧 Manutenção: {'ATIVA ✅' if maintenance_on else 'DESATIVADA ❌'}\n"
            f"🎛 Exibir filtros Companhia/Agências: {'SIM ✅' if show_result_type_filters else 'NÃO ❌'}"
        )
        await query.edit_message_text(texto, parse_mode='Markdown', reply_markup=admin_panel_markup(settings, maintenance_on, show_result_type_filters, get_support_badges(conn, chat_id, admin=True)[0]))

    elif action == 'manutencao':
        novo_estado = not is_maintenance_mode(conn)
        set_maintenance_mode(conn, novo_estado)
        audit.admin("config_alterada", chat_id=chat_id,
                    payload={"campo": "manutencao", "valor_novo": novo_estado})
        settings = get_monetization_settings(conn)
        maintenance_on = is_maintenance_mode(conn)
        show_result_type_filters = should_show_result_type_filters(conn)
        texto = (
            '🛠 *Painel Administrativo*\n\n'
            f"🧪 Modo teste: {'ATIVADO ✅' if int(settings['test_mode']) == 1 else 'DESATIVADO ❌'}\n"
            f"🌐 Cobrança geral: {'ATIVA ✅' if int(settings['charge_global']) == 1 else 'DESATIVADA ❌'}\n"
            f"👤 Cobrança só admin: {'ATIVA ✅' if int(settings['charge_admin_only']) == 1 else 'DESATIVADA ❌'}\n"
            f"🔧 Manutenção: {'ATIVA ✅' if maintenance_on else 'DESATIVADA ❌'}\n"
            f"🎛 Exibir filtros Companhia/Agências: {'SIM ✅' if show_result_type_filters else 'NÃO ❌'}"
        )
        await query.edit_message_text(texto, parse_mode='Markdown', reply_markup=admin_panel_markup(settings, maintenance_on, show_result_type_filters, get_support_badges(conn, chat_id, admin=True)[0]))
        await query.answer('Manutenção ativada ✅' if maintenance_on else 'Manutenção desativada ❌')
        conn.close()
        return

    elif action == 'cobranca_global':
        await query.answer()
        settings = get_monetization_settings(conn)
        novo = 0 if int(settings['charge_global']) == 1 else 1
        conn.execute(sql('UPDATE monetization_settings SET charge_global = %s WHERE id = 1'), (novo,))
        conn.commit()
        audit.admin("config_alterada", chat_id=chat_id,
                    payload={"campo": "cobranca_global", "valor_novo": novo})
        settings = get_monetization_settings(conn)
        maintenance_on = is_maintenance_mode(conn)
        show_result_type_filters = should_show_result_type_filters(conn)
        texto = (
            '🛠 *Painel Administrativo*\n\n'
            f"🧪 Modo teste: {'ATIVADO ✅' if int(settings['test_mode']) == 1 else 'DESATIVADO ❌'}\n"
            f"🌐 Cobrança geral: {'ATIVA ✅' if int(settings['charge_global']) == 1 else 'DESATIVADA ❌'}\n"
            f"👤 Cobrança só admin: {'ATIVA ✅' if int(settings['charge_admin_only']) == 1 else 'DESATIVADA ❌'}\n"
            f"🔧 Manutenção: {'ATIVA ✅' if maintenance_on else 'DESATIVADA ❌'}\n"
            f"🎛 Exibir filtros Companhia/Agências: {'SIM ✅' if show_result_type_filters else 'NÃO ❌'}"
        )
        await query.edit_message_text(texto, parse_mode='Markdown', reply_markup=admin_panel_markup(settings, maintenance_on, show_result_type_filters, get_support_badges(conn, chat_id, admin=True)[0]))

    elif action == 'cobranca_admin':
        await query.answer()
        settings = get_monetization_settings(conn)
        novo = 0 if int(settings['charge_admin_only']) == 1 else 1
        conn.execute(sql('UPDATE monetization_settings SET charge_admin_only = %s WHERE id = 1'), (novo,))
        conn.commit()
        audit.admin("config_alterada", chat_id=chat_id,
                    payload={"campo": "cobranca_admin", "valor_novo": novo})
        settings = get_monetization_settings(conn)
        maintenance_on = is_maintenance_mode(conn)
        show_result_type_filters = should_show_result_type_filters(conn)
        texto = (
            '🛠 *Painel Administrativo*\n\n'
            f"🧪 Modo teste: {'ATIVADO ✅' if int(settings['test_mode']) == 1 else 'DESATIVADO ❌'}\n"
            f"🌐 Cobrança geral: {'ATIVA ✅' if int(settings['charge_global']) == 1 else 'DESATIVADA ❌'}\n"
            f"👤 Cobrança só admin: {'ATIVA ✅' if int(settings['charge_admin_only']) == 1 else 'DESATIVADA ❌'}\n"
            f"🔧 Manutenção: {'ATIVA ✅' if maintenance_on else 'DESATIVADA ❌'}\n"
            f"🎛 Exibir filtros Companhia/Agências: {'SIM ✅' if show_result_type_filters else 'NÃO ❌'}"
        )
        await query.edit_message_text(texto, parse_mode='Markdown', reply_markup=admin_panel_markup(settings, maintenance_on, show_result_type_filters, get_support_badges(conn, chat_id, admin=True)[0]))

    elif action == 'pix':
        await query.answer()
        settings = get_monetization_settings(conn)
        amount = 1.0 if int(settings['test_mode']) == 1 else float(settings['monthly_price'])
        plan_name = 'Teste Admin' if int(settings['test_mode']) == 1 else 'Mensal'
        payment = create_mp_pix_payment(chat_id, plan_name, amount)
        qr_code = payment.get('point_of_interaction', {}).get('transaction_data', {}).get('qr_code', '')
        ticket_url = payment.get('point_of_interaction', {}).get('transaction_data', {}).get('ticket_url', '')
        save_payment(conn, str(payment.get('id')), chat_id, plan_name, amount, payment.get('status', 'pending'), qr_code, ticket_url)
        await query.edit_message_text(
            f"💳 *Pix gerado com sucesso!*\n\n*Valor:* R$ {format_money_br(amount)}\n*ID:* `{payment.get('id')}`",
            parse_mode='Markdown'
        )
        await query.message.reply_text(qr_code or 'Código Pix indisponível no momento.')
        if ticket_url:
            await query.message.reply_text(ticket_url)
        await query.message.reply_text(
            'Selecione uma opção:',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('✅ Verificar pagamento', callback_data=f'painel:checkpay:{payment.get("id")}')],
                [InlineKeyboardButton('Voltar ao painel', callback_data='painel:back')]
            ])
        )

    elif action.startswith('checkpay:') and len(parts) >= 3:
        await query.answer()
        payment_id = parts[2]
        approved, info = apply_approved_payment(conn, payment_id)
        if approved:
            await query.message.reply_text(f'🎉 Pagamento aprovado! Acesso liberado até {info}.')
        else:
            await query.message.reply_text(f'⏳ Pagamento ainda não aprovado. Status atual: {info}')

    elif action.startswith('cancel:') and len(parts) >= 3:
        await query.answer()
        payment_id = parts[2]
        conn.execute(
            sql("UPDATE payments SET status = 'cancelled' WHERE mp_payment_id = %s AND chat_id = %s AND status = 'pending'"),
            (payment_id, chat_id)
        )
        conn.commit()
        await query.message.reply_text(
            '❌ Pagamento cancelado. Escolha um novo plano:',
            reply_markup=user_plan_markup()
        )

    elif action.startswith('plan_edit:'):
        await query.answer()
        field = action.split(':', 1)[1]
        if field.startswith('plan_edit:'):
            field = field.split(':', 1)[1]
        settings = get_monetization_settings(conn)
        mapping = {
            'weekly': ('weekly_price', 'semanal'),
            'biweekly': ('biweekly_price', 'quinzenal'),
            'monthly': ('monthly_price', 'mensal'),
        }
        column, label = mapping[field]
        valor = float(settings[column] or 0)
        context.user_data['awaiting_plan_price_edit'] = field
        await query.message.reply_text(
            f"💰 Valor atual do plano {label}: R$ {format_money_br(valor)}\n\nEnvie o novo valor. Exemplo: 15 ou 15,00",
            reply_markup=cancel_markup('painel:back', '❌ Cancelar edição'),
        )
        return ConversationHandler.END

    elif action.startswith('plan_delete:'):
        await query.answer()
        field = action.split(':', 1)[1]
        if field.startswith('plan_delete:'):
            field = field.split(':', 1)[1]
        settings = get_monetization_settings(conn)
        mapping = {
            'weekly': ('weekly_price', 'semanal'),
            'biweekly': ('biweekly_price', 'quinzenal'),
            'monthly': ('monthly_price', 'mensal'),
        }
        column, label = mapping[field]
        await query.edit_message_text(
            f"🗑️ *Confirmar exclusão do plano {label}%s*\n\nValor atual: R$ {format_money_br(settings[column])}\n\nIsso vai zerar o valor configurado no banco.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('✅ Confirmar exclusão', callback_data=f'painel:plan_delete_confirm:{field}')],
                [InlineKeyboardButton('❌ Cancelar', callback_data='painel:planos')],
            ]),
        )

    elif action.startswith('plan_delete_confirm:'):
        await query.answer()
        field = action.split(':', 1)[1]
        if field.startswith('plan_delete_confirm:'):
            field = field.split(':', 1)[1]
        mapping = {
            'weekly': 'weekly_price',
            'biweekly': 'biweekly_price',
            'monthly': 'monthly_price',
        }
        conn.execute(sql(f"UPDATE monetization_settings SET {mapping[field]} = 0 WHERE id = 1"))
        conn.commit()
        settings = get_monetization_settings(conn)
        plans = plan_catalog(settings)
        detalhes = '\n'.join([f"• {name}: R$ {format_money_br(amount)} ({days} dias)" for name, amount, days in plans]) if plans else 'Nenhum plano ativo no banco.'
        texto = (
            "⚙️ *Configuração de Planos*\n\n"
            f"{detalhes}\n\n"
            "Use os botões abaixo para editar ou excluir cada plano cadastrado no banco."
        )
        await query.edit_message_text(texto, parse_mode='Markdown', reply_markup=plans_adjust_markup(settings))

    elif action == 'toggle_result_type_filters':
        current = 1 if should_show_result_type_filters(conn) else 0
        novo = 0 if current == 1 else 1
        conn.execute(sql('UPDATE app_settings SET show_result_type_filters = %s, updated_at = datetime(\'now\') WHERE id = 1'), (novo,))
        conn.commit()
        settings = get_monetization_settings(conn)
        maintenance_on = is_maintenance_mode(conn)
        show_result_type_filters = should_show_result_type_filters(conn)
        texto = (
            '🛠 *Painel Administrativo*\n\n'
            f"🧪 Modo teste: {'ATIVADO ✅' if int(settings['test_mode']) == 1 else 'DESATIVADO ❌'}\n"
            f"🌐 Cobrança geral: {'ATIVA ✅' if int(settings['charge_global']) == 1 else 'DESATIVADA ❌'}\n"
            f"👤 Cobrança só admin: {'ATIVA ✅' if int(settings['charge_admin_only']) == 1 else 'DESATIVADA ❌'}\n"
            f"🔧 Manutenção: {'ATIVA ✅' if maintenance_on else 'DESATIVADA ❌'}\n"
            f"🎛 Exibir filtros Companhia/Agências: {'SIM ✅' if show_result_type_filters else 'NÃO ❌'}"
        )
        await query.edit_message_text(texto, parse_mode='Markdown', reply_markup=admin_panel_markup(settings, maintenance_on, show_result_type_filters, get_support_badges(conn, chat_id, admin=True)[0]))
        await query.answer('Exibição dos filtros atualizada.')

    elif action == 'notificacoes':
        await query.answer()
        notif_settings = get_notif_settings(conn)
        texto = (
            '🔔 *Notificações Admin*\n\n'
            'Escolha quais alertas deseja receber:\n\n'
            + '\n'.join(
                f"{'✅' if notif_settings[k] else '❌'} {NOTIF_LABELS[k]}"
                for k in notif_settings
            )
        )
        await query.edit_message_text(texto, parse_mode='Markdown',
                                      reply_markup=admin_notif_markup(notif_settings))

    elif action.startswith('notif:'):
        await query.answer()
        key = action[len('notif:'):]
        from notif import NOTIF_COLUMNS  # noqa: PLC0415
        if key in NOTIF_COLUMNS:
            row = conn.execute(
                sql(f'SELECT {key} FROM app_settings WHERE id = 1')
            ).fetchone()
            current = int((row[key] if isinstance(row, dict) else row[0]) or 0) if row else 0
            novo = 0 if current else 1
            conn.execute(
                sql(f"UPDATE app_settings SET {key} = %s, updated_at = NOW() WHERE id = 1"),
                (novo,)
            )
            conn.commit()
            audit.admin("notif_toggle", chat_id=chat_id,
                        payload={"key": key, "valor_novo": novo})
            notif_settings = get_notif_settings(conn)
            texto = (
                '🔔 *Notificações Admin*\n\n'
                'Escolha quais alertas deseja receber:\n\n'
                + '\n'.join(
                    f"{'✅' if notif_settings[k] else '❌'} {NOTIF_LABELS[k]}"
                    for k in notif_settings
                )
            )
            await query.edit_message_text(texto, parse_mode='Markdown',
                                          reply_markup=admin_notif_markup(notif_settings))

    elif action == 'desempenho':
        await query.answer()
        import subprocess  # noqa: PLC0415
        import re  # noqa: PLC0415

        def esc(t):
            '''Escapa caracteres especiais do Markdown do Telegram'''
            s = str(t) if t is not None else ''
            s = s.replace('_', '\\_')
            s = s.replace('*', '\\*')
            s = s.replace('[', '\\[')
            s = s.replace('`', '\\`')
            return s

        def db_query(query_str):
            try:
                r = subprocess.run(
                    ['mysql', '-h127.0.0.1', '-uvooindobot', '-pVooindo820412', 'vooindo', '-B', '-N', '-e', query_str],
                    capture_output=True, text=True, timeout=15
                )
                return r.stdout.strip()
            except Exception:
                return ''

        db_out = db_query(r'''
SELECT CONCAT(
  DATE_FORMAT(created_at, '%m-%d'), "|",
  COUNT(*), "|",
  SUM(CASE WHEN status='done' THEN 1 ELSE 0 END), "|",
  SUM(CASE WHEN status='error' AND error_message LIKE "%filtrados%" THEN 1 ELSE 0 END), "|",
  SUM(CASE WHEN status='error' AND error_message='cancelled_by_new_request' THEN 1 ELSE 0 END), "|",
  SUM(CASE WHEN status='error' AND error_message NOT LIKE "%filtrados%" AND error_message != 'cancelled_by_new_request' AND error_message != '' THEN 1 ELSE 0 END), "|",
  COALESCE(ROUND(AVG(CASE WHEN status='done' AND started_at IS NOT NULL AND finished_at IS NOT NULL THEN TIME_TO_SEC(TIMEDIFF(finished_at, started_at))/60 ELSE NULL END), 1), '-')
)
FROM scan_jobs
WHERE created_at > DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY DATE_FORMAT(created_at, '%Y-%m-%d')
ORDER BY created_at
''')
        resumo_out = db_query(r'''
SELECT CONCAT(
  COUNT(*), "|",
  SUM(CASE WHEN status='done' THEN 1 ELSE 0 END), "|",
  SUM(CASE WHEN status='error' AND error_message LIKE "%filtrados%" THEN 1 ELSE 0 END), "|",
  SUM(CASE WHEN status='error' AND error_message='cancelled_by_new_request' THEN 1 ELSE 0 END), "|",
  SUM(CASE WHEN status='error' AND error_message NOT LIKE "%filtrados%" AND error_message != 'cancelled_by_new_request' AND error_message != '' THEN 1 ELSE 0 END)
)
FROM scan_jobs WHERE created_at > NOW() - INTERVAL 7 DAY
''')
        erros_out = db_query(r'''
SELECT CONCAT(COALESCE(NULLIF(LEFT(error_message, 40), ''), '(vazio)'), "|", COUNT(*))
FROM scan_jobs
WHERE created_at > NOW() - INTERVAL 7 DAY AND status = 'error'
GROUP BY LEFT(error_message, 40)
ORDER BY COUNT(*) DESC
LIMIT 5
''')
        user_out = db_query(r'''
SELECT CONCAT(
  COALESCE(b.first_name, '%s'), "|",
  COUNT(j.id), "|",
  SUM(CASE WHEN j.status='done' THEN 1 ELSE 0 END), "|",
  COALESCE(rcnt.r, 0)
) FROM bot_users b
LEFT JOIN scan_jobs j ON j.user_id=b.user_id AND j.created_at > NOW() - INTERVAL 7 DAY
LEFT JOIN (SELECT user_id, COUNT(*) as r FROM user_routes WHERE active=1 GROUP BY user_id) rcnt ON rcnt.user_id=b.user_id
GROUP BY b.user_id
ORDER BY COUNT(j.id) DESC
LIMIT 15
''')

        texto = ''
        if not any([db_out, resumo_out, erros_out, user_out]):
            texto = '📊 *Desempenho*\n\n_Dados indisponiveis._'
        else:
            # === HEADER ===
            texto += '📊 *Desempenho*'

            if resumo_out:
                cols = resumo_out.split('|')
                if len(cols) >= 5:
                    total = int(cols[0] or 0)
                    ok = int(cols[1] or 0)
                    taxa = round(ok * 100 / max(total, 1), 1) if total else 0
                    status_icon = '✅' if taxa >= 60 else ('\u26a0\ufe0f' if taxa >= 35 else '')
                    texto += f'\n\n{status_icon}*Geral:* {ok}/{total} ({taxa}% ok)'
                    items = []
                    if cols[2] != '0':
                        items.append(f' filtro')
                    if cols[3] != '0':
                        items.append(f' \u2702{cols[3]}')
                    if cols[4] != '0':
                        items.append(f' erro{cols[4]}')
                    if items:
                        texto += '  ' + '  '.join(items)

            texto += f'\n{"\u2500" * 18}'

            # === DIAS ===
            if db_out:
                for row in db_out.split('\n'):
                    cols = row.split('|')
                    if len(cols) >= 7:
                        data = cols[0][:5]
                        total_d = int(cols[1] or 0)
                        ok_d = int(cols[2] or 0)
                        tempo = cols[6] if cols[6] != '-' else ''
                        if total_d > 0:
                            p = round(ok_d * 10 / total_d)
                            bar = '\U0001f7e2' * p + '\U0001f534' * (10 - p)
                        else:
                            bar = '\u26ab' * 10
                        t = f'  \u23f1{tempo}m' if tempo else ''
                        texto += f'\n{data} {bar}{t}'

            texto += f'\n{"\u2500" * 18}'

            # === USUARIOS ===
            if user_out:
                for row in user_out.split('\n'):
                    cols = row.split('|')
                    if len(cols) >= 4 and cols[1] != '0':
                        nome_u = esc(cols[0])[:10]
                        total_u = int(cols[1] or 0)
                        ok_u = int(cols[2] or 0)
                        pct = round(ok_u * 100 / max(total_u, 1), 1) if total_u else 0
                        rotas = int(cols[3] or 0)
                        if total_u > 0:
                            p = round(ok_u * 5 / total_u)
                            bar = '\U0001f7e2' * p + '\U0001f534' * (5 - p)
                        else:
                            bar = '\u26ab' * 5
                        texto += f'\n{nome_u} {bar}  {pct}%  {total_u}c {rotas}r'

            # === LEGENDA ===
            texto += f'\n{"\u2500" * 18}'
            texto += '\n\U0001f7e2=sucesso  \U0001f534=falha  \u23f1=tempo m\u00e9dio'

            if erros_out:
                for row in erros_out.split('\n'):
                    cols = row.split('|')
                    if len(cols) >= 2:
                        nome_erro = esc(cols[0])[:30]
                        texto += f'\n\u2022 {nome_erro}: {cols[1]}x'

        texto = texto.strip()[:4090]
        await query.edit_message_text(
            texto, parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔄 Atualizar', callback_data='painel:desempenho'), InlineKeyboardButton('🔙 Voltar ao Painel', callback_data='painel:back')]]),
        )

    elif action == 'scheduler_status':
        await query.answer()
        from datetime import timedelta  # noqa: PLC0415
        _SCAN_INTERVAL_MINUTES = int(__import__('os').getenv('SCAN_INTERVAL_MINUTES', '60'))
        row = conn.execute(sql('SELECT scan_interval_minutes FROM app_settings WHERE id = 1')).fetchone()
        if row and row['scan_interval_minutes'] is not None:
            interval_s = max(60, int(row['scan_interval_minutes']) * 60)
        else:
            interval_s = max(60, _SCAN_INTERVAL_MINUTES * 60)
        cooldown_s = max(60, interval_s - 100)
        users = conn.execute(sql('''
            SELECT bu.user_id, COALESCE(bu.first_name, '') AS first_name, bu.chat_id,
                   COALESCE(bs.last_sent_at, '') AS last_sent_at,
                   COALESCE(bs.alerts_enabled, 1) AS alerts_enabled
            FROM bot_users bu
            LEFT JOIN bot_settings bs ON bs.user_id = bu.user_id
            WHERE bu.confirmed = 1 AND COALESCE(bu.blocked, 0) = 0
            ORDER BY bu.user_id
        ''')).fetchall()
        now = now_local()
        livres, em_cooldown, sem_alerta = [], [], []
        for u in users:
            if not int(u['alerts_enabled']):
                sem_alerta.append(u['first_name'] or str(u['chat_id']))
                continue
            last = str(u['last_sent_at'] or '')
            in_cooldown = False
            if last:
                try:
                    dt = datetime.fromisoformat(last.replace(' ', 'T'))
                    delta = (now - dt).total_seconds()
                    in_cooldown = 0 <= delta < cooldown_s
                except ValueError:
                    pass
            if in_cooldown:
                em_cooldown.append(u['first_name'] or str(u['chat_id']))
            else:
                livres.append(u['first_name'] or str(u['chat_id']))
        running = conn.execute(sql("SELECT COUNT(*) AS c FROM scan_jobs WHERE status IN ('running','pending')")).fetchone()
        running_count = int((running['c'] if isinstance(running, dict) else running[0]) or 0)
        next_slot = now.replace(minute=0, second=0, microsecond=0) + timedelta(seconds=interval_s)
        if interval_s < 3600:
            elapsed = now.minute * 60 + now.second
            next_offset = ((elapsed // interval_s) + 1) * interval_s
            next_slot = now.replace(minute=0, second=0, microsecond=0) + timedelta(seconds=next_offset)
        mins_left = max(0, int((next_slot - now).total_seconds() // 60))
        texto = (
            f'📅 *Status do Agendador*\n\n'
            f'⏱ Intervalo: {interval_s // 60} min\n'
            f'🕐 Próximo ciclo: {next_slot.strftime("%H:%M")} ({mins_left} min)\n'
            f'⚙️ Jobs ativos agora: {running_count}\n\n'
            f'✅ *Receberão ({len(livres)}):*\n' + (', '.join(livres) if livres else '_nenhum_') + '\n\n'
            f'⏳ *Cooldown ativo ({len(em_cooldown)}):*\n' + (', '.join(em_cooldown) if em_cooldown else '_nenhum_') + '\n\n'
            + (f'🔕 *Alertas desativados ({len(sem_alerta)}):*\n' + ', '.join(sem_alerta) if sem_alerta else '')
        )
        await query.edit_message_text(
            texto.strip(), parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Voltar ao Painel', callback_data='painel:back')]]),
        )

    elif action == 'free_access':
        await query.answer()
        settings = get_monetization_settings(conn)
        current = int(settings['free_uses_limit'] or 20)
        presets = [0, 5, 10, 15, 20, 30, 50]
        texto = (
            '🎁 *Acessos Grátis*'
            '\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500'
            f'\n\nQuantidade atual: *{current}* consultas grátis por usuário.'
            '\n\nEscolha um novo valor:'
        )
        keyboard = []
        row_btns = []
        for opt in presets:
            chk = '\u2705 ' if opt == current else ''
            lbl = f'{chk}{"\u221e" if opt == 0 else opt}'
            row_btns.append(InlineKeyboardButton(lbl, callback_data=f'painel:free_access_set:{opt}'))
            if len(row_btns) == 3:
                keyboard.append(row_btns)
                row_btns = []
        if row_btns:
            keyboard.append(row_btns)
        keyboard.append([InlineKeyboardButton('\U0001f519 Voltar ao Painel', callback_data='painel:back')])
        await query.edit_message_text(texto, parse_mode='Markdown',
                                      reply_markup=InlineKeyboardMarkup(keyboard))

    elif action.startswith('free_access_set:'):
        await query.answer()
        try:
            novo_valor = int(action.split(':', 2)[1])
        except (ValueError, IndexError):
            novo_valor = 20
        if novo_valor < 0:
            novo_valor = 0
        conn.execute(sql('UPDATE monetization_settings SET free_uses_limit = %s WHERE id = 1'), (novo_valor,))
        conn.commit()
        audit.admin('free_access_limit_alterado', chat_id=chat_id, payload={'novo_valor': novo_valor})
        settings = get_monetization_settings(conn)
        current = int(settings['free_uses_limit'] or 20)
        presets = [0, 5, 10, 15, 20, 30, 50]
        texto = (
            '\u2705 *Acessos Grátis*'
            '\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500'
            f'\n\nValor atualizado para *{current}* consultas grátis por usuário.'
            '\n\nEscolha um novo valor:'
        )
        keyboard = []
        row_btns = []
        for opt in presets:
            chk = '\u2705 ' if opt == current else ''
            lbl = f'{chk}{"\u221e" if opt == 0 else opt}'
            row_btns.append(InlineKeyboardButton(lbl, callback_data=f'painel:free_access_set:{opt}'))
            if len(row_btns) == 3:
                keyboard.append(row_btns)
                row_btns = []
        if row_btns:
            keyboard.append(row_btns)
        keyboard.append([InlineKeyboardButton('\U0001f519 Voltar ao Painel', callback_data='painel:back')])
        await query.edit_message_text(texto, parse_mode='Markdown',
                                      reply_markup=InlineKeyboardMarkup(keyboard))

    elif action == 'scan_interval':
        await query.answer()
        row = conn.execute(sql('SELECT scan_interval_minutes FROM app_settings WHERE id = 1')).fetchone()
        current = int(row['scan_interval_minutes'] or 60) if row else 60
        texto = (
            '\u23f1 *Intervalo entre rodadas*'
            '\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500'
            f'\n\nIntervalo atual: *{current} minutos*'
            '\n\nEscolha um novo intervalo (m\u00ednimo 60 min, incrementos de 30):'
        )
        options = [60, 90, 120, 150, 180, 240, 360]
        keyboard = []
        row_buttons = []
        for opt in options:
            chk = '\u2705 ' if opt == current else ''
            lbl = f'{chk}{opt} min'
            row_buttons.append(InlineKeyboardButton(lbl, callback_data=f'painel:scan_interval_set:{opt}'))
            if len(row_buttons) == 3:
                keyboard.append(row_buttons)
                row_buttons = []
        if row_buttons:
            keyboard.append(row_buttons)
        keyboard.append([InlineKeyboardButton('\U0001f519 Voltar ao Painel', callback_data='painel:back')])
        await query.edit_message_text(texto, parse_mode='Markdown',
                                      reply_markup=InlineKeyboardMarkup(keyboard))

    elif action.startswith('scan_interval_set:'):
        await query.answer()
        try:
            novo_valor = int(action.split(':', 2)[1])
        except (ValueError, IndexError):
            novo_valor = 60
        novo_valor = max(60, novo_valor - (novo_valor % 30))
        conn.execute(
            sql("UPDATE app_settings SET scan_interval_minutes = %s, updated_at = NOW() WHERE id = 1"),
            (novo_valor,),
        )
        conn.commit()
        audit.admin('scan_interval_alterado', chat_id=chat_id, payload={'novo_valor': novo_valor})
        row = conn.execute(sql('SELECT scan_interval_minutes FROM app_settings WHERE id = 1')).fetchone()
        current = int(row['scan_interval_minutes'] or 60) if row else 60
        texto = (
            '\u2705 *Intervalo entre rodadas*'
            '\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500'
            f'\n\nIntervalo atualizado para *{current} minutos*'
            '\n\nEscolha um novo intervalo (m\u00ednimo 60 min, incrementos de 30):'
        )
        options = [60, 90, 120, 150, 180, 240, 360]
        keyboard = []
        row_buttons = []
        for opt in options:
            chk = '\u2705 ' if opt == current else ''
            lbl = f'{chk}{opt} min'
            row_buttons.append(InlineKeyboardButton(lbl, callback_data=f'painel:scan_interval_set:{opt}'))
            if len(row_buttons) == 3:
                keyboard.append(row_buttons)
                row_buttons = []
        if row_buttons:
            keyboard.append(row_buttons)
        keyboard.append([InlineKeyboardButton('\U0001f519 Voltar ao Painel', callback_data='painel:back')])
        await query.edit_message_text(texto, parse_mode='Markdown',
                                      reply_markup=InlineKeyboardMarkup(keyboard))

    elif action == 'restart_service':
        await query.answer()
        running_sched = conn.execute(sql("SELECT COUNT(*) AS c FROM scan_jobs WHERE status = 'running' AND job_type != 'manual_now'")) .fetchone()
        pending_sched = conn.execute(sql("SELECT COUNT(*) AS c FROM scan_jobs WHERE status = 'pending' AND job_type != 'manual_now'")) .fetchone()
        running_count = int((running_sched['c'] if isinstance(running_sched, dict) else running_sched[0]) or 0)
        pending_count = int((pending_sched['c'] if isinstance(pending_sched, dict) else pending_sched[0]) or 0)
        if running_count or pending_count:
            warning = (
                '⚠️ *Há consultas agendadas em andamento*\n\n'
                f'• Executando agora: *{running_count}*\n'
                f'• Na fila: *{pending_count}*\n\n'
                'Se reiniciar agora, essas execuções podem ser interrompidas. Deseja continuar mesmo assim?'
            )
        else:
            warning = '🔄 *Confirmar reinício do serviço%s*\n\nIsso vai executar o comando de reinício configurado para o bot.'
        await query.edit_message_text(
            warning,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('✅ Confirmar reinício', callback_data='painel:restart_service_confirm')],
                [InlineKeyboardButton('❌ Cancelar', callback_data='painel:back')],
            ]),
        )

    elif action == 'restart_service_confirm':
        ok, message, should_exit = trigger_service_restart()
        if ok:
            try:
                await query.answer('Reiniciando serviço...')
            except Exception:
                pass
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
        else:
            try:
                await query.answer('Falha ao reiniciar.', show_alert=True)
            except Exception:
                pass
            await query.edit_message_text(
                f'⚠️ {message}',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Voltar ao Painel', callback_data='painel:back')]]),
            )
        if should_exit:
            raise SystemExit(0)

    elif action.startswith('broadcast_confirm'):
        # Ação de confirmar envio do broadcast
        await query.answer()
        context.user_data.pop('awaiting_admin_broadcast', None)
        text = (context.user_data.pop('admin_broadcast_text', None) or '').strip()
        # Extrair texto da callback data se não estava no user_data
        if not text:
            parts = action.split(':', 2)
            if len(parts) == 3 and parts[2]:
                try:
                    text = base64.urlsafe_b64decode(parts[2].encode('ascii')).decode('utf-8').strip()
                except Exception:
                    pass
        if not text:
            logger.warning('broadcast_confirm sem texto | chat_id=%s', chat_id)
            await query.message.reply_text('⚠️ Mensagem não encontrada para envio. Gere a confirmação novamente.')
            return
        settings = get_monetization_settings(conn)
        maintenance_on = is_maintenance_mode(conn)
        show_result_type_filters = should_show_result_type_filters(conn)
        admin_unread_support, _ = get_support_badges(conn, chat_id, admin=True)
        rows = conn.execute(sql("SELECT chat_id FROM bot_users WHERE chat_id IS NOT NULL AND blocked = 0")).fetchall()
        sent = 0
        failed = 0
        bot_for_broadcast = context.bot
        if LEGACY_BROADCAST_TOKEN:
            bot_for_broadcast = Bot(token=LEGACY_BROADCAST_TOKEN)
        for row in rows:
            target_chat_id = str(row['chat_id'])
            try:
                await bot_for_broadcast.send_message(chat_id=target_chat_id, text=text)
                sent += 1
            except Exception as exc:
                failed += 1
                logger.warning('broadcast falhou | target_chat_id=%s | erro=%s', target_chat_id, exc)
        origem = 'bot antigo' if LEGACY_BROADCAST_TOKEN else 'bot atual'
        logger.info('broadcast concluido | chat_id=%s | origem=%s | sent=%s | failed=%s', chat_id, origem, sent, failed)
        try:
            await query.edit_message_text(
                f'📣 Envio concluído via {origem}. Sucesso: {sent} | Falhas: {failed}',
            )
        except Exception:
            pass
        await query.message.reply_text(
            f'📣 Envio concluído via {origem}. Sucesso: {sent} | Falhas: {failed}',
            reply_markup=admin_panel_markup(settings, maintenance_on, show_result_type_filters, admin_unread_support),
        )
        return

    elif action == 'back':
        await query.answer()
        # Limpa estado do broadcast se estava pendente
        context.user_data.pop('awaiting_admin_broadcast', None)
        context.user_data.pop('admin_broadcast_text', None)
        settings = get_monetization_settings(conn)
        maintenance_on = is_maintenance_mode(conn)
        show_result_type_filters = should_show_result_type_filters(conn)
        texto = (
            '🛠 *Painel Administrativo*\n\n'
            f"🧪 Modo teste: {'ATIVADO ✅' if int(settings['test_mode']) == 1 else 'DESATIVADO ❌'}\n"
            f"🌐 Cobrança geral: {'ATIVA ✅' if int(settings['charge_global']) == 1 else 'DESATIVADA ❌'}\n"
            f"👤 Cobrança só admin: {'ATIVA ✅' if int(settings['charge_admin_only']) == 1 else 'DESATIVADA ❌'}\n"
            f"🔧 Manutenção: {'ATIVA ✅' if maintenance_on else 'DESATIVADA ❌'}\n"
            f"🎛 Exibir filtros Companhia/Agências: {'SIM ✅' if show_result_type_filters else 'NÃO ❌'}"
        )
        await query.edit_message_text(texto, parse_mode='Markdown', reply_markup=admin_panel_markup(settings, maintenance_on, show_result_type_filters, get_support_badges(conn, chat_id, admin=True)[0]))

    conn.close()
    await query.answer()

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    try:
        conn = get_db()
    except DatabaseRateLimitError:
        await update.message.reply_text(db_overload_message())
        return
    if not _check_maintenance(conn, chat_id, 'menu'):
        conn.close()
        await update.message.reply_text('🔧 Em manutenção, aguarde um instante.', reply_markup=main_menu_markup())
        return
    msg = require_confirmation(conn, chat_id)

    if msg:
        conn.close()
        await update.message.reply_text(msg, reply_markup=confirmation_markup_for_message(msg))
        return

    ensure_user_access(conn, chat_id)
    ensure_owner_test_access(conn)
    access = ensure_user_access(conn, chat_id)
    should_charge = should_charge_user(conn, chat_id, access)

    if should_charge:
        expires_at = (access['expires_at'] or '').strip()
        if access['status'] == 'active' and expires_at:
            try:
                if datetime.fromisoformat(expires_at) < now_local():
                    conn.execute(sql("UPDATE user_access SET status = 'expired', updated_at = NOW() WHERE chat_id = %s"), (chat_id,))
                    conn.commit()
                    access = ensure_user_access(conn, chat_id)
            except ValueError:
                pass

    row = get_bot_user_by_chat(conn, chat_id)
    cur = conn.execute(sql('SELECT COUNT(*) FROM user_routes WHERE user_id = %s AND active = 1'), (row['user_id'],))
    count_row = cur.fetchone()
    routes_count = count_row[0] if not isinstance(count_row, dict) else next(iter(count_row.values()))
    conn.close()

    msg_text = get_panel_text(chat_id)

    await update.message.reply_text(
        msg_text,
        parse_mode='Markdown',
        reply_markup=full_menu_markup(chat_id),
    )


async def ajuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    conn = get_db()
    try:
        settings = get_monetization_settings(conn)
        show_monetization = bool(int(settings['charge_global']) == 1)
    finally:
        conn.close()
    await update.message.reply_text(
        'ℹ️ *Ajuda rápida*\n────────────────────────\n\n'
        '*Primeiro uso*\n'
        '1. Cadastre uma rota\n'
        '2. Se quiser, ajuste o filtro de preço\n'
        '3. Aguarde as notificações automáticas\n'
        '4. Quando quiser, rode uma consulta manual\n\n'
        '*Comandos principais*\n'
        '/minhasrotas — gerenciar suas rotas\n'
        '/agora — gerar consulta manual\n'
        '/limite — ajustar filtro de preço\n'
        '/manual — ver dúvidas frequentes\n'
        '/start — abrir o menu',
        parse_mode='Markdown',
        reply_markup=manual_topics_markup(show_monetization),
    )


async def minhas_rotas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    conn = get_db()
    msg = require_confirmation(conn, chat_id)
    if msg:
        conn.close()
        await update.message.reply_text(msg, reply_markup=confirmation_markup_for_message(msg))
        return
    if should_block_paid_action(conn, chat_id):
        texto = choose_plan_text(conn, chat_id)
        conn.close()
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=user_plan_markup())
        return

    user_id = get_user_id_by_chat(conn, chat_id)
    rows = conn.execute(
        sql('''
        SELECT id, origin, destination, outbound_date, inbound_date, active
        FROM user_routes
        WHERE user_id = %s AND active = 1
        ORDER BY outbound_date, origin, destination
        '''),
        (user_id,),
    ).fetchall()
    setting = get_user_settings(conn, user_id)
    conn.close()

    limite = setting['max_price'] if setting else None
    limite_txt = 'Sem limite' if limite is None else format_money_br(float(limite))
    if not rows:
        await update.message.reply_text(
            '\n📋 *Minhas rotas*\n────────────────────────\n\n'
            f'Você ainda não tem rotas ativas.\n'
            f'💰 Limite atual: *{limite_txt}*',
            parse_mode='Markdown',
            reply_markup=rotas_management_markup([]),
        )
        return

    linhas = [
        '',
        '📋 *Minhas rotas*',
        '────────────────────────',
        '',
        f'💰 *Limite:* R$ {limite_txt}',
        f'🧭 *Total:* {len(rows)} rota{"s" if len(rows) != 1 else ""}',
        '',
    ]
    for row in rows:
        origem = airport_label(row['origin'])
        destino = airport_label(row['destination'])
        linhas.append(f'🛫 {origem} → {destino}')
        data_text = f'📅 {format_date_br(row["outbound_date"])}'
        if row['inbound_date']:
            data_text += f' → {format_date_br(row["inbound_date"])}'
        linhas.append(data_text)

    await update.message.reply_text('\n'.join(linhas), parse_mode='Markdown', reply_markup=rotas_management_markup(rows))


async def fontes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    conn = get_db()
    msg = require_confirmation(conn, chat_id)
    if msg:
        conn.close()
        await update.message.reply_text(msg, reply_markup=confirmation_markup_for_message(msg))
        return
    if should_block_paid_action(conn, chat_id):
        texto = choose_plan_text(conn, chat_id)
        conn.close()
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=user_plan_markup())
        return
    user_id = get_user_id_by_chat(conn, chat_id)
    setting = get_user_settings(conn, user_id)
    conn.close()

    await update.message.reply_text(
        '🔎 *Fontes de consultas*\n\nAs consultas são realizadas no Google Voos.',
        parse_mode='Markdown',
        reply_markup=sources_menu_markup(bool(setting['enable_google_flights'])),
    )


async def sources_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()


def _airport_search_prompt(prefix: str) -> str:
    label = 'origem' if prefix == 'origem' else 'destino'
    return (
        f'🔎 Digite a *{label}* por código, cidade ou aeroporto.\n'
        'Exemplos: `PVH`, `Miami`, `Londres`, `Guarulhos`.'
    )


async def addrota_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    conn = get_db()
    msg = require_confirmation(conn, chat_id)
    if msg:
        conn.close()
        await update.message.reply_text(msg, reply_markup=confirmation_markup_for_message(msg))
        return ConversationHandler.END
    if should_block_paid_action(conn, chat_id):
        texto = choose_plan_text(conn, chat_id)
        conn.close()
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=user_plan_markup())
        return ConversationHandler.END

    user_id = get_user_id_by_chat(conn, chat_id)
    total_rotas = _fetchcount(conn.execute(
        sql('SELECT COUNT(*) FROM user_routes WHERE user_id = %s AND active = 1'),
        (user_id,),
    ).fetchone())
    max_routes_default = get_max_routes_default(conn)
    admin = is_admin_chat(conn, chat_id)
    conn.close()

    if (not admin) and total_rotas >= max_routes_default:
        await update.message.reply_text(f'⚠️ Você atingiu o limite de {max_routes_default} rotas ativas.')
        return ConversationHandler.END

    context.user_data.clear()
    context.user_data['airport_stage'] = 'origem'
    await update.message.reply_text(
        '\n➕ *Nova rota*\n────────────────────────\n\n🔎 *Buscar aeroporto de origem*\nResponda esta mensagem com a *origem* por código, cidade ou aeroporto.\n\nExemplos: `PVH`, `Miami`, `Guarulhos`, `Lisboa`.\n\nSe quiser sair, use o botão abaixo.',
        parse_mode='Markdown',
        reply_markup=force_reply_markup('Ex.: PVH ou Miami'),
    )
    return ASK_ORIGIN


async def addrota_origin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'airport_stage' not in context.user_data:
        logger.info('[addrota_origin] ignorando mensagem sem conversa ativa | chat_id=%s', update.effective_chat.id)
        return ConversationHandler.END
    conn = get_db()
    try:
        matches = search_airports(conn, update.message.text, limit=8)
    finally:
        conn.close()
    context.user_data['airport_stage'] = 'origem'
    if not matches:
        await update.message.reply_text(
            '⚠️ Origem não encontrada.\n\nTente digitar o código IATA, a cidade, o nome do aeroporto ou até o estado.\nExemplos: `PVH`, `Porto Velho`, `Rondônia`, `Guarulhos`, `Florida`.\n\n✍️ Responda esta mensagem com a origem para tentar novamente.',
            parse_mode='Markdown',
            reply_markup=force_reply_markup('Ex.: PVH ou Porto Velho'),
        )
        return ASK_ORIGIN
    await update.message.reply_text(
        f"🔎 Encontrei {len(matches)} opção(ões) para *origem*. Toque na correta abaixo:",
        parse_mode='Markdown',
        reply_markup=airport_search_results_markup('origem', matches, update.message.text),
    )
    return ASK_ORIGIN


async def addrota_destination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'airport_stage' not in context.user_data or context.user_data.get('airport_stage') != 'destino':
        logger.info('[addrota_destination] ignorando mensagem sem conversa ativa | chat_id=%s', update.effective_chat.id)
        return ConversationHandler.END
    conn = get_db()
    try:
        matches = search_airports(conn, update.message.text, limit=8)
    finally:
        conn.close()
    context.user_data['airport_stage'] = 'destino'
    if not matches:
        await update.message.reply_text(
            '⚠️ Destino não encontrado.\n\nTente digitar o código IATA, a cidade, o nome do aeroporto ou até o estado.\nExemplos: `GRU`, `São Paulo`, `SP`, `Lisboa`, `Florida`.\n\n✍️ Responda esta mensagem com o destino para tentar novamente.',
            parse_mode='Markdown',
            reply_markup=force_reply_markup('Ex.: GRU ou Lisboa'),
        )
        return ASK_DESTINATION
    await update.message.reply_text(
        f"🔎 Encontrei {len(matches)} opção(ões) para *destino*. Toque na correta abaixo:",
        parse_mode='Markdown',
        reply_markup=airport_search_results_markup('destino', matches, update.message.text),
    )
    return ASK_DESTINATION


async def addrota_outbound(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Só processa se realmente houver um cadastro de rota em andamento
    if 'airport_stage' not in context.user_data and 'origin' not in context.user_data:
        logger.info('[addrota_outbound] ignorando mensagem sem conversa ativa | chat_id=%s', update.effective_chat.id)
        clear_pending_input_state(context)
        return ConversationHandler.END
    text_lower = update.message.text.strip().lower()
    
    # Pular etapa de data
    if text_lower in ('pular', 'pule', 'nao', 'não', 'skip', '0', 'qualquer', 'qualquer data'):
        context.user_data['outbound_date'] = ''
        return await _save_route_with_inbound(update, context, '')
    
    try:
        dt_str = normalize_date(update.message.text)
        dt_obj = datetime.strptime(dt_str, '%Y-%m-%d').date()
        days_diff = (dt_obj - datetime.now().date()).days
        
        if days_diff > 365:
            await update.message.reply_text(
                f'⚠️ *Limite de 1 ano excedido.*\n\nVocê informou uma data para daqui a {days_diff} dias.\nO sistema permite o monitoramento de passagens com no máximo *365 dias* de antecedência.\n\n✍️ Por favor, informe uma data mais próxima.',
                parse_mode='Markdown',
                reply_markup=force_reply_markup('Ex.: 25/12/2026'),
            )
            return ASK_OUTBOUND
            
        context.user_data['outbound_date'] = dt_str
    except ValueError:
        await update.message.reply_text(
            '⚠️ Data inválida.\n\nDigite uma data ou responda "pular" para ignorar.\n\nFormatos aceitos:\n`25/12/2026` `25-12-2026` `2026-12-25`\n`25122026` `25 dez 2026` `25 dezembro 2026`',
            parse_mode='Markdown',
            reply_markup=force_reply_markup('Ex.: 25/12/2026 ou "pular"'),
        )
        return ASK_OUTBOUND
    return await _save_route_with_inbound(update, context, '')



async def _save_route_with_inbound(update: Update, context: ContextTypes.DEFAULT_TYPE, inbound_date: str):
    chat_id = str(update.effective_chat.id)
    msg_target = update.message or (update.callback_query.message if update.callback_query else None)
    if msg_target is None:
        clear_pending_input_state(context)
        return ConversationHandler.END
    conn = get_db()

    msg = require_confirmation(conn, chat_id)
    if msg:
        conn.close()
        await msg_target.reply_text(msg, reply_markup=confirmation_markup_for_message(msg))
        return ConversationHandler.END
    if should_block_paid_action(conn, chat_id):
        texto = choose_plan_text(conn, chat_id)
        conn.close()
        await msg_target.reply_text(texto, parse_mode='Markdown', reply_markup=user_plan_markup())
        return ConversationHandler.END

    user_id = get_user_id_by_chat(conn, chat_id)
    conn.execute(
        sql('''
        INSERT INTO user_routes (user_id, origin, destination, outbound_date, inbound_date, active, created_at)
        VALUES (%s, %s, %s, %s, %s, 1, NOW())
        '''),
        (
            user_id,
            context.user_data['origin'],
            context.user_data['destination'],
            context.user_data['outbound_date'],
            inbound_date,
        ),
    )
    conn.commit()

    audit.user_action("rota_salva", chat_id=chat_id, user_id=user_id,
                      payload={"origin": context.user_data['origin'],
                               "destination": context.user_data['destination'],
                               "outbound_date": context.user_data['outbound_date'],
                               "inbound_date": inbound_date})
    conn.close()

    await msg_target.reply_text(
        f"✅ *Rota cadastrada*\n{airport_label(context.user_data['origin'])} → {airport_label(context.user_data['destination'])} | {format_date_br(context.user_data['outbound_date'])}" +
        (f" | {format_date_br(inbound_date)}" if inbound_date else ''),
        parse_mode='Markdown',
    )
    context.user_data.clear()
    # Mostra a lista de rotas atualizada
    fake_update = Update(update.update_id, message=msg_target)
    await minhas_rotas(fake_update, context)
    return ConversationHandler.END


async def addrota_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    clear_pending_input_state(context)
    chat_id = str(query.message.chat.id)
    await query.edit_message_text('❌ Cadastro de rota cancelado.')
    await query.message.reply_text('✅ Cancelado. Ignore a resposta anterior, se ela ainda aparecer aberta no Telegram.', reply_markup=ReplyKeyboardRemove())
    fake_update = Update(update.update_id, message=query.message)
    await minhas_rotas(fake_update, context)
    return ConversationHandler.END


async def aeroporto_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    action, code = query.data.split(':', 1)

    if code == 'search':
        await query.answer()
        context.user_data['airport_stage'] = action
        await query.edit_message_text(
            'Escolha a origem:' if action == 'origem' else 'Escolha o destino:',
            reply_markup=airport_keyboard(action, options=[], include_search_hint=False),
        )
        await query.message.reply_text(
            _airport_search_prompt(action),
            parse_mode='Markdown',
            reply_markup=force_reply_markup('Ex.: GRU, Miami, Lisboa'),
        )
        return ASK_ORIGIN if action == 'origem' else ASK_DESTINATION

    if action == 'origem':
        await query.answer('Origem selecionada. Agora informe o destino.', show_alert=True)
        context.user_data['origin'] = code
        context.user_data['airport_stage'] = 'destino'
        await query.edit_message_text(
            f"✅ Origem: {airport_label(code)}",
            parse_mode='Markdown',
            reply_markup=cancel_markup('addrota:cancel', '❌ Cancelar cadastro de rota'),
        )
        await query.message.reply_text(
            '🔎 *Buscar aeroporto de destino*\nResponda esta mensagem com o *destino* por código, cidade ou aeroporto.\n\nExemplos: `GRU`, `Miami`, `Lisboa`, `Guarulhos`.',
            parse_mode='Markdown',
            reply_markup=force_reply_markup('Ex.: GRU ou Lisboa'),
        )
        return ASK_DESTINATION

    if action == 'destino':
        await query.answer('Destino selecionado. Agora informe a data.', show_alert=True)
        context.user_data['destination'] = code
        await query.edit_message_text(
            f"✅ Destino: {airport_label(code)}",
            parse_mode='Markdown',
            reply_markup=cancel_markup('addrota:cancel', '❌ Cancelar cadastro de rota'),
        )
        await query.message.reply_text(
            '📅 *Data de ida*\nFormatos aceitos: 25/12/2026, 25-12-2026, 2026-12-25, 25122026, 25 dez 2026 ou 25 dezembro 2026.\n\n✍️ Responda esta mensagem com a data de ida.',
            parse_mode='Markdown',
            reply_markup=force_reply_markup('Ex.: 25/12/2026'),
        )
        return ASK_OUTBOUND


    return ConversationHandler.END


async def removerrota(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    conn = get_db()
    msg = require_confirmation(conn, chat_id)
    if msg:
        conn.close()
        await update.message.reply_text(msg, reply_markup=confirmation_markup_for_message(msg))
        return
    if should_block_paid_action(conn, chat_id):
        texto = choose_plan_text(conn, chat_id)
        conn.close()
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=user_plan_markup())
        return

    user_id = get_user_id_by_chat(conn, chat_id)
    rows = conn.execute(
        sql('''
        SELECT id, origin, destination, outbound_date, inbound_date
        FROM user_routes
        WHERE user_id = %s AND active = 1
        ORDER BY outbound_date, origin, destination
        '''),
        (user_id,),
    ).fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text('\n➖ *Remover rota*\n────────────────────────\n\nVocê não tem rotas ativas para remover.', parse_mode='Markdown', reply_markup=main_menu_markup())
        return

    await update.message.reply_text(
        '\n➖ *Remover rota*\n────────────────────────\n\nEscolha a rota que deseja remover:',
        parse_mode='Markdown',
        reply_markup=removerrota_list_markup(rows),
    )


async def removerrota_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = str(query.message.chat.id)
    route_id_str = query.data.split(':', 1)[1]
    
    conn = get_db()
    user_id = get_user_id_by_chat(conn, chat_id)

    if route_id_str == 'cancel_list':
        conn.close()
        await query.edit_message_text('❌ Remoção cancelada.')
        fake_update = Update(update.update_id, message=query.message)
        await minhas_rotas(fake_update, context)
        return
    
    if route_id_str.startswith('confirm_'):
        route_id = int(route_id_str.split('_')[1])
        row = conn.execute(
            sql('SELECT origin, destination, outbound_date, inbound_date FROM user_routes WHERE id = %s AND user_id = %s'),
            (route_id, user_id),
        ).fetchone()
        if not row:
            conn.close()
            await query.edit_message_text('Rota não encontrada ou já removida.')
            return

        conn.execute(
            sql('DELETE FROM user_routes WHERE id = %s AND user_id = %s'),
            (route_id, user_id),
        )
        conn.commit()

        audit.user_action("rota_removida", chat_id=chat_id, user_id=user_id,
                          payload={"route_id": route_id,
                                   "origin": row['origin'],
                                   "destination": row['destination'],
                                   "outbound_date": row['outbound_date']})
        conn.close()

        texto = f"Rota removida com sucesso: {row['origin']}→{row['destination']} | {format_date_br(row['outbound_date'])}"
        if row['inbound_date']:
            texto += f" | {format_date_br(row['inbound_date'])}"
        await query.edit_message_text('🗑️ ' + texto)
        conn2 = get_db()
        remaining_rows = conn2.execute(
            sql('''
            SELECT id, origin, destination, outbound_date, inbound_date
            FROM user_routes
            WHERE user_id = %s AND active = 1
            ORDER BY outbound_date, origin, destination
            '''),
            (user_id,),
        ).fetchall()
        conn2.close()

        if remaining_rows:
            await query.message.reply_text(
                '🗑️ Escolha a próxima rota que deseja remover:',
                reply_markup=removerrota_list_markup(remaining_rows),
            )
        else:
            await query.message.reply_text(
                '✅ Não há mais rotas ativas para remover.',
            )
            fake_update = Update(update.update_id, message=query.message)
            await minhas_rotas(fake_update, context)
        return
        
    elif route_id_str.startswith('cancel_'):
        conn.close()
        await query.edit_message_text('❌ Remoção cancelada.')
        await query.message.reply_text(get_panel_text(str(query.message.chat.id)), parse_mode='HTML', reply_markup=full_menu_markup(chat_id))
        return

    route_id = int(route_id_str)

    msg = require_confirmation(conn, chat_id)
    if msg:
        conn.close()
        await query.edit_message_text(msg)
        return
    if should_block_paid_action(conn, chat_id):
        texto = choose_plan_text(conn, chat_id)
        conn.close()
        await query.edit_message_text(texto, parse_mode='Markdown', reply_markup=user_plan_markup())
        return

    row = conn.execute(
        sql('SELECT origin, destination, outbound_date, inbound_date FROM user_routes WHERE id = %s AND user_id = %s'),
        (route_id, user_id),
    ).fetchone()
    conn.close()
    
    if not row:
        await query.edit_message_text('Rota não encontrada ou já removida.')
        return

    texto = f"{row['origin']}→{row['destination']} | {format_date_br(row['outbound_date'])}"
    if row['inbound_date']:
        texto += f" | {format_date_br(row['inbound_date'])}"
        
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('⚠️ Sim, quero remover', callback_data=f"removerrota:confirm_{route_id}")],
        [InlineKeyboardButton('❌ Não, cancelar', callback_data=f"removerrota:cancel_{route_id}")],
    ])
    
    await query.edit_message_text(f"Tem certeza que deseja remover esta rota%s\n\n{texto}", reply_markup=keyboard)



def _agora_lockfree_conn():
    """Cria conexão pymysql autocommit para o fluxo 'agora', evitando lock contention com workers."""
    _parsed = urlparse(os.environ.get('MYSQL_URL', ''))
    _c = pymysql.connect(
        host=_parsed.hostname or 'localhost', port=_parsed.port or 3306,
        user=_parsed.username or 'vooindobot', password=_parsed.password or '',
        database=_parsed.path.lstrip('/') or 'vooindo',
        autocommit=True, connect_timeout=5,
        cursorclass=pymysql.cursors.DictCursor,
    )
    _c.cursor().execute("SET SESSION lock_wait_timeout = 5")
    _c.cursor().execute("SET SESSION innodb_lock_wait_timeout = 5")
    return _c


def _agora_criar_job(sc, chat_id: str) -> tuple[int, bool]:
    """Cria job manual usando a conexão lockfree. Retorna (user_id, replaced_existing)."""
    scursor = sc.cursor()

    # Maintenance check (query direta para evitar dependência de API conn)
    scursor.execute("SELECT maintenance_mode FROM monetization_settings WHERE id = 1")
    _row_m = scursor.fetchone()
    _maintenance = _row_m and int(_row_m.get('maintenance_mode', 0)) == 1 if _row_m else False
    if _maintenance:
        scursor.execute("SELECT chat_id FROM bot_users WHERE chat_id = %s AND user_id IN (SELECT user_id FROM bot_settings WHERE id = 1) LIMIT 1", (chat_id,))
        _exempt = scursor.fetchone() is not None
        if not _exempt:
            raise RuntimeError('maintenance')

    scursor.execute("SELECT user_id FROM bot_users WHERE chat_id = %s", (chat_id,))
    _row = scursor.fetchone()
    if not _row:
        raise RuntimeError('user_not_found')
    user_id = int(_row['user_id'])

    scursor.execute("SELECT 1 FROM user_routes WHERE user_id = %s AND active = 1 LIMIT 1", (user_id,))
    if not scursor.fetchone():
        raise RuntimeError('no_routes')

    import time as _time

    # Cancela jobs pendentes com retry em caso de lock wait
    replaced_existing = False
    for _attempt in range(10):
        try:
            scursor.execute(
                "UPDATE scan_jobs SET status = 'error', finished_at = NOW(), error_message = 'cancelled_by_new_request' "
                "WHERE user_id = %s AND status IN ('pending', 'running')",
                (user_id,),
            )
            replaced_existing = scursor.rowcount > 0
            break
        except pymysql.err.OperationalError as _le:
            if _le.args[0] == 1205:
                _time.sleep(0.3 * (_attempt + 1))
                scursor = sc.cursor()
                continue
            raise

    # INSERT job manual com retry
    for _attempt in range(10):
        try:
            scursor.execute(
                "INSERT INTO scan_jobs (user_id, chat_id, job_type, status, payload, cost_score) VALUES (%s, %s, 'manual_now', 'pending', %s, 0)",
                (user_id, chat_id, '{}'),
            )
            break
        except pymysql.err.OperationalError as _le:
            if _le.args[0] == 1205:
                _time.sleep(0.3 * (_attempt + 1))
                scursor = sc.cursor()
                continue
            raise

    audit.user_action("cmd_agora", chat_id=chat_id, user_id=user_id,
                      payload={"trigger": "manual_now"})
    return user_id, replaced_existing


async def agora(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_pending_input_state(context)
    chat_id = str(update.effective_chat.id)

    # Responde o callback imediatamente para o Telegram não expirar
    try:
        query = update.callback_query
        if query:
            await query.answer()
    except Exception:
        pass

    # Cria o job em thread separada (pymysql sync) para nao travar event loop
    def _sync_run():
        try:
            sc = _agora_lockfree_conn()
            try:
                uid, replaced = _agora_criar_job(sc, chat_id)
            except RuntimeError as _re:
                sc.close()
                return ('runtime', str(_re))
            except Exception as _exc:
                logger.error('[agora] Erro sync: %s', _exc)
                sc.close()
                return ('error', None)
            sc.close()
            return ('ok', replaced)
        except Exception as _exc:
            logger.error('[agora] Erro sync: %s', _exc)
            return ('error', None)

    try:
        result = await asyncio.get_event_loop().run_in_executor(None, _sync_run)
    except Exception as _exc:
        logger.error('[agora] run_in_executor erro: %s', _exc)
        await update.message.reply_text('Erro ao criar consulta. Tente novamente.')
        return

    status, data = result
    clear_pending_input_state(context)

    if status == 'runtime':
        _msg_map = {
            'maintenance': '🔧 Em manutenção, aguarde um instante.',
            'no_routes': '\n🖼️ Consulta manual agora\n────────────────────────\n\nVocê não tem rotas ativas cadastradas.',
            'user_not_found': 'Erro ao criar consulta. Tente novamente.',
        }
        user_msg = _msg_map.get(data, 'Erro ao criar consulta. Tente novamente.')
        if 'rotas ativas' in user_msg:
            await update.message.reply_text(user_msg, reply_markup=main_menu_markup())
        else:
            await update.message.reply_text(user_msg)
    elif status == 'error':
        await update.message.reply_text('Erro ao criar consulta. Tente novamente.')
    else:
        replaced_existing = bool(data)
        if replaced_existing:
            await update.message.reply_text('🔄 Consulta anterior cancelada. Já comecei a nova e vou te mandar o resultado assim que terminar.')
        else:
            await update.message.reply_text('🕒 Consulta recebida. Vou enviar o resultado aqui assim que terminar.') 


async def limite_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    conn = get_db()
    msg = require_confirmation(conn, chat_id)
    if msg:
        conn.close()
        await update.message.reply_text(msg, reply_markup=confirmation_markup_for_message(msg))
        return ConversationHandler.END
    if should_block_paid_action(conn, chat_id):
        texto = choose_plan_text(conn, chat_id)
        conn.close()
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=user_plan_markup())
        return ConversationHandler.END

    user_id = get_user_id_by_chat(conn, chat_id)
    setting = get_user_settings(conn, user_id)
    selected_airlines = parse_airline_filters(setting['airline_filters_json'])
    conn.close()
    await update.message.reply_text(
        build_filter_menu_text(setting['max_price'], selected_airlines, bool(setting['enable_google_flights'])),
        parse_mode='Markdown',
        reply_markup=filter_menu_markup(setting['max_price'], selected_airlines, bool(setting['enable_google_flights'])),
    )
    return ConversationHandler.END


async def limite_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data.pop('awaiting_limit_input', None)
    context.user_data.clear()

    chat_id = str(query.message.chat.id)
    conn = get_db()
    user_id = get_user_id_by_chat(conn, chat_id)
    setting = get_user_settings(conn, user_id)
    selected_airlines = parse_airline_filters(setting['airline_filters_json'])
    conn.close()

    await query.edit_message_text('❌ Ajuste de limite cancelado.')
    await query.message.reply_text('✅ Cancelado. Ignore a resposta anterior, se ela ainda aparecer aberta no Telegram.', reply_markup=ReplyKeyboardRemove())
    await query.message.reply_text(
        build_filter_menu_text(setting['max_price'], selected_airlines, bool(setting['enable_google_flights'])),
        parse_mode='Markdown',
        reply_markup=filter_menu_markup(setting['max_price'], selected_airlines, bool(setting['enable_google_flights'])),
    )
    return ConversationHandler.END


async def limite_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('awaiting_limit_input'):
        return ConversationHandler.END

    chat_id = str(update.effective_chat.id)
    texto = update.message.text.strip().replace(',', '.')
    if texto.lower() in {'sem limite', 'semlimite', 'qualquer valor', 'qualquer', 'todos'}:
        valor = None
    else:
        try:
            valor = float(texto)
        except ValueError:
            await update.message.reply_text('Valor inválido. Envie um número, ex: 1200. Se quiser sem limite, envie: sem limite')
            return ASK_LIMIT

    conn = get_db()
    msg = require_confirmation(conn, chat_id)
    if msg:
        conn.close()
        await update.message.reply_text(msg, reply_markup=confirmation_markup_for_message(msg))
        return ConversationHandler.END
    if should_block_paid_action(conn, chat_id):
        texto = choose_plan_text(conn, chat_id)
        conn.close()
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=user_plan_markup())
        return ConversationHandler.END

    user_id = get_user_id_by_chat(conn, chat_id)
    ensure_user_settings(conn, user_id)
    conn.execute(
        sql('''
        UPDATE bot_settings
        SET max_price = %s, updated_at = NOW()
        WHERE user_id = %s
        '''),
        (valor, user_id),
    )
    setting = get_user_settings(conn, user_id)
    selected_airlines = parse_airline_filters(setting['airline_filters_json'])
    conn.close()

    context.user_data.pop('awaiting_limit_input', None)
    await update.message.reply_text(
        '💰 Limite removido. Agora a busca aceita qualquer valor.' if valor is None else f'💰 Limite atualizado para R$ {valor:.2f}',
        reply_markup=full_menu_markup(chat_id),
    )
    await update.message.reply_text(
        build_filter_menu_text(valor, selected_airlines, bool(setting['enable_google_flights'])),
        parse_mode='Markdown',
        reply_markup=filter_menu_markup(valor, selected_airlines, bool(setting['enable_google_flights'])),
    )
    return ConversationHandler.END


async def filter_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = str(query.message.chat.id)
    data = query.data

    conn = get_db()
    msg = require_confirmation(conn, chat_id)
    if msg:
        conn.close()
        await query.edit_message_text(msg)
        return
    if should_block_paid_action(conn, chat_id):
        texto = choose_plan_text(conn, chat_id)
        conn.close()
        await query.edit_message_text(texto, parse_mode='Markdown', reply_markup=user_plan_markup())
        return

    user_id = get_user_id_by_chat(conn, chat_id)
    setting = get_user_settings(conn, user_id)
    selected_airlines = parse_airline_filters(setting['airline_filters_json'])

    if data == 'filter:edit_limit':
        conn.close()
        context.user_data['awaiting_limit_input'] = True
        await query.message.reply_text(
            'Qual o preço máximo por trecho%s Exemplo: 1200 ou 1200,50. Se quiser aceitar qualquer valor, envie: sem limite',
            reply_markup=cancel_markup('limite:cancel', '❌ Cancelar ajuste de limite'),
        )
        return ASK_LIMIT

    if data == 'filter:price_info':
        conn.close()
        await query.answer('Valor atual exibido só para leitura.', show_alert=False)
        return
    if data == 'filter:airlines_info':
        conn.close()
        await query.answer('Aqui você escolhe se quer aceitar qualquer companhia aérea e/ou agências.', show_alert=True)
        return
    if data == 'filter:sources_info' or data == 'filter:google_info':
        conn.close()
        return
    if data.startswith('filter:toggle_airline:'):
        airline_key = data.split(':')[-1]
        if airline_key in selected_airlines:
            selected_airlines[airline_key] = not bool(selected_airlines[airline_key])
            if not any(selected_airlines.values()):
                selected_airlines[airline_key] = True
                await query.answer('Pelo menos um tipo de resultado precisa ficar marcado.', show_alert=True)

    serialized_filters = serialize_airline_filters(selected_airlines)
    conn.execute(
        sql("UPDATE bot_settings SET airline_filters_json = %s, updated_at = NOW() WHERE user_id = %s"),
        (serialized_filters, user_id),
    )
    conn.commit()
    setting = get_user_settings(conn, user_id)
    current_airlines = parse_airline_filters(serialized_filters)
    conn.close()

    try:
        await query.edit_message_text(
            build_filter_menu_text(setting['max_price'], current_airlines, bool(setting['enable_google_flights'])),
            parse_mode='Markdown',
            reply_markup=filter_menu_markup(setting['max_price'], current_airlines, bool(setting['enable_google_flights'])),
        )
    except Exception:
        await query.message.reply_text(
            build_filter_menu_text(setting['max_price'], current_airlines, bool(setting['enable_google_flights'])),
            parse_mode='Markdown',
            reply_markup=filter_menu_markup(setting['max_price'], current_airlines, bool(setting['enable_google_flights'])),
        )


async def payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split(':')
    action = parts[1] if len(parts) > 1 else ''
    payment_id = parts[2] if len(parts) > 2 else None
    chat_id = str(query.message.chat.id)
    conn = get_db()
    try:
        if action == 'view' and payment_id:
            row = conn.execute(
                sql('SELECT mp_payment_id, plan_name, amount, status, created_at, approved_at FROM payments WHERE mp_payment_id = %s AND chat_id = %s'),
                (payment_id, chat_id)
            ).fetchone()
            if not row:
                await query.message.reply_text('Pagamento não encontrado.')
                return
            texto = (
                '💳 *Detalhes do pagamento*\n\n'
                f"ID: `{row['mp_payment_id']}`\n"
                f"Plano: {row['plan_name'] or '-'}\n"
                f"Valor: R$ {format_money_br(row['amount'])}\n"
                f"Status: {row['status']}\n"
                f"Criado em: {row['created_at'] or '-'}\n"
                f"Aprovado em: {row['approved_at'] or '-'}"
            )
            await query.message.reply_text(
                texto,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ Voltar aos pagamentos', callback_data='menu:pagamentos')]])
            )
        elif action == 'check' and payment_id:
            approved, info = apply_approved_payment(conn, payment_id)
            if approved:
                await query.message.reply_text(f'🎉 Pagamento aprovado! Acesso liberado até {info}.')
            else:
                await query.message.reply_text(f'⏳ Pagamento ainda não aprovado. Status atual: {info}')
        elif action == 'cancel' and payment_id:
            conn.execute(
                sql("UPDATE payments SET status = 'cancelled' WHERE mp_payment_id = %s AND chat_id = %s AND status = 'pending'"),
                (payment_id, chat_id)
            )
            conn.commit()
            texto = choose_plan_text(conn, chat_id)
            await query.edit_message_text(
                texto,
                parse_mode='Markdown',
                reply_markup=user_plan_markup()
            )
        elif action == 'changeplan':
            texto = choose_plan_text(conn, chat_id)
            await query.edit_message_text(
                texto,
                parse_mode='Markdown',
                reply_markup=user_plan_markup()
            )
    finally:
        conn.close()


async def alerts_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data or ''
    chat_id = str(query.message.chat.id)

    conn = get_db()
    msg = require_confirmation(conn, chat_id)
    conn.close()
    if msg:
        if 'suspensa' in msg.lower():
            await query.answer()
        else:
            await query.answer('Confirme seu cadastro para continuar.', show_alert=True)
        await query.message.reply_text(msg, reply_markup=confirmation_markup_for_message(msg))
        return ConversationHandler.END

    if data == 'menu:togglealerts':
        await query.answer()
        conn = get_db()
        user_id = get_user_id_by_chat(conn, chat_id)
        setting = get_user_settings(conn, user_id) if user_id else None
        conn.close()
        enabled = bool(int(setting['alerts_enabled'])) if setting else True
        action_label = 'desativar' if enabled else 'ativar'
        confirm_label = '🔕 Confirmar desativação dos alertas' if enabled else '🔔 Confirmar ativação dos alertas'
        cancel_label = '⬅️ Voltar ao menu'
        await query.edit_message_text(
            f"Tem certeza que deseja {action_label} os alertas automáticos%s\n\nVocê pode alterar isso depois no menu.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(confirm_label, callback_data=f"menu:confirmalerts:{0 if enabled else 1}")],
                [InlineKeyboardButton(cancel_label, callback_data='menu:back')],
            ])
        )
        return ConversationHandler.END

    if data.startswith('menu:confirmalerts:'):
        parts = data.split(':')
        new_value = int(parts[2]) if len(parts) > 2 else 1
        toast = '🔔 Ativando alertas...' if new_value == 1 else '🔕 Desativando alertas...'
        logger.info('[confirmalerts] início | chat_id=%s | query_data=%s | new_value=%s', chat_id, data, new_value)
        await query.answer(toast)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception as exc:
            logger.warning('[confirmalerts] não foi possível limpar teclado | chat_id=%s | erro=%s', chat_id, exc)
        conn = get_db()
        user_id = get_user_id_by_chat(conn, chat_id)
        logger.info('[confirmalerts] user lookup | chat_id=%s | user_id=%s', chat_id, user_id)
        access = ensure_user_access(conn, chat_id)
        if new_value == 1 and should_charge_user(conn, chat_id, access) and not is_active_access(access):
            texto = choose_plan_text(conn, chat_id)
            conn.close()
            logger.info('[confirmalerts] bloqueado por monetização | chat_id=%s', chat_id)
            await query.message.reply_text(texto, parse_mode='Markdown', reply_markup=user_plan_markup())
            return ConversationHandler.END
        if user_id:
            ensure_user_settings(conn, user_id)
            before_row = conn.execute(sql('SELECT alerts_enabled FROM bot_settings WHERE user_id = %s'), (user_id,)).fetchone()
            logger.info('[confirmalerts] antes do update | chat_id=%s | user_id=%s | alerts_enabled=%s', chat_id, user_id, before_row['alerts_enabled'] if before_row else None)
            conn.execute(
                sql("UPDATE bot_settings SET alerts_enabled = %s, updated_at = NOW() WHERE user_id = %s"),
                (new_value, user_id),
            )
            conn.commit()
            after_row = conn.execute(sql('SELECT alerts_enabled, updated_at FROM bot_settings WHERE user_id = %s'), (user_id,)).fetchone()
            logger.info('[confirmalerts] depois do update | chat_id=%s | user_id=%s | alerts_enabled=%s | updated_at=%s', chat_id, user_id, after_row['alerts_enabled'] if after_row else None, after_row['updated_at'] if after_row else None)
        else:
            logger.warning('[confirmalerts] user_id não encontrado | chat_id=%s', chat_id)
        conn.close()
        estado_atual = 'ATIVADOS ✅' if new_value == 1 else 'DESATIVADOS ❌'
        texto = (
            '🔔 Alertas automáticos ativados com sucesso.\n\nEstado atual: ATIVADOS ✅'
            if new_value == 1
            else '🔕 Alertas automáticos desativados com sucesso.\n\nEstado atual: DESATIVADOS ❌'
        )
        await query.message.reply_text(texto)
        await query.message.reply_text(
            get_panel_text(chat_id) + f"\n\n🔔 Estado atual dos alertas: {estado_atual}",
            parse_mode='HTML',
            reply_markup=full_menu_markup(chat_id),
        )
        return ConversationHandler.END

    if data == 'menu:back':
        logger.info('menu:back callback recebido | chat_id=%s | message_id=%s', chat_id, getattr(query.message, 'message_id', None))
        clear_pending_input_state(context)
        await query.answer('Voltando ao menu...')
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception as exc:
            logger.warning('menu:back falhou ao limpar reply markup | chat_id=%s | erro=%s', chat_id, exc)
        try:
            await query.message.reply_text(get_panel_text(str(query.message.chat.id)), parse_mode='HTML', reply_markup=full_menu_markup(chat_id))
            logger.info('menu:back respondeu com painel principal | chat_id=%s', chat_id)
        except Exception as exc:
            logger.exception('menu:back falhou ao responder painel | chat_id=%s | erro=%s', chat_id, exc)
            raise
        return ConversationHandler.END

    return ConversationHandler.END


async def manual_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ''
    topic = data.split(':', 1)[1] if ':' in data else 'primeiros_passos'
    conn = get_db()
    try:
        settings = get_monetization_settings(conn)
        show_monetization = bool(int(settings['charge_global']) == 1)
        if topic == 'consultas_gratis_status':
            text = charging_status_text(conn, str(query.message.chat.id))
        else:
            text = manual_topic_text(topic)
    finally:
        conn.close()
    await query.edit_message_text(
        text,
        parse_mode='Markdown',
        reply_markup=manual_topics_markup(show_monetization),
    )
    return ConversationHandler.END


_LIBERADAS_EM_MANUTENCAO = frozenset({'support', 'manual', 'back', 'pagamentos', 'fontes', 'togglealerts', 'clear_confirm', 'confirm'})


def _check_maintenance(conn, chat_id: str, action: str) -> bool:
    """Retorna True se o usuário NÃO está em manutenção ou está isento."""
    return not is_maintenance_mode(conn) or is_exempt_from_maintenance(conn, chat_id) or action in _LIBERADAS_EM_MANUTENCAO


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    action = query.data.split(':', 1)[1]
    chat_id = str(query.message.chat.id)
    logger.info('[menu_callback] Ação recebida | action=%s | chat_id=%s', action, chat_id)

    try:
        try:
            conn = get_db()
        except DatabaseRateLimitError:
            await query.answer('\u26a0\ufe0f Banco sobrecarregado, tente novamente.', show_alert=True)
            await query.message.reply_text(db_overload_message())
            return ConversationHandler.END
        if not _check_maintenance(conn, chat_id, action):
            conn.close()
            await query.answer('\U0001f527 Em manuten\u00e7\u00e3o, aguarde um instante.', show_alert=True)
            await query.message.reply_text('\U0001f527 Em manuten\u00e7\u00e3o, aguarde um instante.', reply_markup=main_menu_markup())
            return ConversationHandler.END
        msg = require_confirmation(conn, chat_id) if action != 'manual' else None
        if msg:
            blocked = 'suspensa' in msg.lower()
            if blocked:
                if action not in {'support', 'back'}:
                    conn.close()
                    await query.answer('\U0001f6ab Conta suspensa. Use "Fale conosco" para mais informa\u00e7\u00f5es.', show_alert=True)
                    await query.message.reply_text('\U0001f6ab Sua conta est\u00e1 suspensa.', reply_markup=main_menu_markup())
                    return ConversationHandler.END
            else:
                conn.close()
                await query.answer('Confirme seu cadastro para continuar.', show_alert=True)
                await query.message.reply_text(msg, reply_markup=confirmation_markup_for_message(msg))
                return ConversationHandler.END
        if action in {'addrota', 'minhasrotas', 'removerrota', 'limite', 'fontes', 'agora'} and should_block_paid_action(conn, chat_id):
            texto = choose_plan_text(conn, chat_id)
            audit.access("acesso_bloqueado", chat_id=chat_id, status="blocked",
                         payload={"acao": action})
            conn.close()
            await query.answer('Selecione um plano para continuar.', show_alert=True)
            await query.message.reply_text(texto.replace('*', ''), reply_markup=user_plan_markup())
            return ConversationHandler.END
        conn.close()

        if action == 'addrota':
            await query.answer('Digite a origem da rota.', show_alert=True)
            clear_pending_input_state(context)
            context.user_data['airport_stage'] = 'origem'
            await query.message.reply_text(
                '\n\U0001f449 Nova rota\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n\n\U0001f50e Buscar aeroporto de origem\nResponda esta mensagem com a origem por c\u00f3digo, cidade ou aeroporto.\n\nExemplos: PVH, Miami, Guarulhos, Lisboa.\n\nPara sair, cancele o cadastro.',
                reply_markup=force_reply_markup('Ex.: PVH, Miami, Guarulhos, Lisboa'),
            )
            await query.message.reply_text(
                'Se o campo fechar, toque em cancelar e comece novamente.',
                reply_markup=cancel_markup('addrota:cancel', '\u274c Cancelar cadastro de rota'),
            )
            return ASK_ORIGIN
        if action == 'minhasrotas':
            await query.answer()
            fake_update = Update(update.update_id, message=query.message)
            await minhas_rotas(fake_update, context)
        elif action == 'removerrota':
            await query.answer()
            fake_update = Update(update.update_id, message=query.message)
            await removerrota(fake_update, context)
        elif action == 'limite':
            conn = get_db()
            user_id = get_user_id_by_chat(conn, chat_id)
            setting = get_user_settings(conn, user_id)
            selected_airlines = parse_airline_filters(setting['airline_filters_json'])
            conn.close()
            await query.answer()
            clear_pending_input_state(context)
            await query.message.reply_text(
                build_filter_menu_text(setting['max_price'], selected_airlines, bool(setting['enable_google_flights'])).replace('*', ''),
                reply_markup=filter_menu_markup(setting['max_price'], selected_airlines, bool(setting['enable_google_flights'])),
            )
            return ConversationHandler.END
        elif action == 'fontes':
            await query.answer()
            fake_update = Update(update.update_id, message=query.message)
            await fontes(fake_update, context)
        elif action == 'agora':
            await query.answer()
            clear_pending_input_state(context)
            fake_update = Update(update.update_id, message=query.message)
            await agora(fake_update, context)
        elif action == 'manual':
            await query.answer()
            fake_update = Update(update.update_id, message=query.message)
            await manual(fake_update, context)
        elif action == 'pagamentos':
            await query.answer()
            conn = get_db()
            rows = conn.execute(
                sql('SELECT mp_payment_id, plan_name, amount, status, created_at FROM payments WHERE chat_id = %s AND NOT (status = %s AND datetime(created_at) < datetime(%s, %s)) ORDER BY created_at DESC LIMIT 15' % ("'pending'", "'now'", "'-24 hours'")),
                (chat_id,)
            ).fetchall()
            conn.close()
            if rows:
                texto = '\n\U0001f4b3 Meus pagamentos\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n\nSelecione um pagamento para ver detalhes ou atualizar.'
                await query.message.reply_text(texto, reply_markup=user_payments_markup(rows))
            else:
                await query.message.reply_text('\n\U0001f4b3 Meus pagamentos\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n\nVoc\u00ea ainda n\u00e3o tem pagamentos registrados.', reply_markup=full_menu_markup(chat_id))
        elif action == 'support':
            await query.answer()
            conn = get_db()
            row = get_bot_user_by_chat(conn, chat_id)
            blocked_user = bool(row and int(row['blocked'] or 0) == 1)
            conn.close()
            await query.message.reply_text('\U0001f4ac Escolha o tipo de mensagem que deseja enviar:', reply_markup=support_subjects_markup(blocked_user=blocked_user))
        elif action == 'adminsupport':
            conn = get_db()
            admin = is_admin_chat(conn, chat_id)
            if not admin:
                conn.close()
                await query.answer('N\u00e3o autorizado', show_alert=True)
                return ConversationHandler.END
            rows = conn.execute(
                sql("SELECT st.id, st.subject, st.blocked, bu.first_name, (SELECT COUNT(*) FROM support_messages sm WHERE sm.thread_id = st.id AND sm.sender_role = 'user' AND sm.is_read = 0) AS unread FROM support_threads st LEFT JOIN bot_users bu ON bu.user_id = st.user_id WHERE st.status = 'open' ORDER BY st.updated_at DESC LIMIT 20")
            ).fetchall()
            conn.close()
            await query.answer()
            await query.message.reply_text('\U0001f4e5 *Caixa de entrada do atendimento*', parse_mode='Markdown', reply_markup=list_support_conversations_markup(rows, admin=True))
        elif action == 'adminpainel':
            conn = get_db()
            admin = is_admin_chat(conn, chat_id)
            conn.close()
            if not admin:
                await query.answer('N\u00e3o autorizado', show_alert=True)
                await query.message.reply_text('\U0001f6ab Comando restrito a administradores.')
                await query.answer()
            fake_update = Update(update.update_id, message=query.message)
            await cmd_painel(fake_update, context)
        elif action == 'broadcast':
            conn = get_db()
            admin = is_admin_chat(conn, chat_id)
            conn.close()
            if not admin:
                await query.answer('N\u00e3o autorizado', show_alert=True)
                return ConversationHandler.END
            await query.answer()
            context.user_data['awaiting_admin_broadcast'] = True
            context.user_data.pop('admin_broadcast_text', None)
            await query.message.reply_text(
                '\U0001f4e3 Envio em massa iniciado.\n\nAgora digite a mensagem que deseja disparar para todos os usu\u00e1rios cadastrados.',
                reply_markup=cancel_markup('painel:back', '\u274c Cancelar envio em massa'),
            )
            return ConversationHandler.END
        elif action == 'clear_confirm':
            await query.answer()
            await query.message.reply_text(
                '\U0001f9f9 Confirmar limpeza das mensagens anteriores do bot?',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton('\u2705 Confirmar limpeza', callback_data='menu:clear_do')],
                    [InlineKeyboardButton('\u274c Cancelar', callback_data='menu:back')],
                ])
            )
        elif action == 'clear_do':
            await query.answer()
            clear_pending_input_state(context)
            try:
                await query.message.delete()
            except Exception:
                pass
            panel_text = get_panel_text(chat_id)
            await query.message.reply_text(
                panel_text,
                parse_mode='HTML',
                reply_markup=full_menu_markup(chat_id),
            )
        elif action == 'back':
            await query.answer()
            context.user_data.pop('awaiting_admin_broadcast', None)
            context.user_data.pop('admin_broadcast_text', None)
            clear_pending_input_state(context)
            panel_text = get_panel_text(chat_id)
            await query.message.reply_text(
                panel_text,
                parse_mode='HTML',
                reply_markup=full_menu_markup(chat_id),
            )
        else:
            await query.answer('A\u00e7\u00e3o n\u00e3o reconhecida.', show_alert=True)

    except Exception as exc:
        logger.error('[menu_callback] Erro ao processar a\u00e7\u00e3o do menu | action=%s | chat_id=%s | erro=%s', action, chat_id, exc)
        try:
            await query.answer('\u274c Erro ao processar. Tente novamente.', show_alert=True)
        except Exception:
            pass
        try:
            await query.message.reply_text('\u274c Ocorreu um erro ao processar sua solicita\u00e7\u00e3o. Tente novamente.', reply_markup=main_menu_markup())
        except Exception:
            pass

    return ConversationHandler.END
async def admin_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = str(query.message.chat.id)
    conn = get_db()
    admin = is_admin_chat(conn, chat_id)
    conn.close()
    if not admin:
        await query.message.reply_text('🚫 Comando restrito a administradores.')
        return ConversationHandler.END
    context.user_data['awaiting_admin_broadcast'] = True
    context.user_data.pop('admin_broadcast_text', None)
    await query.message.reply_text(
        '📣 Envio em massa iniciado.\n\nAgora digite a mensagem que deseja disparar para todos os usuários cadastrados.',
        reply_markup=cancel_markup('painel:back', '❌ Cancelar envio em massa'),
    )
    return ConversationHandler.END


async def admin_broadcast_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('awaiting_admin_broadcast'):
        return ConversationHandler.END

    text = (update.message.text or '').strip()
    if not text:
        await update.message.reply_text('Envie uma mensagem válida para o disparo.')
        return ConversationHandler.END

    prefix = '📣 Confirma o envio desta mensagem para todos os usuários?'
    if text.startswith(prefix):
        text = text[len(prefix):].strip()

    context.user_data['admin_broadcast_text'] = text
    context.user_data['awaiting_admin_broadcast'] = False
    encoded = base64.urlsafe_b64encode(text.encode('utf-8')).decode('ascii')
    confirm_callback = f'painel:broadcast_confirm:{encoded}' if len(encoded) <= 48 else 'painel:broadcast_confirm'
    await update.message.reply_text(
        '📣 Confirma o envio desta mensagem para todos os usuários%s\n\n' + text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('✅ Confirmar envio', callback_data=confirm_callback)],
            [InlineKeyboardButton('❌ Cancelar', callback_data='painel:back')],
        ]),
    )
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_pending_input_state(context)
    chat_id = str(update.message.chat.id)
    await update.message.reply_text(
        'ℹ️ Ação cancelada.\n\n' + get_panel_text(chat_id),
        parse_mode='Markdown',
        reply_markup=full_menu_markup(chat_id)
    )
    return ConversationHandler.END


def support_subjects_markup(admin: bool = False, blocked_user: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton('❓ Dúvidas', callback_data='support:new:duvidas')],
        [InlineKeyboardButton('💡 Sugestões', callback_data='support:new:sugestoes')],
        [InlineKeyboardButton('⚠️ Reclamações', callback_data='support:new:reclamacoes')],
        [InlineKeyboardButton('📂 Minhas mensagens', callback_data='support:list')],
    ]
    if admin:
        rows = [[InlineKeyboardButton('📥 Caixa de entrada', callback_data='support:admin:list')]]
    rows.append([InlineKeyboardButton('⬅️ Voltar', callback_data='support:back_blocked' if blocked_user and not admin else 'menu:back')])
    return InlineKeyboardMarkup(rows)


def support_conversation_actions_markup(thread_id: int, admin: bool = False, blocked: bool = False) -> InlineKeyboardMarkup:
    rows = []
    if admin:
        rows.append([InlineKeyboardButton('✍️ Responder usuário', callback_data=f'support:admin:reply:{thread_id}')])
        rows.append([InlineKeyboardButton('🧹 Limpar conversa', callback_data=f'support:admin:clear:{thread_id}')])
        rows.append([InlineKeyboardButton('🚫 Desbloquear usuário' if blocked else '⛔ Bloquear usuário', callback_data=f'support:admin:block:{thread_id}')])
    else:
        rows.append([InlineKeyboardButton('✍️ Enviar nova mensagem', callback_data=f'support:user:reply:{thread_id}')])
        rows.append([InlineKeyboardButton('🧹 Limpar esta conversa', callback_data=f'support:user:clear:{thread_id}')])
    rows.append([InlineKeyboardButton('🧹 Limpar todas', callback_data='support:admin:clearall' if admin else 'support:clear_all')])
    rows.append([InlineKeyboardButton('⬅️ Voltar', callback_data='support:list' if not admin else 'support:admin:list')])
    return InlineKeyboardMarkup(rows)


def _support_conversation_text(conn, thread_id: int, admin: bool = False) -> str:
    thread = get_support_conversation(conn, thread_id)
    if not thread:
        return '⚠️ Conversa não encontrada.'
    messages = conn.execute(
        sql("SELECT sender_role, body, created_at FROM support_messages WHERE thread_id = %s ORDER BY id ASC LIMIT 30"),
        (thread_id,),
    ).fetchall()
    status_label = 'Bloqueado 🚫' if int(thread['blocked'] or 0) == 1 else 'Aberto ✅'
    lines = ['💬 Atendimento', f"Assunto: {support_subject_label(thread['subject'])}", f"Status: {status_label}", '']
    if admin:
        lines.insert(1, f"Pessoa: {thread['first_name'] or thread['chat_id']}")
    if not messages:
        lines.append('Nenhuma mensagem nesta conversa.')
    for msg in messages:
        if admin:
            prefix = '👤 Pessoa' if msg['sender_role'] == 'user' else '🛠 Vooindo'
        else:
            prefix = '👤 Você' if msg['sender_role'] == 'user' else '🛠 Vooindo'
        lines.append(f"{prefix}: {msg['body']}")
    return '\n'.join(lines)


async def support_message_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    conn = get_db()
    row = get_bot_user_by_chat(conn, chat_id)
    msg = require_confirmation(conn, chat_id)
    conn.close()
    if msg and not ('suspensa' in msg.lower() and row):
        await update.message.reply_text(msg, reply_markup=confirmation_markup_for_message(msg))
        return ConversationHandler.END
    blocked_user = bool(row and int(row['blocked'] or 0) == 1)
    await update.message.reply_text('💬 Escolha o tipo de mensagem que deseja enviar:', reply_markup=support_subjects_markup(blocked_user=blocked_user))
    return ConversationHandler.END


async def support_message_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode = context.user_data.get('support_mode')
    if mode not in {'user', 'admin'}:
        return ConversationHandler.END

    chat_id = str(update.effective_chat.id)
    body = (update.message.text or '').strip()
    if not body:
        await update.message.reply_text('⚠️ Envie uma mensagem com texto para continuar.')
        return ASK_ADMIN_SUPPORT_MESSAGE if mode == 'admin' else ASK_SUPPORT_MESSAGE

    conn = get_db()
    thread_id = context.user_data.get('support_conversation_id')
    subject = context.user_data.get('support_subject')

    if mode == 'user':
        row = get_bot_user_by_chat(conn, chat_id)
        if not row:
            conn.close()
            clear_pending_input_state(context)
            await update.message.reply_text('⚠️ Use /start para iniciar seu cadastro.')
            return ConversationHandler.END
        if thread_id:
            thread = get_support_conversation(conn, int(thread_id))
            if not thread or int(thread['user_id']) != int(row['user_id']):
                conn.close()
                clear_pending_input_state(context)
                await update.message.reply_text('⚠️ Conversa não encontrada.', reply_markup=full_menu_markup(chat_id))
                return ConversationHandler.END
            if int(thread['blocked'] or 0) == 1:
                conn.close()
                clear_pending_input_state(context)
                await update.message.reply_text('🚫 Esta conversa foi bloqueada pelo atendimento.', reply_markup=support_conversation_actions_markup(int(thread_id), admin=False, blocked=True))
                return ConversationHandler.END
        else:
            thread_id = create_support_conversation(conn, chat_id, str(subject or 'duvidas'))
    else:
        if not is_admin_chat(conn, chat_id):
            conn.close()
            clear_pending_input_state(context)
            await update.message.reply_text('🚫 Comando restrito a administradores.')
            return ConversationHandler.END
        thread = get_support_conversation(conn, int(thread_id or 0))
        if not thread:
            conn.close()
            clear_pending_input_state(context)
            await update.message.reply_text('⚠️ Conversa não encontrada.', reply_markup=full_menu_markup(chat_id))
            return ConversationHandler.END

    append_support_message(conn, int(thread_id), mode, chat_id, body)
    await notify_support_message(context, conn, int(thread_id), mode, body)
    thread = get_support_conversation(conn, int(thread_id))
    text = _support_conversation_text(conn, int(thread_id), admin=(mode == 'admin'))
    conn.close()

    clear_pending_input_state(context)
    await update.message.reply_text(
        '✅ Mensagem enviada com sucesso.',
        reply_markup=support_conversation_actions_markup(int(thread_id), admin=(mode == 'admin'), blocked=bool(thread and int(thread['blocked'] or 0) == 1)),
    )
    await update.message.reply_text(
        text,
        reply_markup=support_conversation_actions_markup(int(thread_id), admin=(mode == 'admin'), blocked=bool(thread and int(thread['blocked'] or 0) == 1)),
    )
    return ConversationHandler.END


async def support_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    chat_id = str(query.message.chat.id)
    await query.answer()

    conn = get_db()

    if data == 'support:back_blocked':
        conn.close()
        await query.message.reply_text(
            '🚫 Sua conta foi suspensa. Entre em contato com o suporte.',
            reply_markup=blocked_support_markup(),
        )
        return ConversationHandler.END

    if data == 'support:list':
        row = get_bot_user_by_chat(conn, chat_id)
        rows = []
        if row:
            rows = conn.execute(
                sql("SELECT st.id, st.subject, st.blocked, (SELECT COUNT(*) FROM support_messages sm WHERE sm.thread_id = st.id AND sm.sender_role = 'admin' AND sm.is_read = 0) AS unread FROM support_threads st WHERE st.user_id = %s ORDER BY st.updated_at DESC LIMIT 20"),
                (row['user_id'],),
            ).fetchall()
        conn.close()
        await query.message.reply_text('📂 *Suas conversas*', parse_mode='Markdown', reply_markup=list_support_conversations_markup(rows, admin=False))
        return ConversationHandler.END

    if data == 'support:admin:list':
        if not is_admin_chat(conn, chat_id):
            conn.close()
            await query.message.reply_text('🚫 Você não tem permissão para acessar o atendimento admin.')
            return ConversationHandler.END
        rows = conn.execute(
            sql("SELECT st.id, st.subject, st.blocked, bu.first_name, (SELECT COUNT(*) FROM support_messages sm WHERE sm.thread_id = st.id AND sm.sender_role = 'user' AND sm.is_read = 0) AS unread FROM support_threads st LEFT JOIN bot_users bu ON bu.user_id = st.user_id WHERE st.status = 'open' ORDER BY st.updated_at DESC LIMIT 30")
        ).fetchall()
        conn.close()
        await query.message.reply_text('📥 *Caixa de entrada do atendimento*', parse_mode='Markdown', reply_markup=list_support_conversations_markup(rows, admin=True))
        return ConversationHandler.END

    if data.startswith('support:new:'):
        clear_pending_input_state(context)
        context.user_data['support_mode'] = 'user'
        context.user_data['support_subject'] = data.split(':', 2)[2]
        conn.close()
        await query.message.reply_text(
            f"✍️ Envie sua mensagem para {support_subject_label(context.user_data['support_subject'])}.",
            reply_markup=cancel_markup('menu:support', '❌ Cancelar mensagem'),
        )
        return ASK_SUPPORT_MESSAGE

    if data.startswith('support:open:'):
        thread_id = int(data.split(':')[-1])
        row = get_bot_user_by_chat(conn, chat_id)
        thread = get_support_conversation(conn, thread_id)
        if not row or not thread or int(thread['user_id']) != int(row['user_id']):
            conn.close()
            await query.message.reply_text('⚠️ Conversa não encontrada.')
            return ConversationHandler.END
        mark_support_conversation_as_read(conn, thread_id, admin=False)
        text = _support_conversation_text(conn, thread_id, admin=False)
        blocked = bool(int(thread['blocked'] or 0) == 1)
        conn.close()
        await query.message.reply_text(text, reply_markup=support_conversation_actions_markup(thread_id, admin=False, blocked=blocked))
        return ConversationHandler.END

    if data.startswith('support:user:reply:'):
        thread_id = int(data.split(':')[-1])
        row = get_bot_user_by_chat(conn, chat_id)
        thread = get_support_conversation(conn, thread_id)
        if not row or not thread or int(thread['user_id']) != int(row['user_id']):
            conn.close()
            await query.message.reply_text('⚠️ Conversa não encontrada.')
            return ConversationHandler.END
        if int(thread['blocked'] or 0) == 1:
            conn.close()
            await query.message.reply_text('🚫 Esta conversa está bloqueada pelo atendimento.')
            return ConversationHandler.END
        clear_pending_input_state(context)
        context.user_data['support_mode'] = 'user'
        context.user_data['support_conversation_id'] = thread_id
        conn.close()
        await query.message.reply_text('✍️ Envie a nova mensagem para o atendimento.', reply_markup=cancel_markup(f'support:open:{thread_id}', '❌ Cancelar resposta'))
        return ASK_SUPPORT_MESSAGE

    if data.startswith('support:user:clear:'):
        thread_id = int(data.split(':')[-1])
        row = get_bot_user_by_chat(conn, chat_id)
        thread = get_support_conversation(conn, thread_id)
        if row and thread and int(thread['user_id']) == int(row['user_id']):
            clear_support_conversation(conn, thread_id)
        conn.close()
        await query.message.reply_text('🧹 Conversa removida com sucesso.', reply_markup=support_subjects_markup())
        return ConversationHandler.END

    if data == 'support:clear_all':
        row = get_bot_user_by_chat(conn, chat_id)
        if row:
            thread_ids = conn.execute(sql("SELECT id FROM support_threads WHERE user_id = %s"), (row['user_id'],)).fetchall()
            for thread_row in thread_ids:
                clear_support_conversation(conn, int(thread_row['id']))
        conn.close()
        await query.message.reply_text('🧹 Todas as suas conversas foram removidas.', reply_markup=support_subjects_markup())
        return ConversationHandler.END

    if data.startswith('support:admin:open:'):
        thread_id = int(data.split(':')[-1])
        if not is_admin_chat(conn, chat_id):
            conn.close()
            await query.message.reply_text('🚫 Você não tem permissão para acessar o atendimento admin.')
            return ConversationHandler.END
        thread = get_support_conversation(conn, thread_id)
        if not thread:
            conn.close()
            await query.message.reply_text('⚠️ Conversa não encontrada.')
            return ConversationHandler.END
        mark_support_conversation_as_read(conn, thread_id, admin=True)
        text = _support_conversation_text(conn, thread_id, admin=True)
        blocked = bool(int(thread['blocked'] or 0) == 1)
        conn.close()
        await query.message.reply_text(text, reply_markup=support_conversation_actions_markup(thread_id, admin=True, blocked=blocked))
        return ConversationHandler.END

    if data.startswith('support:admin:reply:'):
        thread_id = int(data.split(':')[-1])
        if not is_admin_chat(conn, chat_id):
            conn.close()
            await query.message.reply_text('🚫 Você não tem permissão para acessar o atendimento admin.')
            return ConversationHandler.END
        thread = get_support_conversation(conn, thread_id)
        if not thread:
            conn.close()
            await query.message.reply_text('⚠️ Conversa não encontrada.')
            return ConversationHandler.END
        clear_pending_input_state(context)
        context.user_data['support_mode'] = 'admin'
        context.user_data['support_conversation_id'] = thread_id
        conn.close()
        await query.message.reply_text('✍️ Envie a resposta para o usuário.', reply_markup=cancel_markup(f'support:admin:open:{thread_id}', '❌ Cancelar resposta'))
        return ASK_ADMIN_SUPPORT_MESSAGE

    if data.startswith('support:admin:clear:'):
        thread_id = int(data.split(':')[-1])
        if is_admin_chat(conn, chat_id):
            clear_support_conversation(conn, thread_id)
        conn.close()
        await query.message.reply_text('🧹 Conversa removida da caixa de entrada.', reply_markup=support_subjects_markup(admin=True))
        return ConversationHandler.END

    if data == 'support:admin:clearall':
        if is_admin_chat(conn, chat_id):
            thread_ids = conn.execute(sql("SELECT id FROM support_threads")).fetchall()
            for thread_row in thread_ids:
                clear_support_conversation(conn, int(thread_row['id']))
        conn.close()
        await query.message.reply_text('🧹 Todas as conversas foram removidas da caixa de entrada.', reply_markup=support_subjects_markup(admin=True))
        return ConversationHandler.END

    if data.startswith('support:admin:block:'):
        thread_id = int(data.split(':')[-1])
        if not is_admin_chat(conn, chat_id):
            conn.close()
            await query.message.reply_text('🚫 Você não tem permissão para acessar o atendimento admin.')
            return ConversationHandler.END
        thread = get_support_conversation(conn, thread_id)
        if not thread:
            conn.close()
            await query.message.reply_text('⚠️ Conversa não encontrada.')
            return ConversationHandler.END
        new_blocked = 0 if int(thread['blocked'] or 0) == 1 else 1
        conn.execute(sql("UPDATE support_threads SET blocked = %s, updated_at = NOW() WHERE id = %s"), (new_blocked, thread_id))
        conn.commit()
        text = _support_conversation_text(conn, thread_id, admin=True)
        conn.close()
        try:
            await context.bot.send_message(
                chat_id=str(thread['chat_id']),
                text='🚫 Seu atendimento foi bloqueado temporariamente.' if new_blocked == 1 else '✅ Seu atendimento foi desbloqueado. Você já pode voltar a responder.',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('Abrir conversa', callback_data=f'support:open:{thread_id}')]]),
            )
        except Exception as exc:
            logger.warning('Falha ao avisar usuário sobre bloqueio de atendimento | thread_id=%s | erro=%s', thread_id, exc)
        await query.message.reply_text(text, reply_markup=support_conversation_actions_markup(thread_id, admin=True, blocked=bool(new_blocked)))
        return ConversationHandler.END

    conn.close()
    return ConversationHandler.END


async def unknown_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Garantir que qualquer estado residual de cadastro é limpo
    clear_pending_input_state(context)
    if context.user_data.get('awaiting_admin_broadcast'):
        return await admin_broadcast_save(update, context)
    if context.user_data.get('awaiting_plan_price_edit'):
        field = context.user_data.get('awaiting_plan_price_edit')
        texto = (update.message.text or '').strip().replace('R$', '').replace('.', '').replace(',', '.')
        try:
            novo_valor = max(0.0, float(texto))
        except ValueError:
            await update.message.reply_text('Valor inválido. Envie algo como 15 ou 15,00.')
            return ConversationHandler.END
        mapping = {
            'weekly': ('weekly_price', 'semanal'),
            'biweekly': ('biweekly_price', 'quinzenal'),
            'monthly': ('monthly_price', 'mensal'),
        }
        column, label = mapping[field]
        conn = get_db()
        try:
            conn.execute(sql(f'UPDATE monetization_settings SET {column} = %s WHERE id = 1'), (novo_valor,))
            conn.commit()
        finally:
            conn.close()
        context.user_data.pop('awaiting_plan_price_edit', None)
        await update.message.reply_text(f'✅ Plano {label} atualizado para R$ {format_money_br(novo_valor)}.')
        return ConversationHandler.END
    if context.user_data.pop('awaiting_google_password', False):
        await update.message.reply_text(
            '⏱ A sessão de renovação expirou ou foi redefinida.\n'
            'Abra o painel e clique em *"Renovar Sessão Google"* novamente.',
            parse_mode='Markdown',
        )
        return
    clear_pending_input_state(context)
    chat_id = str(update.effective_chat.id)
    await update.message.reply_text(
        '⚠️ Opção não encontrada.\n\n' + get_panel_text(chat_id),
        parse_mode='Markdown',
        reply_markup=full_menu_markup(chat_id),
    )


async def post_init(app):
    try:
        await app.bot.delete_webhook(drop_pending_updates=False)
    except Exception as exc:
        logger.warning('Não foi possível limpar webhook antes do polling: %s', exc)
    # Garantir que todos os tipos de update sejam recebidos (incluindo callback_query)
    try:
        await app.bot.set_webhook(
            url='',
            allowed_updates=['message', 'callback_query', 'inline_query', 'chosen_inline_result'],
        )
        await app.bot.delete_webhook(drop_pending_updates=False)
    except Exception as exc:
        logger.warning('Não foi possível reconfigurar webhook: %s', exc)
    await app.bot.set_my_commands([
        BotCommand('start', 'Iniciar e confirmar cadastro'),
        BotCommand('menu', 'Abrir o menu principal'),
        BotCommand('addrota', 'Cadastrar uma nova rota'),
        BotCommand('minhasrotas', 'Listar suas rotas'),
        BotCommand('removerrota', 'Remover uma rota'),
        BotCommand('limite', 'Abrir filtro de consultas'),
        BotCommand('agora', 'Rodar consulta e enviar print'),
        BotCommand('suporte', 'Falar com o atendimento'),
        BotCommand('manual', 'Ver manual de uso'),
        BotCommand('ajuda', 'Ver comandos disponíveis'),
    ])


async def _run_login_task(bot, chat_id: str, status_msg_id: int, password: str) -> None:
    import asyncio as _asyncio

    session = _login_sessions.setdefault(chat_id, {})
    session.setdefault('2fa_queue', _asyncio.Queue())
    session['done'] = False

    _last_edit = [0.0]
    _got_final = [False]

    async def _safe_edit(text: str, markup=None) -> None:
        now = _asyncio.get_event_loop().time()
        if now - _last_edit[0] < 1.0:
            return
        _last_edit[0] = now
        try:
            kwargs: dict = {'chat_id': chat_id, 'message_id': status_msg_id, 'text': text, 'parse_mode': 'Markdown'}
            if markup:
                kwargs['reply_markup'] = markup
            await bot.edit_message_text(**kwargs)
        except Exception:
            pass

    proc = None
    try:
        proc = await _asyncio.create_subprocess_exec(
            sys.executable, '/opt/vooindo/google_login_stdin.py',
            stdin=_asyncio.subprocess.PIPE,
            stdout=_asyncio.subprocess.PIPE,
            stderr=_asyncio.subprocess.PIPE,
        )
        session['proc'] = proc

        proc.stdin.write((password + '\n').encode())
        await proc.stdin.drain()

        # Task to log stderr
        async def _log_stderr(stream):
            while True:
                line = await stream.readline()
                if not line: break
                logger.error(f"[google_login_stderr] {line.decode().strip()}")
        
        _asyncio.create_task(_log_stderr(proc.stderr))

        while True:
            try:
                line_bytes = await _asyncio.wait_for(proc.stdout.readline(), timeout=200)
            except _asyncio.TimeoutError:
                break
            if not line_bytes:
                break
            line = line_bytes.decode().strip()
            if not line:
                continue

            if line.startswith('STATUS:NEED_2FA'):
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_msg_id,
                    text='📱 *2FA detectado!*\n\nDigite o código de verificação enviado ao seu telefone:',
                    parse_mode='Markdown',
                )
                _last_edit[0] = _asyncio.get_event_loop().time()
                q = session['2fa_queue']
                try:
                    code = await _asyncio.wait_for(q.get(), timeout=120)
                except _asyncio.TimeoutError:
                    code = ''
                proc.stdin.write((code + '\n').encode())
                await proc.stdin.drain()

            elif line.startswith('STATUS:AUTH_SCORE:'):
                _got_final[0] = True
                score = int(line.split(':')[2])
                if score == 2:
                    text = '✅ *Login concluído!* auth\\_score=2/2\n\nAgências voltarão a aparecer nas próximas buscas.'
                elif score == 1:
                    text = '⚠️ *Login parcial.* auth\\_score=1/2\n\nSessão sem foto de perfil detectada.'
                else:
                    text = '❌ *Login falhou.* auth\\_score=0/2\n\nVerifique screenshots em debug\\_dumps/.'
                markup = InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Voltar ao Painel', callback_data='painel:back')]])
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_msg_id,
                    text=text,
                    parse_mode='Markdown',
                    reply_markup=markup,
                )
                _last_edit[0] = _asyncio.get_event_loop().time()
                break

            elif line.startswith('STATUS:ERROR:'):
                _got_final[0] = True
                err = line[len('STATUS:ERROR:'):]
                markup = InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Voltar ao Painel', callback_data='painel:back')]])
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_msg_id,
                    text=f'❌ *Erro no login:*\n`{err[:200]}`',
                    parse_mode='Markdown',
                    reply_markup=markup,
                )
                break

            elif line.startswith('STATUS:STEP:'):
                step = line[len('STATUS:STEP:'):]
                await _safe_edit(f'⏳ *Login em andamento...*\n\n`{step[:80]}`')

        await proc.wait()

        if not _got_final[0]:
            markup = InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Voltar ao Painel', callback_data='painel:back')]])
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_msg_id,
                    text='⚠️ *Login encerrado sem resultado final.*\n\nVerifique os screenshots em debug\\_dumps/.',
                    parse_mode='Markdown',
                    reply_markup=markup,
                )
            except Exception:
                pass

    except Exception as exc:
        logger.error('Erro no _run_login_task: %s', exc)
        try:
            markup = InlineKeyboardMarkup([[InlineKeyboardButton('🔙 Voltar ao Painel', callback_data='painel:back')]])
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg_id,
                text=f'❌ Erro inesperado no processo de login: {exc}',
                reply_markup=markup,
            )
        except Exception:
            pass
    finally:
        session['done'] = True
        if proc is not None:
            try:
                proc.kill()
            except Exception:
                pass
        _login_sessions.pop(chat_id, None)


async def renovar_sessao_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = str(query.message.chat.id)
    conn = get_db()
    if not is_admin_chat(conn, chat_id):
        conn.close()
        await query.answer('Não autorizado', show_alert=True)
        return ConversationHandler.END
    conn.close()
    await query.answer()
    old_session = _login_sessions.pop(chat_id, None)
    if old_session:
        old_proc = old_session.get('proc')
        if old_proc:
            try:
                old_proc.kill()
            except Exception:
                pass
    # Pula etapa da senha — usa a app password gravada
    password = 'rcwv jvmu yyyx okto'
    try:
        status_msg = await query.message.reply_text(
            '⏳ *Iniciando renovação da sessão Google...*',
            parse_mode='Markdown',
        )
    except Exception as exc:
        logger.error('Erro ao enviar status do login: %s', exc)
        await query.message.reply_text(f'❌ Erro ao iniciar login: {exc}')
        return ConversationHandler.END

    import asyncio as _asyncio
    _login_sessions[chat_id] = {'2fa_queue': _asyncio.Queue(), 'done': False}
    _asyncio.create_task(_run_login_task(context.bot, chat_id, status_msg.message_id, password))
    return ASK_GOOGLE_2FA


async def renovar_sessao_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import asyncio as _asyncio
    chat_id = str(update.effective_chat.id)
    conn = get_db()
    if not is_admin_chat(conn, chat_id):
        conn.close()
        return ConversationHandler.END
    conn.close()

    context.user_data.pop('awaiting_google_password', None)
    password = update.message.text or ''
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    except Exception:
        pass

    if not password.strip():
        await update.message.reply_text('❌ Senha não pode ser vazia. /cancelar para sair.')
        return ASK_GOOGLE_PASSWORD

    try:
        status_msg = await context.bot.send_message(
            chat_id=chat_id,
            text='⏳ *Iniciando login Google...*',
            parse_mode='Markdown',
        )
    except Exception as exc:
        logger.error('Erro ao enviar mensagem de status do login: %s', exc)
        await update.message.reply_text(f'❌ Erro ao iniciar login: {exc}')
        return ASK_GOOGLE_PASSWORD

    _login_sessions[chat_id] = {'2fa_queue': _asyncio.Queue(), 'done': False}
    _asyncio.create_task(_run_login_task(context.bot, chat_id, status_msg.message_id, password.strip()))
    return ASK_GOOGLE_2FA


async def renovar_sessao_2fa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    session = _login_sessions.get(chat_id)

    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    except Exception:
        pass

    if session and not session.get('done'):
        code = (update.message.text or '').strip()
        if code:
            await session['2fa_queue'].put(code)
    return ASK_GOOGLE_2FA


async def renovar_sessao_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    session = _login_sessions.pop(chat_id, None)
    if session:
        proc = session.get('proc')
        if proc:
            try:
                proc.kill()
            except Exception:
                pass
    await update.message.reply_text('❌ Login cancelado.', reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END


async def run_bot():
    if not TOKEN:
        raise SystemExit('Defina TELEGRAM_BOT_TOKEN no .env')

    ensure_bot_tables()
    request = HTTPXRequest(
        connection_pool_size=50,
        pool_timeout=60.0,
        connect_timeout=30.0,
        read_timeout=60.0,
        write_timeout=60.0,
    )
    app = ApplicationBuilder().token(TOKEN).request(request).post_init(post_init).build()
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('menu', menu))
    app.add_handler(CommandHandler('manual', manual))
    app.add_handler(CommandHandler('ajuda', ajuda))
    app.add_handler(CallbackQueryHandler(manual_callback, pattern=r'^manual:'))
    app.add_handler(CommandHandler('minhasrotas', minhas_rotas))
    app.add_handler(CommandHandler('agora', agora))
    app.add_handler(CommandHandler('fontes', fontes))
    app.add_handler(CommandHandler('painel', cmd_painel))
    app.add_handler(CommandHandler('status', cmd_status))

    conv = ConversationHandler(
        entry_points=[CommandHandler('addrota', addrota_start), CallbackQueryHandler(menu_callback, pattern=r'^menu:addrota$')],
        states={
            ASK_ORIGIN: [
                CallbackQueryHandler(addrota_cancel_callback, pattern=r'^addrota:cancel$'),
                CallbackQueryHandler(aeroporto_callback, pattern=r'^(origem|destino):'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, addrota_origin),
            ],
            ASK_DESTINATION: [
                CallbackQueryHandler(addrota_cancel_callback, pattern=r'^addrota:cancel$'),
                CallbackQueryHandler(aeroporto_callback, pattern=r'^(origem|destino):'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, addrota_destination),
            ],
            ASK_OUTBOUND: [
                CallbackQueryHandler(addrota_cancel_callback, pattern=r'^addrota:cancel$'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, addrota_outbound),
            ],
        },
        fallbacks=[CommandHandler('cancelar', cancel)],
        conversation_timeout=120,
    )
    limite_conv = ConversationHandler(
        entry_points=[
            CommandHandler('limite', limite_start),
            CallbackQueryHandler(menu_callback, pattern=r'^menu:limite$'),
            CallbackQueryHandler(filter_callback, pattern=r'^filter:(edit_limit|price_info)$'),
            CallbackQueryHandler(limite_cancel_callback, pattern=r'^limite:cancel$'),
        ],
        states={
            ASK_LIMIT: [
                CallbackQueryHandler(limite_cancel_callback, pattern=r'^limite:cancel$'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, limite_save),
            ],
        },
        fallbacks=[CommandHandler('cancelar', cancel)],
    )
    support_conv = ConversationHandler(
        entry_points=[CommandHandler('suporte', support_message_start), CallbackQueryHandler(support_callback, pattern=r'^support:')],
        states={
            ASK_SUPPORT_MESSAGE: [
                CallbackQueryHandler(support_callback, pattern=r'^support:'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, support_message_save),
            ],
            ASK_ADMIN_SUPPORT_MESSAGE: [
                CallbackQueryHandler(support_callback, pattern=r'^support:'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, support_message_save),
            ],
        },
        fallbacks=[CommandHandler('cancelar', cancel)],
        per_message=True,
    )

    app.add_handler(CallbackQueryHandler(admin_broadcast_start, pattern=r'^painel:broadcast$'))
    app.add_handler(CallbackQueryHandler(alerts_callback, pattern=r'^menu:(togglealerts|confirmalerts:)'))
    app.add_handler(conv)
    app.add_handler(limite_conv)
    app.add_handler(support_conv)
    renovar_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(renovar_sessao_callback, pattern=r'^painel:renovar_sessao$')],
        states={
            ASK_GOOGLE_PASSWORD: [
                CallbackQueryHandler(renovar_sessao_callback, pattern=r'^painel:renovar_sessao$'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, renovar_sessao_password),
            ],
            ASK_GOOGLE_2FA: [
                CallbackQueryHandler(renovar_sessao_callback, pattern=r'^painel:renovar_sessao$'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, renovar_sessao_2fa),
            ],
        },
        fallbacks=[CommandHandler('cancelar', renovar_sessao_cancel)],
        conversation_timeout=300,
        allow_reentry=True,
    )
    app.add_handler(renovar_conv)
    app.add_handler(CommandHandler('removerrota', removerrota))
    app.add_handler(CallbackQueryHandler(confirm_callback, pattern=r'^confirm:cadastro$'))
    app.add_handler(CallbackQueryHandler(removerrota_callback, pattern=r'^removerrota:'))
    app.add_handler(CallbackQueryHandler(sources_callback, pattern=r'^sources:'))
    app.add_handler(CallbackQueryHandler(filter_callback, pattern=r'^filter:'))
    app.add_handler(CallbackQueryHandler(painel_callback, pattern=r'^painel:'))
    app.add_handler(CallbackQueryHandler(painel_callback, pattern=r'^userpix:'))
    app.add_handler(CallbackQueryHandler(painel_callback, pattern=r'^selectorhealth:'))
    app.add_handler(CallbackQueryHandler(payment_callback, pattern=r'^payment:'))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern=r'^menu:'))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, admin_broadcast_save), group=1)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_text))
    try:
        await app.initialize()
        await post_init(app)
        await app.start()
        await app.updater.start_polling(drop_pending_updates=False)
        while True:
            await asyncio.sleep(3600)
    except Conflict:
        logger.exception('Conflito no polling do Telegram: outra instância está consumindo getUpdates')
        raise
    finally:
        try:
            if app.updater and app.updater.running:
                await app.updater.stop()
        finally:
            if app.running:
                await app.stop()
            await app.shutdown()


def main():
    asyncio.run(run_bot())


if __name__ == '__main__':
    main()
