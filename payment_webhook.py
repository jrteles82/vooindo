import os
from datetime import datetime, timedelta

import requests
from flask import Flask, request
from telegram import Bot
from telegram.request import HTTPXRequest

from config import MP_ACCESS_TOKEN, TOKEN, MERCADOPAGO_API_BASE_URL, now_local
from db import connect as connect_db, insert_ignore_sql, now_expression, sql
from notif import push_admin_notif

PORT = int(os.getenv('PAYMENT_WEBHOOK_PORT', '8787'))
app = Flask(__name__)
_request = HTTPXRequest(connection_pool_size=20, pool_timeout=60.0, connect_timeout=30.0, read_timeout=60.0, write_timeout=60.0)
bot = Bot(token=TOKEN, request=_request) if TOKEN else None


def get_db():
    return connect_db()


def get_mp_payment(payment_id: str) -> dict:
    headers = {
        'Authorization': f'Bearer {MP_ACCESS_TOKEN}',
        'Content-Type': 'application/json',
    }
    response = requests.get(f'{MERCADOPAGO_API_BASE_URL}/v1/payments/{payment_id}', headers=headers, timeout=30)
    data = response.json()
    if response.status_code >= 400:
        raise RuntimeError(data.get('message') or 'Erro ao consultar pagamento Pix')
    return data


def plan_days(plan_name: str) -> int:
    return {
        'Semanal': 7,
        'Quinzenal': 15,
        'Mensal': 30,
        'Teste Admin': 7,
    }.get(plan_name, 7)


def add_days_to_expiration(current_expiration: str | None, days: int) -> str:
    base = now_local()
    if current_expiration:
        try:
            parsed = datetime.fromisoformat(current_expiration)
            if parsed > base:
                base = parsed
        except ValueError:
            pass
    return (base + timedelta(days=days)).replace(microsecond=0).isoformat(sep=' ')


def ensure_user_access(conn, chat_id: str):
    conn.execute(
        sql(f'''
        {insert_ignore_sql('user_access', ['chat_id', 'status', 'free_uses', 'test_charge', 'total_paid', 'updated_at'], f"?, 'free', 0, 0, 0, {now_expression()}")}
        '''),
        (chat_id,)
    )
    conn.commit()
    return conn.execute(sql('SELECT * FROM user_access WHERE chat_id = ?'), (chat_id,)).fetchone()


def apply_approved_payment(conn, payment_id: str):
    row = conn.execute(
        sql('SELECT mp_payment_id, chat_id, plan_name, amount, status FROM payments WHERE mp_payment_id = ?'),
        (payment_id,)
    ).fetchone()
    if not row:
        return False, 'pagamento_nao_encontrado'

    payment = get_mp_payment(payment_id)
    status = payment.get('status', row['status'])
    approved_at = payment.get('date_approved')
    conn.execute(
        sql('UPDATE payments SET status = ?, approved_at = COALESCE(?, approved_at) WHERE mp_payment_id = ?'),
        (status, approved_at, payment_id)
    )

    if status != 'approved':
        conn.commit()
        return False, status, str(row['chat_id'])

    chat_id = str(row['chat_id'])
    plan_name = row['plan_name'] or 'Teste Admin'
    access = ensure_user_access(conn, chat_id)
    expires_at = add_days_to_expiration(access['expires_at'], plan_days(plan_name))
    conn.execute(
        sql(f'''
        UPDATE user_access
        SET status = ?, expires_at = ?, free_uses = 0, total_paid = COALESCE(total_paid, 0) + ?, updated_at = {now_expression()}
        WHERE chat_id = ?
        '''),
        ('active', expires_at, float(row['amount'] or 0), chat_id)
    )
    bot_user = conn.execute(sql('SELECT user_id FROM bot_users WHERE chat_id = ?'), (chat_id,)).fetchone()
    if bot_user:
        conn.execute(
            sql(f"UPDATE bot_settings SET alerts_enabled = 1, updated_at = {now_expression()} WHERE user_id = ?"),
            (int(bot_user['user_id']),),
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
    return True, expires_at, chat_id


@app.post('/webhook')
def webhook():
    data = request.get_json(silent=True) or {}
    payment_id = None
    if isinstance(data.get('data'), dict):
        payment_id = data['data'].get('id')
    if not payment_id and data.get('resource'):
        payment_id = str(data['resource']).rstrip('/').split('/')[-1]
    if not payment_id:
        return 'OK', 200

    conn = get_db()
    try:
        approved, info, chat_id = apply_approved_payment(conn, str(payment_id))
        if approved and bot:
            bot.send_message(chat_id=chat_id, text=f'🎉 Pagamento aprovado automaticamente! Acesso liberado até {info}.')
    finally:
        conn.close()
    return 'OK', 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT)
