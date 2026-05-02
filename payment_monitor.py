import asyncio
import os
import time
from datetime import datetime, timedelta

import requests
from telegram import Bot
from telegram.request import HTTPXRequest

from app_logging import get_logger
from audit import audit
from notif import push_admin_notif
from config import MP_ACCESS_TOKEN, TOKEN, MERCADOPAGO_API_BASE_URL, now_local
from db import connect as connect_db, insert_ignore_sql, now_expression, sql, DatabaseRateLimitError

logger = get_logger('payment_monitor')

CHECK_INTERVAL_SECONDS = int(os.getenv("PAYMENT_MONITOR_CHECK_INTERVAL_SECONDS", "20"))
MP_REQUEST_TIMEOUT_SECONDS = int(os.getenv("PAYMENT_MONITOR_MP_TIMEOUT_SECONDS", "30"))


def get_db():
    return connect_db()


def get_mp_payment(payment_id: str) -> dict:
    headers = {
        'Authorization': f'Bearer {MP_ACCESS_TOKEN}',
        'Content-Type': 'application/json',
    }
    response = requests.get(
        f'{MERCADOPAGO_API_BASE_URL}/v1/payments/{payment_id}',
        headers=headers,
        timeout=MP_REQUEST_TIMEOUT_SECONDS,
    )
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
        {insert_ignore_sql('user_access', ['chat_id', 'status', 'free_uses', 'test_charge', 'total_paid', 'updated_at'], f"%s, 'free', 0, 0, 0, {now_expression()}")}
        '''),
        (chat_id,)
    )
    conn.commit()
    return conn.execute(sql('SELECT * FROM user_access WHERE chat_id = %s'), (chat_id,)).fetchone()


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
        audit.payment("pix_verificado", chat_id=str(row['chat_id']), status=status,
                      payload={"mp_payment_id": payment_id, "status_mp": status})
        return False, status

    chat_id = str(row['chat_id'])
    plan_name = row['plan_name'] or 'Teste Admin'
    access = ensure_user_access(conn, chat_id)
    expires_at = add_days_to_expiration(access['expires_at'], plan_days(plan_name))
    conn.execute(
        sql(f'''
        UPDATE user_access
        SET status = %s, expires_at = %s, free_uses = 0, total_paid = COALESCE(total_paid, 0) + %s, updated_at = {now_expression()}
        WHERE chat_id = %s
        '''),
        ('active', expires_at, float(row['amount'] or 0), chat_id)
    )
    bot_user = conn.execute(sql('SELECT user_id FROM bot_users WHERE chat_id = %s'), (chat_id,)).fetchone()
    if bot_user:
        conn.execute(
            sql(f"UPDATE bot_settings SET alerts_enabled = 1, updated_at = {now_expression()} WHERE user_id = %s"),
            (int(bot_user['user_id']),),
        )
    conn.commit()
    audit.payment("pix_aprovado", chat_id=chat_id,
                  payload={"mp_payment_id": payment_id, "plano": plan_name,
                           "valor": float(row['amount'] or 0), "expires_at": expires_at})
    audit.access("acesso_liberado", chat_id=chat_id,
                 payload={"plano": plan_name, "expires_at": expires_at})
    push_admin_notif(
        conn,
        "notif_pagamento_confirmado",
        f"✅ *Pagamento confirmado*\n\n"
        f"*Chat ID:* `{chat_id}`\n"
        f"*Plano:* {plan_name}\n"
        f"*Valor:* R$ {float(row['amount'] or 0):.2f}\n"
        f"*Válido até:* {expires_at}",
    )
    return True, expires_at


def pending_payments(conn):
    return conn.execute(
        sql("SELECT mp_payment_id, chat_id FROM payments WHERE status = 'pending' ORDER BY created_at ASC LIMIT 20")
    ).fetchall()


def main():
    if not TOKEN or not MP_ACCESS_TOKEN:
        raise SystemExit('Defina TELEGRAM_BOT_TOKEN e MP_ACCESS_TOKEN no .env')

    request = HTTPXRequest(connection_pool_size=20, pool_timeout=60.0, connect_timeout=30.0, read_timeout=60.0, write_timeout=60.0)
    bot = Bot(token=TOKEN, request=request)
    notified = set()

    conn = None
    while True:
        try:
            if conn is None:
                conn = get_db()
            for row in pending_payments(conn):
                payment_id = str(row['mp_payment_id'])
                chat_id = str(row['chat_id'])
                try:
                    approved, info = apply_approved_payment(conn, payment_id)
                    if approved and payment_id not in notified:
                        from telegram import InlineKeyboardMarkup, InlineKeyboardButton
                        from db import sql as _sql
                        conn2 = get_db()
                        row = conn2.execute(_sql('SELECT scan_interval_minutes FROM app_settings WHERE id = 1')).fetchone()
                        interval_min = int(row['scan_interval_minutes'] or 60) if row else 60
                        conn2.close()
                        asyncio.run(bot.send_message(chat_id=chat_id, text=f'🎉 Pagamento aprovado automaticamente! Acesso liberado até {info}.'))
                        asyncio.run(bot.send_message(
                            chat_id=chat_id,
                            text=f'✅ Você receberá atualizações automáticas a cada {interval_min} minutos, além de poder fazer consulta a qualquer momento.',
                        ))
                        asyncio.run(bot.send_message(
                            chat_id=chat_id,
                            text='🏠 Seu acesso foi liberado. Escolha uma opção abaixo:',
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton('🏠 Abrir menu', callback_data='menu:back')],
                            ]),
                        ))
                        notified.add(payment_id)
                except Exception as exc:
                    logger.exception('[payment-monitor] erro no pagamento %s: %s', payment_id, exc)
                    audit.error("payment_monitor_erro", chat_id=chat_id,
                                error_msg=str(exc),
                                payload={"mp_payment_id": payment_id})
        except DatabaseRateLimitError as exc:
            audit.error("payment_monitor_db_limit", error_msg=str(exc), status="blocked")
            logger.warning('[payment-monitor] limite de conexão MySQL por hora atingido: %s', exc)
            time.sleep(1800)
            continue

        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == '__main__':
    main()
