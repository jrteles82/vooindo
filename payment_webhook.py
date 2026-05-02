import os
import json
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from flask import Flask, request, jsonify, Response, stream_with_context
from telegram import Bot
from telegram.request import HTTPXRequest

from config import MP_ACCESS_TOKEN, TOKEN, MERCADOPAGO_API_BASE_URL, now_local
from db import connect as connect_db, insert_ignore_sql, now_expression, sql
from notif import push_admin_notif
from main import (
    run_scan_for_routes,
    _result_to_row,
    _expand_result_rows,
    format_brl,
    format_date_display,
    extract_final_price_source,
    filter_rows_by_max_price,
    normalize_rows_for_airline_priority,
    filter_rows_with_vendor,
    _merge_rows_for_combined_result_view,
)
from models import RouteQuery


def build_db_queries():
    """Busca todas as rotas ativas do banco para realizar a consulta completa."""
    conn = get_db()
    try:
        rows = conn.execute(
            sql('''
            SELECT origin, destination, outbound_date, inbound_date
            FROM user_routes
            WHERE active = 1
            ORDER BY user_id, id ASC
            ''')
        ).fetchall()
        routes = []
        for r in rows:
            inbound = (r['inbound_date'] or '').strip()
            routes.append(RouteQuery(
                origin=(r['origin'] or '').upper(),
                destination=(r['destination'] or '').upper(),
                outbound_date=r['outbound_date'],
                inbound_date=inbound,
                trip_type='roundtrip' if inbound else 'oneway',
            ))
        return routes
    finally:
        conn.close()

PORT = int(os.getenv('PAYMENT_WEBHOOK_PORT', '8787'))
BASE_DIR = Path(__file__).resolve().parent
app = Flask(__name__, static_folder=str(BASE_DIR / 'static'), static_url_path='/static')
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
        {insert_ignore_sql('user_access', ['chat_id', 'status', 'free_uses', 'test_charge', 'total_paid', 'updated_at'], f"%s, 'free', 0, 0, 0, {now_expression()}")}
        '''),
        (chat_id,)
    )
    conn.commit()
    return conn.execute(sql('SELECT * FROM user_access WHERE chat_id = %s'), (chat_id,)).fetchone()


def apply_approved_payment(conn, payment_id: str):
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
        return False, status, str(row['chat_id'])

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


@app.route('/')
def index():
    return app.send_static_file('index.html')


@app.route('/consulta')
def consulta():
    origin = request.args.get('origin', '').upper().strip()
    destination = request.args.get('destination', '').upper().strip()
    outbound_date = request.args.get('outbound_date', '').strip()
    inbound_date = request.args.get('inbound_date', '').strip()

    if not origin or not destination or not outbound_date:
        return jsonify({'error': 'Parâmetros obrigatórios: origin, destination, outbound_date'}), 400

    from models import RouteQuery
    route = RouteQuery(
        origin=origin,
        destination=destination,
        outbound_date=outbound_date,
        inbound_date=inbound_date,
        trip_type='roundtrip' if inbound_date else 'oneway',
    )

    try:
        parsed = run_scan_for_routes([route], fast_mode=True)
        if parsed:
            row = parsed[0]
            return jsonify({
                'rota': {
                    'origin': row.get('origin'),
                    'destination': row.get('destination'),
                    'outbound_date': row.get('outbound_date'),
                    'inbound_date': row.get('inbound_date'),
                },
                'resultado': {
                    'price': row.get('price'),
                    'price_fmt': row.get('price_fmt'),
                    'site': row.get('site'),
                    'best_vendor': row.get('best_vendor'),
                    'best_vendor_price': row.get('best_vendor_price'),
                    'final_price_source': extract_final_price_source(row.get('notes')),
                },
                'resultados': parsed,
            })
        return jsonify({'error': 'Nenhum resultado encontrado'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/rotas')
def rotas():
    try:
        routes = build_db_queries()
        rotas_list = []
        for r in routes:
            rotas_list.append({
                'origin': r.origin,
                'destination': r.destination,
                'outbound_date': r.outbound_date,
                'inbound_date': r.inbound_date or '',
                'trip_type': r.trip_type,
            })
        return jsonify({'rotas': rotas_list})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/historico')
def historico():
    limit = request.args.get('limit', '20')
    try:
        limit = max(1, min(200, int(limit)))
    except (ValueError, TypeError):
        limit = 20

    try:
        conn = get_db()
        rows = conn.execute(
            sql(f'''
            SELECT origin, destination, outbound_date, inbound_date,
                   price, site, best_vendor, best_vendor_price, notes, created_at
            FROM results
            ORDER BY created_at DESC
            LIMIT %s
            '''),
            (limit,)
        ).fetchall()
        conn.close()

        items = []
        for r in rows:
            items.append({
                'origin': r['origin'],
                'destination': r['destination'],
                'outbound_date': r['outbound_date'],
                'inbound_date': r['inbound_date'] or '',
                'price': r['price'],
                'site': r['site'],
                'best_vendor': r['best_vendor'],
                'best_vendor_price': r['best_vendor_price'],
                'final_price_source': extract_final_price_source(r['notes']),
                'created_at': str(r['created_at'] or ''),
            })
        return jsonify({'items': items})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/historico/limpar', methods=['POST'])
def limpar_historico():
    try:
        conn = get_db()
        conn.execute(sql('DELETE FROM results'))
        conn.commit()
        conn.close()
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/cron-stream')
def cron_stream():
    def generate():
        try:
            routes = build_db_queries()
            total = len(routes)
            yield f'data: {json.dumps({"type": "start", "total": total})}\n\n'

            parsed = run_scan_for_routes(routes, on_row=lambda idx, total, row: None)

            for idx, row in enumerate(parsed, start=1):
                item = {
                    'origin': row.get('origin'),
                    'destination': row.get('destination'),
                    'outbound_date': row.get('outbound_date'),
                    'inbound_date': row.get('inbound_date'),
                    'price': row.get('price'),
                    'price_fmt': row.get('price_fmt'),
                    'site': row.get('site'),
                    'best_vendor': row.get('best_vendor'),
                    'best_vendor_price': row.get('best_vendor_price'),
                    'final_price_source': extract_final_price_source(row.get('notes')),
                }
                yield f'data: {json.dumps({"type": "row", "index": idx, "total": total, "item": item})}\n\n'

            yield f'data: {json.dumps({"type": "done"})}\n\n'
        except Exception as e:
            yield f'data: {json.dumps({"type": "error", "message": str(e)})}\n\n'

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
        },
    )


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
            from telegram import InlineKeyboardMarkup, InlineKeyboardButton
            bot.send_message(chat_id=chat_id, text=f'🎉 Pagamento aprovado automaticamente! Acesso liberado até {info}.')
            bot.send_message(
                chat_id=chat_id,
                text='🏠 Seu acesso foi liberado. Escolha uma opção abaixo:',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton('🏠 Abrir menu', callback_data='menu:back')],
                ]),
            )
    finally:
        conn.close()
    return 'OK', 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT)
