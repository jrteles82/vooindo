import asyncio
import pymysql
import pymysql.cursors
from urllib.parse import urlparse
import json
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError
from telegram.request import HTTPXRequest

from app_logging import get_logger
from audit import audit

from access_policy import (
    ensure_policy_schema,
    ensure_user_access,
    get_free_uses_limit,
    is_active_access,
    should_charge_user as ap_should_charge_user,
    is_maintenance_mode,
    is_exempt_from_maintenance,
)
from config import TOKEN, now_local, now_local_iso
from db import connect as connect_db, now_expression, sql, DatabaseRateLimitError
from main import _build_user_routes, build_scan_results_image, build_booking_links_message, run_scan_for_routes, filter_rows_by_max_price, filter_rows_with_vendor, normalize_rows_for_airline_priority, _rows_by_result_type, expand_rows_by_result_type, _merge_rows_for_combined_result_view
from bot import filter_rows_by_airlines, parse_airline_filters, should_show_result_type_filters
from cycle_monitor import record_cycle_start, record_cycle_end

# Número de workers paralelos para scheduler
_NUM_SCHED_WORKERS = int(os.getenv('NUM_SCHED_WORKERS', '3'))

logger = get_logger('bot_scheduler')

_SCAN_INTERVAL_MINUTES = int(os.getenv("SCAN_INTERVAL_MINUTES", "60"))
_DEFAULT_SEND_COOLDOWN_SECONDS = 30 * 60
SEND_COOLDOWN_SECONDS = int(
    os.getenv("SCHEDULER_SEND_COOLDOWN_SECONDS", str(_DEFAULT_SEND_COOLDOWN_SECONDS))
)
_METRICS_PATH = Path(__file__).resolve().parent / 'logs' / 'scheduler_cycle_metrics.jsonl'
_ROUND_REPORT_TIMEOUT_SECONDS = int(os.getenv('SCHEDULER_ROUND_REPORT_TIMEOUT_SECONDS', '1800'))
_ROUND_REPORT_POLL_SECONDS = int(os.getenv('SCHEDULER_ROUND_REPORT_POLL_SECONDS', '5'))


def get_db():
    return connect_db()


def get_scan_interval_seconds(conn) -> int:
    row = conn.execute(
        sql("SELECT scan_interval_minutes FROM app_settings WHERE id = 1")
    ).fetchone()
    if row and row["scan_interval_minutes"] is not None:
        return max(60, int(row["scan_interval_minutes"]) * 60)
    return max(60, max(1, _SCAN_INTERVAL_MINUTES) * 60)


def should_charge_user(conn, chat_id: str, access_row) -> bool:
    return ap_should_charge_user(conn, chat_id, access_row)


def iter_users(conn):
    return conn.execute(
        sql('''
        SELECT bu.user_id, bu.chat_id, COALESCE(bu.first_name, '') AS first_name, COALESCE(bu.username, '') AS username,
               bs.max_price AS max_price,
               COALESCE(bs.enable_google_flights, 1) AS enable_google_flights,
               COALESCE(bs.alerts_enabled, 1) AS alerts_enabled,
               COALESCE(bs.last_sent_at, '') AS last_sent_at,
               COALESCE(bs.last_manual_sent_at, '') AS last_manual_sent_at,
               COALESCE(bs.last_scheduled_sent_at, '') AS last_scheduled_sent_at,
               COALESCE(bs.airline_filters_json, '') AS airline_filters_json
        FROM bot_users bu
        LEFT JOIN bot_settings bs ON bs.user_id = bu.user_id
        WHERE bu.confirmed = 1 AND COALESCE(bu.blocked, 0) = 0
        ORDER BY bu.user_id
        ''')
    ).fetchall()


def was_sent_recently(last_sent_at: str, window_seconds: int = SEND_COOLDOWN_SECONDS) -> bool:
    if not last_sent_at:
        return False
    try:
        dt = datetime.fromisoformat(last_sent_at.replace(' ', 'T'))
    except ValueError:
        return False
    now = now_local()
    delta_seconds = (now - dt).total_seconds()
    if delta_seconds < -60:
        return False
    return delta_seconds < max(60, window_seconds)


def mark_sent(conn, user_id: int, send_type: str = 'scheduled'):
    _parsed = urlparse(os.environ.get('MYSQL_URL', ''))
    _cap = pymysql.connect(
        host=_parsed.hostname or 'localhost', port=_parsed.port or 3306,
        user=_parsed.username or 'vooindobot', password=_parsed.password or '',
        database=_parsed.path.lstrip('/') or 'vooindo',
        autocommit=True, connect_timeout=5,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        if send_type == 'manual':
            _cap.cursor().execute(
                f"UPDATE bot_settings SET last_sent_at = {now_expression()}, last_manual_sent_at = {now_expression()}, updated_at = {now_expression()} WHERE user_id = %s",
                (user_id,),
            )
        else:
            _cap.cursor().execute(
                f"UPDATE bot_settings SET last_sent_at = {now_expression()}, last_scheduled_sent_at = {now_expression()}, updated_at = {now_expression()} WHERE user_id = %s",
                (user_id,),
            )
    finally:
        _cap.close()


async def _send_message(bot: Bot, chat_id: str, text: str, reply_markup=None, disable_web_page_preview: bool = False, parse_mode: str | None = None):
    await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, reply_markup=reply_markup, disable_web_page_preview=disable_web_page_preview)


def main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🏠 Abrir menu principal', callback_data='menu:back')],
    ])


async def _send_photo(bot: Bot, chat_id: str, image_path: str):
    with open(image_path, 'rb') as image_file:
        await bot.send_photo(chat_id=chat_id, photo=image_file)


def _send_links_message(bot: Bot, loop, chat_id: str, links_msg: str, reply_markup) -> None:
    try:
        loop.run_until_complete(_send_message(bot, chat_id, links_msg, reply_markup=reply_markup, disable_web_page_preview=True, parse_mode='HTML'))
    except TelegramError as exc:
        if 'parse' in str(exc).lower() or 'entities' in str(exc).lower():
            logger.warning('HTML parse error ao enviar links, fallback para texto puro | chat_id=%s | erro=%s', chat_id, exc)
            plain = re.sub(r'<[^>]+>', '', links_msg)
            loop.run_until_complete(_send_message(bot, chat_id, plain, reply_markup=reply_markup, disable_web_page_preview=True))
        else:
            raise


def user_label(user_row) -> str:
    first_name = str(user_row['first_name'] or '').strip()
    username = str(user_row['username'] or '').strip()
    chat_id = str(user_row['chat_id'])
    if username:
        username = f'@{username.lstrip("@")} '
    else:
        username = ''
    if first_name:
        return f'{first_name} | {username}{chat_id}'.strip()
    return f'{username}{chat_id}'.strip()



def _vendor_filter_label(filters: dict, show_result_type_filters: bool = True) -> str:
    return '🛫 Filtro: Companhias aéreas'


def _scan_failed_by_executor_timeout(rows: list[dict]) -> bool:
    if not rows:
        return False
    timeout_rows = 0
    priced_rows = 0
    for row in rows:
        if isinstance(row.get('price'), (int, float)):
            priced_rows += 1
        notes = str(row.get('notes') or '').lower()
        if 'executor timeout' in notes or 'timeout na página' in notes:
            timeout_rows += 1
    return timeout_rows > 0 and priced_rows == 0


def run_for_user(conn, bot: Bot, loop, user_id: int, chat_id: str, max_price: float, sources: dict, airline_filters_json: str | None = None) -> tuple[bool, str, int]:
    access = ensure_user_access(conn, chat_id)
    charge_now = should_charge_user(conn, chat_id, access) and not is_active_access(access)
    if charge_now:
        free_uses = int(access['free_uses'] or 0)
        free_uses_limit = get_free_uses_limit(conn)
        if free_uses >= free_uses_limit:
            logger.info('[bot-scheduler] chat_id=%s | sem envio agendado | acesso insuficiente', chat_id)
            return False, 'bloqueado_por_monetizacao', 0

    routes = _build_user_routes(conn, user_id, prune_expired=True)
    if not routes:
        return False, 'sem_rotas_ativas', 0

    filters = parse_airline_filters(airline_filters_json)
    show_result_type_filters = should_show_result_type_filters(conn)
    sources_with_filter = dict(sources)

    parsed = run_scan_for_routes(routes, sources=sources_with_filter, allow_agencies=False, skip_booking=False)
    parsed = expand_rows_by_result_type(parsed, airline_filters_json, show_result_type_filters=show_result_type_filters)
    filtered = filter_rows_by_max_price(parsed, max_price)
    filtered = normalize_rows_for_airline_priority(filtered, airline_filters_json)
    filtered = filter_rows_with_vendor(filtered)
    filtered = filter_rows_by_airlines(filtered, airline_filters_json, show_result_type_filters=show_result_type_filters)
    filtered = _merge_rows_for_combined_result_view(filtered)
    if not filtered:
        no_result_reason = 'timeout_executor' if _scan_failed_by_executor_timeout(parsed) else 'sem_resultado_filtrado'
        if no_result_reason == 'timeout_executor':
            logger.warning('[bot-scheduler] chat_id=%s | scan sem resultado por timeout do executor', chat_id)
        loop.run_until_complete(_send_message(bot, chat_id, '⚠️ Nenhuma rota encontrada dentro dos seus filtros.', reply_markup=main_menu_markup()))
        if charge_now:
            conn.execute(
                sql(f"UPDATE user_access SET free_uses = free_uses + 1, updated_at = {now_expression()} WHERE chat_id = %s"),
                (chat_id,)
            )
            conn.commit()
        return False, no_result_reason, 0

    sent_count = 0
    image_path = build_scan_results_image(filtered, trigger='agendada')
    if not image_path:
        return False, 'sem_imagem', len(filtered)
    try:
        loop.run_until_complete(_send_photo(bot, chat_id, image_path))
        links_msg = build_booking_links_message(filtered)
        if links_msg:
            _send_links_message(bot, loop, chat_id, links_msg, main_menu_markup())
        else:
            loop.run_until_complete(_send_message(bot, chat_id, '🏠 Toque abaixo para abrir o menu novamente.', reply_markup=main_menu_markup()))
        sent_count = len(filtered)
    finally:
        try:
            os.remove(image_path)
        except OSError:
            pass

    return True, 'ok', sent_count
import asyncio
import json
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError
from telegram.request import HTTPXRequest

from app_logging import get_logger
from audit import audit

from access_policy import (
    ensure_policy_schema,
    ensure_user_access,
    get_free_uses_limit,
    is_active_access,
    should_charge_user as ap_should_charge_user,
    is_maintenance_mode,
    is_exempt_from_maintenance,
)
from config import TOKEN, now_local, now_local_iso
from db import connect as connect_db, now_expression, sql, DatabaseRateLimitError
from main import _build_user_routes, build_scan_results_image, build_booking_links_message, run_scan_for_routes, filter_rows_by_max_price, filter_rows_with_vendor, normalize_rows_for_airline_priority, _rows_by_result_type, expand_rows_by_result_type, _merge_rows_for_combined_result_view
from bot import filter_rows_by_airlines, parse_airline_filters, should_show_result_type_filters

# Número de workers paralelos para scheduler
_NUM_SCHED_WORKERS = int(os.getenv('NUM_SCHED_WORKERS', '3'))

logger = get_logger('bot_scheduler')

_SCAN_INTERVAL_MINUTES = int(os.getenv("SCAN_INTERVAL_MINUTES", "60"))
_DEFAULT_SEND_COOLDOWN_SECONDS = 30 * 60
SEND_COOLDOWN_SECONDS = int(
    os.getenv("SCHEDULER_SEND_COOLDOWN_SECONDS", str(_DEFAULT_SEND_COOLDOWN_SECONDS))
)
_METRICS_PATH = Path(__file__).resolve().parent / 'logs' / 'scheduler_cycle_metrics.jsonl'
_ROUND_REPORT_TIMEOUT_SECONDS = int(os.getenv('SCHEDULER_ROUND_REPORT_TIMEOUT_SECONDS', '1800'))
_ROUND_REPORT_POLL_SECONDS = int(os.getenv('SCHEDULER_ROUND_REPORT_POLL_SECONDS', '5'))


def get_db():
    return connect_db()


def get_scan_interval_seconds(conn) -> int:
    row = conn.execute(
        sql("SELECT scan_interval_minutes FROM app_settings WHERE id = 1")
    ).fetchone()
    if row and row["scan_interval_minutes"] is not None:
        return max(60, int(row["scan_interval_minutes"]) * 60)
    return max(60, max(1, _SCAN_INTERVAL_MINUTES) * 60)


def should_charge_user(conn, chat_id: str, access_row) -> bool:
    return ap_should_charge_user(conn, chat_id, access_row)


def iter_users(conn):
    return conn.execute(
        sql('''
        SELECT bu.user_id, bu.chat_id, COALESCE(bu.first_name, '') AS first_name, COALESCE(bu.username, '') AS username,
               bs.max_price AS max_price,
               COALESCE(bs.enable_google_flights, 1) AS enable_google_flights,
               COALESCE(bs.alerts_enabled, 1) AS alerts_enabled,
               COALESCE(bs.last_sent_at, '') AS last_sent_at,
               COALESCE(bs.last_manual_sent_at, '') AS last_manual_sent_at,
               COALESCE(bs.last_scheduled_sent_at, '') AS last_scheduled_sent_at,
               COALESCE(bs.airline_filters_json, '') AS airline_filters_json
        FROM bot_users bu
        LEFT JOIN bot_settings bs ON bs.user_id = bu.user_id
        WHERE bu.confirmed = 1 AND COALESCE(bu.blocked, 0) = 0
        ORDER BY bu.user_id
        ''')
    ).fetchall()


def was_sent_recently(last_sent_at: str, window_seconds: int = SEND_COOLDOWN_SECONDS) -> bool:
    if not last_sent_at:
        return False
    try:
        dt = datetime.fromisoformat(last_sent_at.replace(' ', 'T'))
    except ValueError:
        return False
    now = now_local()
    delta_seconds = (now - dt).total_seconds()
    if delta_seconds < -60:
        return False
    return delta_seconds < max(60, window_seconds)


def mark_sent(conn, user_id: int, send_type: str = 'scheduled'):
    _parsed = urlparse(os.environ.get('MYSQL_URL', ''))
    _cap = pymysql.connect(
        host=_parsed.hostname or 'localhost', port=_parsed.port or 3306,
        user=_parsed.username or 'vooindobot', password=_parsed.password or '',
        database=_parsed.path.lstrip('/') or 'vooindo',
        autocommit=True, connect_timeout=5,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        if send_type == 'manual':
            _cap.cursor().execute(
                f"UPDATE bot_settings SET last_sent_at = {now_expression()}, last_manual_sent_at = {now_expression()}, updated_at = {now_expression()} WHERE user_id = %s",
                (user_id,),
            )
        else:
            _cap.cursor().execute(
                f"UPDATE bot_settings SET last_sent_at = {now_expression()}, last_scheduled_sent_at = {now_expression()}, updated_at = {now_expression()} WHERE user_id = %s",
                (user_id,),
            )
    finally:
        _cap.close()


async def _send_message(bot: Bot, chat_id: str, text: str, reply_markup=None, disable_web_page_preview: bool = False, parse_mode: str | None = None):
    await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, reply_markup=reply_markup, disable_web_page_preview=disable_web_page_preview)


def main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🏠 Abrir menu principal', callback_data='menu:back')],
    ])


async def _send_photo(bot: Bot, chat_id: str, image_path: str):
    with open(image_path, 'rb') as image_file:
        await bot.send_photo(chat_id=chat_id, photo=image_file)


def _send_links_message(bot: Bot, loop, chat_id: str, links_msg: str, reply_markup) -> None:
    try:
        loop.run_until_complete(_send_message(bot, chat_id, links_msg, reply_markup=reply_markup, disable_web_page_preview=True, parse_mode='HTML'))
    except TelegramError as exc:
        if 'parse' in str(exc).lower() or 'entities' in str(exc).lower():
            logger.warning('HTML parse error ao enviar links, fallback para texto puro | chat_id=%s | erro=%s', chat_id, exc)
            plain = re.sub(r'<[^>]+>', '', links_msg)
            loop.run_until_complete(_send_message(bot, chat_id, plain, reply_markup=reply_markup, disable_web_page_preview=True))
        else:
            raise


def user_label(user_row) -> str:
    first_name = str(user_row['first_name'] or '').strip()
    username = str(user_row['username'] or '').strip()
    chat_id = str(user_row['chat_id'])
    if username:
        username = f'@{username.lstrip("@")} '
    else:
        username = ''
    if first_name:
        return f'{first_name} | {username}{chat_id}'.strip()
    return f'{username}{chat_id}'.strip()



def _vendor_filter_label(filters: dict, show_result_type_filters: bool = True) -> str:
    return '🛫 Filtro: Companhias aéreas'


def _scan_failed_by_executor_timeout(rows: list[dict]) -> bool:
    if not rows:
        return False
    timeout_rows = 0
    priced_rows = 0
    for row in rows:
        if isinstance(row.get('price'), (int, float)):
            priced_rows += 1
        notes = str(row.get('notes') or '').lower()
        if 'executor timeout' in notes or 'timeout na página' in notes:
            timeout_rows += 1
    return timeout_rows > 0 and priced_rows == 0


def run_for_user(conn, bot: Bot, loop, user_id: int, chat_id: str, max_price: float, sources: dict, airline_filters_json: str | None = None) -> tuple[bool, str, int]:
    access = ensure_user_access(conn, chat_id)
    charge_now = should_charge_user(conn, chat_id, access) and not is_active_access(access)
    if charge_now:
        free_uses = int(access['free_uses'] or 0)
        free_uses_limit = get_free_uses_limit(conn)
        if free_uses >= free_uses_limit:
            logger.info('[bot-scheduler] chat_id=%s | sem envio agendado | acesso insuficiente', chat_id)
            return False, 'bloqueado_por_monetizacao', 0

    routes = _build_user_routes(conn, user_id, prune_expired=True)
    if not routes:
        return False, 'sem_rotas_ativas', 0

    filters = parse_airline_filters(airline_filters_json)
    show_result_type_filters = should_show_result_type_filters(conn)
    sources_with_filter = dict(sources)

    parsed = run_scan_for_routes(routes, sources=sources_with_filter, allow_agencies=False, skip_booking=False)
    parsed = expand_rows_by_result_type(parsed, airline_filters_json, show_result_type_filters=show_result_type_filters)
    filtered = filter_rows_by_max_price(parsed, max_price)
    filtered = normalize_rows_for_airline_priority(filtered, airline_filters_json)
    filtered = filter_rows_with_vendor(filtered)
    filtered = filter_rows_by_airlines(filtered, airline_filters_json, show_result_type_filters=show_result_type_filters)
    should_split = False
    result_type = None
    filtered = _merge_rows_for_combined_result_view(filtered) if should_split else filtered
    if not filtered:
        no_result_reason = 'timeout_executor' if _scan_failed_by_executor_timeout(parsed) else 'sem_resultado_filtrado'
        if no_result_reason == 'timeout_executor':
            logger.warning('[bot-scheduler] chat_id=%s | scan sem resultado por timeout do executor', chat_id)
        loop.run_until_complete(_send_message(bot, chat_id, '⚠️ Nenhuma rota encontrada dentro dos seus filtros.', reply_markup=main_menu_markup()))
        if charge_now:
            conn.execute(
                sql(f"UPDATE user_access SET free_uses = free_uses + 1, updated_at = {now_expression()} WHERE chat_id = %s"),
                (chat_id,)
            )
            conn.commit()
        return False, no_result_reason, 0

    sent_count = 0
    if should_split:
        image_path = build_scan_results_image(filtered, trigger='agendada')
        if not image_path:
            return False, 'sem_imagem', len(filtered)
        try:
            loop.run_until_complete(_send_photo(bot, chat_id, image_path))
            links_msg = build_booking_links_message(filtered)
            if links_msg:
                _send_links_message(bot, loop, chat_id, links_msg, main_menu_markup())
            else:
                loop.run_until_complete(_send_message(bot, chat_id, '🏠 Toque abaixo para abrir o menu novamente.', reply_markup=main_menu_markup()))
            sent_count = len(filtered)
        finally:
            try:
                os.remove(image_path)
            except OSError:
                pass
    else:
        image_path = build_scan_results_image(filtered, trigger='agendada', result_type=result_type)
        if not image_path:
            return False, 'sem_imagem', len(filtered)
        try:
            loop.run_until_complete(_send_photo(bot, chat_id, image_path))
            links_msg = build_booking_links_message(filtered, result_type=result_type)
            if links_msg:
                _send_links_message(bot, loop, chat_id, links_msg, main_menu_markup())
            else:
                loop.run_until_complete(_send_message(bot, chat_id, '🏠 Toque abaixo para abrir o menu novamente.', reply_markup=main_menu_markup()))
            sent_count = len(filtered)
        finally:
            try:
                os.remove(image_path)
            except OSError:
                pass
    if charge_now:
        conn.execute(
            sql(f"UPDATE user_access SET free_uses = free_uses + 1, updated_at = {now_expression()} WHERE chat_id = %s"),
            (chat_id,)
        )
        conn.commit()
    return True, 'enviado', sent_count


_LAST_REPORT_PATH = Path('/tmp/vooindo_last_reported_round.txt')


def _last_reported_round() -> str | None:
    try:
        return _LAST_REPORT_PATH.read_text().strip() or None
    except (OSError, IOError):
        return None


def _mark_round_reported(label: str):
    try:
        _LAST_REPORT_PATH.write_text(label)
    except (OSError, IOError):
        pass


def _recover_missed_report(conn, bot, loop):
    """Na inicialização, verifica se há jobs de rodada completa sem relatório enviado."""
    now = now_local()
    interval = get_scan_interval_seconds(conn)
    
    # Pega a última rodada completa (todos os jobs done/error) nas últimas 3h
    for h in range(1, 4):
        round_start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=h)
        round_end = round_start + timedelta(seconds=interval)
        
        jobs = conn.execute(sql("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done,
                   SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS erro,
                   SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
                   SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending
            FROM scan_jobs
            WHERE job_type = 'scheduled'
              AND created_at >= %s AND created_at < %s
        """), (round_start.strftime('%Y-%m-%d %H:%M:%S'), round_end.strftime('%Y-%m-%d %H:%M:%S'))).fetchone()
        
        if not jobs or jobs['total'] == 0:
            continue
        if jobs['running'] > 0 or jobs['pending'] > 0:
            continue  # Ainda não completou
        
        # Rodada completa! Gerar e enviar relatório
        round_label = round_start.strftime('%H:%M')
        if _last_reported_round() == round_label:
            continue  # Já reportamos (persistido)
        
        logger.info('[bot-scheduler][recovery] recuperando relatório perdido para rodada %s', round_label)
        
        # Buscar job_ids da rodada
        job_rows = conn.execute(sql("""
            SELECT id FROM scan_jobs
            WHERE job_type = 'scheduled'
              AND created_at >= %s AND created_at < %s
            ORDER BY id
        """), (round_start.strftime('%Y-%m-%d %H:%M:%S'), round_end.strftime('%Y-%m-%d %H:%M:%S'))).fetchall()
        job_ids = [int(r['id']) for r in job_rows]
        
        if not job_ids:
            continue
        
        # Sincroniza stats
        wait_result = {
            'complete': True,
            'elapsed_seconds': 0,
            'counts': {
                'done': int(jobs['done']),
                'error': int(jobs['erro']),
                'running': 0,
                'pending': 0,
            }
        }
        cycle_stats = {
            'eligible_users': 0,
            'sent_users': 0,
            'skipped_users': 0,
            'errors': 0,
            'reasons': {},
        }
        
        report_text = _build_round_report(round_start.isoformat(), 0, cycle_stats, job_ids, wait_result)
        admin_chat_id = os.getenv('TELEGRAM_ADMIN_CHAT_ID', '').strip()
        if admin_chat_id and report_text:
            try:
                loop.run_until_complete(_send_message(bot, admin_chat_id, report_text))
                logger.info('[bot-scheduler][recovery] relatório rodada %s enviado ✅', round_label)
            except Exception as exc:
                logger.warning('[bot-scheduler][recovery] erro ao enviar relatório: %s', exc)
        
        _mark_round_reported(round_label)
        break  # Só envia o mais recente


def sleep_until_next_slot(interval_seconds: int):
    now = now_local()
    next_slot = now.replace(minute=0, second=0, microsecond=0) + timedelta(seconds=interval_seconds)
    if interval_seconds < 3600:
        elapsed_in_hour = now.minute * 60 + now.second
        next_offset = ((elapsed_in_hour // interval_seconds) + 1) * interval_seconds
        next_slot = now.replace(minute=0, second=0, microsecond=0) + timedelta(seconds=next_offset)
    wait_seconds = (next_slot - now).total_seconds()
    time.sleep(max(1, wait_seconds))


def _is_chat_not_found(exc: Exception) -> bool:
    msg = str(exc).lower()
    return 'chat not found' in msg or 'forbidden' in msg or 'bot was blocked' in msg or 'user is deactivated' in msg


def _mark_user_blocked(conn, chat_id: str) -> None:
    conn.execute(sql("UPDATE bot_users SET blocked = 1 WHERE chat_id = %s"), (chat_id,))
    conn.commit()
    logger.warning('[bot-scheduler] chat_id=%s marcado como bloqueado (Chat not found)', chat_id)
    audit.system("usuario_bloqueado_automatico", chat_id=chat_id, status="blocked",
                 payload={"motivo": "chat_not_found"})


def _append_cycle_metrics(entry: dict) -> None:
    try:
        _METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _METRICS_PATH.open('a', encoding='utf-8') as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as exc:
        logger.warning('[bot-scheduler] falha ao persistir métricas do ciclo | erro=%s', exc)


def _wait_for_round_completion(job_ids: list[int], timeout_seconds: int = _ROUND_REPORT_TIMEOUT_SECONDS, poll_seconds: int = _ROUND_REPORT_POLL_SECONDS) -> dict:
    if not job_ids:
        return {'complete': True, 'counts': {'done': 0, 'error': 0, 'running': 0, 'pending': 0}, 'elapsed_seconds': 0}

    started = time.time()
    placeholders = ', '.join(['%s'] * len(job_ids))
    counts = {'done': 0, 'error': 0, 'running': 0, 'pending': 0}
    while True:
        conn = None
        try:
            conn = get_db()
            row = conn.execute(sql(f"""
                SELECT
                  SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done_count,
                  SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_count,
                  SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_count,
                  SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_count
                FROM scan_jobs
                WHERE id IN ({placeholders})
            """), tuple(job_ids)).fetchone()
            counts = {
                'done': int((row['done_count'] if isinstance(row, dict) else row[0]) or 0),
                'error': int((row['error_count'] if isinstance(row, dict) else row[1]) or 0),
                'running': int((row['running_count'] if isinstance(row, dict) else row[2]) or 0),
                'pending': int((row['pending_count'] if isinstance(row, dict) else row[3]) or 0),
            }
            if counts['running'] == 0 and counts['pending'] == 0:
                return {'complete': True, 'counts': counts, 'elapsed_seconds': round(time.time() - started, 1)}
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

        if time.time() - started >= timeout_seconds:
            return {'complete': False, 'counts': counts, 'elapsed_seconds': round(time.time() - started, 1)}
        time.sleep(max(1, poll_seconds))


def _build_round_report(cycle_started_iso: str, cycle_duration_ms: int, cycle_stats: dict, job_ids: list[int], wait_result: dict | None = None) -> str:
    if not job_ids:
        reasons = cycle_stats.get('reasons', {}) or {}
        lines = [
            f"📊 RELATÓRIO DA RODADA — {cycle_started_iso[:16]}",
            '',
            '📋 RESUMO',
            f"  👥 Elegíveis: {cycle_stats.get('eligible_users', 0)}",
            '  📭 Nenhum job foi criado nesta rodada',
        ]
        if reasons:
            lines.append('')
            lines.append('⏭ IGNORADOS')
            for motivo, qtd in sorted(reasons.items(), key=lambda x: -x[1]):
                lines.append(f"  {motivo}: {qtd}")
        return '\n'.join(lines)

    conn_report = get_db()
    try:
        placeholders = ', '.join(['%s'] * len(job_ids))
        params = tuple(job_ids)
        try:
            import psutil as _psutil
            mem = _psutil.virtual_memory()
            cpu_pct = _psutil.cpu_percent(interval=0.5)
            load_avg = os.getloadavg()
            proc = _psutil.Process()
            proc_mem = proc.memory_info().rss / 1024 / 1024
        except Exception:
            mem = cpu_pct = proc_mem = None
            load_avg = (0, 0, 0)

        job_stats = conn_report.execute(sql(f"""
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done,
              SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS erro,
              SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
              SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
              ROUND(AVG(CASE WHEN finished_at IS NOT NULL AND started_at IS NOT NULL
                  THEN TIMESTAMPDIFF(SECOND, started_at, finished_at) END), 1) AS avg_duration_s,
              ROUND(COALESCE(SUM(cost_score), 0), 0) AS total_cost,
              MIN(created_at) AS min_created,
              MAX(finished_at) AS max_finished
            FROM scan_jobs
            WHERE id IN ({placeholders})
        """), params).fetchone()

        received = conn_report.execute(sql(f"""
            SELECT bu.user_id, bu.first_name,
                   ROUND(SUM(TIMESTAMPDIFF(SECOND, j.started_at, j.finished_at)), 0) as total_dur
            FROM scan_jobs j
            JOIN bot_users bu ON bu.user_id = j.user_id
            WHERE j.id IN ({placeholders}) AND j.status = 'done'
            GROUP BY bu.user_id, bu.first_name
            ORDER BY total_dur DESC
        """), params).fetchall()

        # Erros com rota específica (do payload JSON)
        erros = conn_report.execute(sql(f"""
            SELECT bu.user_id, bu.first_name,
                   COALESCE(MAX(j.error_message), 'erro') AS erro,
                   GROUP_CONCAT(DISTINCT
                       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(j.payload, '$.route.origin')), '?'),
                       '-',
                       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(j.payload, '$.route.destination')), '?')
                       SEPARATOR ', '
                   ) AS rotas_erro,
                   COUNT(*) AS qtd_erro
            FROM scan_jobs j
            JOIN bot_users bu ON bu.user_id = j.user_id
            WHERE j.id IN ({placeholders}) AND j.status = 'error'
            GROUP BY bu.user_id, bu.first_name
            ORDER BY bu.first_name
        """), params).fetchall()

        # Get route counts from payload JSON and user_routes
        all_uid_ids = set()
        for r in received: all_uid_ids.add(int(r['user_id']))
        for r in erros: all_uid_ids.add(int(r['user_id']))
        user_payload_routes = {}  # routes sent in payload
        user_active_routes = {}   # routes in user_routes table
        if all_uid_ids:
            uid_list_str = ','.join(str(u) for u in all_uid_ids)
            try:
                payload_sql = sql(f"""SELECT j.user_id, COALESCE(JSON_LENGTH(JSON_EXTRACT(j.payload, '$.routes')), 0) as cnt FROM scan_jobs j WHERE j.id IN ({placeholders}) AND j.user_id IN ({uid_list_str})""")
                payload_rows = conn_report.execute(payload_sql, params).fetchall()
                for row in payload_rows:
                    user_payload_routes[int(row['user_id'])] = int(row['cnt'])
            except Exception as e:
                logger.warning('report_cycle: erro ao contar rotas do payload: %s', e, exc_info=True)
            try:
                for row in conn_report.execute(sql(f"""SELECT user_id, COUNT(*) as c FROM user_routes WHERE user_id IN ({uid_list_str}) AND active=1 GROUP BY user_id""")).fetchall():
                    user_active_routes[int(row['user_id'])] = int(row['c'])
            except: pass

        reasons = cycle_stats.get('reasons', {}) or {}
        lines = []
        total_users = len(received) + len(erros)
        total_routes = sum(
            user_payload_routes.get(int(r['user_id']), user_active_routes.get(int(r['user_id']), 0))
            for r in (received or []) + (erros or [])
        )
        def _fmt_dur(s):
            s = int(s)
            return f'{s//60}m{s%60}s' if s >= 60 else f'{s}s'
        avg_dur = _fmt_dur(job_stats['avg_duration_s'] or 0)
        lines.append(f"📊 RODADA {cycle_started_iso[11:16]}")
        lines.append(f'✅ {job_stats["done"]}/{job_stats["total"]} | ❌ {job_stats["erro"]}')
        round_s = int(wait_result.get('elapsed_seconds', 0))
        lines.append(f'⏱ {round_s//60}m{round_s%60}s  📍 {total_users} users | {total_routes} rotas | {avg_dur}/rota')
        lines.append('')
        lines.append('📋 USUÁRIOS')
        for r in received:
            uid = int(r['user_id'])
            name = (r['first_name'] or '---').split()[0][:12]
            dur = _fmt_dur(r['total_dur'])
            total = user_payload_routes.get(uid, user_active_routes.get(uid, 0))
            rf = ''
            if total > 1:
                rf = f' {total}/{total}r'
            lines.append(f'  ✅ {name}  ⏱{dur}{rf}')
        for r in erros:
            uid = int(r['user_id'])
            name = (r['first_name'] or '---').split()[0][:12]
            err = str(r['erro'] or 'erro')[:18]
            icon = '⚠️' if 'stale' in err or 'timeout' in err else '❌'
            total = user_payload_routes.get(uid, user_active_routes.get(uid, 0))
            qtd_erro = int(r['qtd_erro'])
            rf = ''
            if total > 1:
                done = total - qtd_erro
                rf = f' {done}/{total}r'
            rota_info = ''
            if r.get('rotas_erro'):
                rota_info = f'  🗺️ {r["rotas_erro"]}'[:40]
            lines.append(f'  {icon} {name}  {err}{rf}{rota_info}')
        lines.append('')
        lines.append('⚙️')
        if cpu_pct is not None:
            lines.append(f'  CPU {cpu_pct}% | RAM {round(proc_mem,0)}MB')
        if mem:
            lines.append(f"  💾 RAM total: {round(mem.used/1024/1024, 0)}/{round(mem.total/1024/1024, 0)}GB ({mem.percent}%)")
        if reasons:
            for motivo, qtd in sorted(reasons.items(), key=lambda x: -x[1]):
                lines.append(f'  ⏭ {motivo}: {qtd}')

        if wait_result and not wait_result.get('complete', True):
            lines.append('')
            lines.append(f"⚠️ Relatório gerado por timeout de espera ({wait_result.get('elapsed_seconds', 0)}s).")

        return '\n'.join(lines)
    finally:
        conn_report.close()


async def _send_admin_alert(bot: Bot, message: str):
    admin_chat_id = os.getenv('TELEGRAM_ADMIN_CHAT_ID', '').strip()
    if not admin_chat_id:
        return
    try:
        await bot.send_message(chat_id=admin_chat_id, text=message)
    except Exception as exc:
        logger.warning('[ALERT_ADMIN][SCHEDULER] Falha ao enviar alerta admin do scheduler | erro=%s', exc)


def main():
    if not TOKEN:
        raise SystemExit('Defina TELEGRAM_BOT_TOKEN no .env')

    request = HTTPXRequest(connection_pool_size=50, pool_timeout=60.0, connect_timeout=30.0, read_timeout=60.0, write_timeout=60.0)
    bot = Bot(token=TOKEN, request=request)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    first_cycle = True
    conn = None
    while True:
        interval_seconds = max(60, max(1, _SCAN_INTERVAL_MINUTES) * 60)
        try:
            if conn is None:
                conn = get_db()
                ensure_policy_schema(conn)
            interval_seconds = get_scan_interval_seconds(conn)
        except DatabaseRateLimitError as exc:
            audit.error("scheduler_db_limit", error_msg=str(exc), status="blocked")
            logger.warning('[bot-scheduler] limite de conexão MySQL por hora atingido: %s', exc)
            time.sleep(1800)
            continue

        if first_cycle:
            first_cycle = False
            # Recuperar jobs órfãos na inicialização (scheduler/workers anteriores morreram)
            try:
                # Resetar jobs 'running' presos há mais de 5 minutos (workers morreram)
                stuck_running = conn.execute(
                    sql("""
                        UPDATE scan_jobs
                        SET status = 'pending', started_at = NULL, retry_count = COALESCE(retry_count, 0) + 1
                        WHERE status = 'running'
                          AND started_at IS NOT NULL
                          AND started_at < DATE_SUB(NOW(), INTERVAL 5 MINUTE)
                          AND job_type = 'scheduled'
                    """)
                )
                stuck_count = getattr(conn.cursor(), 'rowcount', 0)
                
                # Resetar jobs mortos por SIGTERM (error='143') — restart matou o worker
                stuck_143 = conn.execute(
                    sql("""
                        UPDATE scan_jobs
                        SET status = 'pending', started_at = NULL, finished_at = NULL,
                            error_message = NULL, retry_count = COALESCE(retry_count, 0) + 1
                        WHERE status = 'error' AND error_message = '143'
                          AND finished_at >= DATE_SUB(NOW(), INTERVAL 2 HOUR)
                          AND job_type = 'scheduled'
                    """)
                )
                stuck_143_count = getattr(conn.cursor(), 'rowcount', 0)
                if stuck_143_count > 0:
                    conn.commit()
                    logger.info(
                        "[bot-scheduler] resetados %s jobs mortos por SIGTERM (143) para 'pending'",
                        stuck_143_count,
                    )
                if stuck_count > 0:
                    conn.commit()
                    logger.info(
                        "[bot-scheduler] resetados %s jobs 'running' presos para 'pending'",
                        stuck_count,
                    )

                # Recuperar relatório de rodada perdida (scheduler reiniciou antes de enviar)
                _recover_missed_report(conn, bot, loop)

                # Verificar se há jobs pendentes órfãos
                orphan_count = conn.execute(
                    sql("SELECT COUNT(*) AS c FROM scan_jobs WHERE status = 'pending' AND job_type = 'scheduled'")
                ).fetchone()
                orphan_count = int((orphan_count['c'] if isinstance(orphan_count, dict) else orphan_count[0]) or 0)
                if orphan_count > 0:
                    logger.info(
                        "[bot-scheduler] detectados %s jobs pendentes órfãos na inicialização — executando ciclo imediato",
                        orphan_count,
                    )
                    # Pula o sleep e vai direto para o ciclo
                else:
                    logger.info(
                        "[bot-scheduler] iniciado em %s, aguardando primeiro slot de %ss",
                        now_local_iso(sep='T'),
                        interval_seconds,
                    )
                    sleep_until_next_slot(interval_seconds)
            except Exception as exc:
                logger.warning("[bot-scheduler] erro ao recuperar jobs órfãos: %s", exc)
                sleep_until_next_slot(interval_seconds)

        try:
            if conn is None:
                conn = get_db()
                ensure_policy_schema(conn)
            cycle_started = time.perf_counter()
            cycle_started_iso = now_local_iso(sep='T')
            cycle_metrics = record_cycle_start()
            cycle_metrics['_start_time'] = time.time()
            maintenance_on = is_maintenance_mode(conn)
            users = list(iter_users(conn))
            random.shuffle(users)
            cycle_stats = {
                'eligible_users': len(users),
                'sent_users': 0,
                'sent_results': 0,
                'no_send_users': 0,
                'skipped_users': 0,
                'errors': 0,
                'reasons': {},
                'shuffled_users': True,
            }
            # --- PARALELIZAÇÃO: Filtrar elegíveis e distribuir no ThreadPool ---
            eligible_users = []
            for user in users:
                try:
                    label = user_label(user)
                    if maintenance_on and not is_exempt_from_maintenance(conn, str(user['chat_id'])):
                        cycle_stats['skipped_users'] += 1
                        cycle_stats['reasons']['manutencao'] = cycle_stats['reasons'].get('manutencao', 0) + 1
                        logger.info("[bot-scheduler] %s | ignorado | modo manutenção ativo", label)
                        continue
                    if not bool(int(user['alerts_enabled'])):
                        cycle_stats['skipped_users'] += 1
                        cycle_stats['reasons']['alertas_desativados'] = cycle_stats['reasons'].get('alertas_desativados', 0) + 1
                        logger.info("[bot-scheduler] %s | ignorado | alertas desativados", label)
                        continue
                    # Pular usuários sem rotas ativas
                    route_count_row = conn.execute(
                        sql("SELECT COUNT(*) AS c FROM user_routes WHERE user_id = %s AND active = 1"),
                        (int(user['user_id']),),
                    ).fetchone()
                    route_count = int((route_count_row['c'] if isinstance(route_count_row, dict) else route_count_row[0]) or 0)
                    if route_count == 0:
                        cycle_stats['skipped_users'] += 1
                        cycle_stats['reasons']['sem_rotas'] = cycle_stats['reasons'].get('sem_rotas', 0) + 1
                        logger.info("[bot-scheduler] %s | ignorado | sem rotas ativas", label)
                        continue
                    user_cooldown_seconds = 30 * 60
                    running_row = conn.execute(
                        sql("SELECT COUNT(*) AS c FROM scan_jobs WHERE user_id = %s AND status IN ('pending', 'running')"),
                        (int(user['user_id']),),
                    ).fetchone()
                    running_count = int((running_row['c'] if isinstance(running_row, dict) else running_row[0]) or 0)
                    if running_count > 0:
                        cycle_stats['skipped_users'] += 1
                        cycle_stats['reasons']['execucao_em_andamento'] = cycle_stats['reasons'].get('execucao_em_andamento', 0) + 1
                        logger.info("[bot-scheduler] %s | ignorado | execucao em andamento", label)
                        continue
                    if was_sent_recently(str(user.get('last_scheduled_sent_at') or user['last_sent_at']), window_seconds=user_cooldown_seconds):
                        cycle_stats['skipped_users'] += 1
                        cycle_stats['reasons']['cooldown'] = cycle_stats['reasons'].get('cooldown', 0) + 1
                        logger.info("[bot-scheduler] %s | ignorado | cooldown ativo | last_sent_at=%s", label, user['last_sent_at'])
                        continue
                    eligible_users.append(user)
                except Exception:
                    pass

            # --- DELEGAR PARA JOB_WORKERS (1 job POR ROTA) ---
            # Cria jobs individuais para cada rota ativa de cada usuário.
            # Workers processam rotas individuais e um consolidador junta os
            # resultados do mesmo usuário quando todas as rotas terminarem.
            created_job_ids = []
            for user in eligible_users:
                try:
                    label = user_label(user)
                    chat_id = str(user['chat_id'])
                    user_id = int(user['user_id'])
                    
                    # Buscar rotas ativas do usuário
                    route_rows = conn.execute(
                        sql("SELECT id, origin, destination, outbound_date, inbound_date FROM user_routes WHERE user_id = %s AND active = 1"),
                        (user_id,)
                    ).fetchall()
                    
                    if not route_rows:
                        logger.info("[bot-scheduler] %s | sem rotas ativas, pulando", label)
                        cycle_stats['skipped_users'] += 1
                        continue
                    
                    group_key = f"round_{user_id}_{cycle_started_iso}"
                    num_routes = len(route_rows)
                    
                    for route in route_rows:
                        route_id = route['id'] if isinstance(route, dict) else route[0]
                        origin = route['origin'] if isinstance(route, dict) else route[1]
                        destination = route['destination'] if isinstance(route, dict) else route[2]
                        outbound_date = route['outbound_date'] if isinstance(route, dict) else route[3]
                        inbound_date = route['inbound_date'] if isinstance(route, dict) else route[4] or ''
                        
                        payload = json.dumps({
                            'round_started_at': cycle_started_iso,
                            'route': {
                                'id': route_id,
                                'origin': origin,
                                'destination': destination,
                                'outbound_date': outbound_date,
                                'inbound_date': inbound_date,
                            },
                            'group_info': {
                                'total_routes': num_routes,
                                'label': label,
                            }
                        }, ensure_ascii=False)
                        
                        insert_result = conn.execute(
                            sql("INSERT INTO scan_jobs (user_id, chat_id, job_type, status, payload, cost_score, group_key) VALUES (%s, %s, 'scheduled', 'pending', %s, %s, %s)"),
                            (user_id, chat_id, payload, 1, group_key),
                        )
                        conn.commit()
                        job_id = int(getattr(insert_result, 'lastrowid', 0) or 0)
                        if not job_id:
                            last_id_row = conn.execute(sql("SELECT LAST_INSERT_ID() AS id")).fetchone()
                            job_id = int((last_id_row['id'] if isinstance(last_id_row, dict) else last_id_row[0]) or 0)
                        if job_id:
                            created_job_ids.append(job_id)
                        
                    logger.info("[bot-scheduler] %s | %s jobs de rota criados (group=%s)", label, num_routes, group_key)
                    cycle_stats['sent_users'] += 1
                except Exception as exc:
                    logger.error("[bot-scheduler] erro ao criar job para user %s: %s", user.get('user_id'), exc)
                    cycle_stats['errors'] += 1

            logger.info('[bot-scheduler] %s jobs de rota delegados para job_workers', len(created_job_ids))
        except DatabaseRateLimitError as exc:
            audit.error("scheduler_db_limit", error_msg=str(exc), status="blocked")
            logger.warning('[SCHED_DB_LIMIT] [bot-scheduler] limite de conexão MySQL por hora atingido durante ciclo: %s', exc)
            try:
                loop.run_until_complete(_send_admin_alert(
                    bot,
                    f"🚨 Limite de conexão no banco do scheduler\n\nErro: {str(exc)[:500]}",
                ))
            except Exception:
                pass
            time.sleep(1800)
            continue

        cycle_duration_ms = round((time.perf_counter() - cycle_started) * 1000)
        cycle_finished_iso = now_local_iso(sep='T')
        metrics_entry = {
            'cycle_started_at': cycle_started_iso,
            'cycle_finished_at': cycle_finished_iso,
            'duration_ms': cycle_duration_ms,
            'eligible_users': cycle_stats['eligible_users'],
            'sent_users': cycle_stats['sent_users'],
            'sent_results': cycle_stats['sent_results'],
            'no_send_users': cycle_stats['no_send_users'],
            'skipped_users': cycle_stats['skipped_users'],
            'errors': cycle_stats['errors'],
            'shuffled_users': cycle_stats['shuffled_users'],
            'reasons': cycle_stats['reasons'],
        }
        # Registra métricas no monitor de ciclos
        scan_results = {
            'duration_seconds': round(cycle_duration_ms / 1000, 1),
            'eligible_users': cycle_stats['eligible_users'],
            'sent_users': cycle_stats['sent_users'],
            'skipped_users': cycle_stats['skipped_users'],
            'errors': cycle_stats['errors'],
            'reasons': cycle_stats['reasons'],
        }
        record_cycle_end(cycle_metrics, scan_results=scan_results)
        _append_cycle_metrics(metrics_entry)
        logger.info(
            "[bot-scheduler] ciclo concluído em %s | duracao_ms=%s | elegiveis=%s | enviaram=%s | sem_envio=%s | ignorados=%s | erros=%s | reasons=%s | aguardando próximo slot de %ss",
            cycle_finished_iso,
            cycle_duration_ms,
            cycle_stats['eligible_users'],
            cycle_stats['sent_users'],
            cycle_stats['no_send_users'],
            cycle_stats['skipped_users'],
            cycle_stats['errors'],
            json.dumps(cycle_stats['reasons'], ensure_ascii=False, sort_keys=True),
            interval_seconds,
        )

        # Relatório automático para admin após o término real da rodada
        try:
            wait_result = _wait_for_round_completion(created_job_ids)
            logger.info(
                '[bot-scheduler] rodada %s finalizada | complete=%s | done=%s | error=%s | running=%s | pending=%s | wait_s=%s',
                cycle_started_iso[:16],
                wait_result.get('complete', True),
                wait_result.get('counts', {}).get('done', 0),
                wait_result.get('counts', {}).get('error', 0),
                wait_result.get('counts', {}).get('running', 0),
                wait_result.get('counts', {}).get('pending', 0),
                wait_result.get('elapsed_seconds', 0),
            )
            admin_chat_id = os.getenv('TELEGRAM_ADMIN_CHAT_ID', '').strip()
            if admin_chat_id:
                report_text = _build_round_report(cycle_started_iso, cycle_duration_ms, cycle_stats, created_job_ids, wait_result)
                loop.run_until_complete(_send_message(bot, admin_chat_id, report_text))
        except Exception as exc:
            logger.warning('[bot-scheduler] erro ao enviar relatorio admin: %s', exc)

        try:
            sleep_until_next_slot(interval_seconds)
        except Exception as exc:
            logger.error('[bot-scheduler] erro no sleep_until_next_slot: %s', exc, exc_info=True)
            time.sleep(60)
            continue


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        logger.critical('[bot-scheduler] CRASH não tratado no main(): %s', exc, exc_info=True)
        raise
