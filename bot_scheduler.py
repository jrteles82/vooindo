import json
import os
import re
import subprocess
import sys
import time
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
import random

from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import TelegramError
from telegram.request import HTTPXRequest

from app_logging import get_logger

from access_policy import (
    ensure_policy_schema,
    ensure_user_access,
    get_free_uses_limit,
    is_active_access,
    is_exempt_from_maintenance,
    should_charge_user as ap_should_charge_user,
    is_maintenance_mode,
)
from config import TOKEN, now_local, now_local_iso
from audit import audit
from db import connect as connect_db, now_expression, sql, DatabaseRateLimitError
from main import _build_user_routes, build_scan_results_image, build_booking_links_message, run_scan_for_routes, filter_rows_by_max_price, filter_rows_with_vendor, normalize_rows_for_airline_priority, expand_rows_by_result_type, _merge_rows_for_combined_result_view
from bot import filter_rows_by_airlines, parse_airline_filters, should_show_result_type_filters
from cycle_monitor import record_cycle_start, record_cycle_end
from route_optimizer import compute_priorities, log_cycle_result
from dry_run_utils import build_route_job_payload, parse_job_payload

# Número de workers paralelos para scheduler
_NUM_SCHED_WORKERS = int(os.getenv('NUM_SCHED_WORKERS', '4'))

logger = get_logger('bot_scheduler')

_SCAN_INTERVAL_MINUTES = int(os.getenv("SCAN_INTERVAL_MINUTES", "60"))
_DEFAULT_SEND_COOLDOWN_SECONDS = 30 * 60
SEND_COOLDOWN_SECONDS = int(
    os.getenv("SCHEDULER_SEND_COOLDOWN_SECONDS", str(_DEFAULT_SEND_COOLDOWN_SECONDS))
)
_METRICS_PATH = Path(__file__).resolve().parent / 'logs' / 'scheduler_cycle_metrics.jsonl'
_ROUND_REPORT_TIMEOUT_SECONDS = int(os.getenv('SCHEDULER_ROUND_REPORT_TIMEOUT_SECONDS', '18000'))
_ROUND_REPORT_POLL_SECONDS = int(os.getenv('SCHEDULER_ROUND_REPORT_POLL_SECONDS', '5'))
MAX_ROUTE_ADVANCE_DAYS = 330


def _route_days_ahead(outbound_date: str, date_type: str = 'fixed') -> int | None:
    if str(date_type or 'fixed') != 'fixed':
        return None
    outbound = str(outbound_date or '').strip()
    if not outbound:
        return None
    try:
        dt = datetime.strptime(outbound[:10], '%Y-%m-%d').date()
    except Exception:
        return None
    return (dt - now_local().date()).days


def _is_route_beyond_advance_limit(outbound_date: str, date_type: str = 'fixed') -> bool:
    days = _route_days_ahead(outbound_date, date_type)
    return days is not None and days > MAX_ROUTE_ADVANCE_DAYS


def _is_route_in_past(outbound_date: str, date_type: str = 'fixed') -> bool:
    if str(date_type or 'fixed') != 'fixed':
        return False
    days = _route_days_ahead(outbound_date, date_type)
    return days is not None and days <= 0  # data <= hoje não tem resultado no Google Flights


def _format_skipped_route_label(origin: str, destination: str, outbound_date: str, date_type: str = 'fixed') -> str:
    days = _route_days_ahead(outbound_date, date_type)
    suffix = f' ({days} dias)' if days is not None else ''
    return f'{str(origin).upper()} → {str(destination).upper()} | {outbound_date}{suffix}'


def _notify_skipped_distant_routes(bot: Bot, loop, chat_id: str, skipped_routes: list[str]) -> None:
    if not skipped_routes:
        return
    shown = skipped_routes[:8]
    extra = len(skipped_routes) - len(shown)
    lines = '\n'.join(f'• {item}' for item in shown)
    if extra > 0:
        lines += f'\n• ... e mais {extra}'
    text = (
        f'⚠️ Algumas rotas não entraram na consulta desta rodada porque estão a mais de {MAX_ROUTE_ADVANCE_DAYS} dias.\n\n'
        f'{lines}\n\n'
        'Quando a data ficar dentro da janela permitida, elas voltam a ser consultadas automaticamente.'
    )
    try:
        loop.run_until_complete(_send_message(bot, chat_id, text, reply_markup=main_menu_markup()))
    except Exception as exc:
        logger.warning('[bot-scheduler] falha avisando rotas >%s dias para chat_id=%s: %s', MAX_ROUTE_ADVANCE_DAYS, chat_id, exc)


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
               COALESCE(bs.airline_filters_json, '') AS airline_filters_json,
               bs.scan_interval_minutes
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
    from urllib.parse import urlparse
    import pymysql
    import pymysql.cursors
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


_LAST_REPORT_PATH = Path(__file__).resolve().parent / 'logs' / 'last_round_reported.txt'


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
    """Envia relatório de rodada completa que ficou sem envio após restart.

    O scheduler normalmente espera a rodada terminar e manda o relatório. Se o
    serviço reinicia durante essa espera, ninguém fica responsável por mandar o
    relatório quando os workers terminam. Esta recuperação varre rodadas recentes
    completas e envia a mais nova ainda não marcada.
    """
    last_reported = _last_reported_round()
    rows = conn.execute(sql("""
        SELECT
          created_at,
          COUNT(*) AS total,
          SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done,
          SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS erro,
          SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
          SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending
        FROM scan_jobs
        WHERE job_type = 'scheduled'
          AND created_at >= DATE_SUB(NOW(), INTERVAL 12 HOUR)
        GROUP BY created_at
        HAVING total > 0 AND running = 0 AND pending = 0
        ORDER BY created_at DESC
        LIMIT 6
    """)).fetchall()

    for jobs in rows:
        round_start = jobs['created_at'] if isinstance(jobs, dict) else jobs[0]
        if not isinstance(round_start, datetime):
            round_start = datetime.fromisoformat(str(round_start))
        round_key = round_start.strftime('%Y-%m-%dT%H:%M')
        legacy_label = round_start.strftime('%H:%M')
        if last_reported in {round_key, legacy_label}:
            break

        logger.info('[bot-scheduler][recovery] recuperando relatório perdido para rodada %s', round_key)
        job_rows = conn.execute(sql("""
            SELECT id FROM scan_jobs
            WHERE job_type = 'scheduled' AND created_at = %s
            ORDER BY id
        """), (round_start.strftime('%Y-%m-%d %H:%M:%S'),)).fetchall()
        job_ids = [int(r['id'] if isinstance(r, dict) else r[0]) for r in job_rows]
        if not job_ids:
            continue

        wait_result = {
            'complete': True,
            'elapsed_seconds': _actual_round_elapsed_seconds(conn, job_ids, 0),
            'counts': {
                'done': int((jobs['done'] if isinstance(jobs, dict) else jobs[2]) or 0),
                'error': int((jobs['erro'] if isinstance(jobs, dict) else jobs[3]) or 0),
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
        try:
            metrics_path = Path(__file__).resolve().parent / 'logs' / 'scheduler_cycle_metrics.jsonl'
            if metrics_path.exists():
                for line in metrics_path.read_text().splitlines():
                    try:
                        metric = json.loads(line)
                    except Exception:
                        continue
                    if str(metric.get('cycle_started_at', '')).startswith(round_key):
                        cycle_stats = metric
        except Exception as exc:
            logger.warning('[bot-scheduler][recovery] falha ao carregar métricas da rodada %s: %s', round_key, exc)

        admin_chat_id = os.getenv('TELEGRAM_ADMIN_CHAT_ID', '').strip()
        report_text = _build_round_report(round_start.isoformat(), int(cycle_stats.get('duration_ms') or 0), cycle_stats, job_ids, wait_result)
        if admin_chat_id and report_text:
            try:
                loop.run_until_complete(_send_message(bot, admin_chat_id, '📌 Relatório recuperado após restart:\n\n' + report_text))
                logger.info('[bot-scheduler][recovery] relatório rodada %s enviado ✅', round_key)
            except Exception as exc:
                logger.warning('[bot-scheduler][recovery] erro ao enviar relatório: %s', exc)
                return

        _mark_round_reported(round_key)
        break  # só envia o mais recente por ciclo


def sleep_until_next_slot(interval_seconds: int, check_session: bool = False):
    now = now_local()
    # Alinha na grade do dia começando em 01:00, não em 00:00.
    # Ex.: 90min => 01:00, 02:30, 04:00 ... 22:00, 23:30.
    day_start = now.replace(hour=1, minute=0, second=0, microsecond=0)
    if now < day_start:
        day_start -= timedelta(days=1)
    elapsed_today = int((now - day_start).total_seconds())
    interval = max(60, int(interval_seconds or 60))
    next_offset = ((elapsed_today // interval) + 1) * interval
    next_slot = day_start + timedelta(seconds=next_offset)
    wait_seconds = (next_slot - now).total_seconds()

    # Se é o sleep inicial (antes do primeiro ciclo), verificar sessão
    if check_session and wait_seconds > 0:
        session_check_time = max(1, wait_seconds - 900)  # 15 min antes
        logger.info('[bot-scheduler] agendando verificação de sessão Google em %d segundos', session_check_time)
        time.sleep(session_check_time)
        _check_google_session_and_notify()
        # Dorme o restante
        remaining = wait_seconds - session_check_time
        if remaining > 0:
            time.sleep(remaining)
    else:
        time.sleep(max(1, wait_seconds))


def _check_google_session_and_notify():
    """Verifica sessão Google 15 min antes da rodada. Tenta renovar automaticamente se inválida."""
    base_dir = Path(__file__).resolve().parent
    score_file = base_dir / 'check_google_session.py'

    def _session_score() -> int:
        """Retorna score via guardian (sem abrir Chrome próprio, evitando conflito de profile)."""
        try:
            import urllib.request, json as _json
            _req = urllib.request.urlopen('http://127.0.0.1:9230/status', timeout=5)
            _status = _json.loads(_req.read().decode())
            if bool(_status.get('session_ok', False)):
                return 3
            return 0
        except Exception:
            return 0

    def _renew_session() -> bool:
        """Tenta renovar a sessão Google via app password.
        Tenta Firefox primeiro (menos detectado), fallback Chrome.
        """
        app_password = 'Vooindo#8212'
        scripts = [
            ('Firefox', base_dir / 'google_login_firefox_stdin.py'),
            ('Chrome',  base_dir / 'google_login_stdin.py'),
        ]
        for name, script in scripts:
            if not script.exists():
                continue
            try:
                proc = subprocess.run(
                    [sys.executable, str(script), '--email', 'vooindo.bot@gmail.com'],
                    input=app_password + '\n',
                    capture_output=True, text=True, timeout=180,
                )
                success = 'AUTH_SCORE:1' in proc.stdout or 'AUTH_SCORE:2' in proc.stdout
                if success:
                    logger.info('[bot-scheduler] sessão Google renovada via %s', name)
                    return True
                logger.warning('[bot-scheduler] renovação %s falhou: %s', name, proc.stdout[-300:].strip())
            except Exception as e:
                logger.warning('[bot-scheduler] erro na renovação %s: %s', name, e)
        return False

    try:
        if not score_file.exists():
            return

        score = _session_score()
        logger.info('[bot-scheduler] sessão Google: %d/3', score)

        if score >= 3:
            return  # tudo ok

        logger.warning('[bot-scheduler] sessão Google %d/3 — tentando renovar...', score)
        if _renew_session():
            new_score = _session_score()
            if new_score >= 3:
                logger.info('[bot-scheduler] renovação ok — sessão %d/3', new_score)
            else:
                logger.warning('[bot-scheduler] renovação não restaurou sessão (ainda %d/3)', new_score)
        else:
            logger.warning('[bot-scheduler] renovação automática falhou')

        # Notifica admin se sessão inválida
        try:
            from bot import send_message_sync
            send_message_sync('1748352987',
                '⚠️ *Sessão Google expirada - renovação automática falhou*\n\n'
                'O bot tentou renovar automaticamente mas não conseguiu.\n'
                'Renove manualmente com o comando /renovar_sessao')
        except Exception:
            pass
    except Exception as exc:
        logger.warning('[bot-scheduler] erro ao verificar/renovar sessão Google: %s', exc)


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


_METRO_CODES = {'SAO', 'RIO', 'BHZ'}
_BR_CODES_TIMEOUT = {
    'AJU','BEL','BHZ','BSB','BVB','CGB','CGH','CGR','CNF','CWB',
    'FLN','FOR','GIG','GRU','IGU','IOS','JOI','JPA','LDB','MAO',
    'MCZ','MGF','NAT','NVT','PET','POA','PVH','RAO','REC','SDU',
    'SJP','SLZ','SSA','STM','THE','UDI','VCP','VIX','SAO','RIO'
}


def _adaptive_route_timeout_seconds(route_info: dict, base_timeout: int | None = None) -> int:
    origin = str((route_info or {}).get('origin') or '').upper().strip()
    destination = str((route_info or {}).get('destination') or '').upper().strip()
    outbound = str((route_info or {}).get('outbound_date') or '').strip()
    is_metro = origin in _METRO_CODES or destination in _METRO_CODES
    is_international = bool(origin and destination and (origin not in _BR_CODES_TIMEOUT or destination not in _BR_CODES_TIMEOUT))
    timeout = int(base_timeout or 300)
    if is_international and is_metro:
        timeout = max(timeout, 780)
    elif is_international:
        timeout = max(timeout, 660)
    elif is_metro:
        timeout = max(timeout, 540)
    if outbound.startswith('2027'):
        timeout += 120
    return min(max(timeout, 300), 900)


def _dynamic_round_timeout_seconds(conn, job_ids: list[int]) -> int:
    """Calcula timeout do relatório pela rodada real.

    Evita relatório 52/56 quando a fila está grande, mas não deixa o admin
    esperando indefinidamente em caso de rota travada.
    """
    if not job_ids:
        return 0
    try:
        placeholders = ', '.join(['%s'] * len(job_ids))
        row = conn.execute(sql(f"""
            SELECT
              COUNT(*) AS total_jobs,
              COUNT(DISTINCT user_id) AS total_users,
              COUNT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(payload, '$.dedupe_key'))) AS unique_routes
            FROM scan_jobs
            WHERE id IN ({placeholders})
        """), tuple(job_ids)).fetchone()
        total_jobs = int((row['total_jobs'] if isinstance(row, dict) else row[0]) or len(job_ids))
        unique_routes = int((row['unique_routes'] if isinstance(row, dict) else row[2]) or total_jobs)
    except Exception:
        total_jobs = len(job_ids)
        unique_routes = len(job_ids)

    # Workers scheduled ativos. Fallback conservador: 2 (run_all sobe 2 workers scheduled).
    # Não usar `pgrep -fc`: ele conta o próprio shell/comando de inspeção e superestima
    # a capacidade, encurtando o timeout do relatório (ex.: rodada 07:00 fechou parcial).
    scheduled_workers = 2
    try:
        import subprocess as _subprocess
        ps = _subprocess.run(
            ['pgrep', '-af', r'/opt/vooindo/job_worker.py'],
            capture_output=True, text=True, timeout=5,
        )
        active = 0
        for line in (ps.stdout or '').splitlines():
            if 'job_worker.py' in line and '--pool scheduled' in line:
                if 'pgrep' in line or '/bin/sh' in line or '/bin/bash' in line:
                    continue
                active += 1
        if active > 0:
            scheduled_workers = active
    except Exception:
        pass

    # Histórico recente de jobs agendados concluídos. Usa p75-ish via média + margem
    # para não ser refém de outlier único, com fallback 180s.
    avg_job_s = 180.0
    try:
        row = conn.execute(sql("""
            SELECT AVG(TIMESTAMPDIFF(SECOND, started_at, finished_at)) AS avg_s
            FROM scan_jobs
            WHERE job_type='scheduled'
              AND status='done'
              AND started_at IS NOT NULL
              AND finished_at IS NOT NULL
              AND finished_at >= DATE_SUB(NOW(), INTERVAL 6 HOUR)
        """)).fetchone()
        val = (row['avg_s'] if isinstance(row, dict) else row[0]) if row else None
        if val:
            # Não cortar agressivamente: com booking obrigatório, rotas reais têm passado
            # de 8min. Cap baixo faz a rodada reportar timeout falso e acumular fila.
            avg_job_s = max(90.0, min(900.0, float(val)))
    except Exception:
        pass

    waves = max(1, (max(1, unique_routes) + scheduled_workers - 1) // scheduled_workers)
    # Estimativa = ondas * média recente + margem para rotas metropolitanas/retries internos.
    estimate = int(waves * avg_job_s + 900)
    # Piso por onda: mesmo quando a média recente parece baixa, cada onda real pode ocupar
    # vários minutos por causa de Google/booking/retries. Sem esse piso a rodada 07:00 de
    # 2026-06-28 fechou relatório em 86/109 enquanto 23 jobs legítimos ainda processavam.
    queue_floor = int(waves * 300 + 900)
    # Piso 30min, teto configurável (default 5h). Com 2 workers scheduled e booking
    # obrigatório, rodadas grandes podem passar de 4h sem falha real.
    return max(1800, min(_ROUND_REPORT_TIMEOUT_SECONDS, max(estimate, queue_floor)))


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


def _actual_round_elapsed_seconds(conn, job_ids: list[int], fallback_seconds: float | int = 0) -> int:
    """Tempo real entre criação da rodada e último job finalizado.

    Usado no relatório admin quando a rodada termina depois do timeout inicial:
    evita mostrar o tempo do timeout antigo e remove alerta falso de timeout.
    """
    if not job_ids:
        return int(fallback_seconds or 0)
    try:
        placeholders = ', '.join(['%s'] * len(job_ids))
        row = conn.execute(sql(f"""
            SELECT TIMESTAMPDIFF(SECOND, MIN(created_at), MAX(finished_at)) AS elapsed_s
            FROM scan_jobs
            WHERE id IN ({placeholders})
              AND finished_at IS NOT NULL
        """), tuple(job_ids)).fetchone()
        val = (row['elapsed_s'] if isinstance(row, dict) else row[0]) if row else None
        if val is not None:
            return int(val)
    except Exception:
        pass
    return int(fallback_seconds or 0)


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
                payload_sql = sql(f"""SELECT j.user_id, COALESCE(JSON_EXTRACT(j.payload, '$.group_info.total_routes'), 0) as cnt FROM scan_jobs j WHERE j.id IN ({placeholders}) AND j.user_id IN ({uid_list_str})""")
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


def _scheduled_backlog_counts(conn) -> dict:
    """Jobs agendados ainda abertos de rodadas anteriores.

    Se houver pending/running quando um novo slot começa, criar outra rodada só
    aumenta o congestionamento e faz jobs antigos expirarem sem nunca rodar.
    """
    try:
        row = conn.execute(sql("""
            SELECT
              SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
              SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_count,
              MIN(created_at) AS oldest_created_at
            FROM scan_jobs
            WHERE job_type = 'scheduled'
              AND status IN ('pending', 'running')
        """)).fetchone()
        pending = int((row['pending_count'] if isinstance(row, dict) else row[0]) or 0) if row else 0
        running = int((row['running_count'] if isinstance(row, dict) else row[1]) or 0) if row else 0
        oldest = (row['oldest_created_at'] if isinstance(row, dict) else row[2]) if row else None
        return {'pending': pending, 'running': running, 'total': pending + running, 'oldest_created_at': str(oldest or '')}
    except Exception as exc:
        logger.warning('[bot-scheduler] falha ao calcular backlog agendado: %s', exc)
        return {'pending': 0, 'running': 0, 'total': 0, 'oldest_created_at': ''}


def main():
    import asyncio

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

                # Recuperar relatório de rodada perdida após restart do scheduler.
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
                    sleep_until_next_slot(interval_seconds, check_session=True)
            except Exception as exc:
                logger.warning("[bot-scheduler] erro ao recuperar jobs órfãos: %s", exc)
                sleep_until_next_slot(interval_seconds, check_session=True)

        try:
            if conn is None:
                conn = get_db()
                ensure_policy_schema(conn)
            cycle_started = time.perf_counter()
            cycle_started_iso = now_local_iso(sep='T')
            cycle_metrics = record_cycle_start()
            cycle_metrics['_start_time'] = time.time()
            cycle_stats = {
                'duration_seconds': 0.0,
                'eligible_users': 0,
                'sent_users': 0,
                'skipped_users': 0,
                'errors': 0,
                'reasons': {},
            }

            # Verificar sessão do guardian — só inicia a rodada se estiver OK
            try:
                import urllib.request, json as _json
                _req = urllib.request.urlopen('http://127.0.0.1:9230/status', timeout=5)
                _status = _json.loads(_req.read().decode())
                if not _status.get('session_ok', False):
                    logger.warning('[bot-scheduler] sessão Google INVÁLIDA (session_ok=false). Pulando rodada.')
                    cycle_stats['errors'] += 1
                    cycle_stats['reasons']['sessao_invalida'] = 1
                    record_cycle_end(cycle_metrics)
                    _append_cycle_metrics(cycle_stats)
                    sleep_until_next_slot(interval_seconds, check_session=True)
                    continue
                logger.info('[bot-scheduler] sessão Google OK, iniciando rodada')
            except Exception as _e:
                logger.warning('[bot-scheduler] erro ao verificar sessão guardian: %s. Pulando rodada.', _e)
                cycle_stats['errors'] += 1
                cycle_stats['reasons']['sessao_indisponivel'] = 1
                record_cycle_end(cycle_metrics)
                _append_cycle_metrics(cycle_stats)
                sleep_until_next_slot(interval_seconds, check_session=True)
                continue

            backlog = _scheduled_backlog_counts(conn)
            if backlog.get('total', 0) > 0:
                logger.warning(
                    '[bot-scheduler] rodada %s pulada por backlog anterior | pending=%s running=%s oldest=%s',
                    cycle_started_iso[:16],
                    backlog.get('pending', 0),
                    backlog.get('running', 0),
                    backlog.get('oldest_created_at', ''),
                )
                cycle_stats['skipped_users'] += 1
                cycle_stats['reasons']['backlog_rodada_anterior'] = int(backlog.get('total', 0) or 0)
                record_cycle_end(cycle_metrics, scan_results={
                    'duration_seconds': round(time.perf_counter() - cycle_started, 1),
                    'eligible_users': 0,
                    'sent_users': 0,
                    'skipped_users': 1,
                    'errors': 0,
                    'reasons': cycle_stats['reasons'],
                })
                _append_cycle_metrics({
                    'cycle_started_at': cycle_started_iso,
                    'cycle_finished_at': now_local_iso(sep='T'),
                    'duration_ms': round((time.perf_counter() - cycle_started) * 1000),
                    'eligible_users': 0,
                    'sent_users': 0,
                    'sent_results': 0,
                    'no_send_users': 0,
                    'skipped_users': 1,
                    'errors': 0,
                    'reasons': cycle_stats['reasons'],
                })
                try:
                    loop.run_until_complete(_send_admin_alert(
                        bot,
                        '⏸ Rodada pulada por backlog anterior\n\n'
                        f"Ainda há {backlog.get('running', 0)} running e {backlog.get('pending', 0)} pending. "
                        'Vou deixar os workers esvaziarem a fila antes de criar novos jobs.'
                    ))
                except Exception:
                    pass
                sleep_until_next_slot(interval_seconds, check_session=True)
                continue

            maintenance_on = is_maintenance_mode(conn)
            users = list(iter_users(conn))
            # Otimizar ordem: rápidos primeiro, timeout personalizado por user
            try:
                priorities = compute_priorities(conn)
                opt_metrics = priorities['metrics']
                opt_notes = priorities['optimizations']
                # Reordenar users com base na prioridade calculada
                priority_map = {str(u['user_id']): u for u in priorities['user_priorities']}
                users.sort(key=lambda u: priority_map.get(str(u['user_id']), {}).get('priority_score', 9999))
                for opt in opt_notes:
                    logger.info('[route-optimizer] %s', opt)
                logger.info('[route-optimizer] prioridades calculadas | users=%s | media=%.0fs | otimizacoes=%s',
                            opt_metrics['users_count'], opt_metrics['avg_cycle_dur'], len(opt_notes))
            except Exception as _opt_err:
                logger.warning('[route-optimizer] erro ao calcular prioridades, mantendo ordem original: %s', _opt_err)
                random.shuffle(users)
            cycle_stats = {
                'eligible_users': len(users),
                'sent_users': 0,
                'sent_results': 0,
                'no_send_users': 0,
                'skipped_users': 0,
                'errors': 0,
                'reasons': {},
                'shuffled_users': False,
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
                    # Cooldown só pra quem fez scan manual recentemente
                    # Scans automáticos não entram em cooldown (o intervalo de rodada já regula)
                    if was_sent_recently(str(user.get('last_manual_sent_at') or ''), window_seconds=user_cooldown_seconds):
                        cycle_stats['skipped_users'] += 1
                        cycle_stats['reasons']['cooldown'] = cycle_stats['reasons'].get('cooldown', 0) + 1
                        logger.info("[bot-scheduler] %s | ignorado | cooldown ativo | last_sent_at=%s", label, user['last_sent_at'])
                        continue
                    # Per-user interval check: usuário com intervalo personalizado
                    # deve ser baseado na última RODADA CRIADA/enfileirada, não na
                    # hora da mensagem enviada. Se usar last_scheduled_sent_at, um
                    # usuário de 180 min numa grade global de 90 min acaba entrando
                    # só a cada ~270 min, porque a mensagem chega depois do slot.
                    admin_interval_seconds = interval_seconds
                    user_interval_min = user.get('scan_interval_minutes')
                    if user_interval_min is not None:
                        user_interval_s = max(admin_interval_seconds, int(user_interval_min) * 60)
                        last_round = ''
                        try:
                            _last_round_row = conn.execute(
                                sql("""
                                    SELECT MAX(created_at) AS last_round_at
                                    FROM scan_jobs
                                    WHERE user_id = %s
                                      AND job_type = 'scheduled'
                                      AND group_key LIKE %s
                                      AND group_key NOT LIKE '%%_retry_%%'
                                """),
                                (int(user['user_id']), f"round_{int(user['user_id'])}_%"),
                            ).fetchone()
                            if _last_round_row:
                                last_round = str((_last_round_row.get('last_round_at') if isinstance(_last_round_row, dict) else _last_round_row[0]) or '')
                        except Exception as _last_round_err:
                            logger.warning("[bot-scheduler] %s | falha ao ler última rodada criada: %s", label, _last_round_err)

                        # Fallback só para usuários sem histórico de jobs.
                        last_sched_ref = last_round or str(user.get('last_scheduled_sent_at') or '')
                        if last_sched_ref:
                            try:
                                dt = datetime.fromisoformat(last_sched_ref.replace(' ', 'T'))
                                delta = (now_local() - dt).total_seconds()
                            except Exception:
                                delta = user_interval_s + 1  # força processar se data inválida
                            if delta < user_interval_s:
                                cycle_stats['skipped_users'] += 1
                                cycle_stats['reasons']['intervalo_personalizado'] = cycle_stats['reasons'].get('intervalo_personalizado', 0) + 1
                                logger.info(
                                    "[bot-scheduler] %s | ignorado | intervalo=%s min | delta=%.0f min | ultima_rodada=%s | ultimo_envio=%s",
                                    label, user_interval_min, delta/60, last_round[:16], str(user.get('last_scheduled_sent_at') or '')[:16]
                                )
                                continue
                    eligible_users.append(user)
                except Exception:
                    pass

            def _enqueue_dedupe_key(route_dict: dict) -> str:
                payload = {
                    'v': 2,
                    'origin': str(route_dict.get('origin') or '').upper(),
                    'destination': str(route_dict.get('destination') or '').upper(),
                    'outbound_date': str(route_dict.get('outbound_date') or ''),
                    'inbound_date': str(route_dict.get('inbound_date') or ''),
                    'date_type': str(route_dict.get('date_type') or 'fixed'),
                    'flexible_month': str(route_dict.get('flexible_month') or ''),
                    'mode': 'scheduled_no_agencies',
                }
                raw = json.dumps(payload, sort_keys=True, ensure_ascii=True)
                return hashlib.sha256(raw.encode('utf-8')).hexdigest()

            def _add_dedupe_key_to_payload(payload_raw: str, dedupe_key: str) -> str:
                try:
                    payload_data = json.loads(payload_raw or '{}')
                    if isinstance(payload_data, dict):
                        payload_data['dedupe_key'] = dedupe_key
                        return json.dumps(payload_data, ensure_ascii=False)
                except Exception:
                    pass
                return payload_raw

            # --- DELEGAR PARA JOB_WORKERS (1 job POR ROTA) ---
            # Cria jobs individuais para cada rota ativa de cada usuário.
            # Workers processam rotas individuais e um consolidador junta os
            # resultados do mesmo usuário quando todas as rotas terminarem.
            created_job_ids = []
            route_dedupe_seen: set[str] = set()
            route_dedupe_waiting = 0
            for user in eligible_users:
                try:
                    label = user_label(user)
                    chat_id = str(user['chat_id'])
                    user_id = int(user['user_id'])
                    
                    # Buscar rotas ativas do usuário
                    route_rows = conn.execute(
                        sql("SELECT id, origin, destination, outbound_date, inbound_date, date_type, trip_type, flexible_month FROM user_routes WHERE user_id = %s AND active = 1"),
                        (user_id,)
                    ).fetchall()
                    
                    if not route_rows:
                        logger.info("[bot-scheduler] %s | sem rotas ativas, pulando", label)
                        cycle_stats['skipped_users'] += 1
                        continue
                    
                    skipped_distant_routes = []
                    expired_route_ids = []
                    expired_route_labels = []
                    queryable_route_count = 0
                    for route in route_rows:
                        _origin = route['origin'] if isinstance(route, dict) else route[1]
                        _destination = route['destination'] if isinstance(route, dict) else route[2]
                        _outbound_date = route['outbound_date'] if isinstance(route, dict) else route[3]
                        _date_type = route.get('date_type', 'fixed') if isinstance(route, dict) else (route[5] if len(route) > 5 else 'fixed')
                        if _is_route_in_past(_outbound_date, _date_type):
                            _route_id = route['id'] if isinstance(route, dict) else route[0]
                            expired_route_ids.append(_route_id)
                            expired_route_labels.append(_format_skipped_route_label(_origin, _destination, _outbound_date, _date_type))
                        elif _is_route_beyond_advance_limit(_outbound_date, _date_type):
                            skipped_distant_routes.append(_format_skipped_route_label(_origin, _destination, _outbound_date, _date_type))
                        else:
                            queryable_route_count += 1

                    if expired_route_ids:
                        # Desativa as rotas vencidas
                        conn.execute(sql(f"UPDATE user_routes SET active = 0 WHERE id IN ({','.join(['%s']*len(expired_route_ids))})"), tuple(expired_route_ids))
                        conn.commit()
                        # Notifica o usuário
                        _expired_lines = '\n'.join(f'  ❌ {l}' for l in expired_route_labels)
                        _msg = f'🗑️ *Rotas removidas por data vencida:*\n{_expired_lines}\n\nEssas datas já passaram e não há resultados no Google Flights.'
                        try:
                            loop.run_until_complete(bot.send_message(chat_id=chat_id, text=_msg, parse_mode='Markdown'))
                        except Exception:
                            pass
                        cycle_stats['reasons']['rota_data_vencida'] = cycle_stats['reasons'].get('rota_data_vencida', 0) + len(expired_route_ids)
                        logger.info("[bot-scheduler] %s | %s rota(s) vencida(s) removidas", label, len(expired_route_ids))
                    if skipped_distant_routes:
                        _notify_skipped_distant_routes(bot, loop, chat_id, skipped_distant_routes)
                        cycle_stats['reasons']['rota_acima_330_dias'] = cycle_stats['reasons'].get('rota_acima_330_dias', 0) + len(skipped_distant_routes)
                        logger.info("[bot-scheduler] %s | %s rota(s) pulada(s) por >%s dias", label, len(skipped_distant_routes), MAX_ROUTE_ADVANCE_DAYS)

                    if queryable_route_count == 0:
                        logger.info("[bot-scheduler] %s | todas as rotas estão acima de %s dias, nenhum job criado", label, MAX_ROUTE_ADVANCE_DAYS)
                        cycle_stats['skipped_users'] += 1
                        continue

                    group_key = f"round_{user_id}_{cycle_started_iso}"
                    num_routes = queryable_route_count
                    
                    for route in route_rows:
                        route_id = route['id'] if isinstance(route, dict) else route[0]
                        origin = route['origin'] if isinstance(route, dict) else route[1]
                        destination = route['destination'] if isinstance(route, dict) else route[2]
                        outbound_date = route['outbound_date'] if isinstance(route, dict) else route[3]
                        inbound_date = route['inbound_date'] if isinstance(route, dict) else route[4] or ''
                        date_type = route.get('date_type', 'fixed') if isinstance(route, dict) else (route[5] if len(route) > 5 else 'fixed')
                        trip_type = route.get('trip_type', 'one-way') if isinstance(route, dict) else (route[6] if len(route) > 6 else 'one-way')
                        flexible_month = route.get('flexible_month', '') if isinstance(route, dict) else (route[7] if len(route) > 7 else '')
                        if _is_route_in_past(outbound_date, date_type):
                            continue
                        if _is_route_beyond_advance_limit(outbound_date, date_type):
                            continue
                        
                        route_payload = {
                            'id': route_id,
                            'origin': origin,
                            'destination': destination,
                            'outbound_date': outbound_date,
                            'inbound_date': inbound_date,
                            'date_type': date_type or 'fixed',
                            'trip_type': trip_type or 'one-way',
                            'flexible_month': flexible_month or '',
                        }

                        # Timeout adaptativo por rota (mantém prioridade do optimizer como base,
                        # mas aumenta para metropolitana/internacional/futura).
                        _executor_timeout = 300  # fallback padrão
                        try:
                            _user_timeout = priorities['user_timeouts'].get(str(user_id))
                            if _user_timeout:
                                _executor_timeout = int(_user_timeout)
                        except Exception:
                            pass
                        _executor_timeout = _adaptive_route_timeout_seconds(route_payload, _executor_timeout)
                        
                        dedupe_key = _enqueue_dedupe_key(route_payload)
                        job_status = 'pending'
                        if dedupe_key in route_dedupe_seen:
                            # Não ocupa worker/Chrome: o job primário da mesma rota vai copiar
                            # o resultado para este job e consolidar o usuário.
                            job_status = 'waiting_route_dedupe'
                            route_dedupe_waiting += 1
                        else:
                            route_dedupe_seen.add(dedupe_key)

                        payload = build_route_job_payload(
                            cycle_started_iso=cycle_started_iso,
                            route=route_payload,
                            total_routes=num_routes,
                            label=label,
                            executor_timeout=_executor_timeout,
                        )
                        payload = _add_dedupe_key_to_payload(payload, dedupe_key)
                        
                        insert_result = conn.execute(
                            sql("INSERT INTO scan_jobs (user_id, chat_id, job_type, status, payload, cost_score, group_key) VALUES (%s, %s, 'scheduled', %s, %s, %s, %s)"),
                            (user_id, chat_id, job_status, payload, 1, group_key),
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

            logger.info('[bot-scheduler] %s jobs de rota delegados para job_workers | dedupe_waiting=%s | unique_route_keys=%s', len(created_job_ids), route_dedupe_waiting, len(route_dedupe_seen))
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
        # Registra resultado do ciclo no otimizador
        try:
            log_cycle_result(conn)
        except Exception as _log_err:
            logger.warning('[route-optimizer] erro ao logar resultado do ciclo: %s', _log_err)
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
            dynamic_timeout = _dynamic_round_timeout_seconds(conn, created_job_ids)
            logger.info('[bot-scheduler] rodada %s | timeout dinâmico relatório=%ss', cycle_started_iso[:16], dynamic_timeout)
            wait_result = _wait_for_round_completion(created_job_ids, timeout_seconds=dynamic_timeout)
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
            
            # Se o relatório bateu timeout mas ainda há jobs originais rodando,
            # não abre retry agora. Isso evita duplicar rota ainda viva e evita
            # relatório admin tipo 52/56 enquanto a rodada fecha logo depois.
            if not wait_result.get('complete', True) and (
                wait_result.get('counts', {}).get('running', 0) > 0
                or wait_result.get('counts', {}).get('pending', 0) > 0
            ):
                # Se ainda existem jobs originais vivos, continuar esperando até o teto
                # absoluto do relatório, em vez de fechar parcial após só +15min.
                # Isso evita relatório admin 86/109 enquanto a fila legítima termina depois.
                remaining_wait = max(0, _ROUND_REPORT_TIMEOUT_SECONDS - int(wait_result.get('elapsed_seconds', 0) or 0))
                extra_wait = _wait_for_round_completion(created_job_ids, timeout_seconds=remaining_wait)
                logger.info(
                    '[bot-scheduler] rodada %s | espera extra após timeout | complete=%s | done=%s | error=%s | running=%s | pending=%s | wait_s=%s',
                    cycle_started_iso[:16],
                    extra_wait.get('complete', True),
                    extra_wait.get('counts', {}).get('done', 0),
                    extra_wait.get('counts', {}).get('error', 0),
                    extra_wait.get('counts', {}).get('running', 0),
                    extra_wait.get('counts', {}).get('pending', 0),
                    extra_wait.get('elapsed_seconds', 0),
                )
                if extra_wait.get('complete', False):
                    wait_result = extra_wait

            original_jobs_open = (
                not wait_result.get('complete', True)
                and (
                    wait_result.get('counts', {}).get('running', 0) > 0
                    or wait_result.get('counts', {}).get('pending', 0) > 0
                )
            )
            if original_jobs_open:
                logger.info(
                    '[bot-scheduler] rodada %s | retry adiado: ainda há jobs originais abertos | running=%s | pending=%s',
                    cycle_started_iso[:16],
                    wait_result.get('counts', {}).get('running', 0),
                    wait_result.get('counts', {}).get('pending', 0),
                )

            # --- RETRY: jobs com erro na rodada principal ---
            MAX_RETRIES = 3
            current_ids = list(created_job_ids)
            conn_retry = get_db()
            try:
                if original_jobs_open:
                    logger.info('[bot-scheduler] rodada %s | retry pulado até a rodada original fechar', cycle_started_iso[:16])
                else:
                    for retry_num in range(1, MAX_RETRIES + 1):
                        # Buscar jobs com erro (done + error_message)
                        placeholders = ', '.join(['%s'] * len(current_ids)) if current_ids else 'NULL'
                        if not current_ids:
                            break
                        errored = conn_retry.execute(sql(f'''
                            SELECT j.id, j.user_id, j.chat_id, j.payload, bu.first_name
                            FROM scan_jobs j
                            JOIN bot_users bu ON bu.user_id = j.user_id
                            WHERE j.id IN ({placeholders})
                              AND j.status IN ('done', 'error')
                              AND (j.error_message IS NOT NULL AND j.error_message != '')
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM scan_job_route_results rr
                                  WHERE rr.job_id = j.id
                                    AND rr.num_results > 0
                                    AND rr.result_data REGEXP '"price"[[:space:]]*:[[:space:]]*[0-9]'
                              )
                        '''), tuple(current_ids)).fetchall()
                    
                        if not errored:
                            logger.info('[bot-scheduler] rodada %s | sem erros p/ retry', cycle_started_iso[:16])
                            break
                    
                        # Agrupar por user_id
                        users_to_retry = {}
                        for er in errored:
                            uid = int(er['user_id'])
                            if uid not in users_to_retry:
                                users_to_retry[uid] = {
                                    'chat_id': str(er['chat_id']),
                                    'first_name': er['first_name'],
                                    'routes': set(),
                                }
                            # Extrair rota do payload
                            try:
                                pay = parse_job_payload(er['payload'])
                                route = pay.get('route', {})
                                users_to_retry[uid]['routes'].add((
                                    route.get('id', 0),
                                    route.get('origin',''),
                                    route.get('destination',''),
                                    route.get('outbound_date',''),
                                    route.get('inbound_date','') or '',
                                    route.get('date_type','') or 'fixed',
                                    route.get('flexible_month','') or '',
                                    route.get('trip_type','') or 'one-way',
                                ))
                                if pay.get('dry_run'):
                                    users_to_retry[uid]['dry_run'] = True
                            except Exception:
                                pass
                    
                        logger.info(
                            '[bot-scheduler] rodada %s | retry #%s: %s usuários | %s rotas',
                            cycle_started_iso[:16], retry_num, len(users_to_retry),
                            sum(len(u['routes']) for u in users_to_retry.values()),
                        )
                    
                        # Criar jobs de retry
                        retry_job_ids = []
                        for uid, info in users_to_retry.items():
                            group_key = f"round_{uid}_{cycle_started_iso}_retry_{retry_num}"
                            for route_tuple in info['routes']:
                                route_id, origin, dest, outbound, inbound, date_type, flexible_month, trip_type = route_tuple
                                payload = build_route_job_payload(
                                    cycle_started_iso=cycle_started_iso,
                                    route={
                                        'id': route_id or 0,
                                        'origin': origin,
                                        'destination': dest,
                                        'outbound_date': outbound,
                                        'inbound_date': inbound,
                                        'date_type': date_type or 'fixed',
                                        'flexible_month': flexible_month or '',
                                        'trip_type': trip_type or 'one-way',
                                    },
                                    total_routes=len(info['routes']),
                                    label=info['first_name'],
                                    executor_timeout=480,
                                    retry=retry_num,
                                    dry_run=bool(info.get('dry_run')),
                                )
                                insert_result = conn_retry.execute(
                                    sql("INSERT INTO scan_jobs (user_id, chat_id, job_type, status, payload, cost_score, group_key) VALUES (%s, %s, 'scheduled', 'pending', %s, %s, %s)"),
                                    (uid, info['chat_id'], payload, 1, group_key),
                                )
                                conn_retry.commit()
                                jid = int(getattr(insert_result, 'lastrowid', 0) or 0)
                                if jid:
                                    retry_job_ids.append(jid)
                    
                        if not retry_job_ids:
                            break
                    
                        # Aguardar retry
                        retry_result = _wait_for_round_completion(retry_job_ids, timeout_seconds=600)
                        logger.info(
                            '[bot-scheduler] rodada %s | retry #%s finalizado | done=%s | error=%s | total_s=%s',
                            cycle_started_iso[:16], retry_num,
                            retry_result.get('counts', {}).get('done', 0),
                            retry_result.get('counts', {}).get('error', 0),
                            retry_result.get('elapsed_seconds', 0),
                        )
                        current_ids = retry_job_ids
            finally:
                try:
                    conn_retry.close()
                except Exception:
                    pass
            
            # Reconsulta o status imediatamente antes do relatório. Se a rodada fechou
            # enquanto retries/esperas internas rodavam, remove o alerta falso de timeout.
            final_wait_result = _wait_for_round_completion(created_job_ids, timeout_seconds=1, poll_seconds=1)
            if final_wait_result.get('complete', False):
                final_wait_result['elapsed_seconds'] = _actual_round_elapsed_seconds(
                    conn, created_job_ids, wait_result.get('elapsed_seconds', 0)
                )
                wait_result = final_wait_result

            admin_chat_id = os.getenv('TELEGRAM_ADMIN_CHAT_ID', '').strip()
            if admin_chat_id:
                report_text = _build_round_report(cycle_started_iso, cycle_duration_ms, cycle_stats, created_job_ids, wait_result)
                loop.run_until_complete(_send_message(bot, admin_chat_id, report_text))
                _mark_round_reported(cycle_started_iso[:16])
        except Exception as exc:
            logger.warning('[bot-scheduler] erro ao enviar relatorio admin: %s', exc)

        # Retry rotas internacionais que falharam (sem booking_url ou no_results)
        # Roda UMA de cada vez, sem concorrência, entre as rodadas
        try:
            _retry_international_routes(conn, int(cycle_started_iso[11:13]), int(interval_seconds / 60))
        except Exception as exc:
            logger.warning('[bot-scheduler] erro no retry internacional: %s', exc)

        try:
            sleep_until_next_slot(interval_seconds, check_session=True)
        except Exception as exc:
            logger.error('[bot-scheduler] erro no sleep_until_next_slot: %s', exc, exc_info=True)
            time.sleep(60)
            continue


def _retry_international_routes(conn, cycle_hour: int, interval_minutes: int):
    """Re-executa rotas internacionais que falharam na última rodada.
    Roda UMA de cada vez, sem concorrência de Chrome, entre as rodadas."""
    import json as _json, subprocess as _sp, sys as _sys, os as _os
    from datetime import datetime as _dt, timedelta as _td

    # Busca rotas internacionais da última rodada com 0 resultados ou sem booking_url
    cur = conn.cursor()
    cur.execute(sql("""
        SELECT sjr.job_id, sjr.origin, sjr.destination, sjr.group_key,
               sj.user_id, bu.chat_id, sj.payload
        FROM scan_job_route_results sjr
        JOIN scan_jobs sj ON sj.id = sjr.job_id
        JOIN bot_users bu ON bu.user_id = sj.user_id
        WHERE sjr.finished_at >= NOW() - INTERVAL %s MINUTE
          AND sjr.num_results = 0
          AND sjr.origin NOT IN ('PVH','FOR','GRU','CGH','SDU','REC','NAT','PMW','THE','CGR','CWB','CGB','MAO','SLZ')
        LIMIT 5
    """), (max(interval_minutes, 10),))
    failed_routes = cur.fetchall()

    if not failed_routes:
        return

    logger.info('[bot-scheduler] retry_international: %d rotas internacionais falharam, reprocessando...', len(failed_routes))

    executor = '/opt/vooindo/google_flights_executor.py'
    python = _sys.executable or '/opt/vooindo/.venv/bin/python3'

    for r in failed_routes:
        origin = r['origin']
        destination = r['destination']
        chat_id = r.get('chat_id', '')

        # Extrai data do payload
        outbound_date = ''
        try:
            p = _json.loads(str(r.get('payload') or '{}'))
            route_data = p.get('route', {})
            outbound_date = route_data.get('outbound_date', '')
        except Exception:
            pass

        if not origin or not destination or not outbound_date:
            continue

        # Ignora rotas rapidamente (domesticas conhecidas)
        _br_codes = {'PVH','FOR','GRU','CGH','SDU','REC','NAT','PMW','THE','CGR','CWB','CGB','MAO','SLZ','BEL','GIG','CGF'}
        if origin in _br_codes and destination in _br_codes:
            continue

        logger.info('[bot-scheduler] retry_international: %s->%s %s', origin, destination, outbound_date)

        env = _os.environ.copy()
        env['GOOGLE_FLIGHTS_USE_GUARDIAN'] = '1'
        env['GOOGLE_FLIGHTS_ALLOW_AGENCIES'] = '1'
        env['PYTHONWARNINGS'] = 'ignore::SyntaxWarning'

        cmd = [python, executor, origin, destination, outbound_date]

        try:
            proc = _sp.run(cmd, capture_output=True, text=True, timeout=360, env=env)
            if proc.stdout:
                result = _json.loads(proc.stdout)
                if result.get('ok') and result.get('price') is not None:
                    logger.info('[bot-scheduler] retry_international OK: %s->%s R$%s',
                                origin, destination, result['price'])
                else:
                    logger.info('[bot-scheduler] retry_international ainda sem resultado: %s->%s',
                                origin, destination)
        except Exception as e:
            logger.warning('[bot-scheduler] retry_international erro: %s->%s: %s',
                          origin, destination, e)

        # Delay entre retries para nao sobrecarregar o Chrome
        import time as _t
        _t.sleep(3.0)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        logger.critical('[bot-scheduler] CRASH não tratado no main(): %s', exc, exc_info=True)
        raise
