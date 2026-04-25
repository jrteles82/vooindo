import asyncio
import json
import os
import random
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

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

logger = get_logger('bot_scheduler')

_SCAN_INTERVAL_MINUTES = int(os.getenv("SCAN_INTERVAL_MINUTES", "60"))
_DEFAULT_SEND_COOLDOWN_SECONDS = 30 * 60
SEND_COOLDOWN_SECONDS = int(
    os.getenv("SCHEDULER_SEND_COOLDOWN_SECONDS", str(_DEFAULT_SEND_COOLDOWN_SECONDS))
)
_METRICS_PATH = Path(__file__).resolve().parent / 'logs' / 'scheduler_cycle_metrics.jsonl'


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
               COALESCE(bs.max_price, 1200) AS max_price,
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
    if send_type == 'manual':
        conn.execute(
            sql(f"UPDATE bot_settings SET last_sent_at = {now_expression()}, last_manual_sent_at = {now_expression()}, updated_at = {now_expression()} WHERE user_id = ?"),
            (user_id,),
        )
    else:
        conn.execute(
            sql(f"UPDATE bot_settings SET last_sent_at = {now_expression()}, last_scheduled_sent_at = {now_expression()}, updated_at = {now_expression()} WHERE user_id = ?"),
            (user_id,),
        )
    conn.commit()


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
    if not show_result_type_filters:
        return ''
    any_airline = bool(filters.get('any_airline', True))
    agencies = bool(filters.get('agencies', False))
    if any_airline and agencies:
        return '🛫🏪 Filtro: Companhias aéreas + Agências'
    if agencies:
        return '🏪 Filtro: Agências'
    return '🛫 Filtro: Companhias aéreas'


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
    should_split = not show_result_type_filters or (bool(filters.get('any_airline', True)) and bool(filters.get('agencies', False)))
    allow_agencies = True if should_split else bool(filters.get('agencies', False))
    sources_with_filter = dict(sources)
    sources_with_filter['allow_agencies'] = allow_agencies

    parsed = run_scan_for_routes(routes, sources=sources_with_filter)
    parsed = expand_rows_by_result_type(parsed, airline_filters_json, show_result_type_filters=show_result_type_filters)
    filtered = filter_rows_by_max_price(parsed, max_price)
    filtered = normalize_rows_for_airline_priority(filtered, airline_filters_json)
    filtered = filter_rows_with_vendor(filtered)
    filtered = filter_rows_by_airlines(filtered, airline_filters_json, show_result_type_filters=show_result_type_filters)
    filtered = _merge_rows_for_combined_result_view(filtered) if should_split else filtered
    if not filtered:
        loop.run_until_complete(_send_message(bot, chat_id, '⚠️ Nenhuma rota encontrada dentro dos seus filtros.', reply_markup=main_menu_markup()))
        if charge_now:
            conn.execute(
                sql(f"UPDATE user_access SET free_uses = free_uses + 1, updated_at = {now_expression()} WHERE chat_id = ?"),
                (chat_id,)
            )
            conn.commit()
        return False, 'sem_resultado_filtrado', 0

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
        result_type = 'agency' if bool(filters.get('agencies', False)) and not bool(filters.get('any_airline', True)) else 'airline'  # noqa: E501
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
            sql(f"UPDATE user_access SET free_uses = free_uses + 1, updated_at = {now_expression()} WHERE chat_id = ?"),
            (chat_id,)
        )
        conn.commit()
    return True, 'enviado', sent_count


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
    conn.execute(sql("UPDATE bot_users SET blocked = 1 WHERE chat_id = ?"), (chat_id,))
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
            logger.info(
                "[bot-scheduler] iniciado em %s, aguardando primeiro slot de %ss",
                now_local_iso(sep='T'),
                interval_seconds,
            )
            sleep_until_next_slot(interval_seconds)

        try:
            if conn is None:
                conn = get_db()
                ensure_policy_schema(conn)
            cycle_started = time.perf_counter()
            cycle_started_iso = now_local_iso(sep='T')
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
            for user in users:
                try:
                    label = user_label(user)
                    if maintenance_on and not is_exempt_from_maintenance(conn, str(user['chat_id'])):
                        cycle_stats['skipped_users'] += 1
                        cycle_stats['reasons']['manutencao'] = cycle_stats['reasons'].get('manutencao', 0) + 1
                        logger.info("[bot-scheduler] %s | ignorado | modo manutenção ativo", label)
                        audit.system("scheduler_usuario_ignorado", chat_id=str(user['chat_id']),
                                     user_id=str(user['user_id']), status="skipped",
                                     payload={"motivo": "manutencao"})
                        continue
                    if not bool(int(user['alerts_enabled'])):
                        cycle_stats['skipped_users'] += 1
                        cycle_stats['reasons']['alertas_desativados'] = cycle_stats['reasons'].get('alertas_desativados', 0) + 1
                        logger.info("[bot-scheduler] %s | ignorado | alertas desativados", label)
                        audit.system("scheduler_usuario_ignorado", chat_id=str(user['chat_id']),
                                     user_id=str(user['user_id']), status="skipped",
                                     payload={"motivo": "alertas_desativados"})
                        continue
                    user_cooldown_seconds = 30 * 60
                    running_row = conn.execute(
                        sql("SELECT COUNT(*) AS c FROM scan_jobs WHERE user_id = ? AND status IN ('pending', 'running')"),
                        (int(user['user_id']),),
                    ).fetchone()
                    running_count = int((running_row['c'] if isinstance(running_row, dict) else running_row[0]) or 0)
                    if running_count > 0:
                        cycle_stats['skipped_users'] += 1
                        cycle_stats['reasons']['execucao_em_andamento'] = cycle_stats['reasons'].get('execucao_em_andamento', 0) + 1
                        logger.info("[bot-scheduler] %s | ignorado | execucao em andamento", label)
                        audit.system("scheduler_usuario_ignorado", chat_id=str(user['chat_id']),
                                     user_id=str(user['user_id']), status="skipped",
                                     payload={"motivo": "execucao_em_andamento"})
                        continue
                    if was_sent_recently(str(user.get('last_scheduled_sent_at') or user['last_sent_at']), window_seconds=user_cooldown_seconds):
                        cycle_stats['skipped_users'] += 1
                        cycle_stats['reasons']['cooldown'] = cycle_stats['reasons'].get('cooldown', 0) + 1
                        logger.info(
                            "[bot-scheduler] %s | ignorado | cooldown ativo | last_sent_at=%s",
                            label,
                            user['last_sent_at'],
                        )
                        audit.system("scheduler_usuario_ignorado", chat_id=str(user['chat_id']),
                                     user_id=str(user['user_id']), status="skipped",
                                     payload={"motivo": "cooldown",
                                              "last_sent_at": str(user['last_sent_at'])})
                        continue
                    _t = audit.timer()
                    sent, reason, total_results = run_for_user(
                        conn,
                        bot,
                        loop,
                        int(user['user_id']),
                        str(user['chat_id']),
                        float(user['max_price']),
                        {
                            'google_flights': bool(user['enable_google_flights']),
                            'maxmilhas': False,
                        },
                        str(user['airline_filters_json'] or ''),
                    )
                    user_duration_ms = _t.elapsed()
                    if sent:
                        cycle_stats['sent_users'] += 1
                        cycle_stats['sent_results'] += int(total_results)
                        cycle_stats['reasons']['enviado'] = cycle_stats['reasons'].get('enviado', 0) + 1
                        mark_sent(conn, int(user['user_id']))
                        logger.info("[bot-scheduler] %s | envio concluído | qte_envios=%s | duracao_ms=%s", label, total_results, user_duration_ms)
                        audit.scraping("scan_agendado_enviado",
                                       chat_id=str(user['chat_id']),
                                       user_id=str(user['user_id']),
                                       duration_ms=user_duration_ms,
                                       payload={"resultados": total_results,
                                                "interval_s": interval_seconds})
                    else:
                        cycle_stats['no_send_users'] += 1
                        cycle_stats['reasons'][reason] = cycle_stats['reasons'].get(reason, 0) + 1
                        logger.info("[bot-scheduler] %s | sem envio | %s | qte_envios=%s | duracao_ms=%s", label, reason, total_results, user_duration_ms)
                        audit.scraping("scan_agendado_sem_envio",
                                       chat_id=str(user['chat_id']),
                                       user_id=str(user['user_id']),
                                       status="skipped",
                                       duration_ms=user_duration_ms,
                                       payload={"motivo": reason, "resultados": total_results})
                except Exception as exc:
                    if 'executor timeout' in str(exc).lower():
                        logger.warning('[SCHED_RETRY] %s | timeout, tentando novamente', user_label(user))
                        try:
                            sent, reason, total_results = run_for_user(
                                conn, bot, loop,
                                int(user['user_id']), str(user['chat_id']),
                                float(user['max_price']),
                                {'google_flights': bool(user['enable_google_flights']), 'maxmilhas': False},
                                str(user['airline_filters_json'] or ''),
                            )
                            if sent:
                                mark_sent(conn, int(user['user_id']))
                                logger.info('[SCHED_RETRY] %s | retry ok', user_label(user))
                            continue
                        except Exception as retry_exc:
                            exc = retry_exc
                            try:
                                loop.run_until_complete(_send_admin_alert(
                                    bot,
                                    f"⚠️ Timeout 2x no scheduler\n\nUser ID: {user['user_id']}\nChat ID: {user['chat_id']}\nErro: {str(exc)[:400]}\n\nVerifique a sessão Google.",
                                ))
                            except Exception:
                                pass
                    cycle_stats['errors'] += 1
                    cycle_stats['reasons']['erro'] = cycle_stats['reasons'].get('erro', 0) + 1
                    logger.exception('[SCHED_FAIL] [bot-scheduler] %s | erro no envio: %s', user_label(user), exc)
                    if _is_chat_not_found(exc):
                        _mark_user_blocked(conn, str(user['chat_id']))
                    audit.error("scheduler_erro_envio",
                                chat_id=str(user['chat_id']),
                                user_id=str(user['user_id']),
                                error_msg=str(exc)[:500])
                    try:
                        loop.run_until_complete(_send_admin_alert(
                            bot,
                            f"🚨 Erro no scheduler\n\nUser ID: {user['user_id']}\nChat ID: {user['chat_id']}\nErro: {str(exc)[:500]}",
                        ))
                    except Exception:
                        pass
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
        sleep_until_next_slot(interval_seconds)


if __name__ == '__main__':
    main()
