import asyncio
import atexit
import os
import re
import signal
import sys
import time
import traceback

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError
from telegram.request import HTTPXRequest

from app_logging import get_logger
from access_policy import (
    ensure_policy_schema,
    ensure_user_access,
    get_free_uses_limit,
    is_active_access,
    is_maintenance_mode,
    is_exempt_from_maintenance,
    should_charge_user,
)
from audit import audit
from config import TOKEN, now_local
from db import auto_pk_column, connect as connect_db, indexed_text_column, now_expression, sql, text_column, DatabaseRateLimitError
from main import _build_user_routes, build_scan_results_image, build_booking_links_message, run_scan_for_routes, filter_rows_by_max_price, filter_rows_with_vendor, normalize_rows_for_airline_priority, _rows_by_result_type, expand_rows_by_result_type, _merge_rows_for_combined_result_view, normalize_max_price
from ai_assistant import generate_ai_message
from bot import filter_rows_by_airlines, parse_airline_filters, should_show_result_type_filters
from google_session_sync import sync_current_worker_profile_from_base

POLL_SECONDS = int(os.getenv("JOB_WORKER_POLL_SECONDS", "5"))
ADMIN_CHAT_ID = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "").strip()

logger = get_logger('job_worker')


def _log_process_exit() -> None:
    logger.error('[job-worker][PROCESS_EXIT] worker encerrando | pid=%s', os.getpid())


def _log_fatal_exception(exc_type, exc, tb) -> None:
    try:
        logger.exception('[job-worker][FATAL] exceção não tratada | pid=%s | tipo=%s | erro=%s', os.getpid(), getattr(exc_type, '__name__', str(exc_type)), exc, exc_info=(exc_type, exc, tb))
    except Exception:
        traceback.print_exception(exc_type, exc, tb)


def _log_signal_and_exit(signum, _frame) -> None:
    try:
        logger.error('[job-worker][SIGNAL] sinal recebido | pid=%s | signal=%s', os.getpid(), signum)
        _return_current_job_to_queue()
    finally:
        raise SystemExit(128 + int(signum))


atexit.register(_log_process_exit)
sys.excepthook = _log_fatal_exception
for _sig in (signal.SIGTERM, signal.SIGINT):
    signal.signal(_sig, _log_signal_and_exit)

_session_alert_sent_at: float = 0.0
_SESSION_ALERT_COOLDOWN = 1800.0  # no máximo 1 alerta de sessão a cada 30 min
_GOOGLE_SESSION_INVALID = False
_current_job_id: int | None = None


def _return_current_job_to_queue():
    job_id = _current_job_id
    if job_id is None:
        return
    try:
        conn = connect_db()
        # Pega info do job antes de modificar
        job_info = conn.execute(
            sql("SELECT id, user_id, chat_id, job_type, COALESCE(retry_count, 0) as retry_count FROM scan_jobs WHERE id = ?"),
            (job_id,),
        ).fetchone()

        if job_info:
            retry_count = int(job_info['retry_count']) + 1
            is_manual = job_info['job_type'] in ('manual_now', 'manual')
            chat_id = str(job_info['chat_id'])

            if is_manual and retry_count >= 2:
                # Já tentou uma vez, agora falha e avisa o usuário
                conn.execute(
                    sql("UPDATE scan_jobs SET status = 'error', finished_at = NOW(), error_message = 'consulta_interrompida_restart' WHERE id = ?"),
                    (job_id,),
                )
                conn.commit()
                logger.info('[job-worker] job manual %s falhou apos restart (retry_count=%s)', job_id, retry_count)
                # Tenta avisar o usuário
                try:
                    from telegram import Bot
                    from telegram.request import HTTPXRequest
                    _bot = Bot(token=TOKEN, request=HTTPXRequest())
                    import asyncio
                    asyncio.run(_bot.send_message(
                        chat_id=chat_id,
                        text='Sua consulta manual foi interrompida devido a uma atualizacao do sistema. Por favor, tente novamente.',
                    ))
                except Exception:
                    pass
            else:
                # Devolve para fila para tentar novamente
                conn.execute(
                    sql("UPDATE scan_jobs SET status = 'pending', started_at = NULL, retry_count = ? WHERE id = ? AND status = 'running'"),
                    (retry_count, job_id),
                )
                conn.commit()
                logger.info('[job-worker] job %s devolvido para fila (retry_count=%s)', job_id, retry_count)
        else:
            conn.execute(
                sql("UPDATE scan_jobs SET status = 'pending', started_at = NULL, retry_count = COALESCE(retry_count, 0) + 1 WHERE id = ? AND status = 'running'"),
                (job_id,),
            )
            conn.commit()
        conn.close()
    except Exception as exc:
        logger.warning('[job-worker] falha ao devolver job %s para fila: %s', job_id, exc)


def get_db():
    return connect_db()


def ensure_job_tables(conn):
    conn.execute(
        f'''
        CREATE TABLE IF NOT EXISTS scan_jobs (
            id {auto_pk_column()},
            user_id INTEGER NOT NULL,
            chat_id {indexed_text_column()} NOT NULL,
            job_type {indexed_text_column()} NOT NULL,
            status {indexed_text_column()} NOT NULL DEFAULT 'pending',
            payload {text_column()},
            created_at {indexed_text_column()} DEFAULT CURRENT_TIMESTAMP,
            started_at {indexed_text_column()} NULL,
            finished_at {indexed_text_column()} NULL,
            error_message {text_column()} NULL
        )
        '''
    )
    conn.commit()
    try:
        conn.execute(sql('ALTER TABLE scan_jobs ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0'))
        conn.commit()
    except Exception:
        pass


def recover_stale_jobs(conn, running_timeout_minutes: int = 20, pending_timeout_minutes: int = 120) -> tuple[list[int], list[int]]:
    stale_running = conn.execute(
        sql(
            f"""
            SELECT id
            FROM scan_jobs
            WHERE status = 'running'
              AND started_at IS NOT NULL
              AND started_at < DATE_SUB(NOW(), INTERVAL {int(running_timeout_minutes)} MINUTE)
            ORDER BY id
            """
        )
    ).fetchall()
    stale_pending = conn.execute(
        sql(
            f"""
            SELECT id
            FROM scan_jobs
            WHERE status = 'pending'
              AND created_at IS NOT NULL
              AND created_at < DATE_SUB(NOW(), INTERVAL {int(pending_timeout_minutes)} MINUTE)
            ORDER BY id
            """
        )
    ).fetchall()

    recovered_running_ids = [row['id'] for row in stale_running]
    expired_pending_ids = [row['id'] for row in stale_pending]

    if recovered_running_ids:
        placeholders = ', '.join(['%s'] * len(recovered_running_ids))
        conn.execute(
            f"UPDATE scan_jobs SET status = 'error', finished_at = NOW(), error_message = 'stale_running_recovered' WHERE id IN ({placeholders})",
            tuple(recovered_running_ids),
        )
    if expired_pending_ids:
        placeholders = ', '.join(['%s'] * len(expired_pending_ids))
        conn.execute(
            f"UPDATE scan_jobs SET status = 'error', finished_at = NOW(), error_message = 'stale_pending_expired' WHERE id IN ({placeholders})",
            tuple(expired_pending_ids),
        )
    if recovered_running_ids or expired_pending_ids:
        conn.commit()
    return recovered_running_ids, expired_pending_ids


def fetch_next_job(conn, pool='scheduled'):
    recovered_running_ids, expired_pending_ids = recover_stale_jobs(conn)
    if recovered_running_ids:
        logger.warning('scan_jobs travados recuperados: %s', recovered_running_ids)
    if expired_pending_ids:
        logger.warning('scan_jobs pendentes expirados: %s', expired_pending_ids)

    # Workers do pool 'scheduled' só pegam jobs agendados
    # Workers do pool 'manual' só pegam jobs manuais (prioridade)
    if pool == 'manual':
        job_type_filter = "job_type IN ('manual_now', 'manual')"
    else:
        job_type_filter = "job_type = 'scheduled'"

    # Usa UPDATE atômico (subconsulta aninhada) sem FOR UPDATE para evitar lock longo
    # que trava consultas manuais. Funciona porque o UPDATE com status='pending' é atômico
    # no MySQL/MariaDB — só um worker consegue pegar cada job.
    now_str = now_expression()
    updated = conn.execute(
        sql(f"""
            UPDATE scan_jobs
            SET status = 'running', started_at = {now_str}
            WHERE id = (
                SELECT id FROM (
                    SELECT id
                    FROM scan_jobs
                    WHERE status = 'pending' AND {job_type_filter}
                    ORDER BY cost_score ASC, id ASC
                    LIMIT 1
                ) AS sub
            ) AND status = 'pending'
        """)
    )
    try:
        conn.commit()
    except Exception as commit_err:
        logger.warning("falha no commit do job capture: %s", commit_err)
        return None
    if getattr(updated, 'rowcount', 0) != 1:
        logger.info('nenhum job pendente disponível no pool %s', pool)
        return None

    # Só pode ter 1 'running' recém-capturado: o nosso
    row = conn.execute(
        sql(f"SELECT * FROM scan_jobs WHERE status = 'running' AND {job_type_filter} ORDER BY updated_at DESC LIMIT 1")
    ).fetchone()
    if not row:
        return None

    return row


def finish_job(conn, job_id: int):
    conn.execute(
        sql(f"UPDATE scan_jobs SET status = 'done', finished_at = {now_expression()} WHERE id = ?"),
        (job_id,),
    )
    conn.commit()


def fail_job(conn, job_id: int, error_message: str):
    conn.execute(
        sql(f"UPDATE scan_jobs SET status = 'error', finished_at = {now_expression()}, error_message = ? WHERE id = ?"),
        (error_message[:500], job_id),
    )
    conn.commit()


def retry_job(conn, job_id: int) -> int:
    conn.execute(
        sql("UPDATE scan_jobs SET status = 'pending', started_at = NULL, error_message = NULL, retry_count = retry_count + 1 WHERE id = ?"),
        (job_id,),
    )
    conn.commit()
    row = conn.execute(sql("SELECT retry_count FROM scan_jobs WHERE id = ?"), (job_id,)).fetchone()
    return int(row['retry_count'] if isinstance(row, dict) else row[0])


def is_job_cancelled(conn, job_id: int) -> bool:
    row = conn.execute(sql("SELECT status, COALESCE(error_message, '') AS error_message FROM scan_jobs WHERE id = ?"), (job_id,)).fetchone()
    if not row:
        return True
    status = str(row['status'] if isinstance(row, dict) else row[0] or '')
    error_message = str(row['error_message'] if isinstance(row, dict) else row[1] or '')
    return status != 'running' or error_message == 'cancelled_by_new_request'


def _is_timeout_error(exc: BaseException) -> bool:
    return 'executor timeout' in str(exc).lower()


def _vendor_filter_label(filters: dict, show_result_type_filters: bool = True) -> str:
    return '🛫 Filtro: Companhias aéreas'


def get_user_settings(conn, user_id: int):
    row = conn.execute(
        sql('''
        SELECT max_price,
               COALESCE(enable_google_flights, 1) AS enable_google_flights,
               COALESCE(last_sent_at, '') AS last_sent_at,
               COALESCE(airline_filters_json, '') AS airline_filters_json
        FROM bot_settings
        WHERE user_id = ?
        '''),
        (user_id,),
    ).fetchone()
    if row:
        return row
    return {'max_price': None, 'enable_google_flights': 1, 'last_sent_at': '', 'airline_filters_json': ''}



async def _send_admin_alert(bot: Bot, message: str, reply_markup=None):
    if not ADMIN_CHAT_ID:
        return
    # Tenta Markdown, fallback pra texto puro
    for parse_mode in ('Markdown', None):
        try:
            await bot.send_message(chat_id=ADMIN_CHAT_ID, text=message,
                                   parse_mode=parse_mode, reply_markup=reply_markup)
            break
        except Exception:
            continue


def _alert_admin(bot: Bot, loop, message: str) -> None:
    try:
        loop.run_until_complete(_send_admin_alert(bot, message))
    except Exception as exc:
        logger.warning('[ALERT_ADMIN][JOB_WORKER] Falha ao executar alerta admin no loop | erro=%s', exc)


def _renovar_sessao_markup():
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    return InlineKeyboardMarkup([[
        InlineKeyboardButton('🔐 Renovar Sessão Google', callback_data='painel:renovar_sessao'),
    ]])


GOOGLE_SESSION_DIR = os.environ.get('GOOGLE_PERSISTENT_PROFILE_DIR', '/opt/vooindo/google_session')


def _purge_stale_chrome():
    """Mata Chrome orphans, limpa SingletonLock e locks com ownership errada antes de cada job."""
    import subprocess as _sp
    session_dir = GOOGLE_SESSION_DIR
    try:
        # Mata processos que estejam usando o diretório de sessão
        _sp.run(['fuser', '-k', str(session_dir)], capture_output=True, timeout=5)
    except Exception:
        pass
    try:
        # Mata processos do Playwright e Chrome que usem o diretório de sessão atual
        _sp.run(['pkill', '-9', '-f', session_dir], capture_output=True, timeout=5)
    except Exception:
        pass
    try:
        # Limpeza de arquivos de lock internos do Chrome/Chromium
        if os.path.exists(session_dir):
            # Limpeza recursiva de Singleton* e lock files
            for root, dirs, files in os.walk(session_dir):
                for name in files + dirs:
                    if any(x in name for x in ['SingletonLock', 'SingletonCookie', 'SingletonSocket', 'lock', '.lock']):
                        fp = os.path.join(root, name)
                        try:
                            if os.path.islink(fp) or os.path.isfile(fp):
                                os.remove(fp)
                            elif os.path.isdir(fp):
                                import shutil
                                shutil.rmtree(fp, ignore_errors=True)
                        except Exception:
                            pass
                # Só limpa o topo por performance, a menos que tenhamos muitos problemas
                break
        
        # Limpar locks de arquivo de nível superior (ex: google_session.lock)
        import pathlib as _pl
        session_path = _pl.Path(session_dir)
        lock_file = session_path.parent / f'{session_path.name}.lock'
        if lock_file.exists():
            try:
                os.remove(str(lock_file))
            except Exception:
                pass
    except Exception:
        pass


def _notify_session_expired(bot: Bot, loop, score: int = 0, parsed_rows: list | None = None) -> None:
    global _session_alert_sent_at, _GOOGLE_SESSION_INVALID
    # Só trava o worker se o score for ZERO (realmente deslogado)
    if score == 0:
        _GOOGLE_SESSION_INVALID = True
    
    now = time.monotonic()
    if now - _session_alert_sent_at < _SESSION_ALERT_COOLDOWN:
        return
    _session_alert_sent_at = now
    # Alerta removido conforme solicitado: score 1 não é mais considerado degradado
    # pois o usuário não utiliza mais agências.
    if score == 1:
        return

    msg = (
        "🔴 *Sessão Google expirada* \\(auth\\_score=0/2\\)\n\n"
        "O bot não consegue buscar voos\\. Renove a sessão:"
    )
    loop.run_until_complete(_send_admin_alert(bot, msg, reply_markup=_renovar_sessao_markup()))


def _rows_have_auth_error(parsed: list[dict]) -> bool:
    for row in parsed:
        notes = str(row.get("notes") or "")
        if "google_auth_required" in notes or "auth_score=0" in notes or "auth_score=1" in notes:
            return True
    return False


def _rows_auth_score(parsed: list[dict]) -> int:
    for row in parsed:
        notes = str(row.get("notes") or "")
        for score in (0, 1, 2):
            if f"auth_score={score}" in notes:
                return score
    return -1


def _row_debug_summary(row: dict) -> str:
    origin = str(row.get('origin') or '-').upper()
    destination = str(row.get('destination') or '-').upper()
    vendor = str(row.get('best_vendor') or row.get('site') or '-').strip() or '-'
    company = str(row.get('airline') or row.get('company') or '-').strip() or '-'
    result_type = str(row.get('result_type') or '-').strip() or '-'
    price = row.get('best_vendor_price')
    if not isinstance(price, (int, float)):
        price = row.get('visible_card_price')
    if not isinstance(price, (int, float)):
        price = row.get('price')
    price_txt = f'R${float(price):.2f}' if isinstance(price, (int, float)) else '-'
    notes = str(row.get('notes') or '').strip().replace('\n', ' ')
    if len(notes) > 120:
        notes = notes[:117] + '...'
    return f'{origin}->{destination} | cia={company} | vendor={vendor} | tipo={result_type} | preço={price_txt} | notes={notes or "-"}'


def _log_filter_diagnostics(job_id: int, max_price: float | None, filters: dict, show_result_type_filters: bool,
                            parsed: list[dict], expanded: list[dict], filtered_price: list[dict],
                            filtered_normalized: list[dict], filtered_vendor: list[dict], filtered: list[dict]) -> None:
    logger.info('[job-worker][filters] job_id=%s | max_price=%s | any_airline=%s | type_filter=%s | parsed=%s | expanded=%s | price=%s | norm=%s | vendor=%s | final=%s',
        job_id, max_price, bool(filters.get('any_airline', True)), show_result_type_filters,
        len(parsed), len(expanded), len(filtered_price), len(filtered_normalized),
        len(filtered_vendor), len(filtered))

    if filtered:
        sample = '; '.join(_row_debug_summary(row) for row in filtered[:3])
        logger.info('[job-worker][filters] job_id=%s | final_sample=%s', job_id, sample)
        return

    stages = [
        ('expanded', expanded),
        ('after_price', filtered_price),
        ('after_normalize', filtered_normalized),
        ('after_vendor', filtered_vendor),
    ]
    for stage_name, stage_rows in stages:
        if not stage_rows:
            logger.warning('[job-worker][filters] job_id=%s | stage=%s | empty', job_id, stage_name)
            continue
        sample = '; '.join(_row_debug_summary(row) for row in stage_rows[:3])
        logger.warning('[job-worker][filters] job_id=%s | stage=%s | sample=%s', job_id, stage_name, sample)


async def _send_photo(bot: Bot, chat_id: str, image_path: str):
    with open(image_path, 'rb') as image_file:
        await bot.send_photo(chat_id=chat_id, photo=image_file)


def main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🏠 Abrir menu principal', callback_data='menu:back')],
    ])


def send_photo(bot: Bot, loop, chat_id: str, image_path: str):
    loop.run_until_complete(_send_photo(bot, chat_id, image_path))





def _send_links_message(bot: Bot, loop, chat_id: str, links_msg: str, reply_markup) -> None:
    try:
        loop.run_until_complete(bot.send_message(
            chat_id=chat_id, text=links_msg, parse_mode='HTML',
            disable_web_page_preview=True, reply_markup=reply_markup,
        ))
    except TelegramError as exc:
        if 'parse' in str(exc).lower() or 'entities' in str(exc).lower():
            logger.warning('HTML parse error ao enviar links, fallback para texto sem tags | chat_id=%s | erro=%s', chat_id, exc)
            import re as _re
            plain = _re.sub(r'<[^>]+>', '', links_msg)
            loop.run_until_complete(bot.send_message(
                chat_id=chat_id, text=plain,
                disable_web_page_preview=True, reply_markup=reply_markup,
            ))
        else:
            raise


def mark_sent(conn, user_id: int):
    conn.execute(
        sql(f"UPDATE bot_settings SET last_sent_at = {now_expression()}, updated_at = {now_expression()} WHERE user_id = ?"),
        (user_id,),
    )
    conn.commit()


def process_job(conn, bot: Bot, loop, job):
    global _GOOGLE_SESSION_INVALID

    # Sincronizar cookies do profile mestre para este worker (se for escravo)
    sync_current_worker_profile_from_base()

    # Prevenir stale_running_recovered: limpar qualquer Chrome zumbi
    _purge_stale_chrome()

    user_id = int(job['user_id'])
    chat_id = str(job['chat_id'])
    job_id = int(job['id'])
    job_type = str(job.get('job_type') or '')
    _t = audit.timer()
    logger.info('[job-worker] job_id=%s | user_id=%s | chat_id=%s | tipo=%s | início', job_id, user_id, chat_id, job_type)
    audit.system("job_iniciado", chat_id=chat_id, user_id=user_id,
                 payload={"job_id": job['id'], "job_type": job['job_type']})

    blocked_row = conn.execute(
        sql('SELECT blocked FROM bot_users WHERE user_id = ?'), (user_id,)
    ).fetchone()
    if blocked_row and int((blocked_row['blocked'] if isinstance(blocked_row, dict) else blocked_row[0]) or 0):
        raise RuntimeError('usuario_bloqueado')

    if is_maintenance_mode(conn) and not is_exempt_from_maintenance(conn, chat_id):
        raise RuntimeError('sessao_google_invalida_aguardando_renovacao')
    if _GOOGLE_SESSION_INVALID:
        raise RuntimeError('sessao_google_invalida_aguardando_renovacao')

    settings = get_user_settings(conn, user_id)
    routes = _build_user_routes(conn, user_id)
    logger.info('[job-worker] job_id=%s | rotas=%s', job_id, len(routes))
    if not routes:
        raise RuntimeError('Usuário sem rotas ativas')

    access = ensure_user_access(conn, chat_id)
    charge_now = should_charge_user(conn, chat_id, access) and not is_active_access(access)
    if charge_now:
        free_uses = int(access['free_uses'] or 0)
        free_uses_limit = get_free_uses_limit(conn)
        if free_uses >= free_uses_limit:
            audit.access("acesso_bloqueado", chat_id=chat_id, user_id=user_id,
                         status="blocked",
                         payload={"free_uses": free_uses, "limite": free_uses_limit,
                                  "job_id": job['id']})
            raise RuntimeError('bloqueado_por_monetizacao')

    try:
        airline_filters_json = str(settings['airline_filters_json'] or '')
    except (KeyError, IndexError):
        airline_filters_json = ''
    filters = parse_airline_filters(airline_filters_json)
    show_result_type_filters = should_show_result_type_filters(conn)
    # Agências desativadas conforme solicitação
    should_split = False

    is_manual_now = str(job.get('job_type') or '').strip().lower() == 'manual_now'
    
    parsed = run_scan_for_routes(
        routes,
        sources={
            'google_flights': bool(settings['enable_google_flights']),
            '': False,
        },
        fast_mode=is_manual_now
    )
    logger.info('[job-worker] job_id=%s | scan concluído | parsed=%s', job_id, len(parsed))
    if is_job_cancelled(conn, job_id):
        raise RuntimeError('cancelled_by_new_request')
    if _rows_have_auth_error(parsed):
        _notify_session_expired(bot, loop, score=_rows_auth_score(parsed), parsed_rows=parsed)
        audit.auth("sessao_google_expirada", chat_id=chat_id, status="error",
                   payload={"job_id": job['id']})
    expanded = expand_rows_by_result_type(parsed, airline_filters_json, show_result_type_filters=show_result_type_filters)
    max_price = normalize_max_price(settings['max_price'])
    filtered_price = filter_rows_by_max_price(expanded, max_price)
    filtered_normalized = normalize_rows_for_airline_priority(filtered_price, airline_filters_json)
    filtered_vendor = filter_rows_with_vendor(filtered_normalized)
    filtered_airlines = filter_rows_by_airlines(filtered_vendor, airline_filters_json, show_result_type_filters=show_result_type_filters)
    filtered = _merge_rows_for_combined_result_view(filtered_airlines) if should_split else filtered_airlines
    logger.info('[job-worker] job_id=%s | pós-filtros | filtered=%s | split=%s', job_id, len(filtered), should_split)
    _log_filter_diagnostics(job_id, max_price, filters, show_result_type_filters, parsed, expanded, filtered_price, filtered_normalized, filtered_vendor, filtered)

    if is_job_cancelled(conn, job_id):
        raise RuntimeError('cancelled_by_new_request')

    if not filtered:
        price_filtered_out = bool(expanded) and not filtered_price and max_price is not None
        if price_filtered_out:
            mensagem = f'⚠️ Encontramos voos, mas todos ficaram acima do seu teto atual de R$ {max_price:,.0f}.'.replace(',', '.')
            error_msg = 'Consulta acima do teto configurado'
        else:
            mensagem = '⚠️ Nenhuma rota encontrada dentro dos seus filtros.'
            error_msg = 'Consulta sem resultados filtrados'
        loop.run_until_complete(bot.send_message(chat_id=chat_id, text=mensagem, reply_markup=main_menu_markup()))
        if charge_now:
            conn.execute(
                sql(f"UPDATE user_access SET free_uses = free_uses + 1, updated_at = {now_expression()} WHERE chat_id = ?"),
                (chat_id,)
            )
            conn.commit()
        raise RuntimeError(error_msg)

    if not _rows_have_displayable_result(filtered):
        mensagem = '⚠️ Encontramos a rota, mas sem preço ou link confiável no momento. Tente novamente em alguns minutos.'
        loop.run_until_complete(bot.send_message(chat_id=chat_id, text=mensagem, reply_markup=main_menu_markup()))
        logger.warning('[job-worker] job_id=%s | resultados sem preço/link utilizável após filtros', job_id)
        raise RuntimeError('Consulta sem preço ou link confiável')

    split_blocks = should_split
    is_scheduled_job = str(job.get('job_type') or '').strip().lower() == 'scheduled'
    image_trigger = 'agendada' if is_scheduled_job else 'manual-user'
    send_type = 'scheduled' if is_scheduled_job else 'manual'
    image_path = None
    if split_blocks:
        image_path = build_scan_results_image(filtered, trigger=image_trigger)
    else:
        image_path = build_scan_results_image(filtered, trigger=image_trigger)
    if not image_path:
        logger.warning('[job-worker] job_id=%s | imagem não gerada, usando fallback por texto', job_id)
        fallback_msg = build_booking_links_message(filtered)
        if fallback_msg:
            _send_links_message(bot, loop, chat_id, fallback_msg, main_menu_markup())
            image_path = None
        else:
            raise RuntimeError('Falha ao gerar print da consulta')
    try:
        if is_job_cancelled(conn, job_id):
            raise RuntimeError('cancelled_by_new_request')
        if image_path:
            logger.info('[job-worker] job_id=%s | enviando imagem', job_id)
            send_photo(bot, loop, chat_id, image_path)
        # Mensagem IA + links inline
        try:
            ai_msg = generate_ai_message(filtered)
            if ai_msg:
                _send_links_message(bot, loop, chat_id, ai_msg, main_menu_markup())
            else:
                links_msg = build_booking_links_message(filtered)
                if links_msg:
                    _send_links_message(bot, loop, chat_id, links_msg, main_menu_markup())
                else:
                    loop.run_until_complete(bot.send_message(chat_id=chat_id, text='🏠 Toque abaixo para abrir o menu novamente.', reply_markup=main_menu_markup()))
        except Exception:
            links_msg = build_booking_links_message(filtered)
            if links_msg:
                _send_links_message(bot, loop, chat_id, links_msg, main_menu_markup())
            else:
                loop.run_until_complete(bot.send_message(chat_id=chat_id, text='🏠 Toque abaixo para abrir o menu novamente.', reply_markup=main_menu_markup()))
    finally:
        if image_path:
            try:
                os.remove(image_path)
            except OSError:
                pass
    logger.info('[job-worker] job_id=%s | envio concluído | atualizando last_sent', job_id)
    if is_scheduled_job:
        try:
            mark_sent(conn, user_id, send_type='scheduled')
        except TypeError:
            mark_sent(conn, user_id)
    else:
        try:
            mark_sent(conn, user_id, send_type='manual')
        except TypeError:
            mark_sent(conn, user_id)
    if charge_now:
        conn.execute(
            sql(f"UPDATE user_access SET free_uses = free_uses + 1, updated_at = {now_expression()} WHERE chat_id = ?"),
            (chat_id,)
        )
        conn.commit()
        audit.access("uso_gratuito_consumido", chat_id=chat_id, user_id=user_id,
                     payload={"job_id": job['id']})
    logger.info('[job-worker] job_id=%s | fim ok | resultados=%s | duração_ms=%s', job_id, len(filtered), _t.elapsed())
    audit.scraping("scan_agendado_concluido" if is_scheduled_job else "scan_manual_concluido", chat_id=chat_id, user_id=user_id,
                   duration_ms=_t.elapsed(),
                   payload={"job_id": job['id'], "resultados": len(filtered),
                            "rotas": len(routes)})


def _is_chat_not_found(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return 'chat not found' in msg or 'forbidden' in msg or 'bot was blocked' in msg or 'user is deactivated' in msg


def _rows_have_displayable_result(rows: list[dict]) -> bool:
    for row in rows:
        if isinstance(row.get('price'), (int, float)):
            return True
        if isinstance(row.get('best_vendor_price'), (int, float)):
            return True
        if str(row.get('booking_url') or row.get('url') or '').strip():
            return True
    return False


def _mark_user_blocked(conn, chat_id: str) -> None:
    conn.execute(sql("UPDATE bot_users SET blocked = 1 WHERE chat_id = ?"), (chat_id,))
    conn.commit()
    logger.warning('[job-worker] chat_id=%s marcado como bloqueado (Chat not found)', chat_id)
    audit.system("usuario_bloqueado_automatico", chat_id=chat_id, status="blocked",
                 payload={"motivo": "chat_not_found"})


def main():
    if not TOKEN:
        raise SystemExit('Defina TELEGRAM_BOT_TOKEN no .env')

    # Identificar pool: scheduled (padrão) ou manual
    pool = 'scheduled'
    if '--pool' in sys.argv:
        idx = sys.argv.index('--pool')
        if idx + 1 < len(sys.argv):
            pool = sys.argv[idx + 1].strip().lower()
    logger.info('[job-worker] bootstrap | pid=%s | pool=%s | argv=%s', os.getpid(), pool, sys.argv)

    request = HTTPXRequest(connection_pool_size=50, pool_timeout=60.0, connect_timeout=30.0, read_timeout=60.0, write_timeout=60.0)
    bot = Bot(token=TOKEN, request=request)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        conn = None
        try:
            conn = get_db()
            ensure_policy_schema(conn)
            ensure_job_tables(conn)
            recovered_running_ids, expired_pending_ids = recover_stale_jobs(conn)
            if recovered_running_ids:
                logger.warning('[JOB_RECOVERY] startup/loop recovery | scan_jobs travados recuperados: %s', recovered_running_ids)
                _alert_admin(bot, loop, f"⚠️ Jobs travados recuperados no worker: {recovered_running_ids}")
            if expired_pending_ids:
                logger.warning('[JOB_RECOVERY] startup/loop recovery | scan_jobs pendentes expirados: %s', expired_pending_ids)
                _alert_admin(bot, loop, f"⚠️ Jobs pendentes expirados no worker: {expired_pending_ids}")

            # Sync de perfil desabilitado — workers usam sessão base diretamente (run_all.py)
            pass

            job = fetch_next_job(conn, pool=pool)
            if not job:
                conn.close()
                time.sleep(POLL_SECONDS)
                continue
            _current_job_id = int(job['id'])
            try:
                process_job(conn, bot, loop, job)
                finish_job(conn, int(job['id']))
            except BaseException as exc:
                error_text = str(exc)
                should_retry = False
                if _is_timeout_error(exc) and int(job.get('retry_count') or 0) == 0:
                    should_retry = True
                elif 'Permission denied' in error_text and int(job.get('retry_count') or 0) == 0:
                    should_retry = True
                if should_retry:
                    retry_job(conn, int(job['id']))
                    logger.warning('[JOB_RETRY] %s no job %s, reagendando (tentativa 1)', 'timeout' if _is_timeout_error(exc) else 'permission_denied', job['id'])
                    continue
                fail_job(conn, int(job['id']), error_text)
                if _is_timeout_error(exc):
                    _alert_admin(
                        bot, loop,
                        f"⚠️ Job {job['id']} falhou por timeout 2x\n\nUser: {job['user_id']} | Chat: {job['chat_id']}\n\nVerifique a sessão Google — pode estar expirada.",
                    )
                if error_text == 'sessao_google_invalida_aguardando_renovacao':
                    # Admin/isento não vê mensagem de manutenção
                    if not is_exempt_from_maintenance(conn, str(job['chat_id'])):
                        try:
                            loop.run_until_complete(bot.send_message(
                                chat_id=str(job['chat_id']),
                                text='🔧 Em manutenção, aguarde um instante.',
                                reply_markup=main_menu_markup(),
                            ))
                        except Exception:
                            pass
                if _is_chat_not_found(exc):
                    _mark_user_blocked(conn, str(job['chat_id']))
                audit.error("job_falhou",
                            chat_id=str(job['chat_id']), user_id=str(job['user_id']),
                            error_msg=error_text[:500],
                            payload={"job_id": job['id'], "job_type": job['job_type']})
                expected_silent_errors = {
                    'sessao_google_invalida_aguardando_renovacao',
                    'Usuário sem rotas ativas',
                    'Consulta sem resultados filtrados',
                    'Consulta acima do teto configurado',
                    'Consulta sem preço ou link confiável',
                    'usuario_bloqueado',
                    'bloqueado_por_monetizacao',
                    'cancelled_by_new_request',
                }
                if error_text not in expected_silent_errors:
                    _alert_admin(
                        bot,
                        loop,
                        f"🚨 Falha em job\n\nJob ID: {job['id']}\nUser ID: {job['user_id']}\nChat ID: {job['chat_id']}\nTipo: {job['job_type']}\nErro: {error_text[:500]}",
                    )
            finally:
                _current_job_id = None
                conn.close()
        except DatabaseRateLimitError as exc:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            audit.error("job_worker_db_limit", error_msg=str(exc), status="blocked")
            _alert_admin(bot, loop, f"🚨 Limite de conexão no banco do job worker\n\nErro: {str(exc)[:500]}")
            time.sleep(1800)
            continue
        except Exception:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            # Erros de concorrência não devem derrubar o worker — só tenta de novo
            err_msg = str(sys.exc_info()[1])
            if any(x in err_msg for x in ['OperationalError', '1020', 'Record has changed since last read', 'Lock wait timeout', '1205', 'Deadlock']):
                logger.warning('[RACE_CONDITION] erro de concorrência no loop principal, re-tentando | err=%s', err_msg[:200])
                time.sleep(2)
                continue
            raise


if __name__ == '__main__':
    try:
        logger.info('[job-worker] bootstrap | pid=%s | argv=%s', os.getpid(), sys.argv)
        main()
    except SystemExit as exc:
        logger.exception('[job-worker][SYSTEM_EXIT] encerrando com SystemExit | pid=%s | code=%s', os.getpid(), getattr(exc, 'code', None))
        raise
    except BaseException:
        logger.exception('[job-worker][TOP_LEVEL_FATAL] falha fatal no topo do processo | pid=%s', os.getpid())
        raise
