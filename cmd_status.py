"""
Comando /status para mostrar quem recebeu, quem não e por quê.
"""
from datetime import timedelta

from telegram import Update
from telegram.ext import ContextTypes

from config import now_local
from db import sql
from access_policy import is_admin_chat


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    conn = None
    try:
        from bot import get_db, is_admin_chat as _is_admin
        conn = get_db()
        admin = _is_admin(conn, chat_id)
    except Exception:
        pass

    if conn is None:
        from db import connect as connect_db
        conn = connect_db()
        admin = is_admin_chat(conn, chat_id)

    if not admin:
        conn.close()
        await update.message.reply_text("Comando restrito a administradores.")
        return

    try:
        now = now_local()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_iso = today_start.isoformat()

        # Receberam hoje (scheduled done)
        received = conn.execute(sql("""
            SELECT DISTINCT j.user_id, bu.first_name, bu.chat_id
            FROM scan_jobs j
            JOIN bot_users bu ON bu.user_id = j.user_id
            WHERE j.job_type = 'scheduled' AND j.status = 'done'
              AND j.finished_at >= %s
            ORDER BY bu.first_name
        """), (today_iso,)).fetchall()

        # Nao receberam hoje (scheduled error, sem nenhum done)
        not_received = conn.execute(sql("""
            SELECT DISTINCT j.user_id, bu.first_name, bu.chat_id,
                   COALESCE(j.error_message, 'erro_desconhecido') as error_msg,
                   COUNT(*) as attempts
            FROM scan_jobs j
            JOIN bot_users bu ON bu.user_id = j.user_id
            WHERE j.job_type = 'scheduled' AND j.status = 'error'
              AND j.finished_at >= %s
              AND j.user_id NOT IN (
                  SELECT DISTINCT user_id FROM scan_jobs
                  WHERE job_type = 'scheduled' AND status = 'done'
                    AND finished_at >= %s
              )
            GROUP BY j.user_id
            ORDER BY bu.first_name
        """), (today_iso, today_iso)).fetchall()

        # Alertas desativados
        alerts_off = conn.execute(sql("""
            SELECT bu.user_id, bu.first_name, bu.chat_id
            FROM bot_users bu
            JOIN bot_settings bs ON bs.user_id = bu.user_id
            WHERE bu.confirmed = 1 AND COALESCE(bu.blocked, 0) = 0
              AND COALESCE(bs.alerts_enabled, 1) = 0
            ORDER BY bu.first_name
        """)).fetchall()

        # Bloqueados
        blocked = conn.execute(sql("""
            SELECT user_id, first_name, chat_id
            FROM bot_users
            WHERE COALESCE(blocked, 0) = 1
            ORDER BY first_name
        """)).fetchall()

        # Sem rotas
        no_routes = conn.execute(sql("""
            SELECT bu.user_id, bu.first_name, bu.chat_id
            FROM bot_users bu
            WHERE bu.confirmed = 1 AND COALESCE(bu.blocked, 0) = 0
              AND bu.user_id NOT IN (
                  SELECT DISTINCT user_id FROM user_routes WHERE active = 1
              )
            ORDER BY bu.first_name
        """)).fetchall()

        # Total ativos
        total_row = conn.execute(sql("""
            SELECT COUNT(*) as c FROM bot_users
            WHERE confirmed = 1 AND COALESCE(blocked, 0) = 0
        """)).fetchone()
        total = int(total_row["c"]) if total_row else 0

        conn.close()
    except Exception as exc:
        if conn:
            conn.close()
        await update.message.reply_text(f"Erro ao consultar status: {exc}")
        return

    # Ultimo ciclo
    last_cycle = None
    daily = None
    try:
        from cycle_monitor import get_latest_cycle, get_daily_summary
        last_cycle = get_latest_cycle()
        daily = get_daily_summary()
    except Exception:
        pass

    lines = []
    lines.append("Status de Entrega")
    lines.append("")

    lines.append(f"Total de usuarios ativos: {total}")
    lines.append(f"Receberam hoje: {len(received)}")
    lines.append(f"Nao receberam: {len(not_received)}")
    lines.append(f"Alertas desativados: {len(alerts_off)}")
    lines.append(f"Bloqueados: {len(blocked)}")
    lines.append(f"Sem rotas: {len(no_routes)}")
    lines.append("")

    if received:
        lines.append("Receberam hoje:")
        for r in received:
            lines.append(f"  {r['first_name'] or '---'}")
        lines.append("")

    if not_received:
        lines.append("Nao receberam (com erro):")
        for r in not_received:
            name = r["first_name"] or "---"
            error = (r["error_msg"] or "erro desconhecido")[:80]
            attempts = int(r["attempts"] or 1)
            lines.append(f"  {name} ({attempts}x) -> {error}")
        lines.append("")

    if alerts_off:
        lines.append("Alertas desativados:")
        for r in alerts_off:
            lines.append(f"  {r['first_name'] or '---'}")
        lines.append("")

    if blocked:
        lines.append("Bloqueados:")
        for r in blocked:
            lines.append(f"  {r['first_name'] or '---'}")
        lines.append("")

    if no_routes:
        lines.append("Sem rotas ativas:")
        for r in no_routes:
            lines.append(f"  {r['first_name'] or '---'}")
        lines.append("")

    if last_cycle:
        lines.append("Ultimo ciclo:")
        dur = last_cycle.get("duration_minutes", 0)
        mem = last_cycle.get("memory_mb", 0)
        cpu = last_cycle.get("cpu_percent", 0)
        scan = last_cycle.get("scan", {})
        lines.append(f"  Duracao: {dur}min | Mem: {mem}MB | CPU: {cpu}%")
        if scan:
            lines.append(f"  Elegiveis: {scan.get('eligible_users', '%s')} | Enviados: {scan.get('sent_users', '%s')} | Ignorados: {scan.get('skipped_users', '%s')}")
        errs = last_cycle.get("errors", [])
        if errs:
            lines.append(f"  Alertas: {', '.join(errs[:3])}")
        lines.append("")

    if daily:
        lines.append("Resumo diario:")
        lines.append(f"  Ciclos: {daily.get('cycles', 0)} | Resultados: {daily.get('results_added', 0)}")
        lines.append(f"  Max memoria: {daily.get('max_memory_mb', 0)}MB | CPU media: {daily.get('avg_cpu', 0):.1f}%")
        improvs = daily.get("total_improvements", [])
        if improvs:
            lines.append(f"  Sugestoes: {', '.join(improvs[:3])}")

    await update.message.reply_text("\n".join(lines))
