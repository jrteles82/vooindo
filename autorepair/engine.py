"""
AutoRepair Engine — loop principal

- Monitora jobs com erro
- Classifica o erro
- Aplica estratégias de reparo
- Retry automático
- Só notifica Teles se não conseguir resolver
"""

import logging, time, os, sys
from datetime import datetime, timedelta

# Adiciona path do projeto
sys.path.insert(0, '/opt/vooindo')
os.environ['GOOGLE_PERSISTENT_PROFILE_DIR'] = '/opt/vooindo/google_session'

from db import connect as db_connect, sql
from autorepair.strategies import run_repair

logger = logging.getLogger('autorepair')

# Jobs que falharam e ainda NÃO foram reparados, ou foram reparados mas precisam retry
# Não re-repara o mesmo job múltiplas vezes sem necessidade

def get_failed_jobs(conn, since_minutes: int = 30) -> list:
    """Busca jobs com erro nos últimos N minutos."""
    cutoff = (datetime.now() - timedelta(minutes=since_minutes)).strftime('%Y-%m-%d %H:%M:%S')
    c = conn.cursor()
    c.execute(sql("""
        SELECT id, user_id, error_message, retry_count, chat_id
        FROM scan_jobs
        WHERE status = 'error'
          AND created_at >= %s
          AND error_message IS NOT NULL
          AND error_message NOT LIKE '%%cancelled%%'
          AND error_message NOT LIKE '%%bloqueado%%'
          AND retry_count < 3
        ORDER BY id DESC
        LIMIT 10
    """), (cutoff,))
    return [dict(r) for r in c.fetchall()]


def retry_job(conn, job_id: int) -> int | None:
    """Cria um novo job de retry. Retorna o novo job_id ou None."""
    c = conn.cursor()
    # Pega dados do job original
    c.execute(sql("SELECT user_id, chat_id, retry_count FROM scan_jobs WHERE id = %s"), (job_id,))
    orig = c.fetchone()
    if not orig:
        return None
    
    new_count = (orig['retry_count'] or 0) + 1
    
    # Incrementa retry_count no original
    c.execute(sql("UPDATE scan_jobs SET retry_count = %s WHERE id = %s"), (new_count, job_id))
    
    # Cria novo job
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    c.execute(sql("""
        INSERT INTO scan_jobs (user_id, chat_id, job_type, status, created_at, cost_score)
        VALUES (%s, %s, 'manual_now', 'pending', %s, 0)
    """), (orig['user_id'], orig['chat_id'], now))
    new_id = c.lastrowid
    return new_id


def notify_admin(chat_id: str, message: str):
    """Envia notificação para o admin via banco (a bot.py pega e envia)."""
    try:
        conn = db_connect()
        c = conn.cursor()
        from datetime import datetime
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # Usa a tabela admins pra achar chat_id
        c.execute(sql("SELECT chat_id FROM admins WHERE active = 1 LIMIT 1"))
        admin = c.fetchone()
        if admin and admin['chat_id']:
            # Insere notificação via bot (o bot lê dessa tabela ou envia direto)
            # Por enquanto, loga e envia via support_messages
            actual_chat_id = os.environ.get('TELEGRAM_ADMIN_CHAT_ID', '0') or '1748352987'
            logger.info(f'[notify] NOTIFICACAO para {actual_chat_id}: {message[:200]}')
            # Tenta enviar via telegram se possível
            try:
                import telegram
                bot_token = os.environ.get('TELEGRAM_BOT_TOKEN', '8515270359:AAHg2nipJ9WVkmKwy-P6HXqhXCrz3y5hBBc')
                bot = telegram.Bot(token=bot_token)
                import asyncio
                asyncio.run(bot.send_message(chat_id=actual_chat_id, text=message, parse_mode='HTML'))
            except Exception as e:
                logger.error(f'[notify] falha ao enviar telegram: {e}')
        conn.close()
    except Exception as e:
        logger.error(f'[notify] falha: {e}')


def repair_cycle():
    """Um ciclo completo de reparo."""
    started = time.time()
    try:
        conn = db_connect()
        failed_jobs = get_failed_jobs(conn)
        
        if not failed_jobs:
            return 0
        
        logger.info(f'[repair] ciclo: {len(failed_jobs)} jobs com erro')
        
        repaired_count = 0
        for job in failed_jobs:
            job_id = job['id']
            error_msg = job['error_message'] or ''
            
            # Tenta reparar
            result = run_repair(job_id, error_msg)
            
            if result['repaired']:
                # Deu certo! Faz retry
                new_id = retry_job(conn, job_id)
                if new_id:
                    logger.info(f'[repair] job {job_id} reparado via {result["action"]} → retry #{new_id}')
                    repaired_count += 1
                conn.commit()
            elif result['notify']:
                # Não conseguiu reparar — notifica admin
                notify_admin(
                    '1748352987',
                    f'⚠️ <b>AutoRepair não conseguiu corrigir</b>\n\n'
                    f'Job #{job_id}\n'
                    f'Erro: {error_msg[:200]}\n'
                    f'Ação: {result["action"]}\n'
                    f'Precisa de intervenção manual.'
                )
                logger.warning(f'[repair] job {job_id} NÃO reparado — notificado')
        
        conn.close()
        elapsed = time.time() - started
        if repaired_count:
            logger.info(f'[repair] ciclo completo: {repaired_count} reparados em {elapsed:.1f}s')
        return repaired_count
        
    except Exception as e:
        logger.error(f'[repair] erro no ciclo: {e}', exc_info=True)
        return 0


def main_loop(interval: int = 60):
    """Loop principal do AutoRepair."""
    logger.info('[repair] AutoRepair iniciado — checando a cada %ss', interval)
    
    while True:
        try:
            count = repair_cycle()
            if count:
                logger.info(f'[repair] {count} jobs reparados neste ciclo')
        except Exception as e:
            logger.error(f'[repair] erro no ciclo: {e}')
        
        time.sleep(interval)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('/opt/vooindo/logs/autorepair.log'),
        ]
    )
    main_loop()
