import atexit
import asyncio
import fcntl
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from telegram import Bot
from telegram.request import HTTPXRequest

from app_logging import get_logger
from google_session_sync import sync_base_session_to_worker_profiles

logger = get_logger('run_all')
ADMIN_CHAT_ID = os.getenv('TELEGRAM_ADMIN_CHAT_ID', '').strip()
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()

BASE_DIR = Path(__file__).resolve().parent
LOCK_PATH = BASE_DIR / 'run_all.lock'
processes = []
_lock_handle = None
START_DELAY_SECONDS = float(os.getenv('RUN_ALL_START_DELAY_SECONDS', '2'))
RESTART_GRACE_SECONDS = float(os.getenv('RUN_ALL_RESTART_GRACE_SECONDS', '120'))
NUM_JOB_WORKERS = int(os.getenv("NUM_JOB_WORKERS", "1"))  # 1 scheduled + 1 manual = 2 workers total


def _find_stale_pids(script_names: list[str]) -> list[int]:
    my_pid = os.getpid()
    stale = []
    bot_markers = ['bot.py', 'bot_scheduler.py', 'job_worker.py', 'payment_monitor.py', 'payment_webhook.py']
    try:
        for entry in os.scandir('/proc'):
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            if pid == my_pid:
                continue
            try:
                cmdline_path = f'/proc/{pid}/cmdline'
                with open(cmdline_path, 'rb') as f:
                    cmdline = f.read().decode('utf-8', errors='replace').replace('\x00', ' ').strip()
                # Match por nome de script no final do cmdline (ignora caminho completo)
                if any(name in cmdline for name in script_names):
                    stale.append(pid)
                    continue
                # Match amplo: qualquer processo python rodando scripts do bot,
                # mesmo com caminhos diferentes (ex: /.venv/bin/python vs /usr/bin/python3)
                for marker in bot_markers:
                    # Verifica se o nome do script aparece como argumento (ultimo elemento antes de espaco)
                    # Ex: /opt/vooindo/.venv/bin/python /opt/vooindo/bot.py
                    # Ex: /usr/bin/python3 /opt/vooindo/bot.py
                    if marker in cmdline and 'vooindo' in cmdline:
                        stale.append(pid)
                        break
            except (PermissionError, FileNotFoundError, ProcessLookupError):
                continue
    except Exception:
        pass
    return stale


def kill_stale_processes(script_names: list[str]):
    pids = _find_stale_pids(script_names)
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            logger.info('Encerrado processo antigo pid=%s', pid)
        except ProcessLookupError:
            pass
        except PermissionError:
            logger.warning('Sem permissão para encerrar pid=%s', pid)
    if pids:
        time.sleep(2)
        for pid in pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass


def acquire_single_instance_lock():
    global _lock_handle
    _lock_handle = open(LOCK_PATH, 'w')
    try:
        fcntl.flock(_lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        try:
            _lock_handle.seek(0)
            existing_pid = (_lock_handle.read() or '').strip()
        except Exception:
            existing_pid = ''
        logger.error('Outra instância do run_all.py já está em execução%s', f' (pid={existing_pid})' if existing_pid else '')
        sys.exit(1)
    _lock_handle.seek(0)
    _lock_handle.truncate()
    _lock_handle.write(str(os.getpid()))
    _lock_handle.flush()

    def _cleanup_lock():
        global _lock_handle
        try:
            if _lock_handle:
                _lock_handle.seek(0)
                _lock_handle.truncate()
                fcntl.flock(_lock_handle.fileno(), fcntl.LOCK_UN)
                _lock_handle.close()
        except Exception:
            pass
        _lock_handle = None

    atexit.register(_cleanup_lock)


def shutdown(*_args):
    logger.info('[run_all] Iniciando desligamento gracioso...')
    for proc in processes:
        if proc.poll() is None:
            proc.terminate()
    wait_until = time.monotonic() + 30
    for proc in processes:
        remaining = max(0, wait_until - time.monotonic())
        try:
            proc.wait(timeout=remaining)
        except Exception:
            pass
    for proc in processes:
        if proc.poll() is None:
            proc.kill()
    logger.info('[run_all] Desligamento concluído')
    sys.exit(0)


def _send_admin_alert_sync(message: str) -> None:
    if not ADMIN_CHAT_ID or not TOKEN:
        return
    try:
        request = HTTPXRequest(connection_pool_size=10, pool_timeout=20.0, connect_timeout=10.0, read_timeout=20.0, write_timeout=20.0)
        bot = Bot(token=TOKEN, request=request)
        asyncio.run(bot.send_message(chat_id=ADMIN_CHAT_ID, text=message))
    except Exception as exc:
        logger.warning('[ALERT_ADMIN][RUN_ALL] Falha ao enviar alerta admin do run_all | erro=%s', exc)


def _fix_google_session_permissions():
    """Corrige permissão da google_session para ubuntu:ubuntu automaticamente."""
    sess_dir = BASE_DIR / 'google_session'
    if not sess_dir.is_dir():
        return
    try:
        st = sess_dir.stat()
        if st.st_uid != os.geteuid() or st.st_gid != os.getegid():
            import subprocess as _sp
            _sp.run(['chown', '-R', 'ubuntu:ubuntu', str(sess_dir)], capture_output=True, timeout=5)
            logger.info('[session_watchdog] google_session permissions fixed → ubuntu:ubuntu')
    except Exception as exc:
        logger.warning('[session_watchdog] Falha ao verificar permissão google_session: %s', exc)


def main():
    acquire_single_instance_lock()
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    _fix_google_session_permissions()

    # Zera contagem do semáforo de Chromes
    try:
        with open('/tmp/vooindo_chrome_semaphore.lock', 'w') as f:
            f.write('0')
    except Exception:
        pass

    py = str(BASE_DIR / '.venv' / 'bin' / 'python')

    def _worker_env(worker_index: int) -> dict:
        env = os.environ.copy()
        profile_dir = str(BASE_DIR / 'google_session')
        if worker_index > 1:
            profile_dir = str(BASE_DIR / f'google_session_{worker_index}')
        env['GOOGLE_PERSISTENT_PROFILE_DIR'] = profile_dir
        return env

    children = [
        {'cmd': [py, str(BASE_DIR / 'bot.py')]},
        {'cmd': [py, str(BASE_DIR / 'bot_scheduler.py')]},
        # Workers de job — cada um com perfil DEDICADO para não conflitar
        # Perfil 1 = google_session, Perfil 2 = google_session_2
        # Perfil 3 = google_session_3, Perfil 4 = google_session_4
        {'cmd': [py, str(BASE_DIR / 'job_worker.py'), '--pool', 'scheduled'], 'env': _worker_env(1)},
        {'cmd': [py, str(BASE_DIR / 'job_worker.py'), '--pool', 'scheduled'], 'env': _worker_env(2)},
        {'cmd': [py, str(BASE_DIR / 'job_worker.py'), '--pool', 'manual'], 'env': _worker_env(3)},
        {'cmd': [py, str(BASE_DIR / 'job_worker.py'), '--pool', 'manual'], 'env': _worker_env(4)},
        # Workers de integração e suporte
        {'cmd': [py, str(BASE_DIR / 'payment_monitor.py')]},
        {'cmd': [py, str(BASE_DIR / 'payment_webhook.py')]},
        # Monitor automático de ciclos — detecta falhas, aplica retry, notifica admin
        {'cmd': [py, str(BASE_DIR / 'cycle_monitor_auto.py')]},
        # AutoRepair — diagnóstico + reparo autônomo + retry
        {'cmd': [py, '-m', 'autorepair.engine']},
    ]

    # Delay entre workers: 3s (antes 5s) para subirem mais rápido mas sem race condition
    START_DELAY_SECONDS = 3

    script_names = list({Path(child['cmd'][-1]).name for child in children})
    kill_stale_processes(script_names)

    started_at = {}
    for index, child in enumerate(children):
        cmd = child['cmd']
        env = child.get('env')
        label = ' '.join(cmd) + (f' [profile={env.get("GOOGLE_PERSISTENT_PROFILE_DIR", "")}]' if env else '')
        logger.info('Iniciando processo: %s', label)
        proc = subprocess.Popen(cmd, env=env)
        processes.append(proc)
        started_at[id(proc)] = time.monotonic()
        if index < len(children) - 1:
            time.sleep(max(0.0, START_DELAY_SECONDS))

    _last_session_check = time.monotonic()
    _check_interval = 300  # 5 minutos

    while True:
        time.sleep(2)
        # Verificar permissão da google_session periodicamente
        if time.monotonic() - _last_session_check > _check_interval:
            _fix_google_session_permissions()
            _last_session_check = time.monotonic()
        for i, proc in enumerate(processes):
            code = proc.poll()
            if code is None:
                continue
            child = children[i]
            cmd = child['cmd']
            env = child.get('env')
            lifetime = time.monotonic() - started_at.get(id(proc), time.monotonic())
            label = ' '.join(cmd) + (f' [profile={env.get("GOOGLE_PERSISTENT_PROFILE_DIR", "")}]' if env else '')
            logger.error('[PROCESS_EXIT] Processo finalizou com código %s após %.1fs: %s', code, lifetime, label)
            if code == 0:
                _send_admin_alert_sync("✅ Serviço reiniciado com sucesso. O bot já está de volta e pronto para uso.")
            else:
                _send_admin_alert_sync(f"🚨 Processo do bot finalizou\n\nCódigo: {code}\nTempo: {lifetime:.1f}s\nProcesso: {label}")
            if lifetime < RESTART_GRACE_SECONDS:
                logger.error('[PROCESS_EXIT] Processo muito curto (%.1fs < %.0fs), encerrando stack para evitar loop', lifetime, RESTART_GRACE_SECONDS)
                _send_admin_alert_sync(f"🚨 Processo morreu cedo demais\n\nTempo: {lifetime:.1f}s\nLimite: {RESTART_GRACE_SECONDS:.0f}s\nProcesso: {label}")
                shutdown()
            logger.info('Reiniciando processo: %s', label)
            new_proc = subprocess.Popen(cmd, env=env)
            processes[i] = new_proc
            started_at[id(new_proc)] = time.monotonic()


if __name__ == '__main__':
    main()
