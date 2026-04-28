"""
Módulo de monitoramento de ciclos do scheduler.
Registra desempenho, consumo, erros e soluções a cada ciclo das 00:00.
"""
import json
import os
import time
import platform
from datetime import datetime
from pathlib import Path

from app_logging import get_logger

logger = get_logger('cycle_monitor')

_METRICS_DIR = Path(__file__).resolve().parent / 'logs'
_CYCLE_LOG = _METRICS_DIR / 'cycle_performance.jsonl'
_SUMMARY_LOG = _METRICS_DIR / 'cycle_daily_summary.json'


def _get_process_memory_mb() -> float:
    """Retorna uso de memória em MB do processo atual."""
    try:
        import psutil
        proc = psutil.Process(os.getpid())
        return proc.memory_info().rss / 1024 / 1024
    except ImportError:
        # Fallback: lê de /proc/self/status
        try:
            with open(f'/proc/{os.getpid()}/status') as f:
                for line in f:
                    if line.startswith('VmRSS:'):
                        return float(line.split()[1]) / 1024
        except Exception:
            pass
        return 0.0


def _get_cpu_percent() -> float:
    """Retorna uso de CPU percentual."""
    try:
        import psutil
        return psutil.cpu_percent(interval=0.1)
    except ImportError:
        return 0.0


def _get_system_load() -> tuple:
    """Retorna load average do sistema."""
    try:
        return os.getloadavg()
    except AttributeError:
        return (0.0, 0.0, 0.0)


def _get_disk_usage_mb() -> dict:
    """Retorna uso de disco da pasta do projeto."""
    try:
        import shutil
        total, used, free = shutil.disk_usage(Path(__file__).resolve().parent)
        return {
            'total_mb': round(total / 1024 / 1024, 1),
            'used_mb': round(used / 1024 / 1024, 1),
            'free_mb': round(free / 1024 / 1024, 1),
            'percent_used': round(used / total * 100, 1),
        }
    except Exception:
        return {}


def _get_mysql_status() -> dict:
    """Retorna status básico do MySQL."""
    try:
        from db import connect as connect_db, sql
        conn = connect_db()
        # Tamanho do banco
        row = conn.execute(sql(
            "SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 1) AS size_mb "
            "FROM information_schema.tables WHERE table_schema = DATABASE()"
        )).fetchone()
        db_size = float(row['size_mb']) if row and row['size_mb'] else 0

        # Contagem de tabelas
        tables = conn.execute(sql(
            "SELECT COUNT(*) AS c FROM information_schema.tables WHERE table_schema = DATABASE()"
        )).fetchone()
        table_count = int(tables['c']) if tables else 0

        # Conexões ativas
        conns = conn.execute(sql("SHOW STATUS LIKE 'Threads_connected'")).fetchone()
        threads = int(conns['Value']) if conns else 0

        conn.close()
        return {
            'db_size_mb': db_size,
            'table_count': table_count,
            'threads_connected': threads,
        }
    except Exception as exc:
        return {'error': str(exc)}


def _count_results(conn) -> dict:
    """Contagem de resultados no banco."""
    from db import sql
    try:
        total = conn.execute(sql("SELECT COUNT(*) AS c FROM results")).fetchone()
        total = int(total['c']) if total else 0

        today = conn.execute(sql(
            "SELECT COUNT(*) AS c FROM results WHERE DATE(created_at) = CURDATE()"
        )).fetchone()
        today = int(today['c']) if today else 0

        with_price = conn.execute(sql(
            "SELECT COUNT(*) AS c FROM results WHERE price IS NOT NULL"
        )).fetchone()
        with_price = int(with_price['c']) if with_price else 0

        return {
            'total_results': total,
            'today_results': today,
            'with_price': with_price,
        }
    except Exception as exc:
        return {'error': str(exc)}


def _count_users(conn) -> dict:
    """Contagem de usuários."""
    from db import sql
    try:
        total = conn.execute(sql("SELECT COUNT(*) AS c FROM bot_users")).fetchone()
        total = int(total['c']) if total else 0

        active = conn.execute(sql(
            "SELECT COUNT(*) AS c FROM bot_users WHERE confirmed = 1 AND COALESCE(blocked, 0) = 0"
        )).fetchone()
        active = int(active['c']) if active else 0

        with_routes = conn.execute(sql(
            "SELECT COUNT(DISTINCT user_id) AS c FROM user_routes WHERE active = 1"
        )).fetchone()
        with_routes = int(with_routes['c']) if with_routes else 0

        return {
            'total_users': total,
            'active_users': active,
            'users_with_routes': with_routes,
        }
    except Exception as exc:
        return {'error': str(exc)}


def _count_routes(conn) -> dict:
    """Contagem de rotas."""
    from db import sql
    try:
        total = conn.execute(sql("SELECT COUNT(*) AS c FROM user_routes WHERE active = 1")).fetchone()
        total = int(total['c']) if total else 0
        return {'active_routes': total}
    except Exception as exc:
        return {'error': str(exc)}


def record_cycle_start() -> dict:
    """Registra o início de um ciclo e retorna o objeto de métricas."""
    return {
        'timestamp': datetime.now().isoformat(),
        'hostname': platform.node(),
        'python_version': platform.python_version(),
    }


def record_cycle_end(cycle_metrics: dict, scan_results: dict = None):
    """
    Finaliza o registro de um ciclo, calcula métricas e salva no arquivo JSONL.
    
    Args:
        cycle_metrics: dict com métricas do ciclo (iniciado por record_cycle_start)
        scan_results: dict com resultados do scan (total rotas, ok, erros, etc)
    """
    end_time = time.time()
    start_time = cycle_metrics.get('_start_time', end_time)
    duration_seconds = end_time - start_time

    # Métricas de sistema
    mem_mb = _get_process_memory_mb()
    cpu = _get_cpu_percent()
    load = _get_system_load()
    disk = _get_disk_usage_mb()
    mysql = _get_mysql_status()

    # Conecta no banco para métricas de negócio
    results_count = {}
    users_count = {}
    routes_count = {}
    try:
        from db import connect as connect_db
        conn = connect_db()
        results_count = _count_results(conn)
        users_count = _count_users(conn)
        routes_count = _count_routes(conn)
        conn.close()
    except Exception as exc:
        logger.error('[cycle_monitor] Erro ao coletar métricas do banco: %s', exc)

    # Monta registro completo
    record = {
        'timestamp': datetime.now().isoformat(),
        'type': 'cycle_complete',
        'duration_seconds': round(duration_seconds, 1),
        'duration_minutes': round(duration_seconds / 60, 1),
        'memory_mb': round(mem_mb, 1),
        'cpu_percent': round(cpu, 1),
        'load_1m': round(load[0], 2),
        'load_5m': round(load[1], 2),
        'load_15m': round(load[2], 2),
        'disk': disk,
        'mysql': mysql,
        'results': results_count,
        'users': users_count,
        'routes': routes_count,
        'scan': scan_results or {},
        'errors': [],
        'solutions': [],
        'improvements': [],
    }

    # Análise de problemas e sugestões
    _analyze_and_suggest(record)

    # Salva no JSONL
    _METRICS_DIR.mkdir(parents=True, exist_ok=True)
    with open(_CYCLE_LOG, 'a') as f:
        f.write(json.dumps(record, ensure_ascii=False) + '\n')

    # Atualiza sumário diário
    _update_daily_summary(record)

    # Log resumido
    logger.info(
        '[cycle_monitor] Ciclo completo | duracao=%.1fs | memoria=%.1fMB | cpu=%.1f%% | '
        'resultados=%s | usuarios_ativos=%s | rotas=%s | erros=%s',
        duration_seconds, mem_mb, cpu,
        results_count.get('today_results', '?'),
        users_count.get('active_users', '?'),
        routes_count.get('active_routes', '?'),
        len(record.get('errors', [])),
    )

    return record


def _analyze_and_suggest(record: dict):
    """Analisa métricas e sugere melhorias."""
    errors = []
    solutions = []
    improvements = []

    # Memória alta
    if record['memory_mb'] > 500:
        errors.append(f'memoria_alta:{record["memory_mb"]}MB')
        solutions.append('aumentar memoria do servidor ou reduzir workers')
        improvements.append('revisar vazamento de memoria nos workers')

    # CPU alta
    if record['cpu_percent'] > 80:
        errors.append(f'cpu_alta:{record["cpu_percent"]}%')
        solutions.append('reduzir numero de workers paralelos')
        improvements.append('otimizar scraping com cache')

    # Load alto
    if record['load_1m'] > 4:
        errors.append(f'load_alto:{record["load_1m"]}')
        solutions.append('aumentar recursos do servidor')
        improvements.append('distribuir workers em horarios diferentes')

    # Disco cheio
    disk = record.get('disk', {})
    if disk.get('percent_used', 0) > 85:
        errors.append(f'disco_quase_cheio:{disk["percent_used"]}%')
        solutions.append('limpar logs antigos e resultados nao utilizados')
        improvements.append('implementar rotacao automatica de logs e resultados')

    # Banco grande
    mysql = record.get('mysql', {})
    if mysql.get('db_size_mb', 0) > 1000:
        errors.append(f'banco_grande:{mysql["db_size_mb"]}MB')
        solutions.append('arquivar resultados antigos')
        improvements.append('implementar cleanup periodico de resultados com mais de 90 dias')

    # Muitas conexões MySQL
    if mysql.get('threads_connected', 0) > 50:
        errors.append(f'muitas_conexoes_mysql:{mysql["threads_connected"]}')
        solutions.append('revisar pool de conexoes')
        improvements.append('implementar pooling com limite maximo')

    # Scan lento
    scan = record.get('scan', {})
    scan_duration = scan.get('duration_seconds', 0)
    if scan_duration > 600:
        errors.append(f'scan_lento:{scan_duration}s')
        solutions.append('aumentar numero de workers ou reduzir rotas')
        improvements.append('paralelizar consultas por perfil Google')

    # Muitos resultados sem preço
    results = record.get('results', {})
    total = results.get('total_results', 0)
    with_price = results.get('with_price', 0)
    if total > 0 and with_price / total < 0.3:
        errors.append(f'muitos_resultados_sem_preco:{with_price}/{total}')
        solutions.append('verificar sessao Google Flights')
        improvements.append('implementar renovacao automatica de sessao Google')

    record['errors'] = errors
    record['solutions'] = solutions
    record['improvements'] = improvements


def _update_daily_summary(record: dict):
    """Atualiza o sumário diário com os dados do ciclo."""
    today = datetime.now().strftime('%Y-%m-%d')
    
    summary = {}
    if _SUMMARY_LOG.exists():
        try:
            with open(_SUMMARY_LOG) as f:
                summary = json.load(f)
        except (json.JSONDecodeError, Exception):
            summary = {}

    if summary.get('date') != today:
        summary = {
            'date': today,
            'cycles': 0,
            'total_duration_minutes': 0,
            'max_memory_mb': 0,
            'avg_cpu': 0,
            'total_errors': [],
            'total_solutions': [],
            'total_improvements': [],
            'results_added': 0,
        }

    # Garantir que campos usados como lista existam (resumo de dias anteriores pode ter sido salvo com tipos diferentes)
    for key in ('total_solutions', 'total_improvements'):
        if key not in summary or not isinstance(summary[key], list):
            summary[key] = []

    summary['cycles'] += 1
    summary['total_duration_minutes'] += record.get('duration_minutes', 0)
    summary['max_memory_mb'] = max(summary['max_memory_mb'], record.get('memory_mb', 0))
    
    # Média móvel de CPU
    prev_avg = summary.get('avg_cpu', 0)
    prev_count = summary['cycles'] - 1
    summary['avg_cpu'] = (prev_avg * prev_count + record.get('cpu_percent', 0)) / summary['cycles']

    summary['total_errors'].extend(record.get('errors', []))
    summary['total_solutions'] = list(set(summary.get('total_solutions', [])) | set(record.get('solutions', [])))
    summary['total_improvements'] = list(set(summary.get('total_improvements', [])) | set(record.get('improvements', [])))

    results = record.get('results', {})
    summary['results_added'] += results.get('today_results', 0)

    # Converte sets para listas para JSON
    # Garantir que sejam listas serializáveis (já foram convertidas acima)

    with open(_SUMMARY_LOG, 'w') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


def get_latest_cycle() -> dict | None:
    """Retorna o registro do último ciclo completo."""
    if not _CYCLE_LOG.exists():
        return None
    try:
        with open(_CYCLE_LOG) as f:
            lines = f.readlines()
        if not lines:
            return None
        return json.loads(lines[-1])
    except Exception:
        return None


def get_daily_summary() -> dict | None:
    """Retorna o sumário diário atual."""
    if not _SUMMARY_LOG.exists():
        return None
    try:
        with open(_SUMMARY_LOG) as f:
            return json.load(f)
    except Exception:
        return None
