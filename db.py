"""Database adapter module for Vooindo bot.

Original: emulated sqlite3 API over PyMySQL.
Refactored: native PyMySQL with autocommit=True, DictCursor.
Compatibility: sql() function translates ? -> %s automatically.
"""

import os
import re
import time
import threading
from urllib.parse import urlparse

import pymysql
import pymysql.cursors


# ── Connection helpers ────────────────────────────────────────

# Carrega .env se MYSQL_URL nao estiver definido
if not os.environ.get('MYSQL_URL'):
    _env_path = os.path.join(os.path.dirname(__file__) or '.', '.env')
    if os.path.exists(_env_path):
        with open(_env_path) as _f:
            for _line in _f:
                _line = _line.strip()
                if _line and '=' in _line and not _line.startswith('#'):
                    _k, _v = _line.split('=', 1)
                    _v = _v.strip('"\'')
                    if _k == 'MYSQL_URL':
                        os.environ['MYSQL_URL'] = _v
                        break

_MYSQL_URL = os.environ.get('MYSQL_URL', '')
_parsed = urlparse(_MYSQL_URL)

_MYSQL_CONFIG = {
    'host': _parsed.hostname or 'localhost',
    'port': _parsed.port or 3306,
    'user': _parsed.username or 'vooindobot',
    'password': _parsed.password or '',
    'database': _parsed.path.lstrip('/') or 'vooindo',
    'autocommit': True,
    'connect_timeout': 5,
    'cursorclass': pymysql.cursors.DictCursor,
}


def _make_conn():
    return pymysql.connect(**_MYSQL_CONFIG)


# Thread-local storage for get_db()
_thread_local = threading.local()


class _PyMysqlConnection:
    """Wrapper around a pymysql connection that emulates sqlite3.Connection.execute().

    conn.execute(sql, params) -> cursor  (like sqlite3)
    conn.cursor() -> pymysql cursor
    conn.commit() -> noop (autocommit is on)
    conn.close() -> closes underlying pymysql connection
    """

    def __init__(self, conn=None):
        self._conn = conn or _make_conn()

    def cursor(self):
        return self._conn.cursor()

    def execute(self, sql_query, params=None):
        cur = self._conn.cursor()
        cur.execute(sql_query, params or ())
        return cur

    def executemany(self, sql_query, seq_of_params):
        cur = self._conn.cursor()
        cur.executemany(sql_query, seq_of_params)
        return cur

    def commit(self):
        pass  # autocommit is on

    def rollback(self):
        try:
            self._conn.rollback()
        except Exception:
            pass

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ── Exceptions ────────────────────────────────────────────────

class UnsupportedDatabaseEngineError(RuntimeError):
    pass


class MySqlNotImplementedYetError(RuntimeError):
    pass


class DatabaseOperationalError(RuntimeError):
    pass


class DatabaseIntegrityError(RuntimeError):
    pass


class DatabaseRateLimitError(DatabaseOperationalError):
    pass


# ── Rate limit (kept from original) ───────────────────────────

_rate_limit_lock = threading.Lock()
_rate_limit_gate_ts: float = 0.0


def _check_rate_limit_gate() -> None:
    if _rate_limit_gate_ts and (time.time() - _rate_limit_gate_ts) < 1.0:
        raise DatabaseRateLimitError("rate limit gate active, try again later")


def _set_rate_limit_gate() -> None:
    global _rate_limit_gate_ts
    with _rate_limit_lock:
        _rate_limit_gate_ts = time.time()


class DatabaseRateLimit:
    """Context manager for rate-limiting database writes."""

    def __init__(self, allow_multiple: bool = False):
        pass  # simplified

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


# ── Legacy adapter (minimal stub for compatibility) ───────────

class DatabaseAdapter:
    def __init__(self, conn=None):
        self._conn = conn or connect()

    def execute(self, sql_query, params=None):
        return self._conn.execute(sql_query, params)

    def close(self):
        self._conn.close()


class MySqlConnectionWrapper(_PyMysqlConnection):
    """Kept for backward compatibility."""
    pass


class MySqlAdapter(DatabaseAdapter):
    """Kept for backward compatibility."""
    pass


def get_adapter() -> DatabaseAdapter:
    return DatabaseAdapter()


def is_sqlite() -> bool:
    return False


# ── Core public API ───────────────────────────────────────────

def connect():
    return _PyMysqlConnection()


def get_db():
    """Get or create a thread-local database connection."""
    if not hasattr(_thread_local, '_conn') or _thread_local._conn is None:
        _thread_local._conn = connect()
    return _thread_local._conn


def close_db(conn=None):
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    elif hasattr(_thread_local, '_conn') and _thread_local._conn is not None:
        try:
            _thread_local._conn.close()
        except Exception:
            pass
        _thread_local._conn = None


def now_expression() -> str:
    return 'NOW()'


def sql(query: str) -> str:
    """Translate ? placeholders to %s for PyMySQL compatibility.

    This allows existing code using sql('... ? ...') to work unchanged.
    """
    import re
    # Só traduz se a query tiver placeholders ? e nenhum %s ainda
    if '?' in query and '%s' not in query:
        return query.replace('?', '%s')
    return query


def auto_pk_column() -> str:
    return "INT AUTO_INCREMENT PRIMARY KEY"


def id_ref_column() -> str:
    return "INT"


def indexed_text_column(length: int = 191) -> str:
    return f"VARCHAR({length})"


def text_column() -> str:
    return "TEXT"


def create_index_sql(name: str, table: str, columns: str) -> str:
    return f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({columns})"


def results_route_index_sql() -> str:
    return "CREATE INDEX IF NOT EXISTS idx_results_route ON scan_results(route, captured_at)"


def is_missing_column_error(exc: Exception) -> bool:
    msg = str(exc)
    return 'Unknown column' in msg or 'duplicate column' in msg.lower()


def is_integrity_error(exc: Exception) -> bool:
    msg = str(exc)
    return 'Duplicate entry' in msg or 'IntegrityError' in msg


def insert_ignore_sql(table: str, columns: list[str], placeholders: str | None = None) -> str:
    if placeholders is None:
        placeholders = ', '.join(['%s'] * len(columns))
    cols = ', '.join(columns)
    return f"INSERT IGNORE INTO {table} ({cols}) VALUES ({placeholders})"


def upsert_payment_sql() -> str:
    return (
        "INSERT INTO payments (user_id, payment_id, status, amount, payment_method, "
        "payment_method_id, date_created, date_approved, payer_email, payer_id) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE status=VALUES(status), date_approved=VALUES(date_approved)"
    )


# ── Application-level helpers ─────────────────────────────────

def ensure_user_routes(conn, user_id):
    conn.execute(
        "INSERT IGNORE INTO user_routes (user_id, origin, destination, active) "
        "SELECT %s, origin, destination, 1 FROM user_routes WHERE user_id = %s AND active = 1 LIMIT 1",
        (user_id, user_id),
    )


def get_user_id_by_chat(conn, chat_id):
    cur = conn.execute("SELECT user_id FROM bot_users WHERE chat_id = %s", (chat_id,))
    row = cur.fetchone()
    return int(row['user_id']) if row else None


def get_bot_user_by_chat(conn, chat_id):
    cur = conn.execute(
        "SELECT id, user_id, chat_id, username, first_name, confirmed, exempt_from_maintenance, blocked "
        "FROM bot_users WHERE chat_id = %s", (chat_id,)
    )
    return cur.fetchone()


def get_user_settings(conn, user_id):
    cur = conn.execute(
        "SELECT max_price, enable_google_flights, alerts_enabled, airline_filters_json, "
        "last_sent_at, last_manual_sent_at, last_scheduled_sent_at "
        "FROM bot_settings WHERE user_id = %s", (user_id,)
    )
    return cur.fetchone()


def ensure_table_exists(conn, table_name: str, ddl: str):
    """Create table if it doesn't exist."""
    try:
        conn.execute(ddl)
    except Exception:
        pass
