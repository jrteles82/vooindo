import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse
import threading

from config import MYSQL_URL

try:
    import pymysql
    from pymysql.cursors import DictCursor
    from pymysql.err import IntegrityError as MySqlIntegrityError
    from pymysql.err import InterfaceError as MySqlInterfaceError
    from pymysql.err import OperationalError as MySqlOperationalError
except Exception:  # pragma: no cover - depende do ambiente
    pymysql = None
    DictCursor = None
    MySqlIntegrityError = RuntimeError
    MySqlInterfaceError = RuntimeError
    MySqlOperationalError = RuntimeError


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


@dataclass(frozen=True)
class DatabaseAdapter:
    engine: str

    def connect(self):
        raise NotImplementedError

    def now_expression(self) -> str:
        raise NotImplementedError

    def auto_pk_column(self) -> str:
        raise NotImplementedError

    def id_ref_column(self) -> str:
        raise NotImplementedError

    def indexed_text_column(self, length: int = 191) -> str:
        raise NotImplementedError

    def text_column(self) -> str:
        raise NotImplementedError

    def placeholder(self) -> str:
        return "%s"

    def placeholders(self, count: int) -> str:
        return ", ".join([self.placeholder()] * count)

    def sql(self, query: str) -> str:
        return query

    def insert_ignore_sql(self, table: str, columns: list[str], values_sql: str | None = None) -> str:
        raise NotImplementedError

    def create_index_sql(self, name: str, table: str, columns: str) -> str:
        raise NotImplementedError

    def results_route_index_sql(self) -> str:
        raise NotImplementedError

    def upsert_payment_sql(self) -> str:
        raise NotImplementedError

    def is_missing_column_error(self, exc: Exception) -> bool:
        raise NotImplementedError

    def is_integrity_error(self, exc: Exception) -> bool:
        raise NotImplementedError


_MYSQL_THREAD_LOCAL = threading.local()

import time as _time

_RATE_LIMIT_BLOCKED_UNTIL: float = 0.0
_RATE_LIMIT_BACKOFF_SECONDS: float = 1800.0  # 30 min após 1226
_PING_IDLE_THRESHOLD: float = 30.0  # só faz ping se idle >30s


def _check_rate_limit_gate() -> None:
    if _RATE_LIMIT_BLOCKED_UNTIL > _time.monotonic():
        remaining = int(_RATE_LIMIT_BLOCKED_UNTIL - _time.monotonic())
        raise DatabaseRateLimitError(
            f"(1226, 'Conexão bloqueada por rate limit. Aguarde {remaining}s.')"
        )


def _set_rate_limit_gate() -> None:
    global _RATE_LIMIT_BLOCKED_UNTIL
    _RATE_LIMIT_BLOCKED_UNTIL = _time.monotonic() + _RATE_LIMIT_BACKOFF_SECONDS


class MySqlConnectionWrapper:
    def __init__(self, conn, reconnect_factory=None, cache_key: str | None = None):
        self._conn = conn
        self._reconnect_factory = reconnect_factory
        self._cache_key = cache_key
        self._closed = False
        self._last_used: float = _time.monotonic()

    def _reopen(self):
        _check_rate_limit_gate()
        if self._reconnect_factory is None:
            raise RuntimeError('MySQL reconnect factory ausente')
        self._conn = self._reconnect_factory()
        self._closed = False
        self._last_used = _time.monotonic()

    def _ensure_connection(self):
        if self._closed:
            self._reopen()
            return
        if _time.monotonic() - self._last_used > _PING_IDLE_THRESHOLD:
            try:
                self._conn.ping(reconnect=False)
                self._last_used = _time.monotonic()
            except Exception:
                self._reopen()

    def execute(self, sql, params=None):
        self._ensure_connection()
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql, params or ())
            self._last_used = _time.monotonic()
            return cursor
        except (MySqlOperationalError, MySqlInterfaceError) as exc:
            code = getattr(exc, 'args', [None])[0]
            if code in {0, 2006, 2013} and self._reconnect_factory is not None:
                self._reopen()
                cursor = self._conn.cursor()
                cursor.execute(sql, params or ())
                self._last_used = _time.monotonic()
                return cursor
            raise

    def cursor(self):
        self._ensure_connection()
        return self._conn.cursor()

    def commit(self):
        self._ensure_connection()
        return self._conn.commit()

    def rollback(self):
        self._ensure_connection()
        return self._conn.rollback()

    def close(self):
        self._closed = True
        return None

    def close_physical(self):
        try:
            self._conn.close()
        except Exception:
            return None
        finally:
            self._closed = True
            if self._cache_key:
                try:
                    delattr(_MYSQL_THREAD_LOCAL, self._cache_key)
                except AttributeError:
                    pass
        return None


@dataclass(frozen=True)
class MySqlAdapter(DatabaseAdapter):
    engine: str = "mysql"

    def connect(self):
        if not MYSQL_URL:
            raise MySqlNotImplementedYetError(
                "MYSQL_URL ausente. Defina a variável de ambiente MYSQL_URL."
            )
        if pymysql is None:
            raise MySqlNotImplementedYetError(
                "Driver PyMySQL não está instalado. Rode `pip install -r requirements.txt`."
            )
        parsed = urlparse(MYSQL_URL)
        if parsed.scheme != "mysql":
            raise MySqlNotImplementedYetError(
                "MYSQL_URL inválida. Use o formato mysql://usuario:senha@host:3306/banco"
            )
        query = parse_qs(parsed.query)
        raw_db = (parsed.path or "/").lstrip("/")
        db_name = unquote(raw_db)
        cache_key = f"{parsed.hostname}:{parsed.port or 3306}:{db_name}:{unquote(parsed.username or '')}"

        def _open_raw_connection():
            _check_rate_limit_gate()
            try:
                return pymysql.connect(
                    host=parsed.hostname or "localhost",
                    port=parsed.port or 3306,
                    user=unquote(parsed.username or ""),
                    password=unquote(parsed.password or ""),
                    database=db_name,
                    charset=query.get("charset", ["utf8mb4"])[0],
                    autocommit=False,
                    cursorclass=DictCursor,
                )
            except MySqlOperationalError as exc:
                code = getattr(exc, 'args', [None])[0]
                if code == 1226:
                    _set_rate_limit_gate()
                    raise DatabaseRateLimitError(str(exc)) from exc
                raise

        wrapper = getattr(_MYSQL_THREAD_LOCAL, cache_key, None)
        if wrapper is not None:
            try:
                wrapper._ensure_connection()
                return wrapper
            except Exception:
                try:
                    wrapper.close_physical()
                except Exception:
                    pass

        conn = _open_raw_connection()
        wrapper = MySqlConnectionWrapper(conn, reconnect_factory=_open_raw_connection, cache_key=cache_key)
        setattr(_MYSQL_THREAD_LOCAL, cache_key, wrapper)
        return wrapper

    def now_expression(self) -> str:
        return "NOW()"

    def auto_pk_column(self) -> str:
        return "BIGINT PRIMARY KEY AUTO_INCREMENT"

    def id_ref_column(self) -> str:
        return "BIGINT"

    def indexed_text_column(self, length: int = 191) -> str:
        return f"VARCHAR({max(1, int(length))})"

    def text_column(self) -> str:
        return "LONGTEXT"

    def placeholder(self) -> str:
        return "%s"

    def sql(self, query: str) -> str:
        q = query.replace('?', '%s')
        def _unit(s: str) -> str:
            return s.upper().rstrip('S') if s.upper().endswith('S') and len(s) > 1 else s.upper()

        q = re.sub(
            r"datetime\('now',\s*'-(\d+)\s+(\w+)'\)",
            lambda m: f"DATE_SUB(NOW(), INTERVAL {m.group(1)} {_unit(m.group(2))})",
            q,
        )
        q = re.sub(
            r"datetime\('now',\s*'\+(\d+)\s+(\w+)'\)",
            lambda m: f"DATE_ADD(NOW(), INTERVAL {m.group(1)} {_unit(m.group(2))})",
            q,
        )
        q = (
            q
            .replace("datetime('now', 'localtime')", 'NOW()')
            .replace("date('now', 'localtime')", 'CURDATE()')
            .replace("datetime('now')", 'NOW()')
            .replace("date('now')", 'CURDATE()')
        )
        return q

    def insert_ignore_sql(self, table: str, columns: list[str], values_sql: str | None = None) -> str:
        values = values_sql or self.placeholders(len(columns))
        cols_sql = ", ".join(columns)
        return f"INSERT IGNORE INTO {table} ({cols_sql}) VALUES ({values})"

    def create_index_sql(self, name: str, table: str, columns: str) -> str:
        return f"CREATE INDEX {name} ON {table} ({columns})"

    def results_route_index_sql(self) -> str:
        return (
            "CREATE INDEX idx_results_route ON results ("
            "origin(16), destination(16), outbound_date(16), inbound_date(16), created_at(32)"
            ")"
        )

    def upsert_payment_sql(self) -> str:
        return (
            "INSERT INTO payments "
            "(mp_payment_id, chat_id, plan_name, amount, status, qr_code, ticket_url, created_at) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, {self.now_expression()}) "
            "ON DUPLICATE KEY UPDATE "
            "chat_id = VALUES(chat_id), "
            "plan_name = VALUES(plan_name), "
            "amount = VALUES(amount), "
            "status = VALUES(status), "
            "qr_code = VALUES(qr_code), "
            "ticket_url = VALUES(ticket_url), "
            "created_at = VALUES(created_at)"
        )

    def is_missing_column_error(self, exc: Exception) -> bool:
        if not isinstance(exc, MySqlOperationalError):
            return False
        code = getattr(exc, 'args', [None])[0]
        return code in {1060, 1061, 1091, 1146}

    def is_integrity_error(self, exc: Exception) -> bool:
        return isinstance(exc, MySqlIntegrityError)


def get_adapter() -> DatabaseAdapter:
    return MySqlAdapter()


def is_sqlite() -> bool:
    return False


def connect():
    return get_adapter().connect()


def now_expression() -> str:
    return get_adapter().now_expression()


def sql(query: str) -> str:
    return get_adapter().sql(query)


def auto_pk_column() -> str:
    return get_adapter().auto_pk_column()


def id_ref_column() -> str:
    return get_adapter().id_ref_column()


def indexed_text_column(length: int = 191) -> str:
    return get_adapter().indexed_text_column(length)


def text_column() -> str:
    return get_adapter().text_column()


def create_index_sql(name: str, table: str, columns: str) -> str:
    return get_adapter().create_index_sql(name, table, columns)


def results_route_index_sql() -> str:
    return get_adapter().results_route_index_sql()


def is_missing_column_error(exc: Exception) -> bool:
    return get_adapter().is_missing_column_error(exc)


def is_integrity_error(exc: Exception) -> bool:
    return get_adapter().is_integrity_error(exc)


def insert_ignore_sql(table: str, columns: list[str], placeholders: str | None = None) -> str:
    return get_adapter().insert_ignore_sql(table, columns, placeholders)


def upsert_payment_sql() -> str:
    return get_adapter().upsert_payment_sql()
