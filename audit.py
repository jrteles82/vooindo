"""
audit.py — Auditoria centralizada de eventos do bot.

Registra em dois destinos simultâneos:
  1. Tabela `audit_events` no banco de dados
  2. Arquivo rotativo logs/audit.log  (uma linha JSON por evento)

Categorias
----------
  USER_ACTION  — comandos e botões acionados pelo usuário
  PAYMENT      — ciclo de vida de pagamentos PIX
  SCRAPING     — buscas de voos (Google Flights, MaxMilhas)
  AUTH         — sessão Google Flights (ok / expirada / renovada)
  ACCESS       — controle de acesso (liberado / bloqueado / expirado)
  ADMIN        — ações do administrador no painel
  SYSTEM       — ciclos de scheduler, inicialização, worker
  ERROR        — exceções, timeouts, falhas de integração

Uso
---
    from audit import audit

    audit.log("USER_ACTION", "cmd_start", chat_id="123", payload={"first_name": "João"})

    # helpers semânticos (mesma assinatura):
    audit.user_action("rota_salva", chat_id="123", user_id="5",
                      payload={"origin": "GRU", "destination": "GIG"})
    audit.payment("pix_aprovado", chat_id="123",
                  payload={"plano": "mensal", "valor": 39.90})
    audit.error("scan_timeout", chat_id="123", error_msg="TimeoutError: 30s")
    audit.scraping("scan_concluido", chat_id="123",
                   duration_ms=4200, payload={"resultados": 8})
"""
from __future__ import annotations

import json
import logging
import queue
import threading
import time
import traceback
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from config import BASE_DIR, now_local_iso
from db import connect as connect_db

_AUDIT_DB_DISABLED_UNTIL = 0.0
_AUDIT_DB_BACKOFF_SECONDS = 1800.0

# ── categorias ────────────────────────────────────────────────────────────────
USER_ACTION = "USER_ACTION"
PAYMENT     = "PAYMENT"
SCRAPING    = "SCRAPING"
AUTH        = "AUTH"
ACCESS      = "ACCESS"
ADMIN       = "ADMIN"
SYSTEM      = "SYSTEM"
ERROR       = "ERROR"

# ── status ────────────────────────────────────────────────────────────────────
OK       = "ok"
BLOCKED  = "blocked"
SKIPPED  = "skipped"
PENDING  = "pending"
ERRO     = "error"

_VALID_CATEGORIES = {USER_ACTION, PAYMENT, SCRAPING, AUTH, ACCESS, ADMIN, SYSTEM, ERROR}
_QUEUE_MAX = 5_000

# ── logger de arquivo dedicado ────────────────────────────────────────────────

def _make_file_logger() -> logging.Logger:
    log_dir = BASE_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    lg = logging.getLogger("audit.file")
    if lg.handlers:
        return lg
    lg.setLevel(logging.DEBUG)
    lg.propagate = False
    fh = RotatingFileHandler(
        log_dir / "audit.log",
        maxBytes=20 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter("%(message)s"))
    lg.addHandler(fh)
    return lg


_file_logger = _make_file_logger()
_std_logger   = logging.getLogger("audit")


# ── schema do banco ───────────────────────────────────────────────────────────

_DDL_MYSQL = """
CREATE TABLE IF NOT EXISTS audit_events (
    id          BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    ts          VARCHAR(26)  NOT NULL,
    category    VARCHAR(20)  NOT NULL,
    event_type  VARCHAR(64)  NOT NULL,
    status      VARCHAR(16)  NOT NULL DEFAULT 'ok',
    chat_id     VARCHAR(32),
    user_id     VARCHAR(32),
    duration_ms INT,
    payload     TEXT,
    error_msg   TEXT,
    INDEX idx_audit_ts   (ts),
    INDEX idx_audit_cat  (category),
    INDEX idx_audit_chat (chat_id),
    INDEX idx_audit_type (event_type)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
"""


def ensure_audit_table() -> None:
    global _AUDIT_DB_DISABLED_UNTIL
    if time.time() < _AUDIT_DB_DISABLED_UNTIL:
        return
    try:
        conn = connect_db()
        cur = conn.cursor()
        cur.execute(_DDL_MYSQL)
        conn.commit()
        conn.close()
    except Exception as exc:
        if "1226" in str(exc):
            _AUDIT_DB_DISABLED_UNTIL = time.time() + _AUDIT_DB_BACKOFF_SECONDS
            _std_logger.warning("audit: banco desativado temporariamente por limite de conexões MySQL")
            return
        _std_logger.exception("audit: falha ao criar tabela audit_events")


# ── fila e thread de escrita ──────────────────────────────────────────────────

class _AuditWorker(threading.Thread):
    """Thread daemon que drena a fila e persiste no banco + arquivo."""

    def __init__(self) -> None:
        super().__init__(name="audit-worker", daemon=True)
        self._q: queue.Queue[dict | None] = queue.Queue(maxsize=_QUEUE_MAX)
        self._ready = False

    def enqueue(self, event: dict) -> None:
        try:
            self._q.put_nowait(event)
        except queue.Full:
            # descarta o mais antigo e tenta de novo
            try:
                self._q.get_nowait()
                self._q.put_nowait(event)
            except Exception:
                pass

    def stop(self) -> None:
        self._q.put(None)

    def run(self) -> None:
        self._ready = True
        conn = self._open_conn()
        while True:
            try:
                event = self._q.get(timeout=2)
                if event is None:
                    break
                self._write_file(event)
                self._write_db(conn, event)
            except queue.Empty:
                continue
            except Exception:
                _std_logger.debug("audit-worker: erro ao persistir evento", exc_info=True)
                # tenta reconectar
                try:
                    conn.close()
                except Exception:
                    pass
                conn = self._open_conn()
        try:
            conn.close()
        except Exception:
            pass

    # ── helpers internos ──────────────────────────────────────────────────────

    @staticmethod
    def _open_conn():
        global _AUDIT_DB_DISABLED_UNTIL
        if time.time() < _AUDIT_DB_DISABLED_UNTIL:
            return _NullConn()
        try:
            return connect_db()
        except Exception as exc:
            if "1226" in str(exc):
                _AUDIT_DB_DISABLED_UNTIL = time.time() + _AUDIT_DB_BACKOFF_SECONDS
            _std_logger.debug("audit-worker: não foi possível abrir conexão", exc_info=True)
            return _NullConn()

    @staticmethod
    def _write_file(event: dict) -> None:
        try:
            _file_logger.info(json.dumps(event, ensure_ascii=False, default=str))
        except Exception:
            pass

    @staticmethod
    def _write_db(conn, event: dict) -> None:
        global _AUDIT_DB_DISABLED_UNTIL
        try:
            insert_sql = (
                "INSERT INTO audit_events "
                "(ts, category, event_type, status, chat_id, user_id, duration_ms, payload, error_msg) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            )
            payload_str = json.dumps(event.get("payload") or {}, ensure_ascii=False, default=str)
            conn.execute(insert_sql, (
                event["ts"],
                event["category"],
                event["event_type"],
                event["status"],
                event.get("chat_id"),
                event.get("user_id"),
                event.get("duration_ms"),
                payload_str,
                event.get("error_msg"),
            ))
            conn.commit()
        except Exception as exc:
            if "1226" in str(exc):
                _AUDIT_DB_DISABLED_UNTIL = time.time() + _AUDIT_DB_BACKOFF_SECONDS
            _std_logger.debug("audit-worker: falha ao gravar no banco", exc_info=True)


class _NullConn:
    """Conexão nula usada como fallback quando o banco está indisponível."""
    def execute(self, *a, **kw): pass
    def commit(self): pass
    def close(self): pass


# ── instância global ──────────────────────────────────────────────────────────

_worker = _AuditWorker()
_worker.start()
ensure_audit_table()


# ── API pública ───────────────────────────────────────────────────────────────

class _Audit:
    """Interface pública de auditoria. Use a instância `audit` deste módulo."""

    def log(
        self,
        category: str,
        event_type: str,
        *,
        status: str = OK,
        chat_id: str | int | None = None,
        user_id: str | int | None = None,
        duration_ms: int | None = None,
        payload: dict[str, Any] | None = None,
        error_msg: str | None = None,
    ) -> None:
        """
        Registra um evento de auditoria de forma não-bloqueante.

        Parâmetros
        ----------
        category    : uma das constantes de categoria (USER_ACTION, PAYMENT, …)
        event_type  : nome do evento (snake_case, ex: "rota_salva", "pix_aprovado")
        status      : "ok" | "error" | "blocked" | "skipped" | "pending"
        chat_id     : Telegram chat_id do usuário (opcional)
        user_id     : ID interno do usuário (opcional)
        duration_ms : tempo de execução em milissegundos (opcional)
        payload     : dict com dados extras do evento (opcional)
        error_msg   : mensagem de erro se status == "error" (opcional)
        """
        if category not in _VALID_CATEGORIES:
            category = ERROR
        event: dict[str, Any] = {
            "ts":          now_local_iso(),
            "category":    category,
            "event_type":  event_type,
            "status":      status,
            "chat_id":     str(chat_id) if chat_id is not None else None,
            "user_id":     str(user_id) if user_id is not None else None,
            "duration_ms": duration_ms,
            "payload":     payload or {},
            "error_msg":   error_msg,
        }
        _worker.enqueue(event)

    # ── helpers semânticos ────────────────────────────────────────────────────

    def user_action(self, event_type: str, **kw) -> None:
        self.log(USER_ACTION, event_type, **kw)

    def payment(self, event_type: str, **kw) -> None:
        self.log(PAYMENT, event_type, **kw)

    def scraping(self, event_type: str, **kw) -> None:
        self.log(SCRAPING, event_type, **kw)

    def auth(self, event_type: str, **kw) -> None:
        self.log(AUTH, event_type, **kw)

    def access(self, event_type: str, **kw) -> None:
        self.log(ACCESS, event_type, **kw)

    def admin(self, event_type: str, **kw) -> None:
        self.log(ADMIN, event_type, **kw)

    def system(self, event_type: str, **kw) -> None:
        self.log(SYSTEM, event_type, **kw)

    def error(self, event_type: str, **kw) -> None:
        if "status" not in kw:
            kw["status"] = ERRO
        self.log(ERROR, event_type, **kw)

    # ── utilitário de tempo ───────────────────────────────────────────────────

    @staticmethod
    def timer() -> "_Timer":
        """
        Cronômetro simples para medir duration_ms.

        Uso:
            t = audit.timer()
            ... código ...
            audit.scraping("scan_concluido", duration_ms=t.elapsed())
        """
        return _Timer()


class _Timer:
    def __init__(self) -> None:
        self._start = time.monotonic()

    def elapsed(self) -> int:
        return int((time.monotonic() - self._start) * 1000)


# instância global exportada
audit = _Audit()
