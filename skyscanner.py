#!/usr/bin/env python3
"""
Monitor local de passagens usando automação do navegador.

Fonte alvo: Google Flights (sem API key)
Tecnologia: Playwright + banco relacional + Telegram opcional

O que faz:
- abre o navegador como um usuário normal
- pesquisa várias combinações de datas e destinos
- extrai preço visível da página
- salva histórico em banco relacional
- classifica preço por comparação com histórico
- envia alerta por Telegram opcionalmente
- roda uma vez ou em loop a cada 3 horas

Instalação:
    python3 -m venv .venv
    .venv/bin/pip install -r requirements.txt
    PLAYWRIGHT_BROWSERS_PATH=.playwright-browsers .venv/bin/playwright install chromium

Uso:
    export TELEGRAM_BOT_TOKEN='...'
    export TELEGRAM_CHAT_ID='...'
    .venv/bin/python skyscanner.py run-once
    .venv/bin/python skyscanner.py daemon

Observações:
- Como o site pode mudar, os seletores podem precisar de ajustes.
- O script tenta ser conservador, com poucas consultas e pausas.
- Use apenas para suas consultas pessoais e respeite os termos do serviço do site.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import fcntl
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import quote
from config import now_local_iso

os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(Path(__file__).with_name(".playwright-browsers")))

import requests
from config import TELEGRAM_CHAT_ID, TOKEN
from app_logging import get_logger

_BASE_DIR = Path(__file__).resolve().parent
from db import auto_pk_column, connect as connect_db, is_missing_column_error, results_route_index_sql, sql

logger = get_logger('skyscanner')

try:
    from playwright.sync_api import Browser, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright
except ImportError as exc:
    raise SystemExit(
        "Playwright não está instalado neste ambiente.\n"
        "Use um virtualenv local:\n"
        "  python3 -m venv .venv\n"
        "  .venv/bin/pip install -r requirements.txt\n"
        "  .venv/bin/playwright install chromium\n"
        "  .venv/bin/python skyscanner.py run-once"
    ) from exc


DEFAULT_CONFIG = {
    "check_every_hours": 3,
    "full_scan_seconds": 10800,
    "schedule_minutes": 180,
    "headless": True,
    "timeout_ms": 45000,
    "settle_seconds": 2,
    "request_pause_seconds": 0.2,
    "telegram_bot_token": TOKEN,
    "telegram_chat_id": TELEGRAM_CHAT_ID,
    "price_alert_brl": 1800.0,
    "drop_alert_percent": 8.0,
    "target_site": "google_flights",
    "google_persistent_profile_enabled": False,
    "google_auth_worker_enabled": False,
    "google_persistent_profile_dir": str(_BASE_DIR / "google_session"),
    "google_storage_state_path": str(_BASE_DIR / "google_storage_state.json"),
    "google_auth_worker_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "google_auth_worker_viewport_width": 1280,
    "google_auth_worker_viewport_height": 851,
    "google_flights_executor_enabled": False,
    "google_flights_executor_path": str(_BASE_DIR / "google_flights_executor.py"),
    "google_flights_executor_timeout_ms": 90000,
    "google_flights_executor_headless": True,
    "google_flights_executor_slow_mo_ms": 125,
}


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def _env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Variável obrigatória ausente no .env: {name}")
    return value


def _apply_env_overrides(config: dict) -> dict:
    merged = dict(config)

    scan_interval_minutes_raw = os.getenv("SCAN_INTERVAL_MINUTES", "").strip()
    if scan_interval_minutes_raw:
        try:
            scan_interval_minutes = max(1, int(scan_interval_minutes_raw))
            merged["full_scan_seconds"] = scan_interval_minutes * 60
            merged["schedule_minutes"] = scan_interval_minutes
        except ValueError:
            pass

    for env_key, config_key, cast in [
        ("GOOGLE_CHECK_EVERY_HOURS", "check_every_hours", int),
        ("GOOGLE_FULL_SCAN_SECONDS", "full_scan_seconds", int),
        ("GOOGLE_SCHEDULE_MINUTES", "schedule_minutes", int),
        ("GOOGLE_TIMEOUT_MS", "timeout_ms", int),
        ("GOOGLE_SETTLE_SECONDS", "settle_seconds", float),
        ("GOOGLE_REQUEST_PAUSE_SECONDS", "request_pause_seconds", float),
    ]:
        raw = os.getenv(env_key)
        if raw is None or not raw.strip():
            continue
        try:
            merged[config_key] = cast(raw.strip())
        except ValueError:
            pass

    if os.getenv("GOOGLE_HEADLESS") is not None:
        merged["headless"] = _env_bool("GOOGLE_HEADLESS", bool(merged.get("headless", True)))
    if os.getenv("GOOGLE_PERSISTENT_PROFILE_ENABLED") is not None:
        merged["google_persistent_profile_enabled"] = _env_bool("GOOGLE_PERSISTENT_PROFILE_ENABLED", bool(merged.get("google_persistent_profile_enabled", False)))
    if os.getenv("GOOGLE_AUTH_WORKER_ENABLED") is not None:
        merged["google_auth_worker_enabled"] = _env_bool("GOOGLE_AUTH_WORKER_ENABLED", bool(merged.get("google_auth_worker_enabled", False)))
    if os.getenv("GOOGLE_PERSISTENT_PROFILE_DIR"):
        merged["google_persistent_profile_dir"] = os.getenv("GOOGLE_PERSISTENT_PROFILE_DIR", "").strip()
    if os.getenv("GOOGLE_STORAGE_STATE_PATH"):
        merged["google_storage_state_path"] = os.getenv("GOOGLE_STORAGE_STATE_PATH", "").strip()
    if os.getenv("GOOGLE_AUTH_WORKER_USER_AGENT"):
        merged["google_auth_worker_user_agent"] = os.getenv("GOOGLE_AUTH_WORKER_USER_AGENT", "").strip()
    if os.getenv("GOOGLE_AUTH_WORKER_VIEWPORT_WIDTH"):
        try:
            merged["google_auth_worker_viewport_width"] = int(os.getenv("GOOGLE_AUTH_WORKER_VIEWPORT_WIDTH", "").strip())
        except ValueError:
            pass
    if os.getenv("GOOGLE_AUTH_WORKER_VIEWPORT_HEIGHT"):
        try:
            merged["google_auth_worker_viewport_height"] = int(os.getenv("GOOGLE_AUTH_WORKER_VIEWPORT_HEIGHT", "").strip())
        except ValueError:
            pass
    if os.getenv("GOOGLE_FLIGHTS_EXECUTOR_ENABLED") is not None:
        merged["google_flights_executor_enabled"] = _env_bool("GOOGLE_FLIGHTS_EXECUTOR_ENABLED", bool(merged.get("google_flights_executor_enabled", False)))
    if os.getenv("GOOGLE_FLIGHTS_EXECUTOR_PATH"):
        merged["google_flights_executor_path"] = os.getenv("GOOGLE_FLIGHTS_EXECUTOR_PATH", "").strip()
    if os.getenv("GOOGLE_FLIGHTS_EXECUTOR_TIMEOUT_MS"):
        try:
            merged["google_flights_executor_timeout_ms"] = int(os.getenv("GOOGLE_FLIGHTS_EXECUTOR_TIMEOUT_MS", "").strip())
        except ValueError:
            pass
    if os.getenv("GOOGLE_FLIGHTS_EXECUTOR_HEADLESS") is not None:
        merged["google_flights_executor_headless"] = _env_bool("GOOGLE_FLIGHTS_EXECUTOR_HEADLESS", bool(merged.get("google_flights_executor_headless", True)))
    if os.getenv("GOOGLE_FLIGHTS_EXECUTOR_SLOW_MO_MS"):
        try:
            merged["google_flights_executor_slow_mo_ms"] = int(os.getenv("GOOGLE_FLIGHTS_EXECUTOR_SLOW_MO_MS", "").strip())
        except ValueError:
            pass
    if os.getenv("GOOGLE_FLIGHTS_BASE_URL"):
        merged["google_flights_base_url"] = os.getenv("GOOGLE_FLIGHTS_BASE_URL", "").strip()
    if os.getenv("GOOGLE_HL"):
        merged["google_hl"] = os.getenv("GOOGLE_HL", "").strip()
    if os.getenv("GOOGLE_GL"):
        merged["google_gl"] = os.getenv("GOOGLE_GL", "").strip()
    if os.getenv("GOOGLE_CURR"):
        merged["google_curr"] = os.getenv("GOOGLE_CURR", "").strip()

    return merged

CONFIG = dict(DEFAULT_CONFIG)
CONFIG = _apply_env_overrides(CONFIG)
CONFIG["google_flights_base_url"] = _env_required("GOOGLE_FLIGHTS_BASE_URL")
CONFIG["google_hl"] = _env_required("GOOGLE_HL")
CONFIG["google_gl"] = _env_required("GOOGLE_GL")
CONFIG["google_curr"] = _env_required("GOOGLE_CURR")
CONFIG["telegram_api_base_url"] = _env_required("TELEGRAM_API_BASE_URL").rstrip("/")

@dataclass
class RouteQuery:
    origin: str
    destination: str
    outbound_date: str
    inbound_date: str = ""
    trip_type: str = "oneway"


@dataclass
class FlightResult:
    site: str
    origin: str
    destination: str
    outbound_date: str
    price: Optional[float]
    inbound_date: str = ""
    trip_type: str = "oneway"
    currency: str = "BRL"
    url: str = ""
    booking_url: str = ""
    notes: str = ""
    best_vendor: str = ""
    best_vendor_price: Optional[float] = None
    visible_card_price: Optional[float] = None
    booking_options_json: str = ""
    best_airline_vendor: str = ""
    best_airline_price: Optional[float] = None
    best_airline_url: str = ""
    best_airline_visible_price: Optional[float] = None
    best_agency_vendor: str = ""
    best_agency_price: Optional[float] = None
    best_agency_url: str = ""
    best_agency_visible_price: Optional[float] = None


def utc_now_iso() -> str:
    return now_local_iso(sep="T")


def format_brl(value: Optional[float]) -> str:
    if value is None:
        return "sem preço"
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


class Database:
    def __init__(self, path: str | None = None):
        self.conn = connect_db()
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS results (
                id {auto_pk_column()},
                created_at TEXT NOT NULL,
                site TEXT NOT NULL,
                origin TEXT NOT NULL,
                destination TEXT NOT NULL,
                outbound_date TEXT NOT NULL,
                inbound_date TEXT NOT NULL,
                price REAL,
                currency TEXT,
                url TEXT,
                notes TEXT,
                price_band TEXT,
                best_vendor TEXT,
                best_vendor_price REAL,
                visible_card_price REAL,
                booking_options_json TEXT,
                best_airline_vendor TEXT,
                best_airline_price REAL,
                best_airline_url TEXT,
                best_airline_visible_price REAL,
                best_agency_vendor TEXT,
                best_agency_price REAL,
                best_agency_url TEXT,
                best_agency_visible_price REAL
            )
            """
        )
        try:
            cur.execute(results_route_index_sql())
        except Exception as exc:
            if is_missing_column_error(exc):
                pass
            else:
                code = getattr(exc, 'args', [None])[0]
                if code == 1061:
                    pass
                else:
                    raise

        # Migrações leves para bases já existentes
        for ddl in [
            "ALTER TABLE results ADD COLUMN best_vendor TEXT",
            "ALTER TABLE results ADD COLUMN best_vendor_price REAL",
            "ALTER TABLE results ADD COLUMN visible_card_price REAL",
            "ALTER TABLE results ADD COLUMN booking_options_json TEXT",
            "ALTER TABLE results ADD COLUMN best_airline_vendor TEXT",
            "ALTER TABLE results ADD COLUMN best_airline_price REAL",
            "ALTER TABLE results ADD COLUMN best_airline_url TEXT",
            "ALTER TABLE results ADD COLUMN best_airline_visible_price REAL",
            "ALTER TABLE results ADD COLUMN best_agency_vendor TEXT",
            "ALTER TABLE results ADD COLUMN best_agency_price REAL",
            "ALTER TABLE results ADD COLUMN best_agency_url TEXT",
            "ALTER TABLE results ADD COLUMN best_agency_visible_price REAL",
        ]:
            try:
                cur.execute(ddl)
            except Exception as exc:
                if is_missing_column_error(exc):
                    pass
                else:
                    raise

        self.conn.commit()

    def save(self, result: FlightResult, price_band: str) -> None:
        if (
            result.site == 'google_flights'
            and isinstance(result.price, (int, float))
            and not str(result.best_vendor or '').strip()
            and not str(result.booking_options_json or '').strip()
        ):
            return
        self.conn.execute(
            sql("""
            DELETE FROM results
            WHERE site = ?
              AND origin = ?
              AND destination = ?
              AND outbound_date = ?
              AND COALESCE(inbound_date, '') = COALESCE(?, '')
            """),
            (
                result.site,
                result.origin,
                result.destination,
                result.outbound_date,
                result.inbound_date,
            ),
        )
        self.conn.execute(
            sql("""
            INSERT INTO results (
                created_at, site, origin, destination, outbound_date, inbound_date,
                price, currency, url, notes, price_band,
                best_vendor, best_vendor_price, visible_card_price, booking_options_json,
                best_airline_vendor, best_airline_price, best_airline_url, best_airline_visible_price,
                best_agency_vendor, best_agency_price, best_agency_url, best_agency_visible_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """),
            (
                utc_now_iso(),
                result.site,
                result.origin,
                result.destination,
                result.outbound_date,
                result.inbound_date,
                result.price,
                result.currency,
                result.url,
                result.notes,
                price_band,
                result.best_vendor,
                result.best_vendor_price,
                result.visible_card_price,
                result.booking_options_json,
                result.best_airline_vendor,
                result.best_airline_price,
                result.best_airline_url,
                result.best_airline_visible_price,
                result.best_agency_vendor,
                result.best_agency_price,
                result.best_agency_url,
                result.best_agency_visible_price,
            ),
        )
        self.conn.commit()

    def stats_for(self, route: RouteQuery) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        row = self.conn.execute(
            sql("""
            SELECT MIN(price) AS min_price, AVG(price) AS avg_price
            FROM results
            WHERE origin = ? AND destination = ? AND outbound_date = ? AND inbound_date = ? AND price IS NOT NULL
            """),
            (route.origin, route.destination, route.outbound_date, route.inbound_date),
        ).fetchone()
        last = self.conn.execute(
            sql("""
            SELECT price FROM results
            WHERE origin = ? AND destination = ? AND outbound_date = ? AND inbound_date = ? AND price IS NOT NULL
            ORDER BY id DESC LIMIT 1
            """),
            (route.origin, route.destination, route.outbound_date, route.inbound_date),
        ).fetchone()
        min_price = float(row["min_price"]) if row and row["min_price"] is not None else None
        avg_price = float(row["avg_price"]) if row and row["avg_price"] is not None else None
        last_price = float(last["price"]) if last and last["price"] is not None else None
        return min_price, avg_price, last_price


def build_db_routes_from_rows(rows):
    queries = []
    for row in rows:
        origin = (row["origin"] or "").strip().upper()
        destination = (row["destination"] or "").strip().upper()
        outbound = (row["outbound_date"] or "").strip()
        inbound = (row["inbound_date"] or "").strip()
        if not origin or not destination or not outbound:
            continue
        trip_type = "roundtrip" if inbound else "oneway"
        queries.append(RouteQuery(
            origin=origin,
            destination=destination,
            outbound_date=outbound,
            inbound_date=inbound,
            trip_type=trip_type,
        ))
    return queries


def load_user_routes_from_db() -> List[RouteQuery]:
    conn = connect_db()
    try:
        rows = conn.execute(sql("SELECT origin, destination, outbound_date, inbound_date FROM user_routes WHERE active = 1")).fetchall()
        return build_db_routes_from_rows(rows)
    finally:
        conn.close()


def build_db_queries() -> List[RouteQuery]:
    return load_user_routes_from_db()


def classify_price(price: Optional[float], min_price: Optional[float], avg_price: Optional[float]) -> str:
    if price is None:
        return "sem_preco"
    if min_price is None and avg_price is None:
        return "novo"
    if min_price is not None and price <= min_price:
        return "excelente"
    if avg_price is not None and price <= avg_price * 0.92:
        return "bom"
    if avg_price is not None and price >= avg_price * 1.15:
        return "caro"
    return "normal"


def should_alert(price: Optional[float], min_price: Optional[float], last_price: Optional[float]) -> Tuple[bool, str]:
    if price is None:
        return False, "sem preço"
    if price <= float(CONFIG["price_alert_brl"]):
        return True, f"abaixo do teto configurado ({format_brl(CONFIG['price_alert_brl'])})"
    if min_price is not None and price <= min_price:
        return True, "novo menor preço"
    if last_price is not None and last_price > 0:
        drop = ((last_price - price) / last_price) * 100.0
        if drop >= float(CONFIG["drop_alert_percent"]):
            return True, f"queda de {drop:.1f}%"
    return False, "sem gatilho"


def send_telegram_message(text: str) -> None:
    token = CONFIG["telegram_bot_token"]
    chat_id = CONFIG["telegram_chat_id"]
    if not token or not chat_id:
        print("[alerta] Telegram não configurado")
        print(text)
        return
    url = f"{CONFIG['telegram_api_base_url']}/bot{token}/sendMessage"
    resp = requests.post(url, data={"chat_id": chat_id, "text": text}, timeout=30)
    resp.raise_for_status()


def parse_price_brl(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = text.replace("\xa0", " ")
    patterns = [
        r"R\$\s*([\d\.]+,\d{2})",
        r"R\$\s*([\d\.]+)",
    ]
    for pattern in patterns:
        matches = re.findall(pattern, cleaned)
        if matches:
            raw = matches[0].replace(".", "").replace(",", ".")
            try:
                return float(raw)
            except ValueError:
                pass
    return None


def build_google_flights_url(route: RouteQuery) -> str:
    if route.trip_type == "oneway":
        q = f"{route.origin} to {route.destination} {route.outbound_date} one way"
    else:
        q = f"{route.origin} to {route.destination} {route.outbound_date} return {route.inbound_date}"
    base_url = str(CONFIG["google_flights_base_url"]).rstrip("/")
    if base_url.endswith("/travel/flights"):
        base_url = f"{base_url}/search"
    elif "/travel/flights/search" not in base_url:
        base_url = "https://www.google.com/travel/flights/search"
    hl = str(CONFIG["google_hl"])
    gl = str(CONFIG["google_gl"])
    # Forçar sempre BRL para evitar problemas com filtros de preço do bot
    curr = "BRL"
    return f"{base_url}?q={quote(q)}&hl={hl}&gl={gl}&curr={curr}"


def describe_trip(route: RouteQuery) -> str:
    if route.trip_type == "oneway":
        return f"{route.origin}->{route.destination} | {route.outbound_date} | ida simples"
    return f"{route.origin}->{route.destination} | {route.outbound_date}/{route.inbound_date} | ida e volta"


class GoogleProfileLock:
    def __init__(self, lock_path: str):
        self.lock_path = lock_path
        self.handle = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.lock_path), exist_ok=True)
        try:
            self.handle = open(self.lock_path, 'w')
        except PermissionError:
            # Lock com ownership incorreta (root etc) — limpa e tenta de novo
            try:
                os.unlink(self.lock_path)
            except OSError:
                pass
            self.handle = open(self.lock_path, 'w')
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(str(os.getpid()))
        self.handle.flush()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.handle:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
                self.handle.close()
        except Exception:
            pass


def _clean_vendor_label(vendor: str) -> str:
    cleaned = str(vendor or "").strip()
    cleaned = re.sub(r"\s*companhia a[eé]rea\s*$", "", cleaned, flags=re.IGNORECASE).strip()
    return cleaned


CACHE_DB_PATH = Path(__file__).resolve().parent / 'price_cache.db'
CACHE_TTL_SECONDS = 3600  # 1h


def _price_cache_key(route: RouteQuery) -> str:
    """Chave única para cache: origem-destino-data-ida-data-volta-tipo"""
    parts = [route.origin.upper(), route.destination.upper(), route.outbound_date]
    if (route.inbound_date or '').strip():
        parts.append(route.inbound_date)
    parts.append(route.trip_type or 'oneway')
    return '-'.join(parts)


def _cache_get(route: RouteQuery) -> FlightResult | None:
    """Cache desativado permanentemente por decisao de Teles."""
    return None




def _cache_set(route: RouteQuery, result: FlightResult):
    """Cache desativado permanentemente por decisao de Teles."""
    pass



def run_google_flights_executor(route: RouteQuery, allow_agencies: bool = True, profile_dir: Optional[str] = None) -> FlightResult:
    # Verificar cache primeiro
    cached = _cache_get(route)
    if cached is not None:
        return cached

    executor_path = str(CONFIG.get("google_flights_executor_path") or "").strip()
    if not executor_path:
        raise RuntimeError("GOOGLE_FLIGHTS_EXECUTOR_PATH não configurado")
    repo_python = Path(executor_path).resolve().parent / ".venv" / "bin" / "python"
    python_bin = str(repo_python) if repo_python.exists() else sys.executable
    command = [python_bin, executor_path, route.origin, route.destination, route.outbound_date]
    if (route.inbound_date or "").strip():
        command.append(route.inbound_date)
    env = os.environ.copy()
    env["GOOGLE_FLIGHTS_EXECUTOR_HEADLESS"] = "1" if bool(CONFIG.get("google_flights_executor_headless", True)) else "0"
    env["GOOGLE_FLIGHTS_EXECUTOR_TIMEOUT_MS"] = str(int(CONFIG.get("google_flights_executor_timeout_ms", 90000)))
    env["GOOGLE_FLIGHTS_EXECUTOR_SLOW_MO_MS"] = str(int(CONFIG.get("google_flights_executor_slow_mo_ms", 125)))
    env["GOOGLE_FLIGHTS_ALLOW_AGENCIES"] = "1" if allow_agencies else "0"
    
    _profile_dir = Path(profile_dir or CONFIG.get("google_persistent_profile_dir")).resolve()
    env["GOOGLE_PERSISTENT_PROFILE_DIR"] = str(_profile_dir)
    lock_path = str(_profile_dir.parent / f"{_profile_dir.name}.lock")
    executor_timeout_ms = int(CONFIG.get("google_flights_executor_timeout_ms", 90000))
    _subprocess_timeout = max(60, min(300, int(executor_timeout_ms / 1000) + 20))
    # Timeout adaptativo: se rota é muito distante (>6 meses), reduz timeout
    try:
        from datetime import datetime, date
        out_dt = datetime.strptime(route.outbound_date, '%Y-%m-%d').date()
        today = date.today()
        days_ahead = (out_dt - today).days
        if days_ahead > 240:
            _subprocess_timeout = min(_subprocess_timeout, 120)
            timeout_redux = min(executor_timeout_ms, 110000)
            env['GOOGLE_FLIGHTS_EXECUTOR_TIMEOUT_MS'] = str(timeout_redux)
            env['GOOGLE_FLIGHTS_SHORT_TIMEOUT'] = '1'
            logger.info('[google-executor] rota muito distante (%dd), timeout adaptado: %ss', days_ahead, _subprocess_timeout)
    except Exception:
        pass
    logger.info('[google-executor] rota=%s->%s ida=%s volta=%s | allow_agencies=%s | timeout_ms=%s | hard_timeout_s=%s', route.origin, route.destination, route.outbound_date, route.inbound_date or '-', allow_agencies, executor_timeout_ms, _subprocess_timeout)
    with GoogleProfileLock(lock_path):
        proc = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            cwd=str(Path(executor_path).resolve().parent),
            start_new_session=True,
        )
        logger.info('[google-executor] pid=%s | rota=%s->%s | subprocess iniciado', proc.pid, route.origin, route.destination)
        try:
            _stdout, _stderr = proc.communicate(timeout=_subprocess_timeout)
        except subprocess.TimeoutExpired:
            logger.warning('[google-executor] pid=%s | rota=%s->%s | timeout após %ss, matando processo', proc.pid, route.origin, route.destination, _subprocess_timeout)
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.communicate()
            raise RuntimeError(f'executor timeout após {_subprocess_timeout}s (Chrome morto)')
        completed = type('R', (), {'stdout': _stdout, 'stderr': _stderr, 'returncode': proc.returncode})()
    logger.info('[google-executor] pid=%s | rota=%s->%s | subprocess finalizado rc=%s | stdout_bytes=%s | stderr_bytes=%s', getattr(proc, 'pid', '?'), route.origin, route.destination, completed.returncode, len(completed.stdout or ''), len(completed.stderr or ''))
    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()
    payload = {}
    if stdout:
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError:
            payload = {}
            for line in reversed([ln.strip() for ln in stdout.splitlines() if ln.strip()]):
                try:
                    payload = json.loads(line)
                    break
                except json.JSONDecodeError:
                    continue
            if not payload:
                raise RuntimeError(f"executor stdout inválido: {stdout[:300]}")
    if completed.returncode != 0 and not payload:
        raise RuntimeError(stderr or stdout or f"executor retornou {completed.returncode}")
    notes = ["worker=google_flights_executor"]
    raw_notes = payload.get("notes") if isinstance(payload, dict) else None
    if isinstance(raw_notes, list):
        notes.extend(str(item) for item in raw_notes if str(item).strip())
    elif isinstance(raw_notes, str) and raw_notes.strip():
        notes.append(raw_notes.strip())
    for label in ["summary_price", "main_min", "other_min", "overall_min"]:
        value = payload.get(label) if isinstance(payload, dict) else None
        if isinstance(value, (int, float)):
            notes.append(f"{label}={format_brl(float(value))}")
    if stderr:
        notes.append(f"executor_stderr={stderr[:240]}")
    if payload.get("error"):
        notes.append(f"executor_error={payload['error']}")
    best_vendor = _clean_vendor_label(str(payload.get("best_vendor") or ""))
    best_vendor_price = payload.get("best_vendor_price")
    best_airline_vendor = _clean_vendor_label(str(payload.get("best_airline_vendor") or ""))
    best_airline_price = payload.get("best_airline_price")
    best_airline_url = str(payload.get("best_airline_url") or "")
    best_airline_visible_price = payload.get("best_airline_visible_price")
    best_agency_vendor = _clean_vendor_label(str(payload.get("best_agency_vendor") or ""))
    best_agency_price = payload.get("best_agency_price")
    best_agency_url = str(payload.get("best_agency_url") or "")
    best_agency_visible_price = payload.get("best_agency_visible_price")
    booking_options = payload.get("booking_options") if isinstance(payload.get("booking_options"), list) else []
    price = payload.get("price")
    booking_url = str(payload.get("booking_url") or "")
    final_url = booking_url or str(payload.get("url") or build_google_flights_url(route))
    # Cache resultado p/ evitar re-scraping desnecessário
    result = FlightResult(
        site="google_flights",
        origin=route.origin,
        destination=route.destination,
        outbound_date=route.outbound_date,
        inbound_date=route.inbound_date,
        trip_type=route.trip_type,
        price=float(price) if isinstance(price, (int, float)) else None,
        currency="BRL",
        url=final_url,
        booking_url=booking_url,
        notes=" | ".join(notes),
        best_vendor=best_vendor,
        best_vendor_price=float(best_vendor_price) if isinstance(best_vendor_price, (int, float)) else None,
        visible_card_price=float(payload.get("visible_card_price")) if isinstance(payload.get("visible_card_price"), (int, float)) else None,
        booking_options_json=json.dumps(booking_options, ensure_ascii=False) if booking_options else "",
        best_airline_vendor=best_airline_vendor,
        best_airline_price=float(best_airline_price) if isinstance(best_airline_price, (int, float)) else None,
        best_airline_url=best_airline_url,
        best_airline_visible_price=float(best_airline_visible_price) if isinstance(best_airline_visible_price, (int, float)) else None,
        best_agency_vendor=best_agency_vendor,
        best_agency_price=float(best_agency_price) if isinstance(best_agency_price, (int, float)) else None,
        best_agency_url=best_agency_url,
        best_agency_visible_price=float(best_agency_visible_price) if isinstance(best_agency_visible_price, (int, float)) else None,
    )
    _cache_set(route, result)
    return result


class GoogleFlightsScraper:
    def __init__(self, browser):
        self.browser = browser

    def _configure_context(self, ctx) -> None:
        def _handle_route(route):
            try:
                request = route.request
                resource_type = (request.resource_type or "").lower()
                url = (request.url or "").lower()
                blocked_resource_types = {"image", "media", "font"}
                blocked_url_terms = (
                    "doubleclick",
                    "google-analytics",
                    "googletagmanager",
                    "facebook",
                    "hotjar",
                    "segment",
                    "analytics",
                    "pixel",
                )
                if resource_type in blocked_resource_types or any(term in url for term in blocked_url_terms):
                    route.abort()
                    return
            except Exception:
                pass
            route.continue_()

        try:
            ctx.route("**/*", _handle_route)
        except Exception:
            pass

    def _accept_cookies_if_present(self, page) -> None:
        labels = ["Aceitar tudo", "Aceito", "I agree", "Accept all"]
        for label in labels:
            try:
                page.get_by_role("button", name=label).click(timeout=2000)
                time.sleep(1)
                return
            except Exception:
                pass

    def _wait_briefly_for_results(self, page) -> None:
        ready = False
        for selector in [
            "[role='main'] [role='listitem']",
            "[role='main'] li",
            "[role='main'] div[role='button']",
        ]:
            try:
                page.locator(selector).first.wait_for(timeout=2200)
                ready = True
                break
            except Exception:
                pass
        if not ready:
            for tab_label in ["Melhor opção", "Menores preços"]:
                try:
                    page.locator(f"text={tab_label}").first.wait_for(timeout=2000)
                    ready = True
                    break
                except Exception:
                    pass
        if not ready:
            try:
                page.locator("text=Outros voos").first.wait_for(timeout=2500)
                ready = True
            except Exception:
                pass

        settle_seconds = float(CONFIG.get("settle_seconds", 2))
        extra_wait = min(0.9, settle_seconds * 0.45) if ready else min(1.2, settle_seconds * 0.6)
        if extra_wait > 0:
            time.sleep(extra_wait)
        try:
            page.mouse.wheel(0, 1800)
            time.sleep(0.45)
            page.mouse.wheel(0, -900)
            time.sleep(0.25)
        except Exception:
            pass

    def _ensure_flights_tab(self, page) -> bool:
        switched = False
        url = (page.url or "").lower()
        if "/travel/flights/search" in url:
            return False
        for label in ["Voos", "Flights"]:
            try:
                loc = page.get_by_role("button", name=re.compile(rf"^{label}$", re.I))
                if loc.count() > 0:
                    loc.first.click(timeout=3000)
                    time.sleep(1.0)
                    switched = True
                    return switched
            except Exception:
                pass
            try:
                loc = page.get_by_role("link", name=re.compile(rf"^{label}$", re.I))
                if loc.count() > 0:
                    loc.first.click(timeout=3000)
                    time.sleep(1.0)
                    switched = True
                    return switched
            except Exception:
                pass
            try:
                loc = page.get_by_text(label, exact=True)
                if loc.count() > 0:
                    loc.first.click(timeout=3000)
                    time.sleep(1.0)
                    switched = True
                    return switched
            except Exception:
                pass
        return switched

    def _extract_summary_price(self, page) -> float | None:
        try:
            loc = page.locator('[jsname="v8pSFe"]').first
            if loc.count() > 0:
                txt = loc.inner_text(timeout=2500)
                value = parse_price_brl(txt)
                if value is not None:
                    return value
        except Exception:
            pass
        patterns = [
            r"Menores preços\s+a partir de\s+R\$\s*([\d\.]+(?:,\d{2})?)",
            r"Menores preços.*?R\$\s*([\d\.]+(?:,\d{2})?)",
        ]
        for sel in ["body", "main", "[role='main']"]:
            try:
                txt = page.locator(sel).first.inner_text(timeout=3000)
                if not txt:
                    continue
                for pattern in patterns:
                    m = re.search(pattern, txt, flags=re.IGNORECASE | re.DOTALL)
                    if m:
                        try:
                            return float(m.group(1).replace(".", "").replace(",", "."))
                        except ValueError:
                            pass
            except Exception:
                pass
        return None

    def _extract_section_prices(self, page) -> tuple[list[float], list[float]]:
        try:
            body_text = page.locator("body").inner_text(timeout=4000)
        except Exception:
            body_text = ""
        main_section_prices = []
        other_section_prices = []
        if body_text:
            main_match = re.search(r"Principais voos([\s\S]*?)Outros voos", body_text, flags=re.IGNORECASE)
            if main_match:
                for raw in re.findall(r"R\$\s*([\d\.]+(?:,\d{2})?)", main_match.group(1)):
                    try:
                        val = float(raw.replace('.', '').replace(',', '.'))
                    except Exception:
                        continue
                    if val >= 300:
                        main_section_prices.append(val)
            other_match = re.search(r"Outros voos([\s\S]*?)(?:Mostrar mais voos|Idioma|Localização|Moeda|$)", body_text, flags=re.IGNORECASE)
            if other_match:
                for raw in re.findall(r"R\$\s*([\d\.]+(?:,\d{2})?)", other_match.group(1)):
                    try:
                        val = float(raw.replace('.', '').replace(',', '.'))
                    except Exception:
                        continue
                    if val >= 300:
                        other_section_prices.append(val)
        return main_section_prices, other_section_prices

    def _expand_results_like_human(self, page, notes: list[str] | None = None) -> None:
        for _ in range(4):
            try:
                page.mouse.wheel(0, 900)
            except Exception:
                pass
            time.sleep(1.0)
        try:
            btn = page.get_by_text("Mostrar mais voos", exact=False)
            if btn.count() > 0:
                btn.first.click(timeout=4000)
                time.sleep(2.0)
                if notes is not None:
                    notes.append("clicou_mostrar_mais_voos=sim")
        except Exception:
            pass
        for _ in range(3):
            try:
                page.mouse.wheel(0, 900)
            except Exception:
                pass
            time.sleep(1.0)

    def _refresh_storage_state_from_context(self, ctx) -> None:
        state_path = str(CONFIG.get("google_storage_state_path") or "").strip()
        if not state_path:
            return
        try:
            os.makedirs(os.path.dirname(state_path), exist_ok=True)
            ctx.storage_state(path=state_path)
        except Exception:
            pass

    def _is_authenticated_google_session(self, page) -> bool:
        try:
            body = page.locator("body").inner_text(timeout=2500)
        except Exception:
            body = ""
        low = (body or "").lower()
        if "fazer login" in low or "sign in" in low:
            return False
        selectors = [
            'a[aria-label*="Conta do Google"]',
            'a[aria-label*="Google Account"]',
            'img[alt*="Foto do perfil"]',
            'img[alt*="Profile picture"]',
            '[data-ogsr-up]',
        ]
        for selector in selectors:
            try:
                if page.locator(selector).count() > 0:
                    return True
            except Exception:
                pass
        return False

    def _build_auth_required_result(self, route: RouteQuery, page, url: str, details: str = "") -> FlightResult:
        notes = [
            "error_code=google_auth_required",
            "action_required=reauth_google_profile",
            f"profile_dir={CONFIG.get('google_persistent_profile_dir')}",
        ]
        if details:
            notes.append(details)
        alert = (
            "⚠️ Google Flights precisa de reautenticação\n"
            f"Rota: {route.origin} → {route.destination}\n"
            f"Data: {route.outbound_date}\n"
            f"Perfil: {CONFIG.get('google_persistent_profile_dir')}\n"
            "Motivo: sessão autenticada não detectada. Faça login novamente no perfil persistente."
        )
        try:
            send_telegram_message(alert)
        except Exception as exc:
            notes.append(f"telegram_alert_error={exc}")
        return FlightResult(
            site="google_flights",
            origin=route.origin,
            destination=route.destination,
            outbound_date=route.outbound_date,
            inbound_date=route.inbound_date,
            trip_type=route.trip_type,
            price=None,
            currency="BRL",
            url=page.url if page else url,
            notes=" | ".join(notes),
            best_vendor="AUTH_REQUIRED",
        )

    def _click_best_option_tab(self, page) -> bool:
        def _is_best_option_active() -> bool:
            checks = [
                lambda: page.get_by_role("tab", name=re.compile(r"Melhor opção", re.I)).first,
                lambda: page.get_by_role("button", name=re.compile(r"Melhor opção", re.I)).first,
                lambda: page.get_by_text("Melhor opção", exact=False).first,
            ]
            for getter in checks:
                try:
                    loc = getter()
                    cls = (loc.get_attribute("class") or "").lower()
                    aria = (loc.get_attribute("aria-selected") or "").lower()
                    if aria == 'true' or 'active' in cls or 'selected' in cls:
                        return True
                except Exception:
                    pass
            return False

        candidates = [
            lambda: page.get_by_role("tab", name=re.compile(r"Melhor opção", re.I)),
            lambda: page.get_by_role("button", name=re.compile(r"Melhor opção", re.I)),
            lambda: page.get_by_text("Melhor opção", exact=False),
        ]
        for factory in candidates:
            try:
                loc = factory()
                if loc.count() > 0:
                    self._try_click(loc.first)
                    time.sleep(1.2)
                    if _is_best_option_active():
                        self._wait_briefly_for_results(page)
                        return True
            except Exception:
                pass
        return False

    def _is_probable_flight_card(self, text: str) -> bool:
        low = text.lower()
        if not text or "R$" not in text:
            return False
        if any(x in low for x in ["menores preços", "histórico", "gráfico", "monitorar", "explorar"]):
            return False
        return any(x in low for x in ["parada", "escalas", "co2", "emissões", "voo", "aeroporto"])

    def _extract_card_date_iso_from_text(self, text: str, fallback_year: int) -> str | None:
        month_map = {
            "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
            "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
        }
        low = (text or "").lower()
        m = re.search(r"\b(\d{1,2})\s+de\s+([a-zç]{3,9})\b", low)
        if not m:
            return None
        try:
            day = int(m.group(1))
        except Exception:
            return None
        raw_month = m.group(2)[:3]
        month = month_map.get(raw_month)
        if not month:
            return None
        return f"{fallback_year:04d}-{month:02d}-{day:02d}"

    def _extract_visible_flight_cards(self, page, route: RouteQuery) -> list[dict]:
        cards = []
        selectors = [
            "[role='main'] [role='listitem']",
            "[role='main'] li",
            "[role='main'] div[jscontroller]",
            "[role='main'] div[role='button']",
            "[role='listitem']",
            "li",
            "div[jscontroller]",
            "div[role='button']",
        ]
        seen = set()

        def _add_cards_from_locator(loc, selector_name: str):
            nonlocal cards, seen
            count = min(loc.count(), 260)
            for i in range(count):
                card = loc.nth(i)
                try:
                    txt = card.inner_text(timeout=1200).strip()
                except Exception:
                    continue

                if not self._is_probable_flight_card(txt):
                    continue

                card_date_iso = self._extract_card_date_iso_from_text(txt, fallback_year=int(route.outbound_date[:4]))
                if card_date_iso and card_date_iso != route.outbound_date:
                    continue

                nums = re.findall(r"R\$\s*([\d\.]+(?:,\d{2})?)", txt)
                if not nums:
                    continue
                parsed_prices = []
                for raw in nums:
                    try:
                        parsed_prices.append(float(raw.replace('.', '').replace(',', '.')))
                    except Exception:
                        pass
                if not parsed_prices:
                    continue

                card_price = min(parsed_prices)
                key = (round(card_price, 2), txt[:220])
                if key in seen:
                    continue
                seen.add(key)

                cards.append({
                    "selector": selector_name,
                    "index": i,
                    "price": card_price,
                    "prices": parsed_prices,
                    "text": txt[:500],
                    "loc": card,
                })

        section_patterns = [
            re.compile(r"Principais voos", re.I),
            re.compile(r"Outros voos", re.I),
        ]
        section_hit = False
        for pattern in section_patterns:
            try:
                heading = page.get_by_text(pattern).first
                if heading.count() <= 0:
                    continue
                container = heading.locator("xpath=ancestor::*[self::section or self::div][1]")
                if container.count() > 0:
                    section_hit = True
                    for sel in selectors:
                        try:
                            _add_cards_from_locator(container.locator(sel), f"section:{pattern.pattern}:{sel}")
                        except Exception:
                            pass
            except Exception:
                pass

        if not section_hit:
            for sel in selectors:
                try:
                    _add_cards_from_locator(page.locator(sel), sel)
                except Exception:
                    pass
        else:
            for sel in selectors[:2]:
                try:
                    _add_cards_from_locator(page.locator(sel), f"global-extra:{sel}")
                except Exception:
                    pass

        return sorted(cards, key=lambda item: item.get("price") if item.get("price") is not None else 10**12)

    def _sort_candidate_cards(self, cards: list[dict], summary_price: float | None) -> list[dict]:
        def _score(item: dict):
            price = item.get("price")
            if price is None:
                return (10**12, 10**12)
            if summary_price is None:
                return (0, price)
            return (abs(price - summary_price), price)

        return sorted(cards, key=_score)

    def _extract_airline_from_card_text(self, text: str) -> str:
        txt = (text or "").lower()
        if "azul" in txt:
            return "Azul"
        if "latam" in txt:
            return "LATAM"
        if "gol" in txt:
            return "GOL"
        if "voepass" in txt:
            return "VOEPASS"
        if "avianca" in txt:
            return "Avianca"
        if "tap" in txt:
            return "TAP"
        if "copa" in txt:
            return "Copa"
        return ""

    def _infer_airline_from_page_text(self, page) -> str:
        try:
            txt = page.locator("[role='main']").first.inner_text(timeout=2500).lower()
        except Exception:
            return ""
        hits = []
        for token, label in [
            ("azul", "Azul"),
            ("latam", "LATAM"),
            ("gol", "GOL"),
            ("voepass", "VOEPASS"),
            ("avianca", "Avianca"),
            ("tap", "TAP"),
            ("copa", "Copa"),
        ]:
            if token in txt:
                hits.append(label)
        unique = sorted(set(hits))
        if len(unique) == 1:
            return unique[0]
        return ""

    def _extract_fallback_price_from_page_text(self, page) -> float | None:
        try:
            txt = page.locator("body").inner_text(timeout=3500)
        except Exception:
            return None
        if not txt:
            return None
        prices = re.findall(r"R\$\s*([\d\.]+(?:,\d{2})?)", txt)
        values = []
        for raw in prices[:40]:
            try:
                values.append(float(raw.replace(".", "").replace(",", ".")))
            except Exception:
                pass
        if not values:
            return None
        # Evita pegar valores muito baixos/ruído de taxas isoladas.
        candidates = [v for v in values if v >= 300]
        if not candidates:
            return None
        return min(candidates)

    def _try_click(self, target) -> bool:
        strategies = [
            lambda: target.click(timeout=3500),
            lambda: target.click(timeout=3500, force=True),
        ]
        for fn in strategies:
            try:
                fn()
                return True
            except Exception:
                pass
        return False

    def _wait_for_booking_page(self, page) -> bool:
        if "/travel/flights/booking" in (page.url or ""):
            return True
        try:
            page.wait_for_url(re.compile(r".*/travel/flights/booking.*"), timeout=12000)
            return True
        except Exception:
            return "/travel/flights/booking" in (page.url or "")

    def _open_booking_from_card(self, page, card) -> bool:
        debug_open = False
        try:
            debug_open = (
                'AEP' in (page.url or '') and 'PVH' in (page.url or '') and '2026-06-15' in (page.url or '')
            )
        except Exception:
            debug_open = False

        try:
            card.scroll_into_view_if_needed(timeout=2000)
        except Exception:
            pass

        card_clicked = False
        try:
            card.click(timeout=4000)
            card_clicked = True
            time.sleep(1.5)
        except Exception:
            pass

        if self._wait_for_booking_page(page):
            return True

        action_labels = [
            "Selecionar voo", "Ver voos", "Selecionar", "Reservar", "Opções de reserva",
            "Continuar", "Ver opção", "Escolher"
        ]

        targets = [card, page]
        for target in targets:
            for label in action_labels:
                for role in ["button", "link"]:
                    try:
                        loc = target.get_by_role(role, name=re.compile(label, re.I))
                        if loc.count() > 0 and self._try_click(loc.first):
                            time.sleep(1.8)
                            if self._wait_for_booking_page(page):
                                return True
                    except Exception:
                        pass

        if card_clicked:
            try:
                card.dblclick(timeout=2500)
                time.sleep(1.5)
            except Exception:
                pass

        opened = self._wait_for_booking_page(page)
        if debug_open and not opened:
            try:
                dump_dir = os.path.join(str(_BASE_DIR), 'debug_dumps')
                os.makedirs(dump_dir, exist_ok=True)
                body_text = page.locator('body').inner_text(timeout=5000)
                with open(os.path.join(dump_dir, 'debug_route_open_booking_failure.txt'), 'w', encoding='utf-8') as fh:
                    fh.write(body_text or '')
                page.screenshot(path=os.path.join(dump_dir, 'debug_route_open_booking_failure.png'), full_page=True)
            except Exception:
                pass
        return opened

    def _collect_booking_text_blocks(self, page) -> list[str]:
        blocks = []
        selectors = [
            "[role='main'] [role='listitem']",
            "[role='main'] li",
            "[role='main'] div",
        ]
        for sel in selectors:
            try:
                loc = page.locator(sel)
                count = min(loc.count(), 180)
                for i in range(count):
                    try:
                        txt = loc.nth(i).inner_text(timeout=800).strip()
                    except Exception:
                        continue
                    if txt and "R$" in txt:
                        blocks.append(txt)
                if blocks:
                    break
            except Exception:
                pass
        if not blocks:
            try:
                body = page.locator("body").inner_text(timeout=5000)
                if body:
                    blocks = [body]
            except Exception:
                pass
        return blocks

    def _extract_vendor_options_from_text(self, text: str) -> list[dict]:
        options = []
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

        for idx, line in enumerate(lines):
            context = " ".join(lines[idx:min(len(lines), idx + 4)])
            if not re.search(r"Reserve com(?: a)?|Reservar com(?: a)?|Comprar com(?: a)?|Vendido por|Emitido por", context, re.I):
                continue

            vendor = ""
            m_vendor = re.search(r"(?:Reserve com(?: a)?|Reservar com(?: a)?|Comprar com(?: a)?|Vendido por|Emitido por)\s+([^\n\r]+?)($|\s+Companhia a[ée]rea|\s+R\$)", context, re.I)
            if m_vendor:
                vendor = m_vendor.group(1).strip(" :-")

            price = None
            m_price = re.search(r"R\$\s*([\d\.]+(?:,\d{2})?)", context)
            if m_price:
                try:
                    price = float(m_price.group(1).replace('.', '').replace(',', '.'))
                except Exception:
                    price = None

            is_airline = bool(re.search(r"Companhia a[ée]rea", context, re.I))
            if vendor and price is not None:
                options.append({"vendor": vendor, "price": price, "is_airline": is_airline})

        dedup = []
        seen = set()
        for item in options:
            key = (item["vendor"].lower(), item["price"], bool(item.get("is_airline")))
            if key not in seen:
                seen.add(key)
                dedup.append(item)
        return dedup

    def _extract_booking_total_price(self, page) -> float | None:
        patterns = [
            r"Menor preço total\s*R\$\s*([\d\.]+(?:,\d{2})?)",
            r"Menor preço total.*?R\$\s*([\d\.]+(?:,\d{2})?)",
            r"Menores preços\s+a partir de\s+R\$\s*([\d\.]+(?:,\d{2})?)",
        ]
        for sel in ["body", "main", "[role='main']"]:
            try:
                txt = page.locator(sel).first.inner_text(timeout=4000)
            except Exception:
                continue
            if not txt:
                continue
            for pattern in patterns:
                m = re.search(pattern, txt, flags=re.IGNORECASE | re.DOTALL)
                if not m:
                    continue
                try:
                    return float(m.group(1).replace(".", "").replace(",", "."))
                except Exception:
                    pass
        return None

    def _extract_booking_on_google_agency(self, page) -> str:
        patterns = [
            r"O Google enviar[aá] suas informa[cç][oõ]es(?: com seguran[cç]a)? para\s+([^\.\n\r]+)",
            r"enviar[aá] suas informa[cç][oõ]es(?: com seguran[cç]a)? para\s+([^\.\n\r]+)",
            r"compartilhar[aá] suas informa[cç][oõ]es(?: com seguran[cç]a)? com\s+([^\.\n\r]+)",
        ]
        selectors = ["body", "main", "[role='main']"]
        texts = []
        for sel in selectors:
            try:
                txt = page.locator(sel).first.inner_text(timeout=4000)
            except Exception:
                continue
            if txt:
                texts.append(txt)
        for txt in texts:
            if not re.search(r"Reserva(?:r)? no Google", txt, flags=re.IGNORECASE):
                continue
            for pattern in patterns:
                m = re.search(pattern, txt, flags=re.IGNORECASE)
                if m:
                    agency = m.group(1).strip(" .:-\n\r\t")
                    if agency and agency.lower() not in {"google", "google flights"}:
                        return agency

        try:
            imgs = page.locator("img[alt]")
            count = min(imgs.count(), 30)
            for i in range(count):
                try:
                    alt = (imgs.nth(i).get_attribute("alt") or "").strip()
                except Exception:
                    continue
                if not alt:
                    continue
                low = alt.lower()
                if low in {"google", "google flights"}:
                    continue
                if any(term in low for term in ["gotogate", "mytrip", "decolar", "booking", "kiwi", "expedia", "trip.com", "edreams", "zupper", "maxmilhas", "viajanet"]):
                    return alt
        except Exception:
            pass

        return ""

    def _extract_booking_options(self, page, allow_agencies: bool = False) -> tuple[str, float | None, list[dict]]:
        blocks = self._collect_booking_text_blocks(page)
        options = []
        for block in blocks:
            options.extend(self._extract_vendor_options_from_text(block))

        heuristic_options = []
        airline_hint_re = re.compile(r'reserve com a?\s+([^\n\r]+?)\s*companhia a[eé]rea', re.IGNORECASE)
        generic_airline_re = re.compile(r'([^\n\r]{3,120}?)\s*companhia a[eé]rea', re.IGNORECASE)
        for block in blocks:
            if not block or not isinstance(block, str):
                continue
            block_lower = block.lower()
            if 'companhia aérea' not in block_lower and 'companhia aerea' not in block_lower:
                continue
            for match in airline_hint_re.finditer(block):
                vendor = _clean_vendor_label((match.group(1) or '').strip(" :-–|\n\t"))
                if vendor:
                    heuristic_options.append({'vendor': vendor, 'price': None, 'is_airline': True})
            if not heuristic_options:
                for match in generic_airline_re.finditer(block):
                    vendor = _clean_vendor_label((match.group(1) or '').strip(" :-–|\n\t"))
                    if vendor:
                        heuristic_options.append({'vendor': vendor, 'price': None, 'is_airline': True})
        if heuristic_options:
            options.extend(heuristic_options)

        cleaned = []
        seen = set()
        for item in options:
            vendor = (item.get("vendor") or "").strip()
            price = item.get("price")
            is_airline = bool(item.get("is_airline"))
            if not vendor:
                continue
            key = (vendor.lower(), price, is_airline)
            if key not in seen:
                seen.add(key)
                cleaned.append({"vendor": vendor, "price": price, "is_airline": is_airline})

        booking_total_price = self._extract_booking_total_price(page)
        for item in cleaned:
            if item.get('price') is None and booking_total_price is not None:
                item['price'] = booking_total_price
        booking_google_agency = self._extract_booking_on_google_agency(page)
        if booking_google_agency and booking_total_price is not None:
            cleaned.append({"vendor": booking_google_agency, "price": booking_total_price, "is_airline": False})

        if not cleaned:
            return "", booking_total_price, []

        airline_options = [item for item in cleaned if bool(item.get("is_airline"))]
        agency_options = [item for item in cleaned if not bool(item.get("is_airline"))]

        if allow_agencies:
            pool = airline_options + agency_options
        else:
            pool = airline_options

        if not pool:
            if allow_agencies and agency_options:
                best = sorted(agency_options, key=lambda x: x["price"])[0]
                return best["vendor"], best["price"], cleaned
            return "", booking_total_price, cleaned

        best = sorted(pool, key=lambda x: x["price"])[0]
        return best["vendor"], best["price"], cleaned

    def search(self, route: RouteQuery, allow_agencies: bool = True, profile_dir: Optional[str] = None) -> FlightResult:
        if CONFIG.get("google_flights_executor_enabled"):
            return run_google_flights_executor(route, allow_agencies=allow_agencies, profile_dir=profile_dir)

        is_international = not (
            len((route.origin or '').strip()) == 3
            and len((route.destination or '').strip()) == 3
            and (route.origin or '').upper().startswith(("P", "S", "G", "B"))
            and (route.destination or '').upper().startswith(("P", "S", "G", "B"))
        )

        if is_international and CONFIG.get("google_flights_executor_path"):
            try:
                executor_result = run_google_flights_executor(route, allow_agencies=allow_agencies)
                notes = (executor_result.notes or "")
                if notes:
                    executor_result.notes = f"delegated_to_executor=yes | {notes}"
                else:
                    executor_result.notes = "delegated_to_executor=yes"
                return executor_result
            except Exception:
                pass

        context = getattr(self.browser, "new_context", None)
        ctx = None
        if callable(context):
            state_path = str(CONFIG.get("google_storage_state_path") or "").strip()
            new_context_kwargs = {
                "locale": "pt-BR",
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            }
            if state_path and os.path.exists(state_path):
                new_context_kwargs["storage_state"] = state_path
            ctx = self.browser.new_context(**new_context_kwargs)
            self._configure_context(ctx)
            page = ctx.new_page()
        else:
            page = self.browser.new_page()
        page.set_default_timeout(int(CONFIG["timeout_ms"]))
        url = build_google_flights_url(route)
        notes = []
        route_debug_prefix = None
        if (
            (route.origin or '').upper() == 'AEP'
            and (route.destination or '').upper() == 'PVH'
            and (route.outbound_date or '') == '2026-06-15'
        ):
            debug_dump = True
            route_debug_prefix = 'aep_pvh_debug'
        elif (
            (route.origin or '').upper() == 'PVH'
            and (route.destination or '').upper() == 'CUN'
            and (route.outbound_date or '') == '2026-07-22'
        ):
            debug_dump = True
            route_debug_prefix = 'pvh_cun_debug'
        else:
            debug_dump = False
        try:
            if CONFIG.get("google_persistent_profile_enabled"):
                try:
                    page.goto("https://www.google.com/", wait_until="domcontentloaded")
                    time.sleep(1.5)
                except Exception:
                    pass
            page.goto(url, wait_until="domcontentloaded")
            self._accept_cookies_if_present(page)
            switched_tab = self._ensure_flights_tab(page)
            notes.append(f"url_pos_abertura={page.url}")
            notes.append(f"forcou_aba_voos={'sim' if switched_tab else 'nao'}")
            if CONFIG.get("google_persistent_profile_enabled"):
                notes.append(f"persistent_profile_dir={CONFIG.get('google_persistent_profile_dir')}")
                notes.append(f"storage_state_path={CONFIG.get('google_storage_state_path')}")
                notes.append("persistent_launch=headful_slowmo")
            self._wait_briefly_for_results(page)

            summary_price = self._extract_summary_price(page)
            notes.append(f"summary_price={format_brl(summary_price)}" if summary_price is not None else "summary_price=N/D")
            if CONFIG.get("google_persistent_profile_enabled") and summary_price is not None:
                notes.append("persistent_profile_summary_seen=yes")

            clicked_best = self._click_best_option_tab(page)
            notes.append(f"clicou_melhor_opcao={'sim' if clicked_best else 'nao'}")

            if CONFIG.get("google_persistent_profile_enabled"):
                for _ in range(4):
                    try:
                        page.mouse.wheel(0, 900)
                    except Exception:
                        pass
                    time.sleep(1.0)
                try:
                    btn = page.get_by_text("Mostrar mais voos", exact=False)
                    if btn.count() > 0:
                        btn.first.click(timeout=4000)
                        time.sleep(2.0)
                        notes.append("clicou_mostrar_mais_voos=sim")
                except Exception:
                    pass
                for _ in range(3):
                    try:
                        page.mouse.wheel(0, 900)
                    except Exception:
                        pass
                    time.sleep(1.0)
            else:
                try:
                    page.mouse.wheel(0, 2600)
                    time.sleep(1.0)
                    page.mouse.wheel(0, -1400)
                    time.sleep(0.8)
                except Exception:
                    pass
            cards = self._extract_visible_flight_cards(page, route)
            if not cards:
                notes.append("retry_cards_vazios=sim")
                switched_tab_retry = self._ensure_flights_tab(page)
                if switched_tab_retry:
                    notes.append("retry_forcou_aba_voos=sim")
                self._wait_briefly_for_results(page)
                cards = self._extract_visible_flight_cards(page, route)
            notes.append(f"cards_encontrados={len(cards)}")
            if debug_dump:
                try:
                    dump_dir = os.path.join(str(_BASE_DIR), 'debug_dumps')
                    os.makedirs(dump_dir, exist_ok=True)
                    body_text = page.locator('body').inner_text(timeout=5000)
                    with open(os.path.join(dump_dir, f'{route_debug_prefix}_body.txt'), 'w', encoding='utf-8') as fh:
                        fh.write(body_text or '')
                    with open(os.path.join(dump_dir, f'{route_debug_prefix}_cards.json'), 'w', encoding='utf-8') as fh:
                        json.dump([
                            {
                                'price': c.get('price'),
                                'prices': c.get('prices'),
                                'text': c.get('text'),
                                'selector': c.get('selector'),
                            }
                            for c in cards
                        ], fh, ensure_ascii=False, indent=2)
                    page.screenshot(path=os.path.join(dump_dir, f'{route_debug_prefix}.png'), full_page=True)
                    notes.append('debug_dump_saved=yes')
                except Exception as exc:
                    notes.append(f'debug_dump_error={exc}')

            best_vendor = ""
            best_vendor_price = None
            booking_options = []
            final_price = None
            booking_opened = False
            visible_min_price = min((item.get("price") for item in cards if item.get("price") is not None), default=None)
            try:
                body_text_for_min = page.locator("body").inner_text(timeout=4000)
            except Exception:
                body_text_for_min = ""
            other_section_prices = []
            main_section_prices = []
            if body_text_for_min:
                main_match = re.search(r"Principais voos([\s\S]*?)Outros voos", body_text_for_min, flags=re.IGNORECASE)
                if main_match:
                    for raw in re.findall(r"R\$\s*([\d\.]+(?:,\d{2})?)", main_match.group(1)):
                        try:
                            val = float(raw.replace('.', '').replace(',', '.'))
                        except Exception:
                            continue
                        if val >= 300:
                            main_section_prices.append(val)
                m = re.search(r"Outros voos([\s\S]*?)(?:Mostrar mais voos|Idioma|Localização|Moeda|$)", body_text_for_min, flags=re.IGNORECASE)
                if m:
                    for raw in re.findall(r"R\$\s*([\d\.]+(?:,\d{2})?)", m.group(1)):
                        try:
                            val = float(raw.replace('.', '').replace(',', '.'))
                        except Exception:
                            continue
                        if val >= 300:
                            other_section_prices.append(val)
            if body_text_for_min and "Outros voos" in body_text_for_min:
                notes.append("other_section_detected=yes")
            if main_section_prices:
                main_section_min = min(main_section_prices)
                notes.append(f"main_section_prices_count={len(main_section_prices)}")
                notes.append(f"main_section_min_price={format_brl(main_section_min)}")
            else:
                main_section_min = None
            if other_section_prices:
                other_section_min = min(other_section_prices)
                notes.append(f"other_section_prices_count={len(other_section_prices)}")
                notes.append(f"other_section_min_price={format_brl(other_section_min)}")
                if visible_min_price is None or other_section_min < visible_min_price:
                    visible_min_price = other_section_min
            else:
                other_section_min = None
            if visible_min_price is not None:
                notes.append(f"visible_min_price={format_brl(visible_min_price)}")

            ranked_cards = self._sort_candidate_cards(cards, summary_price)
            allow_agencies_mode = bool(agency_filter_enabled)
            if allow_agencies_mode:
                ranked_cards = sorted(
                    cards,
                    key=lambda item: item.get('price') if item.get('price') is not None else 10**12,
                )
                if CONFIG.get("google_persistent_profile_enabled"):
                    notes.append('rank_strategy=persistent_lowest_visible_price')
                else:
                    notes.append('rank_strategy=lowest_visible_price')
            elif CONFIG.get("google_persistent_profile_enabled"):
                target_cards = []
                if summary_price is not None:
                    target_cards = [item for item in cards if item.get('price') is not None and abs(float(item.get('price')) - float(summary_price)) < 0.01]
                    if target_cards:
                        ranked_cards = target_cards + [item for item in cards if item not in target_cards]
                        notes.append('rank_strategy=persistent_summary_price')
                if not target_cards:
                    if main_section_min is not None:
                        main_cards = [item for item in cards if item.get('price') is not None and abs(float(item.get('price')) - float(main_section_min)) < 0.01]
                        if main_cards:
                            ranked_cards = main_cards + [item for item in cards if item not in main_cards]
                            notes.append(f'rank_strategy=persistent_main_min:{format_brl(main_section_min)}')
            max_attempts = min(len(ranked_cards), 8 if (allow_agencies_mode or CONFIG.get("google_persistent_profile_enabled")) else 4)
            if debug_dump:
                try:
                    dump_dir = os.path.join(str(_BASE_DIR), 'debug_dumps')
                    os.makedirs(dump_dir, exist_ok=True)
                    with open(os.path.join(dump_dir, f'{route_debug_prefix}_ranked_cards.json'), 'w', encoding='utf-8') as fh:
                        json.dump([
                            {
                                'idx': i + 1,
                                'price': item.get('price'),
                                'text': item.get('text'),
                                'selector': item.get('selector'),
                            }
                            for i, item in enumerate(ranked_cards[:max_attempts])
                        ], fh, ensure_ascii=False, indent=2)
                    notes.append(f'ranked_cards_debug_saved={min(len(ranked_cards), max_attempts)}')
                except Exception as exc:
                    notes.append(f'ranked_cards_debug_error={exc}')
            best_candidate_vendor = ""
            best_candidate_price = None
            best_candidate_visible_price = None
            best_candidate_options = []
            for idx, item in enumerate(ranked_cards[:max_attempts], start=1):
                price = item.get("price")
                notes.append(f"tentativa_card_{idx}={format_brl(price)}")
                if self._open_booking_from_card(page, item["loc"]):
                    booking_opened = True
                    notes.append(f"booking_aberto_no_card={idx}")
                    card_vendor, card_vendor_price, card_booking_options = self._extract_booking_options(page, allow_agencies=allow_agencies_mode)
                    if debug_dump and not card_booking_options:
                        try:
                            dump_dir = os.path.join(str(_BASE_DIR), 'debug_dumps')
                            os.makedirs(dump_dir, exist_ok=True)
                            panel_text = page.locator('body').inner_text(timeout=5000)
                            with open(os.path.join(dump_dir, f'{route_debug_prefix}_booking_card_{idx}.txt'), 'w', encoding='utf-8') as fh:
                                fh.write(panel_text or '')
                            page.screenshot(path=os.path.join(dump_dir, f'{route_debug_prefix}_booking_card_{idx}.png'), full_page=True)
                            notes.append(f'booking_debug_dump_card_{idx}=yes')
                        except Exception as exc:
                            notes.append(f'booking_debug_dump_card_{idx}_error={exc}')
                    if card_vendor and card_vendor_price is not None:
                        notes.append(f"booking_best_price_card_{idx}={format_brl(card_vendor_price)}")
                        visible_card_price = price if isinstance(price, (int, float)) else None
                        if visible_card_price is not None and abs(card_vendor_price - visible_card_price) >= 0.01:
                            notes.append(
                                f"booking_card_visible_mismatch_{idx}={format_brl(card_vendor_price)}!={format_brl(visible_card_price)}"
                            )
                        if best_candidate_price is None or card_vendor_price < best_candidate_price:
                            best_candidate_vendor = card_vendor
                            best_candidate_price = card_vendor_price
                            best_candidate_visible_price = visible_card_price
                            best_candidate_options = list(card_booking_options or [])
                            notes.append(f"melhor_card_atualizado={idx}:{card_vendor} ({format_brl(card_vendor_price)})")
                    elif card_vendor_price is not None:
                        notes.append(f"booking_total_sem_vendor_card_{idx}={format_brl(card_vendor_price)}")
                        fallback_card_airline = self._extract_airline_from_card_text(item.get("text", ""))
                        if fallback_card_airline and (best_candidate_price is None or card_vendor_price < best_candidate_price):
                            best_candidate_vendor = _clean_vendor_label(fallback_card_airline)
                            best_candidate_price = card_vendor_price
                            best_candidate_visible_price = price if isinstance(price, (int, float)) else None
                            best_candidate_options = list(card_booking_options or [])
                            notes.append(f"booking_card_airline_fallback_{idx}={fallback_card_airline} ({format_brl(card_vendor_price)})")
                        else:
                            notes.append(f"booking_sem_vendor_no_card={idx}")
                    else:
                        fallback_card_airline = self._extract_airline_from_card_text(item.get("text", ""))
                        if fallback_card_airline and isinstance(price, (int, float)) and (best_candidate_price is None or float(price) < best_candidate_price):
                            best_candidate_vendor = _clean_vendor_label(fallback_card_airline)
                            best_candidate_price = float(price)
                            best_candidate_visible_price = float(price)
                            best_candidate_options = list(card_booking_options or [])
                            notes.append(f"booking_card_text_fallback_{idx}={fallback_card_airline} ({format_brl(float(price))})")
                        else:
                            notes.append(f"booking_sem_vendor_no_card={idx}")
                    try:
                        page.go_back(wait_until="domcontentloaded")
                        self._wait_briefly_for_results(page)
                    except Exception:
                        break
                else:
                    notes.append(f"falha_abrir_booking_card={idx}")

            if best_candidate_vendor and best_candidate_price is not None:
                best_vendor = _clean_vendor_label(best_candidate_vendor)
                best_vendor_price = best_candidate_price
                visible_min_price = best_candidate_visible_price if best_candidate_visible_price is not None else visible_min_price
                booking_options = best_candidate_options

            if not booking_opened and ranked_cards:
                fallback = ranked_cards[0]
                notes.append(f"fallback_primeira_lista={format_brl(fallback.get('price'))}")
                fallback_airline = self._extract_airline_from_card_text(fallback.get("text", ""))
                if not fallback_airline:
                    fallback_airline = self._infer_airline_from_page_text(page)
                if fallback_airline and fallback.get("price") is not None:
                    best_vendor = _clean_vendor_label(fallback_airline)
                    best_vendor_price = fallback.get("price")
                    booking_options = [{"vendor": fallback_airline, "price": fallback.get("price")}]
                    notes.append(f"fallback_card_airline={fallback_airline} ({format_brl(best_vendor_price)})")

            if not ranked_cards:
                fallback_airline = self._infer_airline_from_page_text(page)
                fallback_price = self._extract_fallback_price_from_page_text(page)
                if fallback_airline and fallback_price is not None:
                    best_vendor = _clean_vendor_label(fallback_airline)
                    best_vendor_price = fallback_price
                    booking_options = [{"vendor": fallback_airline, "price": fallback_price}]
                    notes.append(f"fallback_page_airline={fallback_airline} ({format_brl(fallback_price)})")

            if best_vendor:
                notes.append(f"melhor_vendedor={best_vendor} ({format_brl(best_vendor_price)})")
                notes.append(f"opcoes_reserva={len(booking_options)}")

            summary_matches_visible = False
            if summary_price is not None and visible_min_price is not None:
                summary_matches_visible = abs(summary_price - visible_min_price) < 0.01
                if not summary_matches_visible:
                    notes.append(
                        f"summary_visible_mismatch={format_brl(summary_price)}!={format_brl(visible_min_price)}"
                    )

            if not best_vendor and best_vendor_price is None and summary_price is not None:
                if visible_min_price is not None and not summary_matches_visible:
                    notes.append("reject_summary_price_without_vendor")
                    summary_price = None
                elif main_section_min is not None:
                    best_vendor_price = main_section_min
                    if allow_agencies_mode:
                        best_vendor = "Agências"
                        notes.append("fallback_vendor_agencias_main_section")
                    notes.append("fallback_to_main_section_price_without_vendor")
                elif visible_min_price is not None:
                    best_vendor_price = visible_min_price
                    if allow_agencies_mode:
                        best_vendor = "Agências"
                        notes.append("fallback_vendor_agencias_visible_price")
                    notes.append("fallback_to_visible_price_without_vendor")

            if best_vendor_price is not None and visible_min_price is not None and abs(best_vendor_price - visible_min_price) >= 0.01:
                notes.append(
                    f"booking_visible_mismatch={format_brl(best_vendor_price)}!={format_brl(visible_min_price)}"
                )

            # Regra final: só aceita preço final quando houver vendor válido.
            # Se agências internacionais estiverem desligadas, não pode cair em
            # fallback genérico de Agências/Outras via summary_price.
            if best_vendor_price is not None and best_vendor:
                final_price = best_vendor_price
                if allow_agencies_mode:
                    notes.append("final_price_source=booking_or_agency")
                else:
                    notes.append("final_price_source=booking_airline")
            elif allow_agencies_mode and best_vendor_price is not None:
                best_vendor = "Agências"
                final_price = best_vendor_price
                notes.append("final_price_source=agency_fallback")
            elif allow_agencies_mode:
                notes.append("final_price_rejected_no_validated_booking_price")
                notes.append("final_price_rejected_no_agency_or_airline_vendor")
            else:
                notes.append("final_price_rejected_no_airline_vendor")

            if final_price is None:
                notes.append("Preço não identificado automaticamente.")

            return FlightResult(
                site="google_flights",
                origin=route.origin,
                destination=route.destination,
                outbound_date=route.outbound_date,
                inbound_date=route.inbound_date,
                trip_type=route.trip_type,
                price=final_price,
                currency="BRL",
                url=page.url,
                notes=" | ".join(notes),
                best_vendor=best_vendor,
                best_vendor_price=best_vendor_price,
                visible_card_price=summary_price,
                booking_options_json=json.dumps(booking_options, ensure_ascii=False) if booking_options else "",
            )
        except PlaywrightTimeoutError:
            return FlightResult(
                site="google_flights",
                origin=route.origin,
                destination=route.destination,
                outbound_date=route.outbound_date,
                inbound_date=route.inbound_date,
                trip_type=route.trip_type,
                price=None,
                currency="BRL",
                url=page.url if page else url,
                notes="timeout na página",
            )
        finally:
            try:
                page.close()
            except Exception:
                pass
            if ctx is not None:
                try:
                    ctx.close()
                except Exception:
                    pass


class AuthenticatedGoogleFlightsWorker(GoogleFlightsScraper):
    def __init__(self, playwright):
        super().__init__(browser=None)
        self.playwright = playwright
        self._context = None

    def _get_context(self):
        if self._context is None:
            _pd = Path(CONFIG.get("google_persistent_profile_dir")).resolve()
            lock_path = str(_pd.parent / f'{_pd.name}.lock')
            self._profile_lock = GoogleProfileLock(lock_path)
            self._profile_lock.__enter__()
            self._context = self.playwright.chromium.launch_persistent_context(
                str(CONFIG.get("google_persistent_profile_dir")),
                headless=bool(CONFIG.get("headless", True)),
                locale="pt-BR",
                user_agent=str(CONFIG.get("google_auth_worker_user_agent")),
                viewport={
                    "width": int(CONFIG.get("google_auth_worker_viewport_width", 1280)),
                    "height": int(CONFIG.get("google_auth_worker_viewport_height", 851)),
                },
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-features=Translate,OptimizationHints,MediaRouter,DialMediaRouteProvider",
                ],
            )
            self._configure_context(self._context)
        return self._context

    def close(self):
        if self._context is not None:
            try:
                self._context.close()
            except Exception:
                pass
            self._context = None
        if getattr(self, '_profile_lock', None) is not None:
            try:
                self._profile_lock.__exit__(None, None, None)
            except Exception:
                pass
            self._profile_lock = None

    def search(self, route: RouteQuery, allow_agencies: bool = True, profile_dir: Optional[str] = None) -> FlightResult:
        if CONFIG.get("google_flights_executor_enabled"):
            return run_google_flights_executor(route, allow_agencies=allow_agencies, profile_dir=profile_dir)
        ctx = self._get_context()
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.set_default_timeout(int(CONFIG["timeout_ms"]))
        url = build_google_flights_url(route)
        notes = ["worker=authenticated_google_flights"]
        try:
            page.goto("https://www.google.com/", wait_until="domcontentloaded")
            time.sleep(1.5)
            if not self._is_authenticated_google_session(page):
                return self._build_auth_required_result(route, page, url, "auth_probe=google_home")
            page.goto(url, wait_until="domcontentloaded")
            self._accept_cookies_if_present(page)
            self._wait_briefly_for_results(page)
            if not self._is_authenticated_google_session(page):
                return self._build_auth_required_result(route, page, url, "auth_probe=flights_page")
            self._refresh_storage_state_from_context(ctx)

            summary_price = self._extract_summary_price(page)
            notes.append(f"summary_price={format_brl(summary_price)}" if summary_price is not None else "summary_price=N/D")
            clicked_best = self._click_best_option_tab(page)
            notes.append(f"clicou_melhor_opcao={'sim' if clicked_best else 'nao'}")
            self._expand_results_like_human(page, notes)
            cards = self._extract_visible_flight_cards(page, route)
            notes.append(f"cards_encontrados={len(cards)}")
            main_section_prices, other_section_prices = self._extract_section_prices(page)
            main_section_min = min(main_section_prices) if main_section_prices else None
            other_section_min = min(other_section_prices) if other_section_prices else None
            if main_section_min is not None:
                notes.append(f"main_section_min_price={format_brl(main_section_min)}")
            if other_section_min is not None:
                notes.append(f"other_section_min_price={format_brl(other_section_min)}")

            ranked_cards = self._sort_candidate_cards(cards, summary_price)
            if summary_price is not None:
                target_cards = [item for item in cards if item.get("price") is not None and abs(float(item.get("price")) - float(summary_price)) < 0.01]
                if target_cards:
                    ranked_cards = target_cards + [item for item in ranked_cards if item not in target_cards]
                    notes.append("rank_strategy=auth_summary_price")
            elif main_section_min is not None:
                main_cards = [item for item in cards if item.get("price") is not None and abs(float(item.get("price")) - float(main_section_min)) < 0.01]
                if main_cards:
                    ranked_cards = main_cards + [item for item in ranked_cards if item not in main_cards]
                    notes.append("rank_strategy=auth_main_section_min")

            best_vendor = ""
            best_vendor_price = None
            booking_options = []
            booking_opened = False
            best_candidate_vendor = ""
            best_candidate_price = None
            best_candidate_visible_price = None
            best_candidate_options = []
            for idx, item in enumerate(ranked_cards[:8], start=1):
                if self._open_booking_from_card(page, item["loc"]):
                    booking_opened = True
                    notes.append(f"booking_aberto_no_card={idx}")
                    card_vendor, card_vendor_price, card_booking_options = self._extract_booking_options(page, allow_agencies=agency_filter_enabled)
                    if card_vendor and card_vendor_price is not None:
                        notes.append(f"booking_best_price_card_{idx}={format_brl(card_vendor_price)}")
                        visible_card_price = item.get("price") if isinstance(item.get("price"), (int, float)) else None
                        if visible_card_price is not None and abs(card_vendor_price - visible_card_price) >= 0.01:
                            notes.append(
                                f"booking_card_visible_mismatch_{idx}={format_brl(card_vendor_price)}!={format_brl(visible_card_price)}"
                            )
                        if best_candidate_price is None or card_vendor_price < best_candidate_price:
                            best_candidate_vendor = card_vendor
                            best_candidate_price = card_vendor_price
                            best_candidate_visible_price = visible_card_price
                            best_candidate_options = list(card_booking_options or [])
                            notes.append(f"melhor_card_atualizado={idx}:{card_vendor} ({format_brl(card_vendor_price)})")
                    elif card_vendor_price is not None:
                        notes.append(f"booking_total_sem_vendor_card_{idx}={format_brl(card_vendor_price)}")
                        if agency_filter_enabled:
                            if best_candidate_price is None or card_vendor_price < best_candidate_price:
                                best_candidate_vendor = "Agências"
                                best_candidate_price = card_vendor_price
                                best_candidate_visible_price = item.get("price") if isinstance(item.get("price"), (int, float)) else None
                                best_candidate_options = list(card_booking_options or [])
                                notes.append(f"melhor_card_agencia_atualizado={idx}:Agências ({format_brl(card_vendor_price)})")
                    else:
                        notes.append(f"booking_sem_vendor_no_card={idx}")
                    try:
                        page.go_back(wait_until="domcontentloaded")
                        self._wait_briefly_for_results(page)
                    except Exception:
                        break

            if best_candidate_vendor and best_candidate_price is not None:
                best_vendor = _clean_vendor_label(best_candidate_vendor)
                best_vendor_price = best_candidate_price
                booking_options = best_candidate_options

            visible_prices = [item.get("price") for item in cards if item.get("price") is not None]
            visible_min_price = best_candidate_visible_price if best_candidate_visible_price is not None else (min(visible_prices) if visible_prices else None)
            final_price = None
            if best_vendor and best_vendor_price is not None:
                final_price = best_vendor_price
                if agency_filter_enabled and best_vendor == "Agências":
                    notes.append("final_price_source=agency_fallback")
                elif agency_filter_enabled:
                    notes.append("final_price_source=booking_or_agency")
                else:
                    notes.append("final_price_source=booking_airline")
            else:
                if visible_min_price is not None:
                    notes.append(f"visible_min_price={format_brl(visible_min_price)}")
                if summary_price is not None and visible_min_price is not None and abs(summary_price - visible_min_price) >= 50:
                    notes.append("session_degraded_detected=summary_visible_mismatch")
                    try:
                        self.close()
                        notes.append("session_context_reset=1")
                    except Exception:
                        pass
                notes.append("final_price_rejected_no_valid_airline_booking")
                notes.append("Preço não identificado automaticamente.")

            return FlightResult(
                site="google_flights",
                origin=route.origin,
                destination=route.destination,
                outbound_date=route.outbound_date,
                inbound_date=route.inbound_date,
                trip_type=route.trip_type,
                price=final_price,
                currency="BRL",
                url=page.url,
                notes=" | ".join(notes),
                best_vendor=best_vendor,
                best_vendor_price=best_vendor_price,
                visible_card_price=visible_min_price,
                booking_options_json=json.dumps(booking_options, ensure_ascii=False) if booking_options else "",
            )
        except PlaywrightTimeoutError:
            return FlightResult(
                site="google_flights",
                origin=route.origin,
                destination=route.destination,
                outbound_date=route.outbound_date,
                inbound_date=route.inbound_date,
                trip_type=route.trip_type,
                price=None,
                currency="BRL",
                url=page.url if page else url,
                notes="timeout na página | worker=authenticated_google_flights",
            )
        finally:
            try:
                page.close()
            except Exception:
                pass


def build_google_flights_worker(playwright=None, browser=None):
    if CONFIG.get("google_auth_worker_enabled"):
        if playwright is None:
            raise RuntimeError("playwright é obrigatório para o AuthenticatedGoogleFlightsWorker")
        return AuthenticatedGoogleFlightsWorker(playwright)
    if browser is None:
        raise RuntimeError("browser é obrigatório para o GoogleFlightsScraper legado")
    return GoogleFlightsScraper(browser)

class Monitor:
    def __init__(self) -> None:
        self.db = Database()

    def run_once(self) -> List[FlightResult]:
        routes = build_queries()
        if not routes:
            print("[coleta] nenhuma rota ativa em user_routes; nada para coletar")
            return []
        results: List[FlightResult] = []

        with sync_playwright() as p:
            browser = None
            if CONFIG.get("google_auth_worker_enabled"):
                scraper = build_google_flights_worker(playwright=p)
            else:
                if CONFIG.get("google_persistent_profile_enabled"):
                    browser = p.chromium.launch_persistent_context(
                        str(CONFIG.get("google_persistent_profile_dir")),
                        headless=False,
                        slow_mo=100,
                        locale="pt-BR",
                        args=[
                            "--disable-blink-features=AutomationControlled",
                            "--disable-gpu",
                            "--disable-dev-shm-usage",
                            "--no-sandbox",
                            "--disable-setuid-sandbox",
                            "--disable-features=Translate,OptimizationHints,MediaRouter,DialMediaRouteProvider",
                        ],
                    )
                else:
                    browser = p.chromium.launch(headless=bool(CONFIG["headless"]))
                scraper = build_google_flights_worker(playwright=p, browser=browser)

            for route in routes:
                result = scraper.search(route)
                min_price, avg_price, last_price = self.db.stats_for(route)
                band = classify_price(result.price, min_price, avg_price)
                self.db.save(result, band)
                results.append(result)
                print(
                    f"[coleta] {describe_trip(route)} | {format_brl(result.price)} | {band}"
                )

                do_alert, reason = should_alert(result.price, min_price, last_price)
                if do_alert:
                    msg = (
                        f"✈️ Alerta de passagem\n"
                        f"Rota: {route.origin} → {route.destination}\n"
                        f"Data: {route.outbound_date}\n"
                        f"Tipo: {'ida simples' if route.trip_type == 'oneway' else 'ida e volta'}\n"
                        f"Preço: {format_brl(result.price)}\n"
                        f"Motivo: {reason}\n"
                        f"Site: {result.site}"
                    )
                    try:
                        send_telegram_message(msg)
                    except Exception as exc:
                        print(f"[erro] telegram: {exc}")

                time.sleep(CONFIG["request_pause_seconds"])

            try:
                scraper.close()
            except Exception:
                pass
            if browser is not None:
                browser.close()

        return results

    def daemon(self) -> None:
        interval = int(CONFIG["check_every_hours"]) * 3600
        while True:
            started = time.time()
            try:
                self.run_once()
            except Exception as exc:
                print(f"[erro] execução: {exc}")
            elapsed = time.time() - started
            sleep_for = max(60, interval - int(elapsed))
            print(f"[daemon] próxima execução em {sleep_for // 60} min")
            time.sleep(sleep_for)


def print_summary(results: Iterable[FlightResult]) -> None:
    sorted_results = sorted(
        [r for r in results if r.price is not None],
        key=lambda x: x.price if x.price is not None else 10**12,
    )
    print("\n=== MELHORES RESULTADOS ===")
    for item in sorted_results[:10]:
        print(
            f"{describe_trip(RouteQuery(item.origin, item.destination, item.outbound_date, item.inbound_date, item.trip_type))} | "
            f"{format_brl(item.price)} | {item.site}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor local de passagens via navegador")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run-once")
    sub.add_parser("daemon")
    sub.add_parser("show-config")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    monitor = Monitor()

    if args.command == "run-once":
        results = monitor.run_once()
        print_summary(results)
        return 0

    if args.command == "daemon":
        monitor.daemon()
        return 0

    if args.command == "show-config":
        for k, v in CONFIG.items():
            print(f"{k} = {v}")
        return 0

    return 2


if __name__ == "__main__":
    sys.exit(main())

build_queries = build_db_queries
