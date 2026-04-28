from __future__ import annotations

from flask import Flask, Response, jsonify, request, stream_with_context, session, redirect, url_for, render_template_string, g
from pathlib import Path
from tempfile import NamedTemporaryFile
import json
import os
import re
import shlex
import subprocess
import sys
import time
import random
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from datetime import datetime
from math import ceil
from werkzeug.security import generate_password_hash, check_password_hash
from PIL import Image, ImageDraw, ImageFont

from db import auto_pk_column, connect as connect_db, id_ref_column, is_integrity_error, is_missing_column_error, insert_ignore_sql, sql
from app_logging import get_logger

from models import FlightResult, RouteQuery, Database
from skyscanner import GoogleFlightsScraper, build_google_flights_worker, sync_playwright

def format_brl(value: float) -> str:
    if not isinstance(value, (int, float)):
        return "N/D"
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def parse_price_brl(text: str) -> float:
    try:
        cleaned = text.replace("R$", "").replace(".", "").replace(",", ".").strip()
        return float(cleaned)
    except:
        return 0.0

def classify_price(price: float | None, min_price: float | None, average: float | None) -> str:
    if price is None:
        return "⚪️"
    if not isinstance(min_price, (int, float)) or min_price <= 0:
        return "⚪️"
    if price <= min_price:
        return "🟢"
    if isinstance(average, (int, float)) and price <= average:
        return "🟡"
    return "🔴"

logger = get_logger('main')

CITY_HIGHLIGHT_EMOJIS = {
    "NAT": "🟡",
    "FOR": "🔵",
    "REC": "🟢",
    "JPA": "🟣",
}

CITY_HIGHLIGHT_COLORS = {
    "NAT": "#fbbf24",
    "FOR": "#60a5fa",
    "REC": "#34d399",
    "JPA": "#a855f7",
}

FALLBACK_AIRPORT_COLORS = [
    "#2563eb",
    "#16a34a",
    "#ea580c",
    "#7c3aed",
    "#0891b2",
    "#be123c",
    "#0f766e",
    "#1d4ed8",
]
# from  import (
#     buscar_menor_preco as buscar_menor_preco_,
#     filtrar_precos_parcelados,
# )
from config import load_env, now_local, now_local_iso

load_env()

app = Flask(__name__, static_folder="static", static_url_path="/static")
app.secret_key = os.getenv("_SECRET_KEY", "dev-change-this-secret")


def _env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Variável obrigatória ausente no .env: {name}")
    return value


TELEGRAM_API_BASE_URL = _env_required("TELEGRAM_API_BASE_URL").rstrip("/")


CONFIG = {
    "full_scan_seconds": 3 * 60 * 60,
    "schedule_minutes": 60,
    "scan_workers": 1,
    "google_auth_worker_enabled": False,
    "headless": True,
}
DEFAULT_SCAN_INTERVAL = int(CONFIG.get("full_scan_seconds", 3 * 60 * 60))
DEFAULT_SCHEDULE_MINUTES = max(1, int(CONFIG.get("schedule_minutes", DEFAULT_SCAN_INTERVAL // 60)))
DEFAULT_SCAN_INTERVAL_MINUTES = max(1, int(os.getenv("SCAN_INTERVAL_MINUTES", str(DEFAULT_SCHEDULE_MINUTES or 60))))
if DEFAULT_SCAN_INTERVAL_MINUTES < 60:
    DEFAULT_SCAN_INTERVAL_MINUTES = 60
AUTO_SCAN_ENABLED = os.getenv("_AUTO_SCAN", "0") == "1"
USER_SCAN_POLL_SECONDS = int(os.getenv("_USER_SCAN_POLL_SECONDS", "60"))
PANEL_RESTART_COMMAND = os.getenv("_RESTART_COMMAND", "").strip()
_scan_lock = threading.Lock()
_scan_last_run_at = None
SCAN_IMAGE_MAX_ASPECT = float(os.getenv("SCAN_IMAGE_MAX_ASPECT", "4.0"))
SCAN_IMAGE_SCALE = max(0.25, float(os.getenv("SCAN_IMAGE_SCALE", "0.35")))
SCAN_IMAGE_TARGET_WIDTH = max(720, int(os.getenv("SCAN_IMAGE_TARGET_WIDTH", "760")))
SCAN_IMAGE_TELEGRAM_MAX_ASPECT = float(os.getenv("SCAN_IMAGE_TELEGRAM_MAX_ASPECT", "1.20"))
SCHEDULER_SEND_COOLDOWN_SECONDS = int(os.getenv("SCHEDULER_SEND_COOLDOWN_SECONDS", str(max(60, DEFAULT_SCAN_INTERVAL_MINUTES * 60 - 100))))

AIRPORT_OPTIONS = [
    ("PVH", "PVH — Porto Velho (RO)"),
    ("BPS", "BPS — Porto Seguro (BA)"),
    ("RIO", "RIO — Rio de Janeiro (RJ)"),
    ("SAO", "SAO — São Paulo (SP)"),
    ("BSB", "BSB — Brasília (DF)"),
    ("CGB", "CGB — Cuiabá (MT)"),
    ("GYN", "GYN — Goiânia (GO)"),
    ("MCZ", "MCZ — Maceió (AL)"),
    ("AJU", "AJU — Aracaju (SE)"),
    ("SSA", "SSA — Salvador (BA)"),
    ("FOR", "FOR — Fortaleza (CE)"),
    ("SLZ", "SLZ — São Luís (MA)"),
    ("CGR", "CGR — Campo Grande (MS)"),
    ("BHZ", "BHZ — Belo Horizonte (MG)"),
    ("BEL", "BEL — Belém (PA)"),
    ("JPA", "JPA — João Pessoa (PB)"),
    ("CWB", "CWB — Curitiba (PR)"),
    ("REC", "REC — Recife (PE)"),
    ("THE", "THE — Teresina (PI)"),
    ("NAT", "NAT — Natal (RN)"),
    ("POA", "POA — Porto Alegre (RS)"),
    ("FLN", "FLN — Florianópolis (SC)"),
    ("VIX", "VIX — Vitória (ES)"),
    ("MAO", "MAO — Manaus (AM)"),
    ("RBR", "RBR — Rio Branco (AC)"),
    ("BVB", "BVB — Boa Vista (RR)"),
    ("MCP", "MCP — Macapá (AP)"),
    ("PMW", "PMW — Palmas (TO)"),
]


def build_restart_redirect(message: str, level: str = "info"):
    return redirect(url_for("painel", _anchor="cron", restart_status=level, restart_message=message))


def trigger_service_restart() -> tuple[bool, str, bool]:
    command = PANEL_RESTART_COMMAND or 'systemctl restart vooindo-bot.service'
    try:
        completed = subprocess.run(
            shlex.split(command),
            capture_output=True,
            text=True,
            timeout=30,
        )
        if completed.returncode != 0:
            error_details = (completed.stderr or completed.stdout or "").strip()
            suffix = f" Detalhes: {error_details}" if error_details else ""
            return False, f"Falha ao executar reinício.{suffix}", False
        return True, "✅ Serviço reiniciado com sucesso. O bot já está de volta e pronto para uso.", False
    except Exception as exc:
        return False, f"Falha ao executar reinício: {exc}", False


def date_color_token(date_iso: str | None) -> tuple[str, str]:
    txt = (date_iso or "").strip()
    palette = [
        ("🔵", "azul"),
        ("🟢", "verde"),
        ("🟠", "laranja"),
        ("🟣", "roxo"),
        ("🟡", "amarelo"),
        ("🔴", "vermelho"),
        ("🟤", "marrom"),
    ]
    digits = [int(ch) for ch in txt if ch.isdigit()]
    if not digits:
        return "⚪", "cinza"
    return palette[sum(digits) % len(palette)]


def format_date_display(raw: str | None) -> str:
    txt = (raw or "").strip()
    if not txt:
        return txt
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(txt, fmt).strftime("%d-%m-%Y")
        except ValueError:
            continue
    return txt


def build_full_scan_message(parsed: list[dict], trigger: str = "manual") -> str:
    def _price_num(row):
        v = row.get("price")
        return v if isinstance(v, (int, float)) and v is not None else 10**12

    def _dedupe_sorted_rows(rows: list[dict]) -> list[dict]:
        seen = set()
        result = []
        for row in rows:
            key = (
                str(row.get("origin", "")).upper(),
                str(row.get("destination", "")).upper(),
                row.get("outbound_date", ""),
                row.get("inbound_date", "") or "",
            )
            if key in seen:
                continue
            seen.add(key)
            result.append(row)
        return result

    PRICE_BAND_COLORS = {
        "excelente": "🟢",
        "bom": "🟡",
        "normal": "🔵",
        "caro": "🟤",
        "sem_preco": "⚪️",
        "novo": "🔵",
    }

    def _format_direction(rows: list[dict], best_row: dict | None, section_title: str, highlight_axis: str) -> list[str]:
        if not rows:
            return [section_title, "N/D"]

        grouped: dict[str, list[dict]] = {}
        for row in rows:
            date = row.get("outbound_date", "") or ""
            grouped.setdefault(date, []).append(row)

        ordered_dates = sorted(grouped.keys())
        section_lines = [section_title]
        for date_idx, date in enumerate(ordered_dates):
            group = grouped[date]
            header = f"📅 {format_date_display(date)}" if date else "📅 data pendente"
            section_lines.append(header)
            for row in group:
                is_best = best_row is row
                color = PRICE_BAND_COLORS.get((row.get("price_band") or "").lower(), "🔵")
                display_txt = _price_vendor_display(row)
                best_note = " | melhor preço" if is_best else ""
                highlight_value = (row.get(highlight_axis) or "").upper()
                route_label = f"{row.get('origin')}→{row.get('destination')}"
                section_lines.append(
                    f"{route_label} | {color} {format_date_display(row.get('outbound_date'))} | {display_txt}{best_note}"
                )
            if date_idx != len(ordered_dates) - 1:
                section_lines.append("")
        return section_lines

    if not parsed:
        return (
            "- ────────── ✈️ CONSULTA COMPLETA ✈️ ────────── -\n"
            "Sem dados nesta execução."
        )

    rows = _dedupe_sorted_rows(parsed)

    lines = [
        "- ────────── ✈️ CONSULTA COMPLETA ✈️ ────────── -",
        f"Execução: {trigger}",
        "",
        *(_format_direction(rows, rows[0] if rows else None, "ROTAS (ordem de cadastro):", "destination")),
    ]

    total_ok = len([r for r in parsed if r.get("price") is not None])
    lines += ["", f"Resumo: {total_ok}/{len(parsed)} rotas com preço válido."]
    return "\n".join(lines)


def notify_full_scan(parsed: list[dict], trigger: str = "manual", send_fn=None, max_price: float | None = None, airline_filters_json: str | None = None) -> None:
    filtered = filter_rows_by_max_price(parsed, max_price)
    filtered = normalize_rows_for_airline_priority(filtered, airline_filters_json)
    filtered = filter_rows_with_vendor(filtered)
    try:
        from bot import should_show_result_type_filters
        conn = get_db_connection(auth_db_path())
        try:
            show_result_type_filters = should_show_result_type_filters(conn)
        finally:
            conn.close()
    except Exception:
        show_result_type_filters = True
    msg = build_full_scan_message(filtered, trigger=trigger)
    sender = send_fn or send_telegram_message
    try:
        sender(msg, image_rows=filtered, trigger=trigger, airline_filters_json=airline_filters_json, show_result_type_filters=show_result_type_filters)
    except TypeError:
        try:
            sender(msg)
        except Exception:
            pass
    except Exception:
        pass


def _build_user_routes(conn, user_id: int, prune_expired: bool = False) -> list[RouteQuery]:
    if prune_expired:
        conn.execute(
            sql(
                """
                DELETE FROM user_routes
                WHERE user_id = ?
                  AND active = 1
                  AND date(outbound_date) < date('now', 'localtime')
                """
            ),
            (user_id,),
        )
        conn.commit()
    rows = conn.execute(
        sql(
            """
            SELECT origin, destination, outbound_date, inbound_date
            FROM user_routes
            WHERE user_id = ? AND active = 1
            ORDER BY id ASC
            """
        ),
        (user_id,),
    ).fetchall()
    routes = []
    for r in rows:
        inbound = (r["inbound_date"] or "").strip()
        routes.append(
            RouteQuery(
                origin=(r["origin"] or "").upper(),
                destination=(r["destination"] or "").upper(),
                outbound_date=r["outbound_date"],
                inbound_date=inbound,
                trip_type="roundtrip" if inbound else "oneway",
            )
        )
    return routes


def _routes_for_request_user() -> list[RouteQuery]:
    user = current_user()
    if user:
        conn = get_auth_db()
        routes = _build_user_routes(conn, int(user["id"]))
        if routes:
            return routes
    return build_db_queries()


def _result_to_row(result: FlightResult, price_band: str) -> dict:
    return {
        "origin": result.origin,
        "destination": result.destination,
        "outbound_date": result.outbound_date,
        "inbound_date": result.inbound_date,
        "trip_type": result.trip_type,
        "price": result.price,
        "price_fmt": format_brl(result.price),
        "site": result.site,
        "currency": result.currency,
        "url": result.url,
        "booking_url": getattr(result, "booking_url", ""),
        "notes": result.notes,
        "price_band": price_band,
        "best_vendor": getattr(result, "best_vendor", ""),
        "best_vendor_price": getattr(result, "best_vendor_price", None),
        "visible_card_price": getattr(result, "visible_card_price", None),
        "final_price_source": extract_final_price_source(result.notes),
    }


def _expand_result_rows(row: dict) -> list[dict]:
    return [row]


def _search_google_result(scraper: GoogleFlightsScraper, route: RouteQuery, fast_mode: bool = False, profile_dir: Optional[str] = None) -> FlightResult:
    if fast_mode:
        origin_opts = [route.origin]
        destination_opts = [route.destination]
    else:
        metro_expansions = {
            "SAO": ["GRU", "CGH", "VCP"],
            "RIO": ["GIG", "SDU"],
            "BHZ": ["CNF", "PLU"],
            "REC": ["REC"],
            "FOR": ["FOR"],
            "POA": ["POA"],
            "NAT": ["NAT"],
            "JPA": ["JPA"],
            "MCZ": ["MCZ"],
            "SSA": ["SSA"],
            "MAO": ["MAO"],
            "CWB": ["CWB"],
            "BSB": ["BSB"],
            "BEL": ["BEL"],
            "FLN": ["FLN"],
            "VIX": ["VIX"],
            "GYN": ["GYN"],
            "CGB": ["CGB"],
            "SLZ": ["SLZ"],
            "AJU": ["AJU"],
            "THE": ["THE"],
            "RBR": ["RBR"],
        }
        origin_opts = metro_expansions.get(route.origin, [route.origin])
        destination_opts = metro_expansions.get(route.destination, [route.destination])

    variants_to_search = []
    for origin in origin_opts:
        for destination in destination_opts:
            variant = RouteQuery(
                origin=origin,
                destination=destination,
                outbound_date=route.outbound_date,
                inbound_date=route.inbound_date,
                trip_type=route.trip_type,
            )
            variants_to_search.append(variant)

    variants: list[tuple[RouteQuery, FlightResult]] = []
    executor_enabled = CONFIG.get("google_flights_executor_enabled")

    if executor_enabled and len(variants_to_search) > 1:
        # Perfis disponíveis para paralelismo
        base_dir = Path(__file__).resolve().parent
        available_profiles = [str(base_dir / "google_session")]
        for i in range(2, 6):
            p_dir = base_dir / f"google_session_{i}"
            if p_dir.is_dir():
                available_profiles.append(str(p_dir))
        
        # Garantir que temos ao menos um perfil
        if not available_profiles:
             available_profiles = [str(CONFIG.get("google_persistent_profile_dir"))]
        
        # Limitar workers pelo número de perfis e capacidade do servidor (max 2 paralelos)
        max_workers = min(len(variants_to_search), len(available_profiles), 2)
        
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = []
            for i, v in enumerate(variants_to_search):
                # Distribui perfis round-robin
                p_dir = available_profiles[i % len(available_profiles)]
                futures.append(pool.submit(scraper.search, v, p_dir))
            
            for i, future in enumerate(futures):
                try:
                    res = future.result()
                    variants.append((variants_to_search[i], res))
                except Exception as exc:
                    logger.error(f"Erro na busca paralela do Google Flights: {exc}")
                    # Fallback para resultado vazio com erro
                    variants.append((variants_to_search[i], FlightResult(
                        site="google_flights",
                        origin=variants_to_search[i].origin,
                        destination=variants_to_search[i].destination,
                        outbound_date=variants_to_search[i].outbound_date,
                        inbound_date=variants_to_search[i].inbound_date,
                        trip_type=variants_to_search[i].trip_type,
                        price=None,
                        currency="BRL",
                        url="",
                        notes=f"error_parallel_search={exc}",
                    )))
    else:
        # Serial (padrão antigo ou se apenas 1 variante)
        for v in variants_to_search:
            result = scraper.search(v, profile_dir=profile_dir)
            variants.append((v, result))

    def _score(item: tuple[RouteQuery, FlightResult]) -> tuple[int, float]:
        _variant, result = item
        has_vendor = 0 if (result.best_vendor or "").strip() else 1
        price = float(result.price) if isinstance(result.price, (int, float)) and result.price is not None else 10**12
        return (has_vendor, price)

    variants.sort(key=_score)
    chosen_variant, chosen = variants[0]
    
    # Fallback: Se estiver em fast_mode e não achar preço, tenta expansão completa
    if fast_mode and (chosen.price is None or chosen.price >= 10**11):
        logger.info(f"Fast mode falhou para {route.origin}->{route.destination}, tentando expansão completa...")
        return _search_google_result(scraper, route, fast_mode=False, profile_dir=profile_dir)

    notes_parts = [chosen.notes or ""]
    notes_parts.append(f"google_variant={chosen_variant.origin}->{chosen_variant.destination}")
    notes = " | ".join([p for p in notes_parts if p])

    return FlightResult(
        site=chosen.site,
        origin=route.origin,
        destination=route.destination,
        outbound_date=route.outbound_date,
        inbound_date=route.inbound_date,
        trip_type=route.trip_type,
        price=chosen.price,
        currency=chosen.currency,
        url=chosen.url,
        booking_url=getattr(chosen, "booking_url", ""),
        notes=notes,
        best_vendor=chosen.best_vendor,
        best_vendor_price=chosen.best_vendor_price,
        booking_options_json=getattr(chosen, 'booking_options_json', ''),
    )




def _store_result(db: Database, route: RouteQuery, result: FlightResult) -> list[dict]:
    min_price, avg_price, _last_price = db.stats_for(route)
    band = classify_price(result.price, min_price, avg_price)
    db.save(result, band)
    return _expand_result_rows(_result_to_row(result, band))


def _split_routes(routes: list[RouteQuery], chunks: int) -> list[list[RouteQuery]]:
    if not routes or chunks <= 0:
        return []
    chunk_size = ceil(len(routes) / chunks)
    return [routes[i * chunk_size:(i + 1) * chunk_size] for i in range(chunks)]


def run_scan_for_routes(routes: list[RouteQuery], on_row=None, sources: dict | None = None, fast_mode: bool = False):
    if not routes:
        return []

    total = sum(2 if not (route.inbound_date or "").strip() else 1 for route in routes)
    requested_workers = CONFIG.get("scan_workers", 2)
    try:
        requested_workers = int(requested_workers)
    except (TypeError, ValueError):
        requested_workers = 2
    try:
        override_workers = int(os.getenv("_SCAN_WORKERS", requested_workers))
    except ValueError:
        override_workers = requested_workers
    worker_count = max(1, min(len(routes), override_workers))
    source_flags = sources or {"google_flights": True}
    if CONFIG.get("google_auth_worker_enabled"):
        # Mesmo com auth worker, permitimos 2 paralelos para agilizar consultas manuais multi-rota
        worker_count = max(1, min(len(routes), 2))
    route_chunks = _split_routes(routes, worker_count)
    chunk_results: list[list[tuple[RouteQuery, FlightResult]] | None] = [None] * len(route_chunks)

    def _scan_chunk(chunk_idx: int, chunk_routes: list[RouteQuery]) -> list[tuple[RouteQuery, FlightResult]]:
        if not chunk_routes:
            return []
        worker_results: list[tuple[RouteQuery, FlightResult]] = []
        user_data_dir = os.getenv("_USER_DATA_DIR", "/tmp/-profile")
        chunk_user_dir = f"{user_data_dir}-worker-{chunk_idx}"
        with sync_playwright() as p:
            browser = None
            if CONFIG.get("google_auth_worker_enabled"):
                scraper = build_google_flights_worker(playwright=p)
            else:
                browser = p.chromium.launch_persistent_context(
                    user_data_dir=chunk_user_dir,
                    headless=bool(CONFIG.get("headless", True)),
                    locale="pt-BR",
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    args=[
                        "--disable-gpu",
                        "--disable-dev-shm-usage",
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                    ],
                )
                scraper = build_google_flights_worker(playwright=p, browser=browser)
            try:
                # Determinar perfil para este worker
                base_dir = Path(__file__).resolve().parent
                p_dir = None
                if chunk_idx == 0:
                    p_dir = str(base_dir / "google_session")
                else:
                    alt_dir = base_dir / f"google_session_{chunk_idx + 1}"
                    if alt_dir.is_dir():
                        p_dir = str(alt_dir)

                for route in chunk_routes:
                    if source_flags.get("google_flights", True):
                        try:
                            google_result = _search_google_result(scraper, route, fast_mode=fast_mode, profile_dir=p_dir)
                        except Exception as exc:
                            logger.warning('[scan-chunk] rota=%s->%s ida=%s volta=%s | erro google_flights=%s', route.origin, route.destination, route.outbound_date, route.inbound_date or '-', exc)
                            google_result = FlightResult(
                                site='google_flights',
                                origin=route.origin,
                                destination=route.destination,
                                outbound_date=route.outbound_date,
                                inbound_date=route.inbound_date,
                                trip_type=route.trip_type,
                                price=None,
                                currency='BRL',
                                url='',
                                booking_url='',
                                notes=f'google_flights_error={str(exc)[:240]}',
                            )
                        worker_results.append((route, google_result))
            finally:
                try:
                    scraper.close()
                except Exception:
                    pass
                if browser is not None:
                    browser.close()
        return worker_results

    with _scan_lock:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(_scan_chunk, idx, chunk): idx
                for idx, chunk in enumerate(route_chunks)
            }
            for future in as_completed(futures):
                chunk_idx = futures[future]
                chunk_results[chunk_idx] = future.result()

    db = Database()
    parsed: list[dict] = []
    idx = 0
    try:
        for chunk in chunk_results:
            if not chunk:
                continue
            for route, result in chunk:
                rows = _store_result(db, route, result)
                for row in rows:
                    parsed.append(row)
                    idx += 1
                    if on_row:
                        on_row(idx, total, row)
        return parsed
    finally:
        db.conn.close()


def run_full_scan(on_row=None):
    global _scan_last_run_at
    parsed = run_scan_for_routes(build_db_queries(), on_row=on_row)
    _scan_last_run_at = now_local_iso(sep="T")
    return parsed


def _create_user_run(conn, user_id: int, trigger: str = "manual-user") -> int:
    cur = conn.execute(
        sql("INSERT INTO user_runs (user_id, started_at, status, summary, run_trigger) VALUES (?, ?, ?, ?, ?)"),
        (user_id, now_local_iso(sep="T"), "running", "", trigger),
    )
    conn.commit()
    return int(cur.lastrowid)


def _finish_user_run(conn, run_id: int, status: str, summary: str) -> None:
    try:
        conn.execute(
            sql("UPDATE user_runs SET finished_at = ?, status = ?, summary = ? WHERE id = ?"),
            (now_local_iso(sep="T"), status, summary, run_id),
        )
        conn.commit()
        return
    except Exception:
        pass

    retry_conn = get_db_connection(auth_db_path())
    try:
        retry_conn.execute(
            sql("UPDATE user_runs SET finished_at = ?, status = ?, summary = ? WHERE id = ?"),
            (now_local_iso(sep="T"), status, summary, run_id),
        )
        retry_conn.commit()
    finally:
        retry_conn.close()


def get_scheduler_settings() -> tuple[int, int, float | None]:
    conn = get_db_connection(auth_db_path())
    try:
        row = conn.execute(
            sql("SELECT cron_enabled, scan_interval_minutes, max_price_display FROM app_settings WHERE id = 1")
        ).fetchone()
        if not row:
            return 1, max(1, DEFAULT_SCAN_INTERVAL_MINUTES), None
        enabled = 1 if int(row["cron_enabled"] or 0) == 1 else 0
        interval = max(1, int(row["scan_interval_minutes"] or DEFAULT_SCAN_INTERVAL_MINUTES))
        max_price = row["max_price_display"]
        return enabled, interval, float(max_price) if max_price is not None else None
    finally:
        conn.close()


def _user_has_running_scan(conn, user_id: int) -> bool:
    row = conn.execute(
        sql(
            """
            SELECT 1
            FROM user_runs
            WHERE user_id = ? AND status = 'running'
            ORDER BY id DESC
            LIMIT 1
            """
        ),
        (user_id,),
    ).fetchone()
    return bool(row)


def _cleanup_stale_running_user_runs(conn, stale_minutes: int = 5) -> int:
    rows = conn.execute(
        sql(
            """
            SELECT id, started_at
            FROM user_runs
            WHERE status = 'running'
              AND run_trigger = 'agendada'
            """
        )
    ).fetchall()
    if not rows:
        return 0
    now_dt = now_local()
    stale_ids: list[int] = []
    for row in rows:
        raw_started = (row["started_at"] or "").strip()
        if not raw_started:
            stale_ids.append(int(row["id"]))
            continue
        try:
            started_dt = datetime.fromisoformat(raw_started.replace(" ", "T"))
        except ValueError:
            stale_ids.append(int(row["id"]))
            continue
        age_minutes = (now_dt - started_dt).total_seconds() / 60.0
        if age_minutes >= stale_minutes:
            stale_ids.append(int(row["id"]))

    if not stale_ids:
        return 0
    placeholders = ",".join("?" for _ in stale_ids)
    conn.execute(
        sql(
            f"""
            UPDATE user_runs
            SET status = 'error',
                summary = 'encerrado automaticamente: execução travada',
                finished_at = ?
            WHERE id IN ({placeholders})
            """
        ),
        (now_local_iso(sep="T"), *stale_ids),
    )
    conn.commit()
    return len(stale_ids)


def run_user_scan(user_id: int, trigger: str = "manual-user", notify: bool = True, send_text: bool = False):
    conn = get_db_connection(auth_db_path())
    run_id = _create_user_run(conn, user_id, trigger=trigger)
    try:
        routes = _build_user_routes(conn, user_id)
        if not routes:
            summary = "sem rotas ativas"
            _finish_user_run(conn, run_id, "ok", summary)
            return {"status": "ok", "summary": summary, "parsed": []}
        settings = conn.execute(sql("SELECT COALESCE(enable_google_flights, 1) AS enable_google_flights, COALESCE(airline_filters_json, '') AS airline_filters_json FROM bot_settings WHERE user_id = ?"), (user_id,)).fetchone()
        parsed = run_scan_for_routes(
            routes,
            sources={
                'google_flights': bool(settings['enable_google_flights']) if settings else True,
                '': False,
            },
        )
        max_price = get_global_max_price_limit()
        parsed_for_display = filter_rows_by_max_price(parsed, max_price)
        airline_filters_json = str(settings['airline_filters_json'] or '') if settings else ''
        parsed_for_display = normalize_rows_for_airline_priority(parsed_for_display, airline_filters_json)
        parsed_for_display = filter_rows_with_vendor(parsed_for_display)
        try:
            from bot_scheduler import filter_rows_by_airlines
            from bot import should_show_result_type_filters
            parsed_for_display = filter_rows_by_airlines(parsed_for_display, airline_filters_json, show_result_type_filters=should_show_result_type_filters(conn))
            parsed_for_display = _merge_rows_for_combined_result_view(parsed_for_display)
        except Exception:
            pass
        msg = build_full_scan_message(parsed_for_display, trigger=trigger)
        if notify:
            from bot import should_show_result_type_filters
            send_user_telegram_message(
                user_id,
                msg if send_text else "",
                image_rows=parsed_for_display,
                trigger=trigger,
                airline_filters_json=airline_filters_json,
                show_result_type_filters=should_show_result_type_filters(conn),
            )
        total_ok = len([r for r in parsed_for_display if r.get("price") is not None])
        summary = f"ok: {total_ok}/{len(parsed_for_display)} exibidos"
        _finish_user_run(conn, run_id, "ok", summary)
        return {"status": "ok", "summary": summary, "parsed": parsed_for_display}
    except Exception as e:
        _finish_user_run(conn, run_id, "error", str(e)[:500])
        raise
    finally:
        conn.close()




def _auto_scan_loop():
    while True:
        try:
            enabled, interval_minutes, _max_price = get_scheduler_settings()
            if enabled != 1:
                time.sleep(max(30, USER_SCAN_POLL_SECONDS))
                continue

            auth_conn = get_db_connection(auth_db_path())
            try:
                cleaned = _cleanup_stale_running_user_runs(auth_conn, stale_minutes=5)
                if cleaned:
                    print(f"[auto-scan] encerradas {cleaned} execuções travadas em user_runs")
                user_rows = auth_conn.execute(
                    """
                    SELECT DISTINCT user_id
                    FROM user_routes
                    WHERE active = 1
                    ORDER BY user_id ASC
                    """
                ).fetchall()
                user_ids = [int(row["user_id"]) for row in user_rows]
            finally:
                auth_conn.close()

            sent_count = 0
            for user_id in user_ids:
                try:
                    check_conn = get_db_connection(auth_db_path())
                    try:
                        if _user_has_running_scan(check_conn, user_id):
                            print(f"[auto-scan] usuário {user_id} já possui execução running; pulando")
                            continue
                    finally:
                        check_conn.close()
                    result = run_user_scan(
                        user_id,
                        trigger="agendada",
                        notify=True,
                        send_text=False,
                    )
                    if result.get("parsed"):
                        sent_count += 1
                except Exception as user_exc:
                    print(f"[auto-scan] erro no usuário {user_id}: {user_exc}")

            print(f"[auto-scan] consulta agendada executada para {len(user_ids)} usuários; envios com resultado: {sent_count}")
        except Exception as e:
            print(f"[auto-scan] erro: {e}")
        _, interval_minutes, _ = get_scheduler_settings()
        time.sleep(max(60, interval_minutes * 60))


def start_auto_scan_if_needed():
    if not AUTO_SCAN_ENABLED:
        print("[auto-scan] desativado por _AUTO_SCAN=0")
        return

    is_reloader_main = os.getenv("WERKZEUG_RUN_MAIN") == "true"
    is_debug = os.getenv("FLASK_DEBUG") == "1"
    if is_debug and not is_reloader_main:
        return

    t = threading.Thread(target=_auto_scan_loop, daemon=True)
    t.start()
    print("[auto-scan] ligado: intervalo global pelo BD (app_settings.scan_interval_minutes)")



def send_telegram_message_to(text: str, token: str | None = None, chat_id: str | None = None, reply_markup: dict | None = None, disable_web_page_preview: bool = False, parse_mode: str | None = None) -> None:
    token = token or os.getenv("TELEGRAM_BOT_TOKEN") or CONFIG.get("telegram_bot_token")
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID") or CONFIG.get("telegram_chat_id")
    if not token or not chat_id:
        return
    base_url = TELEGRAM_API_BASE_URL
    url = f"{base_url}/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup is not None:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    if disable_web_page_preview:
        payload["disable_web_page_preview"] = True
    if parse_mode:
        payload["parse_mode"] = parse_mode
    requests.post(url, data=payload, timeout=20).raise_for_status()


def _load_font(size: int, bold: bool = False):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf" if bold else "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size=size)
            except Exception:
                continue
    return ImageFont.load_default()


def _group_scan_rows_for_image(rows: list[dict]) -> list[tuple[str, list[dict]]]:
    return [("ROTAS", rows)] if rows else []


def _pretty_vendor_name(raw: str) -> str:
    txt = (raw or "").strip()
    if not txt:
        return "N/D"
    # Remove sufixos de identificação do Google
    import re as _re
    # Remove "Companhia aérea" mesmo se estiver concatenado (ex: "GolCompanhia aérea")
    txt = _re.sub(r'\s*Companhia\s*a[ée]rea\s*', '', txt, flags=_re.I).strip()
    txt = _re.sub(r'Companhia\s*a[ée]rea\s*', '', txt, flags=_re.I).strip()
    txt = _re.sub(r'\s*Companhia\s*aerea\s*', '', txt, flags=_re.I).strip()
    txt = _re.sub(r'Companhia\s*aerea\s*', '', txt, flags=_re.I).strip()
    normalized = txt.lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "google_flights": "Google Flights",
        "google": "Google Flights",
        "": "",
        "latam": "LATAM",
        "gol": "GOL",
        "azul": "Azul",
        "decolar": "Decolar",
        "zupper": "Zupper",
        "booking": "Booking.com",
        "kayak": "KAYAK",
        "123milhas": "123 Milhas",
        "123_milhas": "123 Milhas",
        "viajanet": "ViajaNet",
        "voeazul": "Azul",
        "smiles": "Smiles",
    }
    if normalized in aliases:
        return aliases[normalized]
    return txt.replace("_", " ").strip().title()


def _load_booking_options(row: dict) -> list[dict]:
    raw_booking = row.get("booking_options_json")
    if isinstance(raw_booking, str) and raw_booking.strip():
        try:
            booking_options = json.loads(raw_booking)
            if isinstance(booking_options, list):
                return [item for item in booking_options if isinstance(item, dict)]
        except Exception:
            pass
    return []






def _price_vendor_display(row: dict) -> str:
    display_price = row.get("best_vendor_price")
    if not isinstance(display_price, (int, float)):
        display_price = row.get("price")
    price_text = format_brl(display_price) if isinstance(display_price, (int, float)) else "ainda sem"

    vendor = (row.get("best_vendor") or "").strip()
    booking_options = _load_booking_options(row)

    if not vendor:
        if booking_options:
            first_vendor = str((booking_options[0] or {}).get("vendor") or "").strip()
            if first_vendor:
                vendor = first_vendor

    if not vendor or vendor.lower() in ('google_flights', 'google'):
        import re as _re
        notes = (row.get('notes') or '')
        notes_match = _re.search(r'^([A-Z][a-zA-ZÀ-ÿ]+(?: [A-Z][a-zA-ZÀ-ÿ]+)*)', notes)
        if notes_match:
            vendor = notes_match.group(1).strip()
        airline = row.get('airline', '')
        if not vendor or vendor.lower() in ('google_flights', 'google', ''):
            if airline and airline.lower() not in ('', 'google_flights', 'google', 'n/a'):
                vendor = airline
        if not vendor or vendor.lower() in ('google_flights', 'google', '', 'n/a'):
            vendor = row.get('site', '') or 'N/D'

    vendor_label = _pretty_vendor_name(vendor)
    return f"{price_text} • {vendor_label}"


def _airport_code_color(code: str, default_color: str) -> str:
    airport = (code or "").strip().upper()
    if not airport:
        return default_color
    if airport in CITY_HIGHLIGHT_COLORS:
        return CITY_HIGHLIGHT_COLORS[airport]
    idx = sum(ord(ch) for ch in airport) % len(FALLBACK_AIRPORT_COLORS)
    return FALLBACK_AIRPORT_COLORS[idx]


def _scan_title_from_trigger(trigger: str | None) -> str:
    normalized = (trigger or "").strip().lower()
    if "agend" in normalized:
        return "Consulta automática"
    return "Consulta manual"


def _scan_title_with_result_type(trigger: str | None, result_type: str | None = None) -> str:
    return _scan_title_from_trigger(trigger)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace(" ", "T"))
    except ValueError:
        return None


def _was_sent_recently(last_sent_at: str | None, window_seconds: int) -> bool:
    if window_seconds <= 0:
        return False
    dt = _parse_iso_datetime(last_sent_at)
    if not dt:
        return False
    delta_seconds = (now_local() - dt).total_seconds()
    if delta_seconds < -60:
        return False
    return delta_seconds < window_seconds


def _get_last_sent_at_for_user(conn, user_id: int, send_type: str | None = None) -> str:
    column = 'last_sent_at'
    fallback = 'last_sent_at'
    if send_type == 'manual':
        column = 'last_manual_sent_at'
    elif send_type == 'scheduled':
        column = 'last_scheduled_sent_at'
    try:
        row = conn.execute(
            sql(f"SELECT COALESCE({column}, COALESCE({fallback}, '')) AS last_sent_at FROM bot_settings WHERE user_id = ?"),
            (user_id,),
        ).fetchone()
    except Exception as exc:
        if is_missing_column_error(exc):
            try:
                row = conn.execute(
                    sql("SELECT COALESCE(last_sent_at, '') AS last_sent_at FROM bot_settings WHERE user_id = ?"),
                    (user_id,),
                ).fetchone()
            except Exception as inner_exc:
                if is_missing_column_error(inner_exc):
                    return ""
                raise
        else:
            raise
    if not row:
        return ""
    return str(row["last_sent_at"] or "")


def _has_user_running_scan(conn, user_id: int) -> bool:
    row = conn.execute(
        sql("SELECT COUNT(*) AS c FROM scan_jobs WHERE user_id = ? AND status IN ('pending', 'running')"),
        (user_id,),
    ).fetchone()
    count = int((row['c'] if isinstance(row, dict) else row[0]) or 0)
    return count > 0


def _mark_last_sent_now_for_user(conn, user_id: int, send_type: str | None = None) -> None:
    now_txt = now_local_iso(sep="T")
    if send_type == 'manual':
        columns = 'user_id, last_sent_at, last_manual_sent_at, updated_at'
        values = (user_id, now_txt, now_txt, now_txt)
        update_part = "last_sent_at = VALUES(last_sent_at), last_manual_sent_at = VALUES(last_manual_sent_at), updated_at = VALUES(updated_at)"
    elif send_type == 'scheduled':
        columns = 'user_id, last_sent_at, last_scheduled_sent_at, updated_at'
        values = (user_id, now_txt, now_txt, now_txt)
        update_part = "last_sent_at = VALUES(last_sent_at), last_scheduled_sent_at = VALUES(last_scheduled_sent_at), updated_at = VALUES(updated_at)"
    else:
        columns = 'user_id, last_sent_at, updated_at'
        values = (user_id, now_txt, now_txt)
        update_part = "last_sent_at = VALUES(last_sent_at), updated_at = VALUES(updated_at)"
    try:
        conn.execute(
            sql(
                f"INSERT INTO bot_settings ({columns}) VALUES ({', '.join(['?'] * len(values))})"
                f" ON DUPLICATE KEY UPDATE {update_part}"
            ),
            values,
        )
        conn.commit()
    except Exception as exc:
        if is_missing_column_error(exc):
            pass
        else:
            raise


def build_scan_results_image(rows: list[dict], trigger: str | None = None, result_type: str | None = None) -> str | None:
    source_rows = _merge_rows_for_combined_result_view(rows) if result_type is None else rows
    groups = _group_scan_rows_for_image(source_rows)
    if not groups:
        return None

    is_manual_user = (trigger or '').strip().lower() == 'manual-user'
    effective_result_type = None if is_manual_user else result_type
    source_rows = _merge_rows_for_combined_result_view(rows) if effective_result_type is None else rows
    groups = _group_scan_rows_for_image(source_rows)
    if not groups:
        return None

    row_count = sum(len(items) for _, items in groups)

    def scaled(value: int) -> int:
        return max(1, int(round(value * SCAN_IMAGE_SCALE)))

    def scaled5(value: int) -> int:
        return max(1, int(round(value * SCAN_IMAGE_SCALE * 3.6)))

    # Fontes reduzidas para máximo de compactação
    title_font = _load_font(scaled5(13), bold=True)
    header_font = _load_font(scaled5(7), bold=True)
    body_font = _load_font(scaled5(8), bold=False)
    price_font = _load_font(scaled5(7))
    small_font = _load_font(scaled5(6))

    padding_x = scaled5(5)
    padding_y = scaled5(3)
    row_h = scaled5(20)
    section_h = scaled5(1)
    title_h = scaled5(16)
    meta_h = scaled5(10)
    # Colunas mais estreitas para caber no mobile sem cortar
    col_widths = [scaled5(65), scaled5(55), scaled5(70)]
    headers = ["Trecho", "Data", "Preço / Companhia"]

    split_combined = False
    height = (
        padding_y * 2
        + title_h
        + meta_h
        + row_h
        + sum(section_h + len(items) * (row_h + 1) for _, items in groups)
        + 12
    )

    table_w = sum(col_widths)
    width = table_w + padding_x * 2
    image = Image.new("RGB", (width, height), "#f4f6f8")
    draw = ImageDraw.Draw(image)

    colors = {
        "text": "#1f2937",
        "muted": "#6b7280",
        "header_bg": "#e2e8f0",
        "header_text": "#1e293b",
        "section_bg": "#d8dee9",
        "section_return_bg": "#f4e7bd",
        "border": "#d1d5db",
        "row_a": "#ffffff",
        "row_b": "#f9fafb",
        "price": "#0f8a5f",
        "price_expensive": "#dc2626",
        "date_badge": "#dbeafe",
        "date_badge_return": "#fef3c7",
        "separator": "#e5e7eb",
        "card_shadow": "#e9edf3",
    }

    x0 = padding_x
    y = padding_y
    title_text = _scan_title_with_result_type(trigger, result_type=effective_result_type)
    title_bbox = draw.textbbox((0, 0), title_text, font=title_font)
    title_w = title_bbox[2] - title_bbox[0]
    draw.text((x0 + max(0, (table_w - title_w) / 2), y), title_text, font=title_font, fill=colors["text"])
    y += title_h
    meta_text = now_local().strftime("%d/%m/%Y %H:%M")
    meta_bbox = draw.textbbox((0, 0), meta_text, font=small_font)
    meta_w = meta_bbox[2] - meta_bbox[0]
    draw.text((x0 + max(0, (table_w - meta_w) / 2), y), meta_text, font=small_font, fill=colors["muted"])
    y += meta_h

    # Cabeçalho da tabela
    x = x0
    draw.rectangle([x0, y, x0 + table_w, y + row_h], fill=colors["header_bg"], outline=colors["border"])
    for idx, header in enumerate(headers):
        w = col_widths[idx]
        header_bbox = draw.textbbox((0, 0), header, font=header_font)
        header_w = header_bbox[2] - header_bbox[0]
        header_h = header_bbox[3] - header_bbox[1]
        draw.text((x + (w - header_w) / 2, y + (row_h - header_h) / 2), header, font=header_font, fill=colors["header_text"])
        x += w
    y += row_h

    def _truncate_text(txt: str, font, max_w: int) -> str:
        out = txt
        while draw.textlength(out, font=font) > max_w and len(out) > 4:
            out = out[:-2].rstrip() + '…'
        return out

    def _draw_route_cell(base_y: int, draw_h: int, row: dict):
        origin_txt = (row.get("origin") or "").upper()
        destination_txt = (row.get("destination") or "").upper()
        origin_color = _airport_code_color(origin_txt, colors["text"])
        destination_color = _airport_code_color(destination_txt, colors["text"])
        route_text = f"{origin_txt} → {destination_txt}"
        route_bbox = draw.textbbox((0, 0), route_text, font=body_font)
        route_w = route_bbox[2] - route_bbox[0]
        route_h = route_bbox[3] - route_bbox[1]
        route_x = x0 + max(0, (col_widths[0] - route_w) / 2)
        route_y = base_y + (draw_h - route_h) / 2 - 1
        # Desenha origem e destino com cores diferentes
        origin_w = draw.textlength(f"{origin_txt} → ", font=body_font)
        draw.text((route_x, route_y), f"{origin_txt} → ", font=body_font, fill=origin_color)
        draw.text((route_x + origin_w, route_y), destination_txt, font=body_font, fill=destination_color)

    def _draw_date_cell(base_y: int, draw_h: int, row: dict):
        date_txt = format_date_display(str(row.get("outbound_date") or ""))
        date_col_x = x0 + col_widths[0]
        badge_bbox = draw.textbbox((0, 0), date_txt, font=small_font)
        badge_text_w = badge_bbox[2] - badge_bbox[0]
        badge_text_h = badge_bbox[3] - badge_bbox[1]
        badge_h = scaled5(13)
        badge_w = min(col_widths[1] - scaled5(8), badge_text_w + scaled5(12))
        date_x = date_col_x + max(0, (col_widths[1] - badge_w) / 2)
        badge_y = base_y + (draw_h - badge_h) / 2
        draw.rounded_rectangle([date_x, badge_y, date_x + badge_w, badge_y + badge_h], radius=scaled5(6), fill=colors["date_badge"])
        text_x = date_x + (badge_w - badge_text_w) / 2 - badge_bbox[0]
        text_y = badge_y + (badge_h - badge_text_h) / 2 - badge_bbox[1]
        draw.text((text_x, text_y), date_txt, font=small_font, fill=colors["text"])

    def _draw_price_cell(base_y: int, draw_h: int, row: dict):
        vendor_txt = _price_vendor_display(row)
        shown_value = (vendor_txt or '').strip()
        if not shown_value:
            return
        if ' • ' in shown_value:
            price_part, vendor_part = shown_value.split(' • ', 1)
        else:
            price_part, vendor_part = shown_value, ''
        cell_x = x0 + col_widths[0] + col_widths[1]
        cell_w = col_widths[2]
        inner_width = max(10, cell_w - scaled5(12))
        price_part = _truncate_text(price_part, price_font, inner_width)
        vendor_part = _truncate_text(vendor_part, small_font, inner_width) if vendor_part else ''
        price_bbox = draw.textbbox((0, 0), price_part, font=price_font)
        price_h = price_bbox[3] - price_bbox[1]
        text_x = cell_x + scaled5(6) + max(0, (inner_width - (price_bbox[2] - price_bbox[0])) / 2)
        if vendor_part:
            vendor_bbox = draw.textbbox((0, 0), vendor_part, font=small_font)
            vendor_h = vendor_bbox[3] - vendor_bbox[1]
            gap = 2
            total_h = price_h + gap + vendor_h
            price_y = base_y + (draw_h - total_h) / 2
            vendor_y = price_y + price_h + gap
            vendor_x = cell_x + scaled5(6) + max(0, (inner_width - (vendor_bbox[2] - vendor_bbox[0])) / 2)
            draw.text((text_x, price_y), price_part, font=price_font, fill=colors["price"])
            draw.text((vendor_x, vendor_y), vendor_part, font=small_font, fill=colors["muted"])
        else:
            price_y = base_y + (draw_h - price_h) / 2
            draw.text((text_x, price_y), price_part, font=price_font, fill=colors["price"])

    for group_idx, (title, items) in enumerate(groups):
        if group_idx > 0:
            # Pequeno espaçamento entre grupos
            y += 2

        for item_idx, row in enumerate(items):
            fill = colors["row_a"] if item_idx % 2 == 0 else colors["row_b"]

            # Desenha a linha da tabela
            draw.rectangle([x0, y, x0 + table_w, y + row_h], fill=fill, outline=colors["border"])

            # Linha separadora horizontal entre linhas (exceto após a última)
            if item_idx < len(items) - 1:
                draw.line([x0, y + row_h, x0 + table_w, y + row_h], fill=colors["separator"], width=1)

            # Linhas verticais separando colunas
            sep_x1 = x0 + col_widths[0]
            sep_x2 = x0 + col_widths[0] + col_widths[1]
            draw.line([sep_x1, y, sep_x1, y + row_h], fill=colors["border"], width=1)
            draw.line([sep_x2, y, sep_x2, y + row_h], fill=colors["border"], width=1)

            _draw_route_cell(y, row_h, row)
            _draw_date_cell(y, row_h, row)
            _draw_price_cell(y, row_h, row)

            y += row_h + 1

    final_height = y + padding_y
    cropped = image.crop((0, 0, width, final_height))

    safe_max_width = 520 if is_manual_user and row_count <= 1 else 560
    if cropped.width > safe_max_width:
        ratio = safe_max_width / float(cropped.width)
        safe_height = max(1, int(cropped.height * ratio))
        cropped = cropped.resize((safe_max_width, safe_height), Image.LANCZOS)

    tmp = NamedTemporaryFile(prefix="telegram_scan_", suffix=".png", delete=False)
    tmp.close()
    cropped.save(tmp.name, format="PNG")
    return tmp.name


_AIRPORT_NAMES: dict[str, str] = {
    code: label.split("—", 1)[1].strip() if "—" in label else label
    for code, label in AIRPORT_OPTIONS
}
_AIRPORT_NAMES.update({
    "AEP": "Buenos Aires", "EZE": "Buenos Aires", "SCL": "Santiago",
    "LIM": "Lima", "BOG": "Bogotá", "GRU": "São Paulo", "GIG": "Rio de Janeiro",
    "CUN": "Cancún", "MIA": "Miami", "JFK": "Nova York", "MCO": "Orlando",
    "MXP": "Milão", "CDG": "Paris", "LHR": "Londres", "MAD": "Madri",
    "LIS": "Lisboa", "FCO": "Roma", "FRA": "Frankfurt", "AMS": "Amsterdã",
    "DXB": "Dubai", "PTY": "Panamá", "MVD": "Montevidéu", "ASU": "Assunção",
})


def _airport_label(code: str) -> str:
    name = _AIRPORT_NAMES.get(code.upper(), "")
    return f"{code} {name}".strip() if name else code


def _rows_by_result_type(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    return rows, []

def _should_split_result_blocks(trigger: str | None, airline_filters_json: str | None = None, show_result_type_filters: bool = True) -> bool:
    return False


def _merge_rows_for_combined_result_view(rows: list[dict]) -> list[dict]:
    return rows


def _rows_for_link_type(rows: list[dict], link_type: str) -> list[dict]:
    # Como não temos mais distinção, apenas preparamos os campos básicos
    prepared = []
    for row in rows:
        item = dict(row)
        item['booking_url'] = item.get('booking_url') or item.get('url') or ''
        prepared.append(item)
    return prepared


def build_booking_links_message(rows: list[dict], result_type: str | None = None) -> str | None:
    def _prefix() -> str:
        return '🔗 Acesse os voos encontrados:\n'

    def _build_lines(block_rows: list[dict]) -> list[str]:
        from html import escape
        lines = []
        for row in block_rows:
            url = str(row.get("booking_url") or row.get("url") or "").strip()
            if not url:
                continue
            origin = str(row.get("origin") or "").upper()
            destination = str(row.get("destination") or "").upper()
            date = str(row.get("outbound_date") or "")
            try:
                date = datetime.strptime(date, "%Y-%m-%d").strftime("%d/%m/%y")
            except Exception:
                pass
            label = f"{_airport_label(origin)} → {_airport_label(destination)} em {date}"
            lines.append(f"• <a href=\"{escape(url, quote=True)}\">{escape(label)}</a>")
        return lines

    lines = _build_lines(rows)
    if not lines:
        return None
    return _prefix() + "\n".join(lines)


def send_telegram_photo_to(image_path: str, caption: str | None = None, token: str | None = None, chat_id: str | None = None) -> None:
    token = token or os.getenv("TELEGRAM_BOT_TOKEN") or CONFIG.get("telegram_bot_token")
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID") or CONFIG.get("telegram_chat_id")
    if not token or not chat_id or not image_path or not os.path.exists(image_path):
        return
    base_url = TELEGRAM_API_BASE_URL
    url = f"{base_url}/bot{token}/sendPhoto"
    with open(image_path, "rb") as image_file:
        requests.post(
            url,
            data={"chat_id": chat_id, "caption": caption or ""},
            files={"photo": image_file},
            timeout=60,
        ).raise_for_status()


def send_telegram_message(text: str, image_rows: list[dict] | None = None, trigger: str | None = None, airline_filters_json: str | None = None, show_result_type_filters: bool = True) -> None:
    rows = image_rows or []
    if _should_split_result_blocks(trigger, airline_filters_json, show_result_type_filters):
        image_path = build_scan_results_image(rows, trigger=trigger)
        try:
            if image_path:
                send_telegram_photo_to(image_path)
            links_msg = build_booking_links_message(rows)
            if links_msg:
                send_telegram_message_to(links_msg, disable_web_page_preview=True, parse_mode='HTML')
        finally:
            if image_path:
                try:
                    os.remove(image_path)
                except OSError:
                    pass
        return

    send_telegram_message_to(text)
    image_path = build_scan_results_image(rows, trigger=trigger)
    if not image_path:
        return
    try:
        send_telegram_photo_to(image_path)
    finally:
        try:
            os.remove(image_path)
        except OSError:
            pass


def send_user_telegram_message(
    user_id: int,
    text: str,
    image_rows: list[dict] | None = None,
    trigger: str | None = None,
    airline_filters_json: str | None = None,
    show_result_type_filters: bool = True,
) -> None:
    conn = get_db_connection(auth_db_path())
    try:
        row = conn.execute(
            sql("SELECT bot_token, chat_id FROM user_telegram WHERE user_id = ?"),
            (user_id,),
        ).fetchone()
        if not row:
            return
        token = (row["bot_token"] or "").strip()
        chat_id = (row["chat_id"] or "").strip()
        if not token or not chat_id:
            return
        if "agend" in (trigger or "").strip().lower():
            if _has_user_running_scan(conn, user_id):
                return
            last_sent_at = _get_last_sent_at_for_user(conn, user_id, send_type='scheduled')
            if _was_sent_recently(last_sent_at, 30 * 60):
                return
        from bot import full_menu_markup
        menu_markup = full_menu_markup(chat_id).to_dict()
        rows = image_rows or []
        if _should_split_result_blocks(trigger, airline_filters_json, show_result_type_filters):
            image_path = build_scan_results_image(rows, trigger=trigger)
            sent_any = False
            try:
                if image_path:
                    send_telegram_photo_to(image_path, token=token, chat_id=chat_id)
                    sent_any = True
                links_msg = build_booking_links_message(rows)
                if links_msg:
                    send_telegram_message_to(links_msg, token=token, chat_id=chat_id, reply_markup=menu_markup, disable_web_page_preview=True, parse_mode='HTML')
                    sent_any = True
                elif image_path:
                    send_telegram_message_to('🏠 Toque abaixo para abrir o menu principal.', token=token, chat_id=chat_id, reply_markup=menu_markup)
                    sent_any = True
            finally:
                if image_path:
                    try:
                        os.remove(image_path)
                    except OSError:
                        pass
            if sent_any:
                _mark_last_sent_now_for_user(conn, user_id, send_type='scheduled' if 'agend' in (trigger or '').strip().lower() else None)
            return

        if (text or "").strip():
            send_telegram_message_to(text, token=token, chat_id=chat_id, reply_markup=menu_markup)
        image_path = build_scan_results_image(rows, trigger=trigger)
        if not image_path:
            send_telegram_message_to('🏠 Toque abaixo para abrir o menu principal.', token=token, chat_id=chat_id, reply_markup=menu_markup)
            return
        try:
            send_telegram_photo_to(image_path, token=token, chat_id=chat_id)
            links_msg = build_booking_links_message(rows)
            if links_msg:
                send_telegram_message_to(links_msg, token=token, chat_id=chat_id, reply_markup=menu_markup, disable_web_page_preview=True, parse_mode='HTML')
            else:
                send_telegram_message_to('🏠 Toque abaixo para abrir o menu principal.', token=token, chat_id=chat_id, reply_markup=menu_markup)
            _mark_last_sent_now_for_user(conn, user_id, send_type='scheduled' if 'agend' in (trigger or '').strip().lower() else None)
        finally:
            try:
                os.remove(image_path)
            except OSError:
                pass
    finally:
        conn.close()



def extract_final_price_source(notes: str | None) -> str:
    txt = (notes or "")
    m = re.search(r"final_price_source=([^|]+)", txt)
    if not m:
        return ""
    return (m.group(1) or "").strip()

def filter_rows_by_max_price(rows: list[dict], max_price: float | None) -> list[dict]:
    if max_price is None:
        return rows
    return [r for r in rows if isinstance(r.get('price'), (int, float)) and r.get('price') <= max_price]

def filter_rows_with_vendor(rows: list[dict]) -> list[dict]:
    # Desabilitado — filtros de agências removidos
    return rows

def normalize_rows_for_airline_priority(rows: list[dict], airline_filters_json: str | None) -> list[dict]:
    return rows

def expand_rows_by_result_type(rows: list[dict], airline_filters_json: str | None, show_result_type_filters: bool = True) -> list[dict]:
    expanded = []
    for r in rows:
        expanded.extend(_expand_result_rows(r))
    return expanded

def normalize_max_price(val) -> float | None:
    try:
        if val is None or str(val).strip() == '': return None
        return float(val)
    except:
        return None
