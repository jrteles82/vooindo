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

from skyscanner import (
    CONFIG,
    Database,
    FlightResult,
    GoogleFlightsScraper,
    build_google_flights_worker,
    RouteQuery,
    build_db_queries,
    classify_price,
    format_brl,
    parse_price_brl,
    sync_playwright,
)

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
from maxmilhas import (
    buscar_menor_preco as buscar_menor_preco_maxmilhas,
    filtrar_precos_parcelados,
)
from config import load_env, now_local, now_local_iso

load_env()

app = Flask(__name__, static_folder="static", static_url_path="/static")
app.secret_key = os.getenv("SKYSCANNER_SECRET_KEY", "dev-change-this-secret")


def _env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Variável obrigatória ausente no .env: {name}")
    return value


TELEGRAM_API_BASE_URL = _env_required("TELEGRAM_API_BASE_URL").rstrip("/")


DEFAULT_SCAN_INTERVAL = int(CONFIG.get("full_scan_seconds", 3 * 60 * 60))
DEFAULT_SCHEDULE_MINUTES = max(1, int(CONFIG.get("schedule_minutes", DEFAULT_SCAN_INTERVAL // 60)))
DEFAULT_SCAN_INTERVAL_MINUTES = max(1, int(os.getenv("SCAN_INTERVAL_MINUTES", str(DEFAULT_SCHEDULE_MINUTES or 60))))
if DEFAULT_SCAN_INTERVAL_MINUTES < 60:
    DEFAULT_SCAN_INTERVAL_MINUTES = 60
AUTO_SCAN_ENABLED = os.getenv("SKYSCANNER_AUTO_SCAN", "0") == "1"
USER_SCAN_POLL_SECONDS = int(os.getenv("SKYSCANNER_USER_SCAN_POLL_SECONDS", "60"))
PANEL_RESTART_COMMAND = os.getenv("SKYSCANNER_RESTART_COMMAND", "").strip()
_scan_lock = threading.Lock()
_scan_last_run_at = None
SCAN_IMAGE_MAX_ASPECT = float(os.getenv("SCAN_IMAGE_MAX_ASPECT", "4.0"))
SCAN_IMAGE_SCALE = max(0.85, float(os.getenv("SCAN_IMAGE_SCALE", "0.9")))
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
    command = PANEL_RESTART_COMMAND or 'systemctl restart skyscanner-bot.service'
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
        "best_airline_vendor": getattr(result, "best_airline_vendor", ""),
        "best_airline_price": getattr(result, "best_airline_price", None),
        "best_airline_url": getattr(result, "best_airline_url", ""),
        "best_airline_visible_price": getattr(result, "best_airline_visible_price", None),
        "best_agency_vendor": getattr(result, "best_agency_vendor", ""),
        "best_agency_price": getattr(result, "best_agency_price", None),
        "best_agency_url": getattr(result, "best_agency_url", ""),
        "best_agency_visible_price": getattr(result, "best_agency_visible_price", None),
        "final_price_source": extract_final_price_source(result.notes),
    }


def _expand_result_rows(row: dict) -> list[dict]:
    expanded = []
    if row.get('best_airline_vendor') and isinstance(row.get('best_airline_price'), (int, float)):
        airline_row = dict(row)
        airline_row['best_vendor'] = row.get('best_airline_vendor')
        airline_row['best_vendor_price'] = row.get('best_airline_price')
        airline_row['booking_url'] = row.get('best_airline_url') or row.get('booking_url') or row.get('url') or ''
        airline_row['visible_card_price'] = row.get('best_airline_visible_price')
        airline_row['price'] = row.get('best_airline_price')
        airline_row['result_type'] = 'airline'
        expanded.append(airline_row)
    if row.get('best_agency_vendor') and isinstance(row.get('best_agency_price'), (int, float)):
        agency_row = dict(row)
        agency_row['best_vendor'] = row.get('best_agency_vendor')
        agency_row['best_vendor_price'] = row.get('best_agency_price')
        agency_row['booking_url'] = row.get('best_agency_url') or row.get('booking_url') or row.get('url') or ''
        agency_row['visible_card_price'] = row.get('best_agency_visible_price')
        agency_row['price'] = row.get('best_agency_price')
        agency_row['result_type'] = 'agency'
        expanded.append(agency_row)
    if expanded:
        return expanded
    return [row]


def _search_google_result(scraper: GoogleFlightsScraper, route: RouteQuery, allow_agencies: bool = True) -> FlightResult:
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

    variants: list[tuple[RouteQuery, FlightResult]] = []
    for origin in origin_opts:
        for destination in destination_opts:
            variant = RouteQuery(
                origin=origin,
                destination=destination,
                outbound_date=route.outbound_date,
                inbound_date=route.inbound_date,
                trip_type=route.trip_type,
            )
            result = scraper.search(variant, allow_agencies=allow_agencies)
            variants.append((variant, result))

    def _score(item: tuple[RouteQuery, FlightResult]) -> tuple[int, float]:
        _variant, result = item
        has_vendor = 0 if (result.best_vendor or "").strip() else 1
        price = float(result.price) if isinstance(result.price, (int, float)) else 10**12
        return (has_vendor, price)

    chosen_variant, chosen = sorted(variants, key=_score)[0]
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
        visible_card_price=chosen.visible_card_price,
        booking_options_json=chosen.booking_options_json,
        best_airline_vendor=getattr(chosen, "best_airline_vendor", ""),
        best_airline_price=getattr(chosen, "best_airline_price", None),
        best_airline_url=getattr(chosen, "best_airline_url", ""),
        best_airline_visible_price=getattr(chosen, "best_airline_visible_price", None),
        best_agency_vendor=getattr(chosen, "best_agency_vendor", ""),
        best_agency_price=getattr(chosen, "best_agency_price", None),
        best_agency_url=getattr(chosen, "best_agency_url", ""),
        best_agency_visible_price=getattr(chosen, "best_agency_visible_price", None),
    )


def _search_maxmilhas_result(playwright, route: RouteQuery) -> FlightResult | None:
    if (route.inbound_date or "").strip():
        return None

    resultado = buscar_menor_preco_maxmilhas(
        origem=route.origin,
        destino=route.destination,
        data_ida_iso=route.outbound_date,
        playwright=playwright,
        salvar_arquivo_json=False,
        max_tentativas=1,
    )

    ok = bool(resultado and resultado.get("ok"))
    menor_preco = resultado.get("menor_preco") if resultado else None
    filtered_threshold = None
    final_threshold = None
    if ok and resultado:
        valores = []
        for raw in resultado.get("precos_encontrados") or []:
            try:
                valores.append(float(raw))
            except (TypeError, ValueError):
                pass
        valores = sorted(set(valores))
        limiar = float(CONFIG.get("maxmilhas_min_price", 400))
        candidatos = [valor for valor in valores if valor >= limiar]
        total_limit = float(CONFIG.get("maxmilhas_final_price_threshold", 1000))
        selected_price = None
        if candidatos:
            for price in candidatos:
                if price >= total_limit:
                    selected_price = price
                    break
            if selected_price is None:
                selected_price = candidatos[-1]
        if selected_price is not None:
            menor_preco = selected_price
            filtered_threshold = limiar
            final_threshold = total_limit
    vendedor = "MaxMilhas" if ok and menor_preco is not None else ""
    notes_parts = []
    if resultado:
        if resultado.get("motivo"):
            notes_parts.append(f"motivo={resultado['motivo']}")
        if resultado.get("url_final"):
            notes_parts.append(f"url_final={resultado['url_final']}")
        if ok and menor_preco is not None:
            notes_parts.append("final_price_source=maxmilhas")
            notes_parts.append(f"precos={resultado.get('precos_encontrados', [])}")
            if filtered_threshold is not None:
                notes_parts.append(f"maxmilhas_min_price={filtered_threshold}")
            if final_threshold is not None:
                notes_parts.append(f"maxmilhas_final_price_threshold={final_threshold}")

    return FlightResult(
        site="maxmilhas",
        origin=route.origin,
        destination=route.destination,
        outbound_date=route.outbound_date,
        inbound_date=route.inbound_date,
        trip_type=route.trip_type,
        price=menor_preco if ok else None,
        currency="BRL",
        url=(resultado or {}).get("url_final", ""),
        notes=" | ".join(notes_parts),
        best_vendor=vendedor,
        best_vendor_price=menor_preco if ok else None,
        visible_card_price=menor_preco if ok else None,
        booking_options_json=json.dumps(
            [{"vendor": "MaxMilhas", "price": menor_preco}] if ok and menor_preco is not None else [],
            ensure_ascii=False,
        ),
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


def run_scan_for_routes(routes: list[RouteQuery], on_row=None, sources: dict | None = None):
    if not routes:
        return []

    total = sum(2 if not (route.inbound_date or "").strip() else 1 for route in routes)
    requested_workers = CONFIG.get("scan_workers", 2)
    try:
        requested_workers = int(requested_workers)
    except (TypeError, ValueError):
        requested_workers = 2
    try:
        override_workers = int(os.getenv("SKYSCANNER_SCAN_WORKERS", requested_workers))
    except ValueError:
        override_workers = requested_workers
    worker_count = max(1, min(len(routes), override_workers))
    source_flags = sources or {"google_flights": True, "maxmilhas": True}
    if source_flags.get("google_flights", True) and CONFIG.get("google_auth_worker_enabled"):
        worker_count = 1
    route_chunks = _split_routes(routes, worker_count)
    chunk_results: list[list[tuple[RouteQuery, FlightResult]] | None] = [None] * len(route_chunks)

    def _scan_chunk(chunk_idx: int, chunk_routes: list[RouteQuery]) -> list[tuple[RouteQuery, FlightResult]]:
        if not chunk_routes:
            return []
        worker_results: list[tuple[RouteQuery, FlightResult]] = []
        user_data_dir = os.getenv("SKYSCANNER_USER_DATA_DIR", "/tmp/skyscanner-profile")
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
                for route in chunk_routes:
                    if source_flags.get("google_flights", True):
                        try:
                            google_result = _search_google_result(scraper, route, allow_agencies=source_flags.get('allow_agencies', True))
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
                    if source_flags.get("maxmilhas", True):
                        maxmilhas_result = _search_maxmilhas_result(p, route)
                        if maxmilhas_result is not None:
                            worker_results.append((route, maxmilhas_result))
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
                'maxmilhas': False,
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
        print("[auto-scan] desativado por SKYSCANNER_AUTO_SCAN=0")
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
    normalized = txt.lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "google_flights": "Google Flights",
        "google": "Google Flights",
        "maxmilhas": "MaxMilhas",
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


def _pick_agency_option(options: list[dict]) -> tuple[str, float | None]:
    airline_names = {
        "LATAM", "GOL", "AZUL", "VOEPASS", "TAP", "AVIANCA", "COPA",
        "AEROLINEAS ARGENTINAS", "AEROLÍNEAS ARGENTINAS", "AEROMEXICO", "AIR CANADA",
        "AMERICAN", "UNITED", "DELTA", "AIR FRANCE", "KLM", "IBERIA", "LUFTHANSA",
    }
    best_vendor = ""
    best_price = None
    for item in options:
        vendor = str((item or {}).get("vendor") or "").strip()
        price = (item or {}).get("price")
        if not vendor or not isinstance(price, (int, float)):
            continue
        if vendor.upper() in airline_names:
            continue
        price = float(price)
        if best_price is None or price < best_price:
            best_vendor = vendor
            best_price = price
    return best_vendor, best_price


def _pick_agency_vendor(options: list[dict]) -> str:
    vendor, _ = _pick_agency_option(options)
    return vendor


def _price_vendor_display(row: dict) -> str:
    display_price = row.get("best_vendor_price")
    if not isinstance(display_price, (int, float)):
        display_price = row.get("price")
    price_text = format_brl(display_price) if isinstance(display_price, (int, float)) else "sem preço"

    vendor = (row.get("best_vendor") or "").strip()
    booking_options = _load_booking_options(row)

    if vendor in {"Agências", "Outras"}:
        agency_vendor = _pick_agency_vendor(booking_options)
        if agency_vendor:
            vendor = agency_vendor
    elif not vendor:
        if booking_options:
            first_vendor = str((booking_options[0] or {}).get("vendor") or "").strip()
            if first_vendor:
                vendor = first_vendor

    if not vendor:
        vendor = (row.get("site") or "").strip() or "N/D"

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
    base = _scan_title_from_trigger(trigger)
    if result_type == 'airline':
        return f"{base} Companhia Aérea"
    if result_type == 'agency':
        return f"{base} Agência"
    return base


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

    title_font = _load_font(scaled5(22), bold=True)
    header_font = _load_font(scaled5(16), bold=False)
    body_font = _load_font(scaled5(14), bold=False)
    price_font = _load_font(scaled5(12))
    small_font = _load_font(scaled5(11))

    padding_x = scaled5(10)
    padding_y = scaled5(7)
    row_h = scaled5(36)
    row_pair_h = row_h * 2
    section_h = scaled5(10)
    title_h = scaled5(26)
    meta_h = scaled5(18)
    col_widths = [scaled5(102), scaled5(90), scaled5(92)]
    if effective_result_type is None:
        col_widths.append(scaled5(92))
    headers = ["Trecho", "Data", "Companhia"]
    if effective_result_type is None:
        headers.append("Agência")

    split_combined = effective_result_type is None
    height = (
        padding_y * 2
        + title_h
        + meta_h
        + row_h
        + sum(section_h + len(items) * ((row_pair_h if split_combined else row_h) + scaled5(10)) for _, items in groups)
        + 24
    )

    table_w = sum(col_widths)
    width = table_w + padding_x * 2
    image = Image.new("RGB", (width, height), "#f4f6f8")
    draw = ImageDraw.Draw(image)

    colors = {
        "text": "#1f2937",
        "muted": "#6b7280",
        "header_bg": "#eef2f7",
        "section_bg": "#d8dee9",
        "section_return_bg": "#f4e7bd",
        "border": "#d8dee9",
        "row_a": "#ffffff",
        "row_b": "#ffffff",
        "price": "#0f8a5f",
        "agency": "#2563eb",
        "date_badge": "#dbeafe",
        "date_badge_return": "#fef3c7",
        "result_line": "#e5e7eb",
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

    x = x0
    for idx, header in enumerate(headers):
        w = col_widths[idx]
        draw.rectangle([x, y, x + w, y + row_h], fill=colors["header_bg"], outline=colors["border"])
        header_bbox = draw.textbbox((0, 0), header, font=header_font)
        header_w = header_bbox[2] - header_bbox[0]
        header_h = header_bbox[3] - header_bbox[1]
        draw.text((x + (w - header_w) / 2, y + (row_h - header_h) / 2), header, font=header_font, fill=colors["text"])
        x += w
    y += row_h

    def _truncate_text(txt: str, font, max_w: int) -> str:
        out = txt
        while draw.textlength(out, font=font) > max_w and len(out) > 4:
            out = out[:-2].rstrip() + '…'
        return out

    def _draw_route_and_date(base_y: int, draw_h: int, row: dict, section_title: str):
        origin_txt = (row.get("origin") or "").upper()
        destination_txt = (row.get("destination") or "").upper()
        origin_color = _airport_code_color(origin_txt, colors["text"])
        destination_color = _airport_code_color(destination_txt, colors["text"])
        origin_part = f"{origin_txt} → "
        destination_part = destination_txt
        origin_w = draw.textlength(origin_part, font=body_font)
        destination_w = draw.textlength(destination_part, font=body_font)
        route_w = origin_w + destination_w
        route_x = x0 + max(0, (col_widths[0] - route_w) / 2)
        route_bbox = draw.textbbox((0, 0), f"{origin_part}{destination_part}", font=body_font)
        route_h = route_bbox[3] - route_bbox[1]
        route_y = base_y + (draw_h - route_h) / 2 - scaled5(1)
        draw.text((route_x, route_y), origin_part, font=body_font, fill=origin_color)
        draw.text((route_x + origin_w, route_y), destination_part, font=body_font, fill=destination_color)

        date_txt = format_date_display(str(row.get("outbound_date") or ""))
        date_col_x = x0 + col_widths[0]
        badge_fill = colors["date_badge_return"] if section_title.startswith("VOLTAS") else colors["date_badge"]
        badge_bbox = draw.textbbox((0, 0), date_txt, font=small_font)
        badge_text_w = badge_bbox[2] - badge_bbox[0]
        badge_text_h = badge_bbox[3] - badge_bbox[1]
        badge_h = scaled5(22)
        badge_w = min(col_widths[1] - scaled5(12), badge_text_w + scaled5(18))
        date_x = date_col_x + max(0, (col_widths[1] - badge_w) / 2)
        badge_y = base_y + (draw_h - badge_h) / 2
        draw.rounded_rectangle([date_x, badge_y, date_x + badge_w, badge_y + badge_h], radius=scaled5(8), fill=badge_fill)
        text_x = date_x + (badge_w - badge_text_w) / 2 - badge_bbox[0]
        text_y = badge_y + (badge_h - badge_text_h) / 2 - badge_bbox[1]
        draw.text((text_x, text_y), date_txt, font=small_font, fill=colors["text"])

    def _draw_result_cell(base_x: int, base_y: int, width: int, height: int, value: str, color: str) -> bool:
        shown_value = (value or '').strip()
        if not shown_value:
            return False
        if ' • ' in shown_value:
            price_part, vendor_part = shown_value.split(' • ', 1)
        else:
            price_part, vendor_part = shown_value, ''
        inner_width = max(10, width - scaled5(18))
        price_part = _truncate_text(price_part, price_font, inner_width)
        vendor_part = _truncate_text(vendor_part, small_font, inner_width) if vendor_part else ''
        price_bbox = draw.textbbox((0, 0), price_part, font=price_font)
        price_w = price_bbox[2] - price_bbox[0]
        price_h = price_bbox[3] - price_bbox[1]
        text_x = base_x + scaled5(8) + max(0, (inner_width - price_w) / 2)
        if vendor_part:
            vendor_bbox = draw.textbbox((0, 0), vendor_part, font=small_font)
            vendor_w = vendor_bbox[2] - vendor_bbox[0]
            vendor_h = vendor_bbox[3] - vendor_bbox[1]
            gap = scaled5(4)
            total_h = price_h + gap + vendor_h
            price_y = base_y + (height - total_h) / 2
            vendor_y = price_y + price_h + gap
            vendor_x = base_x + scaled5(8) + max(0, (inner_width - vendor_w) / 2)
            draw.text((text_x, price_y), price_part, font=price_font, fill=color)
            draw.text((vendor_x, vendor_y), vendor_part, font=small_font, fill=color)
        else:
            price_y = base_y + (height - price_h) / 2
            draw.text((text_x, price_y), price_part, font=price_font, fill=color)
        return True

    for group_idx, (title, items) in enumerate(groups):
        section_bg = colors["section_return_bg"] if title.startswith("VOLTAS") else colors["section_bg"]
        draw.rectangle([x0, y, x0 + table_w, y + section_h], fill=section_bg, outline=colors["border"])
        y += section_h

        for item_idx, row in enumerate(items):
            fill = colors["row_a"] if item_idx % 2 == 0 else colors["row_b"]
            route_color = _airport_code_color(str(row.get("origin") or '').upper(), colors["border"])
            airline_rows, agency_rows = _rows_by_result_type([row]) if split_combined else ([], [])
            if split_combined:
                airline_rows = airline_rows or _rows_for_link_type([row], 'airline')
                agency_rows = agency_rows or _rows_for_link_type([row], 'agency')
            airline_row = airline_rows[0] if airline_rows else None
            agency_row = agency_rows[0] if agency_rows else None
            airline_value = _price_vendor_display(airline_row) if airline_row else ''
            agency_value = _price_vendor_display(agency_row) if agency_row else ''
            line_count = 0
            if split_combined:
                line_count = (1 if airline_value else 0) + (1 if agency_value else 0)
                if line_count == 0:
                    line_count = 1
                draw_h = row_h * line_count
            else:
                draw_h = row_h

            card_top = y
            card_bottom = y + draw_h
            draw.rounded_rectangle([x0, card_top, x0 + table_w, card_bottom], radius=scaled5(10), fill=fill, outline=colors["border"])
            _draw_route_and_date(y, draw_h, row, title)

            company_x = x0 + col_widths[0] + col_widths[1]
            agency_x = company_x + col_widths[2]
            draw.line([company_x, card_top + scaled5(8), company_x, card_bottom - scaled5(8)], fill=colors['border'], width=1)
            draw.line([agency_x, card_top + scaled5(8), agency_x, card_bottom - scaled5(8)], fill=colors['border'], width=1)

            if split_combined:
                _draw_result_cell(company_x, y, col_widths[2], draw_h, airline_value, colors['price'])
                _draw_result_cell(agency_x, y, col_widths[3], draw_h, agency_value, colors['agency'])
                if not airline_value and not agency_value:
                    vendor_txt = _price_vendor_display(row)
                    _draw_result_cell(company_x, y, col_widths[2], draw_h, vendor_txt, colors['text'])
            else:
                vendor_txt = _price_vendor_display(row)
                if effective_result_type == 'agency':
                    _draw_result_cell(agency_x, y, col_widths[3], draw_h, vendor_txt, colors['agency'])
                else:
                    _draw_result_cell(company_x, y, col_widths[2], draw_h, vendor_txt, colors['price'])

            y += draw_h + scaled5(10)

        if group_idx != len(groups) - 1:
            y += scaled5(4)
    final_height = y + padding_y
    cropped = image.crop((0, 0, width, final_height))

    safe_max_width = 920 if is_manual_user and row_count <= 1 else 980
    if is_manual_user and row_count <= 1:
        if cropped.width < safe_max_width:
            ratio = safe_max_width / float(cropped.width)
            safe_height = max(1, int(cropped.height * ratio))
            cropped = cropped.resize((safe_max_width, safe_height), Image.LANCZOS)
    elif cropped.width > safe_max_width:
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
    airline_rows = []
    agency_rows = []
    for row in rows:
        airline_vendor_name = _pretty_vendor_name(
            row.get('best_airline_vendor') or row.get('best_vendor') or ''
        )
        if row.get('result_type') == 'airline':
            airline_rows.append(row)
            continue
        if row.get('result_type') == 'agency':
            agency_rows.append(row)
            continue
        if row.get('best_airline_vendor') and isinstance(row.get('best_airline_price'), (int, float)):
            airline_item = dict(row)
            airline_item['best_vendor'] = row.get('best_airline_vendor')
            airline_item['best_vendor_price'] = row.get('best_airline_price')
            airline_item['booking_url'] = row.get('best_airline_url') or row.get('booking_url') or row.get('url') or ''
            airline_item['visible_card_price'] = row.get('best_airline_visible_price')
            airline_item['price'] = row.get('best_airline_price')
            airline_rows.append(airline_item)
        else:
            vendor = str(row.get('best_vendor') or '').strip()
            try:
                from bot import is_international_agency_vendor
                if vendor and not is_international_agency_vendor(vendor):
                    airline_rows.append(dict(row))
            except Exception:
                if vendor:
                    airline_rows.append(dict(row))

        if row.get('best_agency_vendor') and isinstance(row.get('best_agency_price'), (int, float)):
            agency_vendor_name = _pretty_vendor_name(row.get('best_agency_vendor') or '')
            if agency_vendor_name and agency_vendor_name != airline_vendor_name:
                agency_item = dict(row)
                agency_item['best_vendor'] = row.get('best_agency_vendor')
                agency_item['best_vendor_price'] = row.get('best_agency_price')
                agency_item['booking_url'] = row.get('best_agency_url') or row.get('booking_url') or row.get('url') or ''
                agency_item['visible_card_price'] = row.get('best_agency_visible_price')
                agency_item['price'] = row.get('best_agency_price')
                agency_rows.append(agency_item)
        else:
            vendor = str(row.get('best_vendor') or '').strip()
            if vendor and vendor.lower() in {'agências', 'agencias', 'outras', 'maxmilhas'}:
                agency_rows.append(dict(row))
                continue
            try:
                from bot import is_international_agency_vendor
                if is_international_agency_vendor(vendor):
                    agency_rows.append(dict(row))
                    continue
            except Exception:
                pass

            booking_options = _load_booking_options(row)
            agency_vendor, agency_price = _pick_agency_option(booking_options)
            agency_url = row.get('best_agency_url') or row.get('booking_url') or row.get('url') or ''
            agency_vendor_name = _pretty_vendor_name(agency_vendor)
            if agency_vendor and agency_url and isinstance(agency_price, (int, float)) and agency_vendor_name != airline_vendor_name:
                agency_item = dict(row)
                agency_item['best_vendor'] = agency_vendor
                agency_item['best_vendor_price'] = agency_price
                agency_item['booking_url'] = agency_url
                agency_item['price'] = agency_price
                agency_item['visible_card_price'] = agency_price
                agency_rows.append(agency_item)
    return airline_rows, agency_rows


def _should_split_result_blocks(trigger: str | None, airline_filters_json: str | None = None, show_result_type_filters: bool = True) -> bool:
    try:
        from bot import parse_airline_filters
        filters = parse_airline_filters(airline_filters_json)
    except Exception:
        filters = {'any_airline': True, 'agencies': False}
    if not show_result_type_filters:
        return True
    return bool(filters.get('any_airline', True)) and bool(filters.get('agencies', False))


def _merge_rows_for_combined_result_view(rows: list[dict]) -> list[dict]:
    merged: dict[tuple[str, str, str, str, str], dict] = {}

    for row in rows:
        item = dict(row)
        key = (
            str(item.get('origin') or '').upper(),
            str(item.get('destination') or '').upper(),
            str(item.get('outbound_date') or ''),
            str(item.get('inbound_date') or ''),
            str(item.get('trip_type') or ''),
        )
        bucket = merged.get(key)
        if bucket is None:
            bucket = dict(item)
            bucket.pop('result_type', None)
            merged[key] = bucket
        result_type = str(item.get('result_type') or '').strip().lower()
        vendor = str(item.get('best_vendor') or '').strip()
        booking_url = item.get('booking_url') or item.get('url') or ''
        visible_price = item.get('visible_card_price')
        generic_price = item.get('price')
        booking_options_json = item.get('booking_options_json')

        if result_type == 'airline':
            if vendor:
                bucket['best_airline_vendor'] = vendor
            if isinstance(item.get('best_vendor_price'), (int, float)):
                bucket['best_airline_price'] = item.get('best_vendor_price')
            elif isinstance(generic_price, (int, float)):
                bucket['best_airline_price'] = generic_price
            if booking_url:
                bucket['best_airline_url'] = booking_url
            if isinstance(visible_price, (int, float)):
                bucket['best_airline_visible_price'] = visible_price
            if booking_options_json and not bucket.get('booking_options_json'):
                bucket['booking_options_json'] = booking_options_json
        elif result_type == 'agency':
            if vendor:
                bucket['best_agency_vendor'] = vendor
            if isinstance(item.get('best_vendor_price'), (int, float)):
                bucket['best_agency_price'] = item.get('best_vendor_price')
            elif isinstance(generic_price, (int, float)):
                bucket['best_agency_price'] = generic_price
            if booking_url:
                bucket['best_agency_url'] = booking_url
            if isinstance(visible_price, (int, float)):
                bucket['best_agency_visible_price'] = visible_price
            if booking_options_json and not bucket.get('booking_options_json'):
                bucket['booking_options_json'] = booking_options_json
        else:
            if not bucket.get('best_vendor') and vendor:
                bucket['best_vendor'] = vendor
            if not isinstance(bucket.get('best_vendor_price'), (int, float)) and isinstance(item.get('best_vendor_price'), (int, float)):
                bucket['best_vendor_price'] = item.get('best_vendor_price')
            if not bucket.get('booking_url') and booking_url:
                bucket['booking_url'] = booking_url
            if not isinstance(bucket.get('visible_card_price'), (int, float)) and isinstance(visible_price, (int, float)):
                bucket['visible_card_price'] = visible_price

        if not isinstance(bucket.get('price'), (int, float)) and isinstance(generic_price, (int, float)):
            bucket['price'] = generic_price
        if not bucket.get('site') and item.get('site'):
            bucket['site'] = item.get('site')
        if not bucket.get('notes') and item.get('notes'):
            bucket['notes'] = item.get('notes')

    return list(merged.values())


def _rows_for_link_type(rows: list[dict], link_type: str) -> list[dict]:
    prepared = []
    for row in rows:
        item = dict(row)
        if link_type == 'airline':
            url = item.get('best_airline_url') or item.get('booking_url') or item.get('url') or ''
            vendor = item.get('best_airline_vendor') or item.get('best_vendor') or ''
            price = item.get('best_airline_price')
            visible_price = item.get('best_airline_visible_price')
        else:
            url = item.get('best_agency_url') or item.get('booking_url') or item.get('url') or ''
            vendor = item.get('best_agency_vendor') or ''
            price = item.get('best_agency_price')
            visible_price = item.get('best_agency_visible_price')
            if not vendor or not isinstance(price, (int, float)):
                fallback_vendor, fallback_price = _pick_agency_option(_load_booking_options(item))
                if fallback_vendor and isinstance(fallback_price, (int, float)):
                    vendor = fallback_vendor
                    price = fallback_price
                    visible_price = fallback_price
        if not url:
            continue
        if link_type == 'agency':
            airline_vendor = _pretty_vendor_name(item.get('best_airline_vendor') or item.get('best_vendor') or '')
            agency_vendor = _pretty_vendor_name(vendor)
            if not vendor or (agency_vendor and airline_vendor and agency_vendor == airline_vendor):
                continue
        item['booking_url'] = url
        item['best_vendor'] = vendor
        if isinstance(price, (int, float)):
            item['best_vendor_price'] = price
            item['price'] = price
        if isinstance(visible_price, (int, float)):
            item['visible_card_price'] = visible_price
        prepared.append(item)
    return prepared


def build_booking_links_message(rows: list[dict], result_type: str | None = None) -> str | None:
    def _prefix_for_result_type(kind: str | None) -> str:
        if kind == 'agency':
            return '🔗 Acesse os voos encontrados por agência:\n'
        if kind == 'airline':
            return '🔗 Acesse os voos encontrados por companhia:\n'
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

    if result_type in {'agency', 'airline'}:
        lines = _build_lines(rows)
        if not lines:
            return None
        return _prefix_for_result_type(result_type) + "\n".join(lines)

    merged_rows = _merge_rows_for_combined_result_view(rows)
    blocks = []
    airline_lines = _build_lines(_rows_for_link_type(merged_rows, 'airline'))
    if airline_lines:
        blocks.append(_prefix_for_result_type('airline') + "\n".join(airline_lines))
    agency_lines = _build_lines(_rows_for_link_type(merged_rows, 'agency'))
    if agency_lines:
        blocks.append(_prefix_for_result_type('agency') + "\n".join(agency_lines))

    if blocks:
        return "\n".join(blocks)

    fallback_lines = _build_lines(rows)
    if not fallback_lines:
        return None
    return _prefix_for_result_type(None) + "\n".join(fallback_lines)


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


def _extract_maxmilhas_prices_from_notes(notes: str | None) -> list[float]:
    txt = notes or ""
    match = re.search(r"precos=\[([^\]]+)\]", txt)
    if not match:
        return []

    prices = []
    for raw in match.group(1).split(","):
        raw = raw.strip()
        if not raw:
            continue
        try:
            prices.append(float(raw))
        except ValueError:
            continue
    return prices


def normalize_maxmilhas_history() -> int:
    db = Database()
    rows = db.conn.execute(
        """
        SELECT id, price, best_vendor_price, notes
        FROM results
        WHERE site = 'maxmilhas' AND notes LIKE ?
        """,
        ('%precos=[%',)
    ).fetchall()

    updated = 0
    for row in rows:
        prices = _extract_maxmilhas_prices_from_notes(row["notes"])
        if not prices:
            continue
        filtered = filtrar_precos_parcelados(prices)
        if not filtered:
            continue
        expected = min(filtered)
        current = row["price"]
        if current is None or abs(float(current) - float(expected)) < 0.01:
            continue
        db.conn.execute(
            "UPDATE results SET price = ?, best_vendor_price = ? WHERE id = ?",
            (expected, expected, row["id"]),
        )
        updated += 1

    if updated:
        db.conn.commit()
    return updated


def get_user_max_display_price(user_id: int | None) -> float | None:
    _ = user_id
    return get_global_max_price_limit()


def expand_rows_by_result_type(rows: list[dict], airline_filters_json: str | None = None, show_result_type_filters: bool = True) -> list[dict]:
    expanded = []
    split_blocks = _should_split_result_blocks(None, airline_filters_json, show_result_type_filters)
    for row in rows:
        if row.get('result_type'):
            expanded.append(row)
            continue
        if split_blocks:
            airline_rows, agency_rows = _rows_by_result_type([row])
            expanded.extend(airline_rows)
            expanded.extend(agency_rows)
        else:
            expanded.append(row)
    return expanded


def normalize_max_price(max_price) -> float | None:
    if max_price is None:
        return None
    if isinstance(max_price, (int, float)):
        return float(max_price)
    txt = str(max_price or '').strip()
    if not txt:
        return None
    lowered = txt.lower()
    if lowered in {'sem limite', 'semlimite', 'qualquer valor', 'qualquer', 'todos'}:
        return None
    txt = txt.replace('R$', '').replace('r$', '').strip()
    txt = txt.replace('.', '').replace(',', '.') if ',' in txt else txt
    try:
        return float(txt)
    except ValueError:
        return None


def filter_rows_by_max_price(rows: list[dict], max_price: float | None) -> list[dict]:
    max_price = normalize_max_price(max_price)
    if max_price is None:
        return rows

    kept = []
    for row in rows:
        vendor = str(row.get("best_vendor") or "").strip().lower()
        best_vendor_price = row.get("best_vendor_price")
        visible_card_price = row.get("visible_card_price")
        generic_price = row.get("price")

        target_price = None
        if "companhia aérea" in vendor or "companhia aerea" in vendor:
            if isinstance(best_vendor_price, (int, float)):
                target_price = float(best_vendor_price)
        elif isinstance(best_vendor_price, (int, float)):
            target_price = float(best_vendor_price)
        elif isinstance(visible_card_price, (int, float)):
            target_price = float(visible_card_price)
        elif isinstance(generic_price, (int, float)):
            target_price = float(generic_price)

        if target_price is None or target_price <= max_price:
            kept.append(row)
    return kept


def _route_is_international(row: dict) -> bool:
    origin = str(row.get("origin") or "").upper().strip()
    destination = str(row.get("destination") or "").upper().strip()
    domestic_prefixes = ("P", "S", "G", "B")
    return not (
        len(origin) == 3 and len(destination) == 3
        and origin.startswith(domestic_prefixes)
        and destination.startswith(domestic_prefixes)
    )


def normalize_rows_for_airline_priority(rows: list[dict], airline_filters_json: str | None = None) -> list[dict]:
    try:
        from bot import parse_airline_filters
    except Exception:
        def parse_airline_filters(_raw):
            return {'any_airline': True, 'agencies': True}

    selected = parse_airline_filters(airline_filters_json)
    allow_agencies = bool(selected.get('agencies', True))

    normalized = []
    for row in rows:
        item = dict(row)
        vendor = str(item.get("best_vendor") or "").strip()
        if vendor:
            normalized.append(item)
            continue

        price = item.get("price")
        if price is None:
            normalized.append(item)
            continue

        if allow_agencies:
            notes = str(item.get("notes") or "")
            if "booking_sem_vendor_no_card" in notes or "booking_total_sem_vendor_card_" in notes or str(item.get('site') or '') == 'maxmilhas':
                item["best_vendor"] = "Agências"
                item["best_vendor_price"] = price
        normalized.append(item)
    return normalized


def filter_rows_with_vendor(rows: list[dict]) -> list[dict]:
    kept = []
    for row in rows:
        vendor = str(row.get("best_vendor") or "").strip()
        if vendor:
            vendor_norm = vendor.lower()
            if vendor_norm in {"agências", "agencias", "outras"}:
                notes = str(row.get("notes") or "")
                if (
                    "final_price_source=booking_airline" not in notes
                    and "final_price_source=booking_validated" not in notes
                    and "final_price_source=booking_agency_fallback" not in notes
                ):
                    continue
            kept.append(row)
            continue
        notes = str(row.get("notes") or "")
        site = str(row.get("site") or "").strip()
        price = row.get("price")
        if site == 'google_flights' and isinstance(price, (int, float)) and ('main_min=' in notes or 'overall_min=' in notes):
            row = dict(row)
            row["best_vendor"] = "Google Flights"
            kept.append(row)
    return kept


def get_global_max_price_limit() -> float | None:
    _, _, max_price = get_scheduler_settings()
    return max_price


def _to_route(query_args) -> RouteQuery:
    origin = query_args.get("origin", CONFIG.get("origin", "PVH")).upper()
    destination = query_args.get("destination", "JPA").upper()
    outbound_date = query_args.get("outbound_date", "")
    inbound_date = query_args.get("inbound_date", "")
    trip_type = "roundtrip" if inbound_date else "oneway"

    if not outbound_date:
        raise ValueError("Parâmetro obrigatório: outbound_date (YYYY-MM-DD)")

    return RouteQuery(
        origin=origin,
        destination=destination,
        outbound_date=outbound_date,
        inbound_date=inbound_date,
        trip_type=trip_type,
    )


def _resolve_requested_sources(query_args, route: RouteQuery) -> list[str]:
    fonte = (query_args.get("fonte") or "").strip().lower()
    if fonte in {"maxmilhas"}:
        return [] if (route.inbound_date or "").strip() else ["maxmilhas"]
    if fonte in {"google", "google_flights"}:
        return ["google_flights"]
    sources = ["google_flights"]
    if not (route.inbound_date or "").strip():
        sources.append("maxmilhas")
    return sources


@app.route("/", methods=["GET"])
def index():
    if session.get("user_id"):
        return redirect(url_for("painel"))
    return redirect(url_for("auth_login"))


@app.route("/app", methods=["GET"])
def app_front():
    static_path = Path(app.static_folder or "static") / "index.html"
    html = static_path.read_text(encoding="utf-8")
    return render_template_string(html)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "service": "voobot-monitor"})


@app.route("/rotas", methods=["GET"])
def rotas():
    routes = _routes_for_request_user()
    return jsonify(
        {
            "count": len(routes),
            "rotas": [
                {
                    "origin": r.origin,
                    "destination": r.destination,
                    "outbound_date": r.outbound_date,
                    "inbound_date": r.inbound_date,
                    "trip_type": r.trip_type,
                }
                for r in routes
            ],
        }
    )




@app.route("/consulta", methods=["GET"])
def consulta():
    try:
        route = _to_route(request.args)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    user = current_user()
    max_price = get_user_max_display_price(int(user["id"])) if user else None

    db = Database()
    requested_sources = _resolve_requested_sources(request.args, route)
    if not requested_sources:
        return jsonify({"error": "A MaxMilhas atualmente só está habilitada para consultas somente ida."}), 400

    results = []
    with sync_playwright() as p:
        browser = None
        scraper = None
        if "google_flights" in requested_sources:
            browser = p.chromium.launch(headless=bool(CONFIG.get("headless", True)))
            scraper = GoogleFlightsScraper(browser)

        try:
            for source in requested_sources:
                if source == "google_flights":
                    result = _search_google_result(scraper, route)
                else:
                    result = _search_maxmilhas_result(p, route)

                if result is None:
                    continue

                rows = _store_result(db, route, result)
                results.extend(rows)
        finally:
            if browser:
                browser.close()

    if not results:
        return jsonify({"error": "Nenhum resultado foi retornado para a rota consultada."}), 502

    results = filter_rows_by_max_price(results, max_price)
    if not results:
        return jsonify({"error": "Nenhum resultado está dentro do valor máximo configurado."}), 200

    chosen = min(
        results,
        key=lambda item: item["price"] if isinstance(item.get("price"), (int, float)) and item.get("price") is not None else 10**12,
    )
    min_price, avg_price, last_price = db.stats_for(route)

    try:
        detalhes = []
        for item in results:
            detalhes.append(_price_vendor_display(item))
        resumo = (
            "────────── ✈️ CONSULTA RÁPIDA ✈️ ──────────\n"
            f"Rota: {route.origin} → {route.destination}\n"
            f"Data: {date_color_token(route.outbound_date)[0]} {format_date_display(route.outbound_date)}\n"
            + (f" / {format_date_display(route.inbound_date)}" if route.inbound_date else "")
            + "\n"
            + "Resultados:\n"
            + "\n".join(detalhes)
        )
        send_telegram_message(resumo)
    except Exception:
        pass

    return jsonify(
        {
            "rota": {
                "origin": route.origin,
                "destination": route.destination,
                "outbound_date": route.outbound_date,
                "inbound_date": route.inbound_date,
                "trip_type": route.trip_type,
            },
            "resultado": {
                "price": chosen["price"],
                "price_fmt": chosen["price_fmt"],
                "price_band": chosen["price_band"],
                "site": chosen["site"],
                "currency": "BRL",
                "url": chosen.get("url", ""),
                "notes": chosen["notes"],
                "best_vendor": chosen["best_vendor"],
                "best_vendor_price": chosen["best_vendor_price"],
                "final_price_source": chosen["final_price_source"],
            },
            "resultados": results,
            "historico": {
                "min_price": min_price,
                "avg_price": avg_price,
                "last_price": last_price,
            },
        }
    )


@app.route("/consulta-maxmilhas", methods=["GET"])
def consulta_maxmilhas():
    args = request.args.to_dict(flat=True)
    args["fonte"] = "maxmilhas"
    with app.test_request_context(query_string=args):
        return consulta()


@app.route("/historico", methods=["GET"])
def historico():
    limit = request.args.get("limit", default=20, type=int)
    limit = max(1, min(limit, 200))

    db = Database()
    rows = db.conn.execute(
        """
        SELECT created_at, site, origin, destination, outbound_date, inbound_date,
               price, currency, price_band, notes, url,
               best_vendor, best_vendor_price, booking_options_json
        FROM results
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    items = [dict(r) for r in rows]
    for item in items:
        item["final_price_source"] = extract_final_price_source(item.get("notes"))
    user = current_user()
    max_price = get_user_max_display_price(int(user["id"])) if user else None
    items = filter_rows_by_max_price(items, max_price)
    return jsonify({"total": len(items), "items": items})


@app.route("/historico/limpar", methods=["POST"])
def limpar_historico():
    db = Database()
    deleted = db.conn.execute("DELETE FROM results").rowcount
    db.conn.commit()
    return jsonify({"status": "ok", "deleted": deleted})


@app.route("/cron", methods=["GET"])
def cron():
    parsed = run_full_scan()
    user = current_user()
    max_price = get_user_max_display_price(int(user["id"])) if user else None
    parsed_filtered = filter_rows_by_max_price(parsed, max_price)
    notify_full_scan(parsed, trigger="manual", max_price=max_price)
    return jsonify({"status": "ok", "resultados": parsed_filtered, "last_run_at": _scan_last_run_at})


@app.route("/cron-stream", methods=["GET"])
def cron_stream():
    def event_stream():
        user = current_user()
        max_price = get_user_max_display_price(int(user["id"])) if user else None
        routes = _routes_for_request_user()
        total = sum(2 if not (route.inbound_date or "").strip() else 1 for route in routes)
        yield f"data: {json.dumps({'type': 'start', 'total': total})}\n\n"

        # evita concorrência com auto-scan/execuções manuais
        if not _scan_lock.acquire(blocking=False):
            yield f"data: {json.dumps({'type': 'error', 'message': 'Já existe uma varredura em andamento. Tente novamente em instantes.'})}\n\n"
            return

        try:
            parsed = []
            db = Database()
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=bool(CONFIG.get("headless", True)))
                scraper = GoogleFlightsScraper(browser)

                for idx, route in enumerate(routes, start=1):
                    result = _search_google_result(scraper, route)
                    rows = _store_result(db, route, result)
                    for row in rows:
                        parsed.append(row)
                        if row.get("price") is None or max_price is None or float(row["price"]) <= max_price:
                            payload = {"type": "row", "index": len(parsed), "total": total, "item": row}
                            yield f"data: {json.dumps(payload)}\n\n"
                            time.sleep(0.05)

                    maxmilhas_result = _search_maxmilhas_result(p, route)
                    if maxmilhas_result is not None:
                        rows = _store_result(db, route, maxmilhas_result)
                        for row in rows:
                            parsed.append(row)
                            if row.get("price") is None or max_price is None or float(row["price"]) <= max_price:
                                payload = {"type": "row", "index": len(parsed), "total": total, "item": row}
                                yield f"data: {json.dumps(payload)}\n\n"
                                time.sleep(0.05)

                browser.close()

            notify_full_scan(parsed, trigger="completa", max_price=max_price)
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
        finally:
            if _scan_lock.locked():
                _scan_lock.release()

    return Response(
        stream_with_context(event_stream()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )




@app.route("/app-page", methods=["GET"])
def app_page():
    if not session.get("user_id"):
        return redirect(url_for("auth_login"))
    return render_template_string(
        """
        <!doctype html>
        <html lang='pt-BR'>
        <head>
          <meta charset='utf-8'>
          <meta name='viewport' content='width=device-width, initial-scale=1'>
          <title>App Consultas</title>
          <link href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css' rel='stylesheet'>
        </head>
        <body class='bg-light'>
          <nav class='navbar navbar-dark bg-dark'>
            <div class='container-fluid'>
              <span class='navbar-brand mb-0 h1'>App Consultas</span>
              <a class='btn btn-outline-light btn-sm' href='{{ url_for("painel") }}'>Voltar ao Painel</a>
            </div>
          </nav>
          <div class='container-fluid p-0'>
            <iframe src='{{ url_for("app_front") }}' style='width:100%;height:92vh;border:0;'></iframe>
          </div>
        </body>
        </html>
        """,
    )

def get_db_connection():
    return connect_db()


def get_auth_db():
    if "auth_db" not in g:
        g.auth_db = get_db_connection()
    return g.auth_db


def _current_iso_ts() -> str:
    return now_local_iso(sep="T")

def _ensure_user_telegram_defaults(conn, user_id: int) -> None:
    exists = conn.execute(sql("SELECT 1 FROM user_telegram WHERE user_id = ? LIMIT 1"), (user_id,)).fetchone()
    if exists:
        return
    token = os.getenv("TELEGRAM_BOT_TOKEN") or CONFIG.get("telegram_bot_token")
    bot_user = conn.execute(
        sql("SELECT chat_id FROM bot_users WHERE user_id = ? ORDER BY id DESC LIMIT 1"),
        (user_id,),
    ).fetchone()
    chat_id = str(bot_user["chat_id"]).strip() if bot_user and bot_user["chat_id"] else ""
    if not chat_id:
        chat_id = os.getenv("TELEGRAM_CHAT_ID") or CONFIG.get("telegram_chat_id") or ""
    if not token and not chat_id:
        return
    conn.execute(sql("INSERT INTO user_telegram (user_id, bot_token, chat_id, updated_at) VALUES (?, ?, ?, ?)"),
        (user_id, token or "", chat_id or "", _current_iso_ts()),
    )
    conn.commit()

def ensure_user_defaults(conn, user_id: int) -> None:
    _ensure_user_telegram_defaults(conn, user_id)






def init_auth_tables():
    db = get_db_connection(auth_db_path())
    cur = db.cursor()
    cur.execute("DROP TABLE IF EXISTS user_cron")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS users (
            id {auto_pk_column()},
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS user_routes (
            id {auto_pk_column()},
            user_id {id_ref_column()} NOT NULL,
            origin TEXT NOT NULL,
            destination TEXT NOT NULL,
            outbound_date TEXT NOT NULL,
            inbound_date TEXT DEFAULT '',
            active INTEGER DEFAULT 1,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS user_telegram (
            user_id {id_ref_column()} PRIMARY KEY,
            bot_token TEXT,
            chat_id TEXT,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            cron_enabled INTEGER DEFAULT 1,
            scan_interval_minutes INTEGER DEFAULT 60,
            max_price_display REAL,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS user_runs (
            id {auto_pk_column()},
            user_id {id_ref_column()} NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            summary TEXT,
            run_trigger TEXT DEFAULT 'manual-user',
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    for ddl in [
        "ALTER TABLE user_runs ADD COLUMN run_trigger TEXT DEFAULT 'manual-user'",
    ]:
        try:
            cur.execute(ddl)
        except Exception as exc:
            if is_missing_column_error(exc):
                pass
            else:
                raise
    cur.execute(
        sql(insert_ignore_sql('app_settings', ['id', 'cron_enabled', 'scan_interval_minutes', 'max_price_display', 'updated_at'], '1, 1, ?, NULL, ?')),
        (max(1, DEFAULT_SCAN_INTERVAL_MINUTES), now_local_iso(sep="T")),
    )
    cur.execute(
        sql("""
        UPDATE app_settings
        SET scan_interval_minutes = COALESCE(scan_interval_minutes, ?),
            cron_enabled = COALESCE(cron_enabled, 1),
            updated_at = COALESCE(updated_at, ?)
        WHERE id = 1
        """),
        (max(1, DEFAULT_SCAN_INTERVAL_MINUTES), now_local_iso(sep="T")),
    )

    db.commit()
    db.close()


@app.teardown_appcontext
def close_auth_db(_exc):
    db = g.pop("auth_db", None)
    if db is not None:
        db.close()


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("auth_login"))
        return fn(*args, **kwargs)

    return wrapper


def current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    db = get_auth_db()
    return db.execute("SELECT id, email FROM users WHERE id = ?", (uid,)).fetchone()


@app.route("/auth/register", methods=["GET", "POST"])
def auth_register():
    error = ""
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        if not email or len(password) < 6:
            error = "Informe email válido e senha com pelo menos 6 caracteres."
        else:
            db = get_auth_db()
            try:
                db.execute(
                    "INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)",
                    (email, generate_password_hash(password), now_local_iso(sep="T")),
                )
                db.commit()
                return redirect(url_for("auth_login"))
            except Exception as exc:
                if is_integrity_error(exc):
                    error = "Esse email já está cadastrado."
                else:
                    raise

    return render_template_string(
        """
        <!doctype html>
        <html lang='pt-BR'>
        <head>
          <meta charset='utf-8'>
          <meta name='viewport' content='width=device-width, initial-scale=1'>
          <title>Cadastro | Vooindo Admin</title>
          <link href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css' rel='stylesheet'>
        </head>
        <body class='bg-light d-flex align-items-center' style='min-height:100vh;'>
          <div class='container'>
            <div class='row justify-content-center'>
              <div class='col-md-5'>
                <div class='card shadow-sm'>
                  <div class='card-header bg-primary text-white'>Cadastro</div>
                  <div class='card-body'>
                    <form method='post'>
                      <div class='mb-3'><input class='form-control' name='email' type='email' placeholder='Email' required></div>
                      <div class='mb-3'><input class='form-control' name='password' type='password' placeholder='Senha (mín 6)' required></div>
                      <button class='btn btn-primary w-100' type='submit'>Cadastrar</button>
                    </form>
                    {% if error %}<div class='alert alert-danger mt-3 mb-0'>{{error}}</div>{% endif %}
                    <div class='mt-3 text-center'><a href='{{ url_for("auth_login") }}'>Já tenho login</a></div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </body>
        </html>
        """,
        error=error,
    )


@app.route("/auth/login", methods=["GET", "POST"])
def auth_login():
    error = ""
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        db = get_auth_db()
        user = db.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if not user or not check_password_hash(user["password_hash"], password):
            error = "Login inválido."
        else:
            session["user_id"] = user["id"]
            return redirect(url_for("painel"))

    return render_template_string(
        """
        <!doctype html>
        <html lang='pt-BR'>
        <head>
          <meta charset='utf-8'>
          <meta name='viewport' content='width=device-width, initial-scale=1'>
          <title>Login | Vooindo Admin</title>
          <link href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css' rel='stylesheet'>
        </head>
        <body class='bg-light d-flex align-items-center' style='min-height:100vh;'>
          <div class='container'>
            <div class='row justify-content-center'>
              <div class='col-md-5'>
                <div class='card shadow-sm'>
                  <div class='card-header bg-dark text-white'>Vooindo Admin</div>
                  <div class='card-body'>
                    <form method='post'>
                      <div class='mb-3'><input class='form-control' name='email' type='email' placeholder='Email' required></div>
                      <div class='mb-3'><input class='form-control' name='password' type='password' placeholder='Senha' required></div>
                      <button class='btn btn-dark w-100' type='submit'>Entrar</button>
                    </form>
                    {% if error %}<div class='alert alert-danger mt-3 mb-0'>{{error}}</div>{% endif %}
                    <div class='mt-3 text-center'><a href='{{ url_for("auth_register") }}'>Criar conta</a></div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </body>
        </html>
        """,
        error=error,
    )


@app.route("/auth/logout")
def auth_logout():
    session.clear()
    return redirect(url_for("auth_login"))


@app.route("/painel", methods=["GET"])
@login_required
def painel():
    db = get_auth_db()
    user = current_user()
    restart_status = (request.args.get("restart_status") or "").strip().lower()
    restart_message = (request.args.get("restart_message") or "").strip()
    ensure_user_defaults(db, user["id"])
    routes = db.execute(
        "SELECT id, origin, destination, outbound_date, inbound_date, active FROM user_routes WHERE user_id = ? ORDER BY id DESC",
        (user["id"],),
    ).fetchall()
    tg = db.execute("SELECT bot_token, chat_id FROM user_telegram WHERE user_id = ?", (user["id"],)).fetchone()
    cron = db.execute("SELECT cron_enabled, scan_interval_minutes, max_price_display FROM app_settings WHERE id = 1").fetchone()
    cron_minutes = max(1, DEFAULT_SCAN_INTERVAL_MINUTES)
    cron_max_price = ""
    if cron is not None:
        schedule_minutes = cron["scan_interval_minutes"]
        if schedule_minutes is not None:
            cron_minutes = max(1, int(schedule_minutes))
        if cron["max_price_display"] is not None:
            cron_max_price = str(int(cron["max_price_display"])) if float(cron["max_price_display"]).is_integer() else str(cron["max_price_display"])
    last_run = db.execute("SELECT started_at, finished_at, status, summary FROM user_runs WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user["id"],)).fetchone()
    default_tg_bot = os.getenv("TELEGRAM_BOT_TOKEN") or CONFIG.get("telegram_bot_token", "")
    default_tg_chat = os.getenv("TELEGRAM_CHAT_ID") or CONFIG.get("telegram_chat_id", "")

    return render_template_string(
        """
        <!doctype html>
        <html lang='pt-BR'>
        <head>
          <meta charset='utf-8'>
          <meta name='viewport' content='width=device-width, initial-scale=1'>
          <title>Painel Admin | Vooindo</title>
          <link href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css' rel='stylesheet'>
          <link href='https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css' rel='stylesheet'>
          <style>
            body { background:#f4f6f9; }
            .sidebar { min-height: 100vh; background: #343a40; }
            .sidebar a { color: #c2c7d0; text-decoration: none; display:block; padding:.65rem 1rem; }
            .sidebar a:hover { background:#495057; color:#fff; }
            .brand { color:#fff; font-weight:700; padding:1rem; border-bottom:1px solid #495057; }
            .topbar { background:#fff; border-bottom:1px solid #dee2e6; }
            .kpi { border-left:4px solid #0d6efd; }
            body.dark-mode { background:#1f2d3d; color:#dee2e6; }
            body.dark-mode .card, body.dark-mode .topbar { background:#2c3b4b; color:#dee2e6; border-color:#3d4b5a; }
            body.dark-mode .text-muted { color:#adb5bd !important; }
            body.sidebar-collapsed .sidebar { width: 72px; }
            body.sidebar-collapsed .sidebar .brand, body.sidebar-collapsed .sidebar a { text-align:center; }
            body.sidebar-collapsed .sidebar a { font-size:0; }
            body.sidebar-collapsed .sidebar a i { font-size:1rem; margin:0 !important; }
          </style>
        </head>
        <body class='bg-light'>
          <div class='container-fluid'>
            <div class='row'>
              <aside class='col-md-3 col-lg-2 p-0 sidebar'>
                <div class='brand'><i class='bi bi-activity'></i> Vooindo Admin</div>
                <a href='#rotas'><i class='bi bi-signpost-split me-2'></i>Rotas</a>
                <a href='#consultas'><i class='bi bi-window me-2'></i>Consultas</a>
                <a href='#telegram'><i class='bi bi-telegram me-2'></i>Telegram</a>
                <a href='#cron'><i class='bi bi-clock-history me-2'></i>Cron</a>
                <a href='{{ url_for("auth_logout") }}'><i class='bi bi-box-arrow-right me-2'></i>Sair</a>
              </aside>
              <main class='col-md-9 col-lg-10 p-0'>
                <div class='topbar d-flex justify-content-between align-items-center px-4 py-3'>
                  <div><strong>Painel</strong> <span class='text-muted'>/ Dashboard</span></div>
                  <div class='d-flex align-items-center gap-2'>
                    <button class='btn btn-sm btn-outline-secondary' type='button' onclick='toggleSidebar()'><i class='bi bi-list'></i></button>
                    <button class='btn btn-sm btn-outline-secondary' type='button' onclick='toggleTheme()'><i class='bi bi-moon-stars'></i></button>
                    <div class='text-muted small'>{{user['email']}}</div>
                  </div>
                </div>
                <div class='p-4'>
                {% if restart_message %}
                  <div class='alert alert-{% if restart_status == "success" %}success{% else %}danger{% endif %} mb-3'>{{ restart_message }}</div>
                {% endif %}
                <div class='row g-3 mb-3'>
                  <div class='col-md-4'><div class='card kpi'><div class='card-body'><div class='text-muted'>Rotas</div><div class='h4 mb-0'>{{ routes|length }}</div></div></div></div>
                  <div class='col-md-4'><div class='card kpi'><div class='card-body'><div class='text-muted'>Cron</div><div class='h6 mb-0'>{% if not cron or cron['cron_enabled'] %}Ativo{% else %}Inativo{% endif %} ({{ cron_minutes }} min)</div></div></div></div>
                  <div class='col-md-4'><div class='card kpi'><div class='card-body'><div class='text-muted'>Última execução</div><div class='small mb-0'>{% if last_run %}{{last_run['status']}}{% else %}sem execução{% endif %}</div></div></div></div>
                </div>

                <div class='card mb-3 shadow-sm dashboard-section' id='rotas'>
                  <div class='card-header'><i class='bi bi-signpost-split me-2'></i>Rotas configuradas</div>
                  <div class='card-body'>
                    <form method='post' action='{{ url_for("add_route") }}' class='row g-2 mb-3 align-items-end'>
                      <div class='col-md-2'>
                        <label class='form-label small text-uppercase mb-1'>Origem</label>
                        <select class='form-select form-select-sm' name='origin' required>
                          {% for code, label in airport_options %}
                            <option value='{{ code }}' {% if code == 'PVH' %}selected{% endif %}>{{ label }}</option>
                          {% endfor %}
                        </select>
                      </div>
                      <div class='col-md-2'>
                        <label class='form-label small text-uppercase mb-1'>Destino</label>
                        <select class='form-select form-select-sm' name='destination' required>
                          {% for code, label in airport_options %}
                            <option value='{{ code }}' {% if code == 'JPA' %}selected{% endif %}>{{ label }}</option>
                          {% endfor %}
                        </select>
                      </div>
                      <div class='col-md-3'>
                        <label class='form-label small text-uppercase mb-1'>Ida</label>
                        <input class='form-control form-control-sm' name='outbound_date' type='date' required>
                      </div>
                      <div class='col-md-3'>
                        <label class='form-label small text-uppercase mb-1'>Volta</label>
                        <input class='form-control form-control-sm' name='inbound_date' type='date'>
                      </div>
                      <div class='col-md-2 d-grid'>
                        <button class='btn btn-primary btn-sm' type='submit'>Adicionar</button>
                      </div>
                    </form>
                    <div class='small text-muted mb-3'>As datas padrão globais de ida foram reduzidas para 04 e 05 de junho.</div>
                    <div class='table-responsive border rounded'>
                      <table class='table table-hover table-striped mb-0 align-middle'>
                        <thead class='table-light'>
                          <tr>
                            <th>Origem</th>
                            <th>Destino</th>
                            <th>Data de Ida</th>
                            <th>Data de Volta</th>
                            <th class='text-end'>Ações</th>
                          </tr>
                        </thead>
                        <tbody>
                          {% for r in routes %}
                            <tr>
                              <form method='post' action='{{ url_for("update_route", route_id=r["id"]) }}'>
                                <td>
                                  <select class='form-select form-select-sm' name='origin' required>
                                    {% for code, label in airport_options %}
                                      <option value='{{ code }}' {% if code == (r["origin"] or "").upper() %}selected{% endif %}>{{ label }}</option>
                                    {% endfor %}
                                  </select>
                                </td>
                                <td>
                                  <select class='form-select form-select-sm' name='destination' required>
                                    {% for code, label in airport_options %}
                                      <option value='{{ code }}' {% if code == (r["destination"] or "").upper() %}selected{% endif %}>{{ label }}</option>
                                    {% endfor %}
                                  </select>
                                </td>
                                <td><input class='form-control form-control-sm' name='outbound_date' type='date' value='{{r["outbound_date"]}}' required></td>
                                <td><input class='form-control form-control-sm' name='inbound_date' type='date' value='{{r["inbound_date"] if r["inbound_date"] else ""}}'></td>
                                <td class='text-end text-nowrap'>
                                  <button class='btn btn-sm btn-outline-primary' type='submit'><i class='bi bi-save'></i> Salvar</button>
                                  <a class='btn btn-sm btn-outline-danger' href='{{ url_for("delete_route", route_id=r["id"]) }}'>
                                    <i class='bi bi-trash'></i> Excluir
                                  </a>
                                </td>
                              </form>
                            </tr>
                          {% else %}
                            <tr><td colspan='5' class='text-center text-muted py-3'>Nenhuma rota cadastrada.</td></tr>
                          {% endfor %}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>

                <div class='card mb-3 shadow-sm dashboard-section d-none' id='consultas'>
                  <div class='card-header d-flex justify-content-between align-items-center'>
                    <div>
                      <i class='bi bi-window me-2'></i>App Consultas
                      <small class='text-muted d-block'>Executa buscas com o cron integrando histórico, rotas e consultas manuais.</small>
                    </div>
                    <button class='btn btn-sm btn-outline-secondary' type='button' onclick='document.getElementById('btn-consultar').scrollIntoView({behavior: "smooth"});'>Ir para consulta</button>
                  </div>
                  <div class='card-body'>
                    <section class='mb-4'>
                      <div class='row g-2 align-items-end'>
                        <div class='col-md-3'>
                          <label class='form-label small text-uppercase'>Origem</label>
                          <select id='origin' class='form-select form-select-sm'>
                            <option value='PVH' selected>PVH — Porto Velho (RO)</option>
                            <option value='BPS'>BPS — Porto Seguro (BA)</option>
                            <option value='RIO'>RIO — Rio de Janeiro (RJ)</option>
                            <option value='SAO'>SAO — São Paulo (SP)</option>
                            <option value='BSB'>BSB — Brasília (DF)</option>
                            <option value='CGB'>CGB — Cuiabá (MT)</option>
                            <option value='GYN'>GYN — Goiânia (GO)</option>
                            <option value='MCZ'>MCZ — Maceió (AL)</option>
                            <option value='AJU'>AJU — Aracaju (SE)</option>
                            <option value='SSA'>SSA — Salvador (BA)</option>
                            <option value='FOR'>FOR — Fortaleza (CE)</option>
                            <option value='SLZ'>SLZ — São Luís (MA)</option>
                            <option value='CGR'>CGR — Campo Grande (MS)</option>
                            <option value='BHZ'>BHZ — Belo Horizonte (MG)</option>
                            <option value='BEL'>BEL — Belém (PA)</option>
                            <option value='JPA'>JPA — João Pessoa (PB)</option>
                            <option value='CWB'>CWB — Curitiba (PR)</option>
                            <option value='REC'>REC — Recife (PE)</option>
                            <option value='THE'>THE — Teresina (PI)</option>
                            <option value='NAT'>NAT — Natal (RN)</option>
                            <option value='POA'>POA — Porto Alegre (RS)</option>
                            <option value='FLN'>FLN — Florianópolis (SC)</option>
                            <option value='VIX'>VIX — Vitória (ES)</option>
                            <option value='MAO'>MAO — Manaus (AM)</option>
                            <option value='RBR'>RBR — Rio Branco (AC)</option>
                            <option value='BVB'>BVB — Boa Vista (RR)</option>
                            <option value='MCP'>MCP — Macapá (AP)</option>
                            <option value='PMW'>PMW — Palmas (TO)</option>
                          </select>
                        </div>
                        <div class='col-md-3'>
                          <label class='form-label small text-uppercase'>Destino</label>
                          <select id='destination' class='form-select form-select-sm'>
                            <option value='JPA' selected>JPA — João Pessoa (PB)</option>
                            <option value='BPS'>BPS — Porto Seguro (BA)</option>
                            <option value='REC'>REC — Recife (PE)</option>
                            <option value='NAT'>NAT — Natal (RN)</option>
                            <option value='SLZ'>SLZ — São Luís (MA)</option>
                            <option value='THE'>THE — Teresina (PI)</option>
                            <option value='FOR'>FOR — Fortaleza (CE)</option>
                            <option value='MCZ'>MCZ — Maceió (AL)</option>
                            <option value='AJU'>AJU — Aracaju (SE)</option>
                            <option value='SSA'>SSA — Salvador (BA)</option>
                            <option value='PVH'>PVH — Porto Velho (RO)</option>
                            <option value='RIO'>RIO — Rio de Janeiro (RJ)</option>
                            <option value='SAO'>SAO — São Paulo (SP)</option>
                            <option value='BSB'>BSB — Brasília (DF)</option>
                            <option value='CGB'>CGB — Cuiabá (MT)</option>
                            <option value='GYN'>GYN — Goiânia (GO)</option>
                            <option value='CGR'>CGR — Campo Grande (MS)</option>
                            <option value='BHZ'>BHZ — Belo Horizonte (MG)</option>
                            <option value='BEL'>BEL — Belém (PA)</option>
                            <option value='CWB'>CWB — Curitiba (PR)</option>
                            <option value='POA'>POA — Porto Alegre (RS)</option>
                            <option value='FLN'>FLN — Florianópolis (SC)</option>
                            <option value='VIX'>VIX — Vitória (ES)</option>
                            <option value='MAO'>MAO — Manaus (AM)</option>
                            <option value='RBR'>RBR — Rio Branco (AC)</option>
                            <option value='BVB'>BVB — Boa Vista (RR)</option>
                            <option value='MCP'>MCP — Macapá (AP)</option>
                            <option value='PMW'>PMW — Palmas (TO)</option>
                          </select>
                        </div>
                        <div class='col-md-2'>
                          <label class='form-label small text-uppercase'>Ida</label>
                          <input id='outbound_date' type='date' class='form-control form-control-sm' value='2026-06-05' />
                        </div>
                        <div class='col-md-2'>
                          <label class='form-label small text-uppercase'>Volta</label>
                          <input id='inbound_date' type='date' class='form-control form-control-sm' value='' />
                        </div>
                        <div class='col-12 col-md-1 d-grid'>
                          <button id='btn-consultar' class='btn btn-primary btn-sm' onclick='consultar()'>Consultar</button>
                        </div>
                      </div>
                      <small class='text-muted d-block mt-2'>Se preencher volta, consulta como ida e volta.</small>
                    </section>
                    <section class='mb-4'>
                      <h6 class='text-uppercase text-muted mb-3'>Resultados da consulta</h6>
                      <div class='table-responsive'>
                        <table class='table table-striped table-hover align-middle text-center mb-0' id='consulta-table'>
                          <thead class='table-light'>
                            <tr>
                              <th>Rota</th>
                              <th>Data voo</th>
                              <th>Preço</th>
                              <th>Onde comprar mais barato</th>
                              <th>Fonte</th>
                              <th>Origem preço</th>
                              <th>Data/Hora</th>
                            </tr>
                          </thead>
                          <tbody id='consulta-body'>
                            <tr>
                              <td colspan='7' class='text-center text-muted'>Faça uma consulta para ver resultados.</td>
                            </tr>
                          </tbody>
                        </table>
                      </div>
                    </section>
                    <section class='mb-4'>
                      <div class='d-flex justify-content-between align-items-center mb-2'>
                        <h6 class='text-uppercase text-muted m-0'>Buscar todos (cron)</h6>
                        <button id='btn-cron' class='btn btn-warning btn-sm' type='button' onclick='executarCron()'>Executar busca completa</button>
                      </div>
                      <div id='cron-loading' class='text-muted mb-2' style='display:none;'>Buscando rotas... isso pode levar alguns minutos.</div>
                      <div class='table-responsive'>
                        <table class='table table-striped table-hover align-middle text-center mb-0' id='cron-table'>
                          <thead class='table-light'>
                            <tr>
                              <th>Rota</th>
                              <th>Data voo</th>
                              <th>Preço</th>
                              <th>Onde comprar mais barato</th>
                              <th>Fonte</th>
                              <th>Origem preço</th>
                              <th>Data/Hora</th>
                            </tr>
                          </thead>
                          <tbody id='cron-body'>
                            <tr><td colspan='7' class='text-center text-muted'>Clique em “Executar busca completa”.</td></tr>
                          </tbody>
                        </table>
                      </div>
                    </section>
                    <section class='mb-4'>
                      <div class='d-flex justify-content-between align-items-center mb-2'>
                        <h6 class='text-uppercase text-muted m-0'>Histórico</h6>
                        <div class='d-flex gap-2'>
                          <input id='historico-limit' type='number' class='form-control form-control-sm' value='20' min='1' max='200' style='width: 90px;' />
                          <button class='btn btn-outline-success btn-sm' type='button' onclick='historico()'>Atualizar</button>
                          <button class='btn btn-outline-danger btn-sm' type='button' onclick='limparHistorico()'>Limpar</button>
                        </div>
                      </div>
                      <div id='historico-loading' class='text-muted mb-2' style='display:none;'>Carregando histórico...</div>
                      <div class='table-responsive'>
                        <table class='table table-striped table-hover align-middle text-center mb-0' id='historico-table'>
                          <thead class='table-light'>
                            <tr>
                              <th>Rota</th>
                              <th>Data voo</th>
                              <th>Preço</th>
                              <th>Onde comprar mais barato</th>
                              <th>Fonte</th>
                              <th>Origem preço</th>
                              <th>Data/Hora</th>
                            </tr>
                          </thead>
                          <tbody id='historico-body'>
                            <tr>
                              <td colspan='7' class='text-center text-muted'>Clique em “Atualizar” para carregar.</td>
                            </tr>
                          </tbody>
                        </table>
                      </div>
                    </section>
                    <section>
                      <div class='d-flex justify-content-between align-items-center mb-2'>
                        <h6 class='text-uppercase text-muted m-0'>Rotas configuradas</h6>
                        <button class='btn btn-outline-secondary btn-sm' type='button' onclick='rotas()'>Atualizar</button>
                      </div>
                      <div id='rotas-loading' class='text-muted mb-2' style='display:none;'>Carregando rotas...</div>
                      <div class='table-responsive'>
                        <table class='table table-striped table-hover align-middle text-center mb-0' id='rotas-table'>
                          <thead class='table-light'>
                            <tr>
                              <th>Origem</th>
                              <th>Destino</th>
                              <th>Ida</th>
                              <th>Volta</th>
                              <th>Tipo</th>
                            </tr>
                          </thead>
                          <tbody id='rotas-body'>
                            <tr>
                              <td colspan='5' class='text-center text-muted'>Clique em “Atualizar” para carregar.</td>
                            </tr>
                          </tbody>
                        </table>
                      </div>
                    </section>
                  </div>
                </div>

                <div class='card mb-3 shadow-sm dashboard-section d-none' id='telegram'>
                  <div class='card-header'><i class='bi bi-telegram me-2'></i>Telegram do usuário</div>
                  <div class='card-body'>
                    <form method='post' action='{{ url_for("save_telegram") }}' class='row g-2'>
                      <div class='col-md-6'><input class='form-control' name='bot_token' placeholder='Bot token' value='{{ tg["bot_token"] if tg and tg["bot_token"] else default_tg_bot }}'></div>
                      <div class='col-md-4'><input class='form-control' name='chat_id' placeholder='Chat ID' value='{{ tg["chat_id"] if tg and tg["chat_id"] else default_tg_chat }}'></div>
                      <div class='col-md-2 d-grid'><button class='btn btn-success' type='submit'>Salvar</button></div>
                    </form>
                  </div>
                </div>

                <div class='card shadow-sm dashboard-section d-none' id='cron'>
                  <div class='card-header'><i class='bi bi-clock-history me-2'></i>Cron do usuário</div>
                  <div class='card-body'>
                    <form method='post' action='{{ url_for("save_cron") }}' class='row g-2 align-items-center'>
                      <div class='col-md-2 form-check ms-2'>
                        <input class='form-check-input' type='checkbox' name='enabled' id='enabled' {% if not cron or cron['cron_enabled'] %}checked{% endif %}>
                        <label class='form-check-label' for='enabled'>Ativo</label>
                      </div>
                      <div class='col-md-3'><input class='form-control' name='schedule_minutes' type='number' min='1' max='1440' step='1' value='{{ cron_minutes }}'></div>
                      <div class='col-md-4'><input class='form-control' name='max_price_display' type='number' min='0' step='0.01' placeholder='Preço máximo exibido por trecho' value='{{ cron_max_price }}'></div>
                      <div class='col-md-2 d-grid'><button class='btn btn-primary' type='submit'>Salvar</button></div>
                    </form>
                    <form method='post' action='{{ url_for("run_now_user") }}' class='mt-3'>
                      <button class='btn btn-warning' type='submit'>Executar agora</button>
                    </form>
                    <form method='post' action='{{ url_for("restart_service") }}' class='mt-2' onsubmit='return confirm("Reiniciar o serviço agora?");'>
                      <button class='btn btn-outline-danger' type='submit'>Reiniciar serviço</button>
                    </form>
                    <div class='small text-muted mt-2'>
                      {% if restart_command_configured %}
                        O painel usará o comando configurado em <code>SKYSCANNER_RESTART_COMMAND</code>.
                      {% else %}
                        Nenhum comando de reinício configurado. O painel tentará reabrir o processo Python atual.
                      {% endif %}
                    </div>
                    <div class='mt-3'><strong>Última execução:</strong><br>
                      {% if last_run %}
                        {{last_run['started_at']}} → {{last_run['finished_at']}} | {{last_run['status']}} | {{last_run['summary']}}
                      {% else %}
                        sem execução
                      {% endif %}
                    </div>
                  </div>
                </div>


                </div>
              </main>
            </div>
          </div>
        <script src='{{ url_for("static", filename="consulta-app.js") }}'></script>
        <script>
          function showSection(hash) {
            document.querySelectorAll('.dashboard-section').forEach(el => el.classList.add('d-none'));
            var target = document.getElementById(hash);
            if (target) {
              target.classList.remove('d-none');
              localStorage.setItem('adminActiveTab', hash);
            } else {
              document.getElementById('rotas').classList.remove('d-none');
              localStorage.setItem('adminActiveTab', 'rotas');
            }
            document.querySelectorAll('.sidebar a').forEach(el => el.classList.remove('fw-bold', 'text-white'));
            var activeLink = document.querySelector('.sidebar a[href="#' + hash + '"]');
            if (activeLink) activeLink.classList.add('fw-bold', 'text-white');
          }
          window.addEventListener('hashchange', () => {
            let hash = window.location.hash.substring(1);
            if(hash) {
              showSection(hash);
            }
          });
          window.addEventListener('load', () => {
            let hash = window.location.hash.substring(1) || localStorage.getItem('adminActiveTab') || 'rotas';
            showSection(hash);
          });
          function toggleTheme() {
            document.body.classList.toggle('dark-mode');
            localStorage.setItem('adminThemeDark', document.body.classList.contains('dark-mode') ? '1' : '0');
          }
          function toggleSidebar() {
            document.body.classList.toggle('sidebar-collapsed');
            localStorage.setItem('adminSidebarCollapsed', document.body.classList.contains('sidebar-collapsed') ? '1' : '0');
          }
          (function restoreUiState() {
            if (localStorage.getItem('adminThemeDark') === '1') document.body.classList.add('dark-mode');
            if (localStorage.getItem('adminSidebarCollapsed') === '1') document.body.classList.add('sidebar-collapsed');
          })();
        </script>
        </body>
        </html>
        """,
        user=user,
        routes=routes,
        tg=tg,
        cron=cron,
        cron_minutes=cron_minutes,
        cron_max_price=cron_max_price,
        last_run=last_run,
        default_tg_bot=default_tg_bot,
        default_tg_chat=default_tg_chat,
        airport_options=AIRPORT_OPTIONS,
        restart_command_configured=bool(PANEL_RESTART_COMMAND),
    )


@app.route("/painel/route/add", methods=["POST"])
@login_required
def add_route():
    db = get_auth_db()
    user = current_user()
    db.execute(
        "INSERT INTO user_routes (user_id, origin, destination, outbound_date, inbound_date, active, created_at) VALUES (?, ?, ?, ?, ?, 1, ?)",
        (
            user["id"],
            request.form.get("origin", "").strip().upper(),
            request.form.get("destination", "").strip().upper(),
            request.form.get("outbound_date", "").strip(),
            request.form.get("inbound_date", "").strip(),
            now_local_iso(sep="T"),
        ),
    )
    db.commit()
    return redirect(url_for("painel"))


@app.route("/painel/route/delete/<int:route_id>", methods=["GET"])
@login_required
def delete_route(route_id: int):
    db = get_auth_db()
    user = current_user()
    db.execute("DELETE FROM user_routes WHERE id = ? AND user_id = ?", (route_id, user["id"]))
    db.commit()
    return redirect(url_for("painel"))

@app.route("/painel/route/update/<int:route_id>", methods=["POST"])
@login_required
def update_route(route_id: int):
    db = get_auth_db()
    user = current_user()
    db.execute(
        """
        UPDATE user_routes
        SET origin = ?, destination = ?, outbound_date = ?, inbound_date = ?
        WHERE id = ? AND user_id = ?
        """,
        (
            request.form.get("origin", "").strip().upper(),
            request.form.get("destination", "").strip().upper(),
            request.form.get("outbound_date", "").strip(),
            request.form.get("inbound_date", "").strip(),
            route_id,
            user["id"],
        ),
    )
    db.commit()
    return redirect(url_for("painel", _anchor="rotas"))


@app.route("/painel/telegram", methods=["POST"])
@login_required
def save_telegram():
    db = get_auth_db()
    user = current_user()
    _upsert_telegram = sql(
        "INSERT INTO user_telegram (user_id, bot_token, chat_id, updated_at)"
        " VALUES (?, ?, ?, ?)"
        " ON DUPLICATE KEY UPDATE"
        "  bot_token = VALUES(bot_token),"
        "  chat_id = VALUES(chat_id),"
        "  updated_at = VALUES(updated_at)"
    )
    db.execute(
        _upsert_telegram,
        (
            user["id"],
            request.form.get("bot_token", "").strip(),
            request.form.get("chat_id", "").strip(),
            now_local_iso(sep="T"),
        ),
    )
    db.commit()
    return redirect(url_for("painel"))


@app.route("/painel/run-now", methods=["POST"])
@login_required
def run_now_user():
    user = current_user()
    run_user_scan(int(user["id"]), trigger="painel-manual", notify=True)
    return redirect(url_for("painel", _anchor="cron"))


@app.route("/painel/restart", methods=["POST"])
@login_required
def restart_service():
    ok, message, should_exit = trigger_service_restart()
    if not ok:
        return build_restart_redirect(message, level="error")

    if should_exit:
        def _shutdown_later():
            time.sleep(1)
            os._exit(0)

        threading.Thread(target=_shutdown_later, daemon=True).start()
    return build_restart_redirect(message, level="success")


@app.route("/painel/cron", methods=["POST"])
@login_required
def save_cron():
    db = get_auth_db()
    enabled = 1 if request.form.get("enabled") else 0
    schedule_minutes = max(1, min(1440, int(request.form.get("schedule_minutes", DEFAULT_SCAN_INTERVAL_MINUTES))))
    max_price_display_raw = request.form.get("max_price_display", "").strip()
    max_price_display = None
    if max_price_display_raw:
        max_price_display = max(0.0, float(max_price_display_raw))
    _upsert_app_settings = sql(
        "INSERT INTO app_settings (id, cron_enabled, scan_interval_minutes, max_price_display, updated_at)"
        " VALUES (1, ?, ?, ?, ?)"
        " ON DUPLICATE KEY UPDATE"
        "  cron_enabled = VALUES(cron_enabled),"
        "  scan_interval_minutes = VALUES(scan_interval_minutes),"
        "  max_price_display = VALUES(max_price_display),"
        "  updated_at = VALUES(updated_at)"
    )
    db.execute(
        _upsert_app_settings,
        (enabled, schedule_minutes, max_price_display, now_local_iso(sep="T")),
    )
    db.commit()
    return redirect(url_for("painel", _anchor="cron"))


if __name__ == "__main__":
    init_auth_tables()
    normalize_maxmilhas_history()
    start_auto_scan_if_needed()
    debug_mode = os.getenv("FLASK_DEBUG", "0").strip().lower() in ("1", "true", "yes")
    app.run(debug=debug_mode)
