import re
import time
import unicodedata
from datetime import datetime

from config import (
    TELEGRAM_CHAT_ID,
    now_local,
)
from db import indexed_text_column, insert_ignore_sql, is_missing_column_error, now_expression, sql

DEFAULT_AIRPORT_OPTIONS = [
    ("PVH", "Porto Velho, Brasil"),
    ("RIO", "Rio de Janeiro, Brasil"),
    ("SAO", "São Paulo, Brasil"),
    ("BSB", "Brasília, Brasil"),
    ("CGB", "Cuiabá, Brasil"),
    ("GYN", "Goiânia, Brasil"),
    ("MCZ", "Maceió, Brasil"),
    ("AJU", "Aracaju, Brasil"),
    ("SSA", "Salvador, Brasil"),
    ("FOR", "Fortaleza, Brasil"),
    ("SLZ", "São Luís, Brasil"),
    ("CGR", "Campo Grande, Brasil"),
    ("BHZ", "Belo Horizonte, Brasil"),
    ("BEL", "Belém, Brasil"),
    ("JPA", "João Pessoa, Brasil"),
    ("CWB", "Curitiba, Brasil"),
    ("REC", "Recife, Brasil"),
    ("THE", "Teresina, Brasil"),
    ("NAT", "Natal, Brasil"),
    ("POA", "Porto Alegre, Brasil"),
    ("FLN", "Florianópolis, Brasil"),
    ("VIX", "Vitória, Brasil"),
    ("MAO", "Manaus, Brasil"),
    ("RBR", "Rio Branco, Brasil"),
    ("BVB", "Boa Vista, Brasil"),
    ("MCP", "Macapá, Brasil"),
    ("PMW", "Palmas, Brasil"),
    ("GRU", "São Paulo, Brasil - Guarulhos"),
    ("CGH", "São Paulo, Brasil - Congonhas"),
    ("VCP", "Campinas, Brasil - Viracopos"),
    ("GIG", "Rio de Janeiro, Brasil - Galeão"),
    ("SDU", "Rio de Janeiro, Brasil - Santos Dumont"),
    ("CNF", "Belo Horizonte, Brasil - Confins"),
    ("LDB", "Londrina, Brasil"),
    ("IGU", "Foz do Iguaçu, Brasil"),
    ("MIA", "Miami, Estados Unidos"),
    ("FLL", "Fort Lauderdale, Estados Unidos"),
    ("MCO", "Orlando, Estados Unidos"),
    ("JFK", "Nova York, Estados Unidos - JFK"),
    ("EWR", "Nova York, Estados Unidos - Newark"),
    ("LGA", "Nova York, Estados Unidos - LaGuardia"),
    ("LAX", "Los Angeles, Estados Unidos"),
    ("SFO", "San Francisco, Estados Unidos"),
    ("IAD", "Washington, Estados Unidos - Dulles"),
    ("ORD", "Chicago, Estados Unidos - O'Hare"),
    ("ATL", "Atlanta, Estados Unidos"),
    ("BOS", "Boston, Estados Unidos"),
    ("DFW", "Dallas, Estados Unidos - Fort Worth"),
    ("LAS", "Las Vegas, Estados Unidos"),
    ("SEA", "Seattle, Estados Unidos"),
    ("YYZ", "Toronto, Canadá - Pearson"),
    ("YUL", "Montreal, Canadá"),
    ("MEX", "Cidade do México, México"),
    ("CUN", "Cancún, México"),
    ("BOG", "Bogotá, Colômbia"),
    ("LIM", "Lima, Peru"),
    ("SCL", "Santiago, Chile"),
    ("EZE", "Buenos Aires, Argentina - Ezeiza"),
    ("AEP", "Buenos Aires, Argentina - Aeroparque"),
    ("MVD", "Montevidéu, Uruguai"),
    ("ASU", "Assunção, Paraguai"),
    ("MAD", "Madri, Espanha"),
    ("BCN", "Barcelona, Espanha"),
    ("LIS", "Lisboa, Portugal"),
    ("OPO", "Porto, Portugal"),
    ("LHR", "Londres, Reino Unido - Heathrow"),
    ("LGW", "Londres, Reino Unido - Gatwick"),
    ("CDG", "Paris, França - Charles de Gaulle"),
    ("ORY", "Paris, França - Orly"),
    ("FCO", "Roma, Itália - Fiumicino"),
    ("MXP", "Milão, Itália - Malpensa"),
    ("AMS", "Amsterdã, Holanda"),
    ("FRA", "Frankfurt, Alemanha"),
    ("MUC", "Munique, Alemanha"),
    ("ZRH", "Zurique, Suíça"),
    ("IST", "Istambul, Turquia"),
    ("DXB", "Dubai, Emirados Árabes"),
    ("DOH", "Doha, Catar"),
    ("TLV", "Tel Aviv, Israel"),
    ("CAI", "Cairo, Egito"),
    ("NRT", "Tóquio, Japão - Narita"),
    ("HND", "Tóquio, Japão - Haneda"),
    ("ICN", "Seul, Coreia do Sul"),
    ("HKG", "Hong Kong"),
    ("SIN", "Singapura"),
    ("BKK", "Bangkok, Tailândia"),
    ("KUL", "Kuala Lumpur, Malásia"),
    ("DPS", "Bali, Indonésia"),
    ("SYD", "Sydney, Austrália"),
    ("MEL", "Melbourne, Austrália"),
    ("JNB", "Joanesburgo, África do Sul"),
    ("CPT", "Cidade do Cabo, África do Sul"),
]
AIRPORT_SEARCH_ALIASES = {
    'new york': ['JFK', 'EWR', 'LGA'],
    'nova york': ['JFK', 'EWR', 'LGA'],
    'nyc': ['JFK', 'EWR', 'LGA'],
    'london': ['LHR', 'LGW'],
    'paris': ['CDG', 'ORY'],
    'rome': ['FCO'],
    'milan': ['MXP'],
    'tokyo': ['NRT', 'HND'],
    'seoul': ['ICN'],
    'istanbul': ['IST'],
    'dubai': ['DXB'],
    'doha': ['DOH'],
    'miami beach': ['MIA'],
    'rondonia': ['PVH'],
    'ro': ['PVH'],
    'acre': ['RBR'],
    'ac': ['RBR'],
    'amazonas': ['MAO'],
    'am': ['MAO'],
    'roraima': ['BVB'],
    'rr': ['BVB'],
    'amapa': ['MCP'],
    'ap': ['MCP'],
    'tocantins': ['PMW'],
    'to': ['PMW'],
    'mato grosso': ['CGB'],
    'mt': ['CGB'],
    'mato grosso do sul': ['CGR'],
    'ms': ['CGR'],
    'goias': ['GYN'],
    'go': ['GYN'],
    'distrito federal': ['BSB'],
    'df': ['BSB'],
    'bahia': ['SSA'],
    'ba': ['SSA'],
    'ceara': ['FOR'],
    'ce': ['FOR'],
    'pernambuco': ['REC'],
    'pe': ['REC'],
    'paraiba': ['JPA'],
    'pb': ['JPA'],
    'rio grande do norte': ['NAT'],
    'rn': ['NAT'],
    'piaui': ['THE'],
    'pi': ['THE'],
    'maranhao': ['SLZ'],
    'ma': ['SLZ'],
    'sergipe': ['AJU'],
    'se': ['AJU'],
    'alagoas': ['MCZ'],
    'al': ['MCZ'],
    'para': ['BEL'],
    'pa': ['BEL'],
    'parana': ['CWB', 'LDB', 'IGU'],
    'pr': ['CWB', 'LDB', 'IGU'],
    'santa catarina': ['FLN'],
    'sc': ['FLN'],
    'rio grande do sul': ['POA'],
    'rs': ['POA'],
    'espirito santo': ['VIX'],
    'es': ['VIX'],
    'minas gerais': ['CNF', 'BHZ'],
    'mg': ['CNF', 'BHZ'],
    'rio de janeiro': ['GIG', 'SDU', 'RIO'],
    'rj': ['GIG', 'SDU', 'RIO'],
    'sao paulo': ['GRU', 'CGH', 'VCP', 'SAO'],
    'sp': ['GRU', 'CGH', 'VCP', 'SAO'],
    'brasilia': ['BSB'],
    'brasil': ['BSB', 'GRU', 'GIG', 'REC', 'SSA'],
    'florida': ['MIA', 'FLL', 'MCO'],
    'california': ['LAX', 'SFO'],
}

DEFAULT_FREE_USES_LIMIT = 20
DEFAULT_MAX_ROUTES_DEFAULT = 4
DEFAULT_PIX_PENDING_EXPIRATION_HOURS = 24

_policy_schema_ensured = False


def ensure_policy_schema(conn) -> None:
    global _policy_schema_ensured
    if _policy_schema_ensured:
        return
    for attempt in range(3):
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS monetization_settings (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    test_mode INTEGER DEFAULT 1,
                    charge_global INTEGER DEFAULT 0,
                    charge_admin_only INTEGER DEFAULT 1,
                    weekly_price REAL DEFAULT 5,
                    biweekly_price REAL DEFAULT 10,
                    monthly_price REAL DEFAULT 15,
                    free_uses_limit INTEGER DEFAULT 20,
                    max_routes_default INTEGER DEFAULT 6,
                    pix_pending_expiration_hours INTEGER DEFAULT 24
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS user_access (
                    chat_id {indexed_text_column()} PRIMARY KEY,
                    status {indexed_text_column()} DEFAULT 'free',
                    expires_at {indexed_text_column()} NULL,
                    free_uses INTEGER DEFAULT 0,
                    test_charge INTEGER DEFAULT 0,
                    total_paid REAL DEFAULT 0,
                    updated_at {indexed_text_column()} DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS admins (
                    chat_id {indexed_text_column()} PRIMARY KEY,
                    active INTEGER DEFAULT 1,
                    created_at {indexed_text_column()} DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS airports (
                    code {indexed_text_column()} PRIMARY KEY,
                    name {indexed_text_column(255)} NOT NULL,
                    active INTEGER DEFAULT 1,
                    sort_order INTEGER DEFAULT 0,
                    created_at {indexed_text_column()} DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            for ddl in [
                "ALTER TABLE monetization_settings ADD COLUMN free_uses_limit INTEGER DEFAULT 20",
                "ALTER TABLE monetization_settings ADD COLUMN max_routes_default INTEGER DEFAULT 6",
                "ALTER TABLE monetization_settings ADD COLUMN pix_pending_expiration_hours INTEGER DEFAULT 24",
                "ALTER TABLE monetization_settings ADD COLUMN maintenance_mode INTEGER DEFAULT 0",
                "ALTER TABLE bot_users ADD COLUMN exempt_from_maintenance INTEGER DEFAULT 0",
            ]:
                try:
                    conn.execute(ddl)
                except Exception as exc:
                    if is_missing_column_error(exc):
                        pass
                    else:
                        raise
            conn.execute(
                sql(f"""
                {insert_ignore_sql('monetization_settings', ['id', 'test_mode', 'charge_global', 'charge_admin_only', 'weekly_price', 'biweekly_price', 'monthly_price', 'free_uses_limit', 'max_routes_default', 'pix_pending_expiration_hours'], '1, 1, 0, 1, 5, 10, 15, %s, %s, %s')}
                """),
                (
                    DEFAULT_FREE_USES_LIMIT,
                    DEFAULT_MAX_ROUTES_DEFAULT,
                    DEFAULT_PIX_PENDING_EXPIRATION_HOURS,
                ),
            )
            conn.execute(
                sql("""
                UPDATE monetization_settings
                SET free_uses_limit = COALESCE(free_uses_limit, %s),
                    max_routes_default = COALESCE(max_routes_default, %s),
                    pix_pending_expiration_hours = COALESCE(pix_pending_expiration_hours, %s)
                WHERE id = 1
                """),
                (
                    DEFAULT_FREE_USES_LIMIT,
                    DEFAULT_MAX_ROUTES_DEFAULT,
                    DEFAULT_PIX_PENDING_EXPIRATION_HOURS,
                ),
            )
            admins_count = conn.execute(sql("SELECT COUNT(*) AS total FROM admins")).fetchone()["total"]
            if int(admins_count or 0) == 0:
                conn.execute(
                    sql("INSERT INTO admins (chat_id, active) VALUES (%s, 1)"),
                    (TELEGRAM_CHAT_ID,),
                )
            for idx, (code, name) in enumerate(DEFAULT_AIRPORT_OPTIONS, start=1):
                conn.execute(
                    sql(f"""
                    {insert_ignore_sql('airports', ['code', 'name', 'active', 'sort_order'], '%s, %s, 1, %s')}
                    """),
                    (str(code).upper(), str(name), idx),
                )
            conn.commit()
            _policy_schema_ensured = True
            return
        except Exception as exc:
            conn.rollback()
            code = getattr(exc, 'args', [None])[0]
            if code == 1213 and attempt < 2:
                time.sleep(0.5 * (attempt + 1))
                continue
            raise


def get_monetization_settings(conn):
    ensure_policy_schema(conn)
    return conn.execute(sql("SELECT * FROM monetization_settings WHERE id = 1")).fetchone()


def is_admin_chat(conn, chat_id: str) -> bool:
    ensure_policy_schema(conn)
    row = conn.execute(
        sql("SELECT 1 FROM admins WHERE chat_id = %s AND active = 1 LIMIT 1"),
        (chat_id,),
    ).fetchone()
    return bool(row)


def list_active_admin_chat_ids(conn) -> list[str]:
    ensure_policy_schema(conn)
    rows = conn.execute(
        sql("SELECT chat_id FROM admins WHERE active = 1 ORDER BY created_at ASC")
    ).fetchall()
    return [str(row["chat_id"]) for row in rows]


def is_maintenance_mode(conn) -> bool:
    ensure_policy_schema(conn)
    row = conn.execute(sql("SELECT maintenance_mode FROM monetization_settings WHERE id = 1")).fetchone()
    return bool(int((row["maintenance_mode"] if row else None) or 0))


def set_maintenance_mode(conn, enabled: bool) -> None:
    ensure_policy_schema(conn)
    conn.execute(sql("UPDATE monetization_settings SET maintenance_mode = %s WHERE id = 1"), (1 if enabled else 0,))
    conn.commit()


def is_exempt_from_maintenance(conn, chat_id: str) -> bool:
    ensure_policy_schema(conn)
    # Admins são sempre isentos
    if is_admin_chat(conn, chat_id):
        return True
    row = conn.execute(
        sql("SELECT exempt_from_maintenance FROM bot_users WHERE chat_id = %s LIMIT 1"), (chat_id,)
    ).fetchone()
    return bool(int((row["exempt_from_maintenance"] if row else None) or 0))


def set_exempt_from_maintenance(conn, chat_id: str, exempt: bool) -> None:
    ensure_policy_schema(conn)
    conn.execute(
        sql("UPDATE bot_users SET exempt_from_maintenance = %s WHERE chat_id = %s"),
        (1 if exempt else 0, chat_id),
    )
    conn.commit()


def list_airports(conn) -> list[tuple[str, str]]:
    ensure_policy_schema(conn)
    rows = conn.execute(
        """
        SELECT code, name
        FROM airports
        WHERE active = 1
        ORDER BY sort_order ASC, code ASC
        """
    ).fetchall()
    return [(str(row["code"]).upper(), str(row["name"])) for row in rows]


def _normalize_airport_search_text(value: str) -> str:
    text = unicodedata.normalize('NFKD', (value or '').strip())
    text = ''.join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def search_airports(conn, query: str, limit: int = 8) -> list[tuple[str, str]]:
    ensure_policy_schema(conn)
    term = _normalize_airport_search_text(query)
    options = list_airports(conn)
    if not term:
        return options[:limit]

    alias_codes = AIRPORT_SEARCH_ALIASES.get(term, [])

    ranked = []
    for idx, (code, name) in enumerate(options):
        code_norm = _normalize_airport_search_text(code)
        name_norm = _normalize_airport_search_text(name)
        haystack = f"{code_norm} {name_norm}".strip()
        if not haystack:
            continue
        if code in alias_codes:
            rank = (0, idx)
        elif term == code_norm:
            rank = (1, idx)
        elif code_norm.startswith(term):
            rank = (1, idx)
        elif name_norm.startswith(term):
            rank = (2, idx)
        elif f" {term}" in f" {name_norm}":
            rank = (3, idx)
        elif term in haystack:
            rank = (4, idx)
        else:
            continue
        ranked.append((rank, code, name))

    ranked.sort(key=lambda item: (item[0][0], item[0][1], item[1]))
    return [(code, name) for _, code, name in ranked[: max(1, limit)]]


def get_airport_labels(conn) -> dict[str, str]:
    options = list_airports(conn)
    return {code: f"{code} — {name}" for code, name in options}


def get_free_uses_limit(conn) -> int:
    settings = get_monetization_settings(conn)
    return int(settings["free_uses_limit"] or DEFAULT_FREE_USES_LIMIT)


def get_max_routes_default(conn) -> int:
    settings = get_monetization_settings(conn)
    return int(settings["max_routes_default"] or DEFAULT_MAX_ROUTES_DEFAULT)


def set_max_routes_default(conn, value: int) -> None:
    conn.execute(
        sql("UPDATE monetization_settings SET max_routes_default = %s WHERE id = 1"),
        (max(1, value),),
    )
    conn.commit()


def get_pix_pending_expiration_hours(conn) -> int:
    settings = get_monetization_settings(conn)
    return int(
        settings["pix_pending_expiration_hours"] or DEFAULT_PIX_PENDING_EXPIRATION_HOURS
    )


def ensure_user_access(conn, chat_id: str):
    ensure_policy_schema(conn)
    conn.execute(
        sql(f"""
        {insert_ignore_sql('user_access', ['chat_id', 'status', 'free_uses', 'test_charge', 'total_paid', 'updated_at'], f"%s, 'free', 0, 0, 0, {now_expression()}")}
        """),
        (chat_id,),
    )
    conn.commit()
    return conn.execute(sql("SELECT * FROM user_access WHERE chat_id = %s"), (chat_id,)).fetchone()


def is_active_access(access_row) -> bool:
    if not access_row:
        return False
    if (access_row["status"] or "") != "active":
        return False
    expires_at = (access_row["expires_at"] or "").strip()
    if not expires_at:
        return False
    try:
        return datetime.fromisoformat(expires_at) > now_local()
    except ValueError:
        return False


def should_charge_user(conn, chat_id: str, access_row) -> bool:
    settings = get_monetization_settings(conn)
    admin_chat = is_admin_chat(conn, chat_id)
    if admin_chat:
        if int(settings["charge_global"]) == 1:
            return True
        return bool(int(settings["charge_admin_only"]) == 1 and int(access_row["test_charge"] or 0) == 1)
    if int(settings["charge_admin_only"]) == 1:
        return False
    return bool(int(settings["charge_global"]) or int(access_row["test_charge"] or 0))
