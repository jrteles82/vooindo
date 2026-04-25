"""
notif.py — Notificações admin configuráveis.

Tipos suportados (correspondem a colunas em app_settings):
  notif_novo_usuario          — novo usuário iniciou o bot
  notif_acesso_expirado       — usos gratuitos esgotados ao tentar usar
  notif_pix_gerado            — usuário gerou um PIX
  notif_pagamento_confirmado  — pagamento aprovado pelo Mercado Pago

Uso:
    from notif import push_admin_notif
    push_admin_notif(conn, "notif_novo_usuario", "👤 *Novo usuário*\n...")
"""
from __future__ import annotations

import requests

from access_policy import list_active_admin_chat_ids
from app_logging import get_logger
from config import TELEGRAM_API_BASE_URL, TOKEN
from db import sql

logger = get_logger("notif")

NOTIF_COLUMNS = [
    "notif_novo_usuario",
    "notif_acesso_expirado",
    "notif_pix_gerado",
    "notif_pagamento_confirmado",
]

NOTIF_LABELS = {
    "notif_novo_usuario":         "Novo usuário no bot",
    "notif_acesso_expirado":      "Acesso gratuito esgotado",
    "notif_pix_gerado":           "PIX gerado por usuário",
    "notif_pagamento_confirmado": "Pagamento confirmado",
}


def is_notif_enabled(conn, key: str) -> bool:
    try:
        row = conn.execute(
            sql(f"SELECT {key} FROM app_settings WHERE id = 1")
        ).fetchone()
        if row is None:
            return False
        val = row[key] if isinstance(row, dict) else row[0]
        return bool(int(val or 0))
    except Exception:
        return False


def get_notif_settings(conn) -> dict[str, bool]:
    """Retorna dict {coluna: bool} com o estado de cada notificação."""
    result: dict[str, bool] = {}
    for key in NOTIF_COLUMNS:
        result[key] = is_notif_enabled(conn, key)
    return result


def push_admin_notif(conn, key: str, text: str) -> None:
    """
    Envia `text` para todos os admins ativos se `key` estiver habilitado.
    Nunca levanta exceção — falhas são logadas silenciosamente.
    """
    try:
        if not is_notif_enabled(conn, key):
            return
        admin_ids = list_active_admin_chat_ids(conn)
        if not admin_ids:
            return
        url = f"{TELEGRAM_API_BASE_URL}/bot{TOKEN}/sendMessage"
        for admin_chat_id in admin_ids:
            try:
                requests.post(
                    url,
                    data={"chat_id": admin_chat_id, "text": text, "parse_mode": "Markdown"},
                    timeout=10,
                )
            except Exception as exc:
                logger.debug("push_admin_notif: falha ao enviar para %s: %s", admin_chat_id, exc)
    except Exception as exc:
        logger.debug("push_admin_notif: erro geral: %s", exc)
