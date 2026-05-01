"""Cliente do Chrome Pool — usado pelos workers em main.py.

Se o pool manager estiver rodando, faz o executor se conectar via WS.
Caso contrário, funciona como antes (semáforo + subprocesso normal).
"""
from __future__ import annotations

import json
import os
import time
from typing import Optional

CHROME_POOL_STATUS_URL = "http://127.0.0.1:9230/status"

def is_pool_running() -> bool:
    """Pool manager está ativo?"""
    try:
        import urllib.request
        resp = urllib.request.urlopen(CHROME_POOL_STATUS_URL, timeout=3)
        data = json.loads(resp.read().decode())
        return data.get("ready", 0) > 0
    except Exception:
        return False


def acquire_pool_instance(timeout: float = 30.0) -> Optional[dict]:
    """Pega uma instância do pool. Retorna dict com ws_endpoint, port, profile."""
    import urllib.request
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = urllib.request.urlopen(CHROME_POOL_STATUS_URL, timeout=3)
            data = json.loads(resp.read().decode())
            for inst in data.get("instances", []):
                if inst.get("alive") and inst.get("ready"):
                    ws = inst.get("ws_endpoint", "")
                    if ws:
                        return {
                            "ws_endpoint": ws,
                            "port": inst.get("port"),
                            "profile": inst.get("profile"),
                            "instance_id": inst.get("id"),
                        }
        except Exception:
            pass
        time.sleep(0.5)
    return None
