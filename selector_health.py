#!/home/teles/dev/python/vooindo-bot/.venv/bin/python
"""
selector_health.py — Verifica seletores Playwright do Google Flights,
detecta quebras, avisa no Telegram e só altera após confirmação explícita.

Fluxo de correção:
1. heurísticas Python
2. Deepseek (responsável principal)

Se um não resolver, tenta o próximo.

Uso standalone:
    python selector_health.py
    python selector_health.py --dry-run   # sem gravar correções
    python selector_health.py --approve <token>   # aplica correções já detectadas e aprovadas
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import traceback
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus
import hashlib

os.environ.setdefault(
    "PLAYWRIGHT_BROWSERS_PATH",
    str(Path(__file__).with_name(".cache/ms-playwright")),
)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    BASE_DIR,
    TELEGRAM_API_BASE_URL,
    TELEGRAM_CHAT_ID,
    TOKEN,
    now_local_iso,
)

# ── logger dedicado ──────────────────────────────────────────────────────────

def _make_health_logger() -> logging.Logger:
    log_dir = BASE_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    lg = logging.getLogger("selector_health")
    if lg.handlers:
        return lg
    lg.setLevel(logging.DEBUG)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    lg.addHandler(sh)

    fh = RotatingFileHandler(
        log_dir / "selector_health.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    lg.addHandler(fh)
    return lg


logger = _make_health_logger()

# ── registro de seletores críticos ───────────────────────────────────────────
#   Cada entrada:
#     selector   — string exata usada no código-fonte
#     description— o que o seletor localiza
#     files      — arquivos que contêm esse seletor
#     critical   — se True, tenta auto-correção quando quebrado
#     kind       — "css" | "text"  (text = get_by_text, não CSS puro)

SELECTOR_REGISTRY: list[dict[str, Any]] = [
    # ── área principal ───────────────────────────────────────────────────────
    {
        "key": "main_role",
        "selector": "[role='main']",
        "description": "Área principal da página de resultados",
        "files": ["google_flights_executor.py", ".py", "update_scraper.py"],
        "critical": True,
        "kind": "css",
    },
    # ── cards de voo ─────────────────────────────────────────────────────────
    {
        "key": "flight_cards_listitem",
        "selector": "[role='main'] [role='listitem']",
        "description": "Cards de voo (listitem na área principal)",
        "files": ["google_flights_executor.py", ".py", "update_scraper.py"],
        "critical": True,
        "kind": "css",
    },
    {
        "key": "flight_cards_li",
        "selector": "[role='main'] li",
        "description": "Items de voo como elemento li dentro da área principal",
        "files": ["google_flights_executor.py", ".py"],
        "critical": False,
        "kind": "css",
    },
    {
        "key": "flight_cards_jscontroller",
        "selector": "[role='main'] div[jscontroller]",
        "description": "Cards de voo com atributo jscontroller do Google",
        "files": [".py", "update_scraper.py"],
        "critical": False,
        "kind": "css",
    },
    {
        "key": "flight_card_link",
        "selector": "div.JMc5Xc[role='link']",
        "description": "Card de voo com classe JMc5Xc e role link",
        "files": ["google_flights_executor.py"],
        "critical": False,
        "kind": "css",
    },
    {
        "key": "flight_jsname",
        "selector": "[jsname='v8pSFe']",
        "description": "Componente JS interno v8pSFe (painel de detalhes)",
        "files": [".py"],
        "critical": False,
        "kind": "css",
    },
    # ── autenticação / perfil ────────────────────────────────────────────────
    {
        "key": "profile_conta_google",
        "selector": 'a[aria-label*="Conta do Google"]',
        "description": "Link da conta Google (interface PT-BR)",
        "files": ["google_flights_executor.py", ".py", "renew_google_session.py"],
        "critical": False,
        "kind": "css",
    },
    {
        "key": "profile_google_account",
        "selector": 'a[aria-label*="Google Account"]',
        "description": "Link da conta Google (interface EN)",
        "files": ["google_flights_executor.py", ".py", "renew_google_session.py"],
        "critical": False,
        "kind": "css",
    },
    {
        "key": "profile_data_ogsr",
        "selector": "[data-ogsr-up]",
        "description": "Atributo data-ogsr-up presente quando usuário está autenticado",
        "files": ["google_flights_executor.py", ".py", "renew_google_session.py"],
        "critical": False,
        "kind": "css",
    },
    # ── abas de resultados (texto visível) ───────────────────────────────────
    {
        "key": "tab_melhor_opcao",
        "selector": "Melhor opção",
        "description": "Aba/botão 'Melhor opção' nos resultados",
        "files": ["google_flights_executor.py", ".py"],
        "critical": True,
        "kind": "text",
    },
    {
        "key": "tab_menores_precos",
        "selector": "Menores preços",
        "description": "Aba/botão 'Menores preços' nos resultados",
        "files": ["update_scraper.py", ".py"],
        "critical": False,
        "kind": "text",
    },
    {
        "key": "tab_outros_voos",
        "selector": "Outros voos",
        "description": "Seção 'Outros voos' nos resultados",
        "files": ["google_flights_executor.py", ".py"],
        "critical": False,
        "kind": "text",
    },
    {
        "key": "show_more_flights",
        "selector": "Mostrar mais voos",
        "description": "Botão para expandir mais resultados de voo",
        "files": [
            "google_flights_executor.py",
            "google_flights_profile_debug.py",
            ".py",
        ],
        "critical": False,
        "kind": "text",
    },
    # ── booking ──────────────────────────────────────────────────────────────
    {
        "key": "booking_link",
        "selector": "a[href*='/travel/flights/booking']",
        "description": "Link de reserva no painel de detalhes do voo",
        "files": ["google_flights_executor.py"],
        "critical": False,
        "kind": "css",
    },
]


# ── Telegram ──────────────────────────────────────────────────────────────────

def _send_telegram(text: str, reply_markup: dict[str, Any] | None = None) -> None:
    import requests  # noqa: PLC0415

    admin_chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID", TELEGRAM_CHAT_ID).strip()
    url = f"{TELEGRAM_API_BASE_URL}/bot{TOKEN}/sendMessage"
    payload: dict[str, Any] = {"chat_id": admin_chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    try:
        resp = requests.post(url, data=payload, timeout=20)
        resp.raise_for_status()
        logger.info("Notificação Telegram enviada")
    except Exception as exc:
        logger.error("Falha ao enviar Telegram: %s", exc)


# ── testa um seletor na página ────────────────────────────────────────────────

def _count_elements(page, entry: dict[str, Any]) -> int:
    sel = entry["selector"]
    try:
        if entry["kind"] == "text":
            return page.get_by_text(sel, exact=False).count()
        return page.locator(sel).count()
    except Exception:
        return -1  # erro de sintaxe no seletor


# ── auto-correção Python / GPT / Claude ──────────────────────────────────────

def _capture_dom_html(page) -> str:
    dom_html = ""
    for loc_sel in ["[role='main']", "body"]:
        try:
            dom_html = page.locator(loc_sel).first.inner_html(timeout=6000)
            if dom_html:
                break
        except Exception:
            pass
    if not dom_html:
        return ""
    max_chars = 18_000
    if len(dom_html) > max_chars:
        dom_html = dom_html[:max_chars] + "\n<!-- HTML truncado -->"
    return dom_html


def _build_llm_prompt(entry: dict[str, Any], dom_html: str) -> str:
    kind_hint = (
        "seletor CSS Playwright válido (ex: [role='listitem'], div[jscontroller])"
        if entry["kind"] == "css"
        else "texto visível exato ou quase exato da página (ex: Melhor opção)"
    )
    return (
        f"Você é um especialista em automação web com Playwright.\n\n"
        f"O seletor `{entry['selector']}` parou de funcionar no Google Flights.\n"
        f"Esse seletor identifica: {entry['description']}.\n"
        f"Tipo esperado: {kind_hint}.\n\n"
        f"HTML atual:\n```html\n{dom_html}\n```\n\n"
        f"Sugira UM substituto que maximize estabilidade. Prefira role, aria-*, data-* e jsname antes de classes voláteis.\n"
        f"Responda APENAS com o seletor, sem aspas e sem explicação."
    )


def _autocorrect_via_python(page, entry: dict[str, Any]) -> str | None:
    candidates_map = {
        "main_role": ["[role='main']", "main", "[aria-label*='voos']"],
        "flight_cards_listitem": [
            "[role='main'] [role='listitem']",
            "[role='listitem']",
            "[role='main'] li",
            "[role='main'] div[role='button']",
            "[role='main'] div[jscontroller]",
        ],
        "flight_cards_li": ["[role='main'] li", "[role='listitem']", "li"],
        "flight_cards_jscontroller": ["[role='main'] div[jscontroller]", "div[jscontroller]"],
        "flight_jsname": ["[jsname='v8pSFe']", "[jsname=\"v8pSFe\"]"],
        "tab_melhor_opcao": ["Melhor opção", "Melhor opcao", "Best", "Best flights"],
        "tab_menores_precos": ["Menores preços", "Menores precos", "Cheapest", "Lowest prices"],
        "tab_outros_voos": ["Outros voos", "More flights", "Other flights"],
        "show_more_flights": ["Mostrar mais voos", "Mostrar mais", "More flights", "Show more flights"],
        "booking_link": ["a[href*='/travel/flights/booking']", "[href*='/travel/flights/booking']"],
    }
    for candidate in candidates_map.get(entry["key"], []):
        test_entry = dict(entry, selector=candidate, kind=entry["kind"])
        if _count_elements(page, test_entry) > 0:
            logger.info("Python sugeriu: '%s'", candidate)
            return candidate
    return None


def _autocorrect_via_deepseek(page, entry: dict[str, Any]) -> str | None:
    """Usa Deepseek para sugerir correção de seletor."""
    import requests  # noqa: PLC0415
    api_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        logger.warning("DEEPSEEK_API_KEY não configurado — Deepseek desabilitado")
        return None
    dom_html = _capture_dom_html(page)
    if not dom_html:
        logger.error("Não foi possível capturar o DOM para Deepseek")
        return None
    prompt = _build_llm_prompt(entry, dom_html)
    try:
        resp = requests.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": os.getenv("DEEPSEEK_MODEL", "deepseek-v4-flash"),
                "messages": [
                    {"role": "system", "content": "Você é um especialista em automação web com Playwright. Seja conciso."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.3,
                "max_tokens": 150,
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        suggestion = data["choices"][0]["message"]["content"].strip().strip("\"'`")
        logger.info("Deepseek sugeriu: '%s'", suggestion)
        return suggestion or None
    except Exception as exc:
        logger.error("Erro na chamada Deepseek API: %s", exc)
        return None

# ── aplica correção nos arquivos-fonte ────────────────────────────────────────

def _apply_file_patch(
    old_selector: str,
    new_selector: str,
    files: list[str],
    dry_run: bool = False,
) -> list[str]:
    """
    Substitui old_selector por new_selector em cada arquivo listado.
    Cria backup .selector_bak antes de alterar.
    Retorna lista dos arquivos efetivamente alterados.
    """
    patched: list[str] = []
    escaped = re.escape(old_selector)

    # Padrão: seletor entre aspas simples ou duplas em código Python
    pattern = re.compile(rf'(["\']){escaped}(["\'])')

    for filename in files:
        filepath = BASE_DIR / filename
        if not filepath.exists():
            logger.debug("Arquivo não encontrado, pulando: %s", filename)
            continue

        original = filepath.read_text(encoding="utf-8")
        replaced = pattern.sub(
            lambda m: f"{m.group(1)}{new_selector}{m.group(2)}", original
        )

        if replaced == original:
            logger.debug("Nenhuma ocorrência encontrada em %s", filename)
            continue

        if dry_run:
            logger.info("[DRY-RUN] Correção seria aplicada em %s", filename)
            patched.append(filename)
            continue

        bak = filepath.with_suffix(filepath.suffix + ".selector_bak")
        bak.write_text(original, encoding="utf-8")
        filepath.write_text(replaced, encoding="utf-8")
        logger.info("Arquivo corrigido: %s (backup: %s)", filename, bak.name)
        patched.append(filename)

    return patched


# ── health check principal ────────────────────────────────────────────────────

def _approval_token(results: dict[str, Any]) -> str:
    payload = json.dumps(results.get("pending", []), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def _approval_markup(token: str) -> dict[str, Any]:
    return {
        "inline_keyboard": [[
            {"text": "✅ Aprovar correções", "callback_data": f"selectorhealth:approve:{token}"},
            {"text": "❌ Cancelar", "callback_data": f"selectorhealth:reject:{token}"},
        ]]
    }


def run_health_check(url: str | None = None, dry_run: bool = False, approve_token: str | None = None) -> dict[str, Any]:
    """
    Abre o Google Flights com Playwright, testa todos os seletores,
    tenta auto-correção nos críticos, grava log e notifica via Telegram.

    Returns:
        {
          "ok": bool,
          "timestamp": str,
          "working": [...],
          "broken": [...],
          "fixed": [...],
          "no_fix": [...],
        }
    """
    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    timestamp = now_local_iso()
    logger.info("=== Iniciando health check de seletores — %s ===", timestamp)

    if url is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
        base = os.getenv("GOOGLE_FLIGHTS_BASE_URL", "https://www.google.com/travel/flights")
        base = base.rstrip("/")
        if not base.endswith("/search"):
            base += "/search"
        hl = os.getenv("GOOGLE_HL", "pt-BR")
        gl = os.getenv("GOOGLE_GL", "BR")
        curr = os.getenv("GOOGLE_CURR", "BRL")
        url = f"{base}%sq=GRU+to+GIG+{date_str}+one+way&hl={hl}&gl={gl}&curr={curr}"

    logger.info("URL de teste: %s", url)

    results: dict[str, Any] = {
        "ok": True,
        "timestamp": timestamp,
        "working": [],
        "broken": [],
        "fixed": [],
        "no_fix": [],
        "pending": [],
        "approved": False,
    }

    session_dir = BASE_DIR / "google_session"

    try:
        with sync_playwright() as pw:
            ctx = pw.chromium.launch_persistent_context(
                str(session_dir),
                headless=os.getenv("GOOGLE_HEADLESS", "1") in {"1", "true", "yes"},
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-setuid-sandbox",
                ],
                locale="pt-BR",
                timezone_id="America/Sao_Paulo",
                viewport={"width": 1280, "height": 900},
            )

            page = ctx.pages[0] if ctx.pages else ctx.new_page()

            try:
                page.goto(url, timeout=50_000, wait_until="domcontentloaded")
            except Exception as exc:
                logger.error("Erro ao carregar página: %s", exc)
                results["ok"] = False
                results["no_fix"].append({"key": "page_load", "error": str(exc)})
                ctx.close()
                return results

            # Aguarda resultados aparecerem
            for wait_sel in ["[role='main']", "[role='listitem']", "text=Melhor opção"]:
                try:
                    if wait_sel.startswith("text="):
                        page.get_by_text(wait_sel[5:], exact=False).first.wait_for(timeout=8000)
                    else:
                        page.locator(wait_sel).first.wait_for(timeout=8000)
                    break
                except Exception:
                    pass

            time.sleep(2.5)

            # ── testa cada seletor ────────────────────────────────────────────
            for entry in SELECTOR_REGISTRY:
                key = entry["key"]
                sel = entry["selector"]
                count = _count_elements(page, entry)

                if count > 0:
                    results["working"].append({"key": key, "selector": sel, "count": count})
                    logger.info("✅  %-32s '%s' → %d elemento(s)", key, sel, count)
                    continue

                logger.warning("❌  %-32s '%s' → %s", key, sel,
                               "0 elementos" if count == 0 else "erro de sintaxe")
                results["ok"] = False
                broken_entry: dict[str, Any] = {
                    "key": key,
                    "selector": sel,
                    "description": entry["description"],
                    "files": entry["files"],
                    "critical": entry["critical"],
                    "kind": entry["kind"],
                }

                if not entry["critical"]:
                    results["broken"].append(broken_entry)
                    continue

                # ── tenta auto-correção na ordem: Python -> GPT -> Claude ───
                suggestion = None
                source = None
                for source_name, resolver in [
                    ("python", _autocorrect_via_python),
                    ("deepseek", _autocorrect_via_deepseek),
                ]:
                    logger.info("🔧  Tentando auto-correção para '%s' via %s…", key, source_name)
                    candidate = resolver(page, entry)
                    if not candidate:
                        continue
                    test_entry = dict(entry, selector=candidate)
                    new_count = _count_elements(page, test_entry)
                    if new_count > 0:
                        suggestion = candidate
                        source = source_name
                        break
                    logger.warning("⚠️  Sugestão de %s '%s' não encontrou elementos", source_name, candidate)

                if not suggestion:
                    broken_entry["suggested"] = None
                    results["no_fix"].append(broken_entry)
                    continue

                pending_entry = {
                    "key": key,
                    "old_selector": sel,
                    "new_selector": suggestion,
                    "files": entry["files"],
                    "new_count": new_count,
                    "description": entry["description"],
                    "source": source,
                }
                results["pending"].append(pending_entry)
                logger.info("📝 Correção pendente de aprovação: %s -> %s (%s)", sel, suggestion, source)

            ctx.close()

    except Exception:
        logger.exception("Erro inesperado no health check")
        results["ok"] = False

    if approve_token and results["pending"]:
        expected = _approval_token(results)
        if approve_token == expected:
            results["approved"] = True
            for item in results["pending"]:
                patched = _apply_file_patch(item["old_selector"], item["new_selector"], item["files"], dry_run=dry_run)
                results["fixed"].append({
                    **item,
                    "files_patched": patched,
                })
        else:
            logger.warning("Token de aprovação inválido: recebido=%s esperado=%s", approve_token, expected)
            results["ok"] = False

    # ── salva JSON do relatório ───────────────────────────────────────────────
    report_path = BASE_DIR / "logs" / "selector_health_last.json"
    try:
        report_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Relatório salvo em %s", report_path)
    except Exception as exc:
        logger.error("Erro ao salvar relatório: %s", exc)

    _notify_telegram(results, dry_run=dry_run)
    return results


# ── formata e envia notificação ───────────────────────────────────────────────

def _notify_telegram(results: dict[str, Any], dry_run: bool = False) -> None:
    ts = results["timestamp"]
    total = len(results["working"]) + len(results["broken"]) + len(results["fixed"]) + len(results["no_fix"])
    ok_count = len(results["working"]) + len(results["fixed"])
    dry_tag = " *(dry-run)*" if dry_run else ""

    lines: list[str] = [
        f"🔍 *Health Check de Seletores — Google Flights*{dry_tag}",
        f"📅 {ts}",
        "",
    ]

    if results["ok"] and not results["fixed"]:
        lines.append(f"✅ Todos os {total} seletores funcionando normalmente.")
    else:
        lines.append(f"📊 {ok_count}/{total} seletores OK")

    if results["pending"] and not results.get("approved"):
        token = _approval_token(results)
        lines.append("")
        lines.append("🟡 *Alterações detectadas, aguardando sua confirmação antes de mexer no código:*" )
        for f in results["pending"]:
            lines.append(
                f"  • `{f['key']}` via *{f['source']}*\n"
                f"    ❌ `{f['old_selector']}`\n"
                f"    ✅ `{f['new_selector']}`\n"
                f"    📁 {', '.join(f['files'])}"
            )
        lines.append("")
        lines.append(f"Se aprovar, rode: `python selector_health.py --approve {token}`")
        _send_telegram("\n".join(lines), reply_markup=_approval_markup(token))
        return

    if results["fixed"]:
        lines.append("")
        lines.append("🔧 *Corrigidos após aprovação:*")
        for f in results["fixed"]:
            files_str = ", ".join(f.get("files_patched", [])) or "nenhum arquivo"
            lines.append(
                f"  • `{f['key']}` via *{f.get('source', 'desconhecido')}*\n"
                f"    ❌ `{f['old_selector']}`\n"
                f"    ✅ `{f['new_selector']}`\n"
                f"    📁 {files_str}"
            )

    broken_all = results["broken"] + results["no_fix"]
    if broken_all:
        lines.append("")
        lines.append("❌ *Quebrados sem correção automática:*")
        for b in broken_all:
            suggested = b.get("suggested")
            crit = "⚠️ crítico" if b.get("critical") else "não-crítico"
            s = f"  • `{b['key']}` ({crit})\n    `{b['selector']}`"
            if suggested:
                s += f"\n    💡 Sugestão (inválida): `{suggested}`"
            lines.append(s)

    if not results["ok"]:
        lines.append("")
        lines.append("🚨 *Ação necessária: verifique os seletores acima.*")

    _send_telegram("\n".join(lines))


# ── entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Health check de seletores Playwright do Google Flights")
    parser.add_argument("--dry-run", action="store_true", help="Não altera arquivos, apenas reporta")
    parser.add_argument("--url", default=None, help="URL do Google Flights a testar")
    parser.add_argument("--approve", default=None, help="Token de aprovação para aplicar mudanças detectadas")
    args = parser.parse_args()

    results = run_health_check(url=args.url, dry_run=args.dry_run, approve_token=args.approve)

    broken = len(results["broken"]) + len(results["no_fix"])
    fixed = len(results["fixed"])
    total = len(SELECTOR_REGISTRY)

    print(f"\n{'=' * 50}")
    print(f"Resultado: {'OK' if results['ok'] else 'FALHA'}")
    print(f"Funcionando: {len(results['working'])}/{total}")
    print(f"Auto-corrigidos: {fixed}")
    print(f"Quebrados sem correção: {broken}")
    print(f"{'=' * 50}\n")

    sys.exit(0 if results["ok"] else 1)


if __name__ == "__main__":
    main()
