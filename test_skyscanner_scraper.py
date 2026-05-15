#!/opt/vooindo/.venv/bin/python3
"""
Teste ISOLADO de scraping no Skyscanner com suporte a 2Captcha.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

# 2Captcha
from twocaptcha import TwoCaptcha

DEFAULT_PROFILE_DIR = Path("/tmp/vooindo_skyscanner_test_profile")
DEFAULT_DEBUG_DIR = Path("/opt/vooindo/debug_dumps")


@dataclass
class PriceContext:
    price: float
    raw_price: str
    context: str


@dataclass
class FlightOption:
    option: int
    price: float
    raw_price: str
    airline: str | None
    depart_time: str | None
    arrive_time: str | None
    origin: str | None
    destination: str | None
    duration: str | None
    stops: str | None
    sponsored: bool
    offers_text: str | None
    context: str


@dataclass
class ScrapeResult:
    ok: bool
    site: str
    url: str
    origin: str
    destination: str
    depart: str
    return_date: str | None
    cheapest_price: float | None
    prices_found: int
    price_contexts: list[dict[str, Any]]
    flight_options: list[dict[str, Any]]
    cheapest_flight: dict[str, Any] | None
    title: str | None = None
    final_url: str | None = None
    screenshot: str | None = None
    html: str | None = None
    text_dump: str | None = None
    error: str | None = None
    notes: list[str] | None = None


def brl_to_float(raw: str) -> float:
    return float(raw.replace(".", "").replace(",", "."))


def yymmdd(date_yyyy_mm_dd: str) -> str:
    dt = datetime.strptime(date_yyyy_mm_dd, "%Y-%m-%d")
    return dt.strftime("%y%m%d")


def build_skyscanner_url(origin: str, destination: str, depart: str, return_date: str | None = None) -> str:
    origin = origin.strip().lower()
    destination = destination.strip().lower()
    depart_part = yymmdd(depart)
    return_part = f"/{yymmdd(return_date)}" if return_date else ""

    base = f"https://www.skyscanner.com.br/transport/flights/{origin}/{destination}/{depart_part}{return_part}/"
    query = {
        "adultsv2": "1",
        "cabinclass": "economy",
        "childrenv2": "",
        "currency": "BRL",
        "locale": "pt-BR",
        "market": "BR",
        "preferdirects": "false",
        "ref": "home",
        "rtn": "1" if return_date else "0",
    }
    return f"{base}?{urlencode(query)}"


def accept_cookies_if_present(page) -> None:
    candidates = [
        "Aceitar tudo",
        "Aceitar todos",
        "Aceito",
        "Accept all",
        "I agree",
        "Concordo",
    ]
    for label in candidates:
        try:
            page.get_by_role("button", name=re.compile(label, re.I)).click(timeout=1500)
            time.sleep(0.8)
            return
        except Exception:
            pass


def looks_like_captcha(page, body_text: str | None = None) -> bool:
    if "/captcha" in page.url or "captcha-v2" in page.url:
        return True
    if body_text is None:
        try:
            body_text = page.locator("body").inner_text(timeout=3000)
        except Exception:
            body_text = ""
    return bool(re.search(r"Are you a person or a robot|captcha|rob[oô]|pressione e segure|verification required", body_text, re.I))


def solve_captcha_with_2captcha(page, api_key: str, notes: list[str], timeout_seconds: int = 120) -> bool:
    """
    Resolve o captcha do Skyscanner usando 2Captcha.
    Retorna True se conseguiu resolver.
    """
    try:
        notes.append("2captcha: starting solver")
        
        # Inicializa o cliente
        solver = TwoCaptcha(api_key)
        
        # Tenta encontrar o sitekey do captcha
        # O Skyscanner usa PerimeterX - vamos tentar detectar
        sitekey = None
        
        # Procura por sitekey no HTML
        html = page.content()
        sitekey_match = re.search(r'data-sitekey=["\']([^"\']+)["\']', html)
        if sitekey_match:
            sitekey = sitekey_match.group(1)
            notes.append(f"2captcha: sitekey found = {sitekey}")
        
        # Se não achou sitekey, tenta padrão do PerimeterX
        if not sitekey:
            # Tenta encontrar no iframe ou script
            sitekey_match = re.search(r'sitekey["\']?\s*[=:]\s*["\']([^"\']+)["\']', html)
            if sitekey_match:
                sitekey = sitekey_match.group(1)
                notes.append(f"2captcha: alternative sitekey = {sitekey}")
        
        if not sitekey:
            notes.append("2captcha: sitekey not found, trying generic hcaptcha")
            # Tenta como hcaptcha genérico
            try:
                result = solver.hcaptcha(
                    page_url=page.url,
                    sitekey="00000000-0000-0000-0000-000000000000",  # genérico
                )
                notes.append(f"2captcha: generic hcaptcha result = {result}")
                # Aplica o token
                page.evaluate(f"""
                    document.querySelector('textarea[name="h-captcha-response"]').innerHTML = '{result["code"]}';
                    document.querySelector('form').submit();
                """)
                time.sleep(3)
                return True
            except Exception as e:
                notes.append(f"2captcha: generic hcaptcha failed = {e}")
        
        # Se tem sitekey, resolve com hcaptcha
        if sitekey:
            notes.append(f"2captcha: solving hcaptcha with sitekey {sitekey}")
            result = solver.hcaptcha(
                page_url=page.url,
                sitekey=sitekey,
            )
            notes.append(f"2captcha: solved = {result}")
            
            # Injeta o token na página
            try:
                # Tenta diferentes formas de aplicar o token
                page.evaluate(f"""
                    var textarea = document.querySelector('textarea[name="h-captcha-response"]');
                    if (textarea) {{
                        textarea.innerHTML = '{result["code"]}';
                        textarea.dispatchEvent(new Event('input', {{ bubbles: true }}));
                    }}
                    
                    var callback = window.hcaptcha && window.hcaptcha.setResponse;
                    if (callback) {{
                        callback('{result["code"]}');
                    }}
                """)
                time.sleep(2)
                
                # Aguarda o captcha ser validado
                page.wait_for_function("""
                    () => {
                        const element = document.querySelector('iframe[src*="hcaptcha"]');
                        return !element || element.style.display === 'none';
                    }
                """, timeout=10000)
                
                notes.append("2captcha: token injected successfully")
                return True
                
            except Exception as e:
                notes.append(f"2captcha: injection failed = {e}")
                return False
        
        # Se não encontrou sitekey, tenta Turnstile (Cloudflare)
        turnstile_match = re.search(r'data-turnstile-key=["\']([^"\']+)["\']', html)
        if turnstile_match:
            sitekey = turnstile_match.group(1)
            notes.append(f"2captcha: turnstile sitekey = {sitekey}")
            try:
                result = solver.turnstile(
                    page_url=page.url,
                    sitekey=sitekey,
                )
                notes.append(f"2captcha: turnstile solved = {result}")
                page.evaluate(f"""
                    document.querySelector('input[name="cf-turnstile-response"]').value = '{result["code"]}';
                """)
                return True
            except Exception as e:
                notes.append(f"2captcha: turnstile failed = {e}")
        
        notes.append("2captcha: no captcha type detected")
        return False
        
    except Exception as e:
        notes.append(f"2captcha: error = {type(e).__name__}: {e}")
        return False


def save_debug(page, debug_dir: Path, prefix: str) -> tuple[str | None, str | None, str | None]:
    debug_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = debug_dir / f"{prefix}_{stamp}"

    screenshot_path = str(base.with_suffix(".png"))
    html_path = str(base.with_suffix(".html"))
    text_path = str(base.with_suffix(".txt"))

    try:
        page.screenshot(path=screenshot_path, full_page=True)
    except Exception:
        screenshot_path = None

    try:
        Path(html_path).write_text(page.content(), encoding="utf-8")
    except Exception:
        html_path = None

    try:
        text = page.locator("body").inner_text(timeout=5000)
        Path(text_path).write_text(text, encoding="utf-8")
    except Exception:
        text_path = None

    return screenshot_path, html_path, text_path


def extract_price_contexts(text: str, limit: int = 25) -> list[PriceContext]:
    results: list[PriceContext] = []
    seen: set[tuple[str, str]] = set()

    for match in re.finditer(r"R\$\s*([\d\.]+(?:,\d{2})?)", text):
        raw = match.group(1)
        start = max(0, match.start() - 180)
        end = min(len(text), match.end() + 220)
        context = re.sub(r"\s+", " ", text[start:end]).strip()
        if any(term in context.lower() for term in ["hotel", "hotéis", "aluguel de carros"]):
            continue
        key = (raw, context[:120])
        if key in seen:
            continue
        seen.add(key)
        try:
            results.append(PriceContext(price=brl_to_float(raw), raw_price=f"R$ {raw}", context=context))
        except ValueError:
            continue

    results.sort(key=lambda item: item.price)
    return results[:limit]


def extract_flight_options(text: str, limit: int = 50) -> list[FlightOption]:
    normalized = text.replace("\xa0", " ")
    starts = list(re.finditer(r"Opção de voo\s+(\d+)\s*:", normalized, flags=re.I))
    options: list[FlightOption] = []
    seen: set[tuple[int, float, str | None, str | None]] = set()

    for idx, start_match in enumerate(starts):
        block_start = start_match.start()
        block_end = starts[idx + 1].start() if idx + 1 < len(starts) else len(normalized)
        block = normalized[block_start:block_end].strip()
        if not block:
            continue

        option_num = int(start_match.group(1))
        price_match = re.search(r"Preço total\s+R\$\s*([\d\.]+(?:,\d{2})?)", block, flags=re.I)
        if not price_match:
            price_match = re.search(r"(?:ofertas?|reserve com[^\n]*)\s+a partir de\s*R\$\s*([\d\.]+(?:,\d{2})?)", block, flags=re.I)
        if not price_match:
            continue

        raw_price = price_match.group(1).rstrip(".")
        try:
            price = brl_to_float(raw_price)
        except ValueError:
            continue

        airline = None
        airline_match = re.search(r"Voo com\s+([^\.\n]+)", block, flags=re.I)
        if airline_match:
            airline = airline_match.group(1).strip()
        elif "Patrocinado por" in block:
            sponsor_match = re.search(r"Patrocinado por\s+([^\.\n]+)", block, flags=re.I)
            if sponsor_match:
                airline = sponsor_match.group(1).strip()

        route_match = re.search(
            r"Partindo de\s+.+?\s+às\s+(\d{2}:\d{2}),\s+chegando em\s+.+?\s+às\s+(\d{2}:\d{2})",
            block,
            flags=re.I | re.S,
        )
        depart_time = route_match.group(1) if route_match else None
        arrive_time = route_match.group(2) if route_match else None

        compact = re.sub(r"\s+", " ", block)
        details_match = re.search(
            r"(\d{2}:\d{2})\s+([A-Z]{3})\s+([\dh\s]+?)\s+(Direto|\d+\s+paradas?|\d+\s+escalas?)\s+.*?(\d{2}:\d{2})\s+([A-Z]{3})",
            compact,
            flags=re.I,
        )
        origin = destination = duration = stops = None
        if details_match:
            depart_time = depart_time or details_match.group(1)
            origin = details_match.group(2).upper()
            duration = re.sub(r"\s+", " ", details_match.group(3)).strip()
            stops = details_match.group(4).strip()
            arrive_time = arrive_time or details_match.group(5)
            destination = details_match.group(6).upper()

        offers_text = None
        offers_match = re.search(r"((?:\d+\s+ofertas?|Reserve com[^\n]+)\s+a partir de\s+R\$\s*[\d\.]+(?:,\d{2})?)", block, flags=re.I)
        if offers_match:
            offers_text = re.sub(r"\s+", " ", offers_match.group(1)).strip()

        sponsored = "patrocinado" in block.lower()
        key = (option_num, price, depart_time, arrive_time)
        if key in seen:
            continue
        seen.add(key)
        options.append(FlightOption(
            option=option_num,
            price=price,
            raw_price=f"R$ {raw_price}",
            airline=airline,
            depart_time=depart_time,
            arrive_time=arrive_time,
            origin=origin,
            destination=destination,
            duration=duration,
            stops=stops,
            sponsored=sponsored,
            offers_text=offers_text,
            context=compact[:900],
        ))

    options.sort(key=lambda item: (item.price, item.sponsored, item.option))
    return options[:limit]


def scrape(args: argparse.Namespace) -> ScrapeResult:
    url = args.url or build_skyscanner_url(args.origin, args.destination, args.depart, args.return_date)
    notes: list[str] = ["isolated_test_script", f"profile={args.profile_dir}"]

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(args.profile_dir),
            headless=args.headless,
            slow_mo=args.slow,
            locale="pt-BR",
            timezone_id="America/Porto_Velho",
            viewport={"width": 1366, "height": 900},
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        page = context.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=args.timeout_ms)
            accept_cookies_if_present(page)
            
            try:
                page.wait_for_load_state("networkidle", timeout=min(args.timeout_ms, 30000))
            except PlaywrightTimeoutError:
                notes.append("networkidle_timeout")
            
            # Verifica se tem captcha e resolve com 2Captcha
            if looks_like_captcha(page) and args.twocaptcha_key:
                notes.append("captcha_detected_using_2captcha")
                
                for attempt in range(args.captcha_max_attempts):
                    if solve_captcha_with_2captcha(page, args.twocaptcha_key, notes):
                        notes.append(f"captcha_solved_on_attempt_{attempt + 1}")
                        # Aguarda a página recarregar
                        time.sleep(3)
                        try:
                            page.wait_for_load_state("networkidle", timeout=15000)
                        except Exception:
                            pass
                        break
                    else:
                        notes.append(f"captcha_attempt_{attempt + 1}_failed")
                        time.sleep(5)
                else:
                    notes.append("all_captcha_attempts_failed")
            
            # Scroll para carregar resultados
            for _ in range(args.scrolls):
                page.mouse.wheel(0, 900)
                time.sleep(args.settle)
            
            try:
                page.wait_for_selector("text=/R\\$\\s*[0-9]/", timeout=10000)
            except PlaywrightTimeoutError:
                notes.append("price_selector_timeout")
            
            body_text = page.locator("body").inner_text(timeout=10000)
            
            flight_options = extract_flight_options(body_text, limit=args.limit)
            prices = extract_price_contexts(body_text, limit=args.limit)
            screenshot, html, text_dump = save_debug(page, args.debug_dir, "skyscanner_test")
            
            cheapest_flight = asdict(flight_options[0]) if flight_options else None
            cheapest = flight_options[0].price if flight_options else (prices[0].price if prices else None)
            
            ok = bool(flight_options or prices) and not looks_like_captcha(page)
            
            return ScrapeResult(
                ok=ok,
                site="skyscanner",
                url=url,
                origin=args.origin.upper(),
                destination=args.destination.upper(),
                depart=args.depart,
                return_date=args.return_date,
                cheapest_price=cheapest,
                prices_found=len(flight_options) if flight_options else len(prices),
                price_contexts=[asdict(p) for p in prices],
                flight_options=[asdict(item) for item in flight_options],
                cheapest_flight=cheapest_flight,
                title=page.title(),
                final_url=page.url,
                screenshot=screenshot,
                html=html,
                text_dump=text_dump,
                notes=notes,
            )
        except Exception as exc:
            screenshot, html, text_dump = save_debug(page, args.debug_dir, "skyscanner_error")
            return ScrapeResult(
                ok=False,
                site="skyscanner",
                url=url,
                origin=args.origin.upper(),
                destination=args.destination.upper(),
                depart=args.depart,
                return_date=args.return_date,
                cheapest_price=None,
                prices_found=0,
                price_contexts=[],
                flight_options=[],
                cheapest_flight=None,
                title=None,
                final_url=page.url if page else None,
                screenshot=screenshot,
                html=html,
                text_dump=text_dump,
                error=f"{type(exc).__name__}: {exc}",
                notes=notes,
            )
        finally:
            context.close()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Teste isolado de scraping Skyscanner com 2Captcha")
    parser.add_argument("--origin", default="PVH", help="IATA origem, ex: PVH")
    parser.add_argument("--destination", default="FOR", help="IATA destino, ex: FOR")
    parser.add_argument("--depart", default="2026-06-04", help="Data ida YYYY-MM-DD")
    parser.add_argument("--return-date", default=None, help="Data volta YYYY-MM-DD")
    parser.add_argument("--url", default=None, help="URL Skyscanner pronta, opcional")
    parser.add_argument("--profile-dir", type=Path, default=DEFAULT_PROFILE_DIR)
    parser.add_argument("--debug-dir", type=Path, default=DEFAULT_DEBUG_DIR)
    parser.add_argument("--timeout-ms", type=int, default=60000)
    parser.add_argument("--settle", type=float, default=2.0)
    parser.add_argument("--scrolls", type=int, default=4)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--headed", action="store_true", help="Abre navegador visível")
    parser.add_argument("--headless", action="store_true", default=True, help="Modo headless (padrão)")
    parser.add_argument("--slow", type=int, default=0, help="Slow motion em ms")
    
    # 2Captcha configuration
    parser.add_argument("--twocaptcha-key", type=str, default=None, help="API key do 2Captcha")
    parser.add_argument("--captcha-max-attempts", type=int, default=3, help="Máximo de tentativas para resolver captcha")
    
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    
    if not args.twocaptcha_key:
        print("ERRO: Você precisa fornecer a chave do 2Captcha com --twocaptcha-key", file=sys.stderr)
        print("Registre-se em https://2captcha.com e obtenha sua chave", file=sys.stderr)
        return 1
    
    result = scrape(args)
    print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    return 0 if result.ok else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
