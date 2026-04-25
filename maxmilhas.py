from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime
import json
import os
import re
import time
import traceback
from urllib.parse import urlparse
from config import now_local_iso

def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


ORIGEM = os.getenv("MAXMILHAS_ORIGEM", "PVH")
DESTINO = os.getenv("MAXMILHAS_DESTINO", "FOR")
DATA_IDA_ISO = os.getenv("MAXMILHAS_DATA_IDA_ISO", "2026-06-05")
URL = os.getenv("MAXMILHAS_URL", "").strip()
MAXMILHAS_SEARCH_BASE_URL = os.getenv("MAXMILHAS_SEARCH_BASE_URL", "").strip().rstrip("/")

HEADLESS = _env_bool("MAXMILHAS_HEADLESS", True)
MAX_TENTATIVAS = int(os.getenv("MAXMILHAS_MAX_TENTATIVAS", "1"))
TIMEOUT_PADRAO = int(os.getenv("MAXMILHAS_TIMEOUT_PADRAO_MS", "30000"))
SALVAR_DEBUG = _env_bool("MAXMILHAS_SALVAR_DEBUG", False)
LIMPAR_DEBUGS_ANTIGOS = _env_bool("MAXMILHAS_LIMPAR_DEBUGS_ANTIGOS", False)
BUSCA_RESULT_TIMEOUT = int(os.getenv("MAXMILHAS_BUSCA_RESULT_TIMEOUT_MS", "20000"))
MAX_ROUTAS_SEGUNDOS = int(os.getenv("MAXMILHAS_MAX_ROTAS_SEGUNDOS", "45"))
RESULT_SELECTORS = [
    "div[data-testid='flight-card']",
    "div[data-testid='flight-list-item']",
    "div[class*='flight-card']",
    "div[class*='resultado-voo']",
]


def log(msg: str):
    print(f"[LOG] {msg}", flush=True)


def warn(msg: str):
    print(f"[WARN] {msg}", flush=True)


def err(msg: str):
    print(f"[ERRO] {msg}", flush=True)


def salvar_json(dados: dict, arquivo="maxmilhas_resultado.json"):
    with open(arquivo, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    log(f"JSON salvo em: {arquivo}")


def tirar_screenshot(page, nome: str):
    try:
        page.screenshot(path=nome, full_page=True)
        log(f"Screenshot salva: {nome}")
    except Exception as e:
        warn(f"Falha ao salvar screenshot {nome}: {e}")


def salvar_html(page, nome="debug_pagina.html"):
    try:
        html = page.content()
        with open(nome, "w", encoding="utf-8") as f:
            f.write(html)
        log(f"HTML salvo em: {nome}")
    except Exception as e:
        warn(f"Falha ao salvar HTML: {e}")


def salvar_debug(page, nome_base: str, salvar_png=True, salvar_pagina_html=False):
    if not SALVAR_DEBUG:
        return

    if salvar_png:
        tirar_screenshot(page, f"{nome_base}.png")
    if salvar_pagina_html:
        salvar_html(page, f"{nome_base}.html")


def limpar_arquivos_debug():
    if not LIMPAR_DEBUGS_ANTIGOS:
        return

    for nome in os.listdir("."):
        if not nome.startswith("debug_") or not os.path.isfile(nome):
            continue
        try:
            os.remove(nome)
            log(f"Arquivo de debug removido: {nome}")
        except Exception as e:
            warn(f"Falha removendo arquivo de debug {nome}: {e}")


def montar_parametros_consulta(origem: str, destino: str, data_ida_iso: str) -> dict:
    return {
        "origem": (origem or ORIGEM).upper(),
        "destino": (destino or DESTINO).upper(),
        "data_ida": data_ida_iso or DATA_IDA_ISO,
        "url_base": URL,
    }


def normalizar_preco(texto: str):
    m = re.search(r"R\$\s*([\d\.\,]+)", texto)
    if not m:
        return None
    valor = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(valor)
    except ValueError:
        return None


def filtrar_precos_parcelados(precos: list[float]) -> list[float]:
    if not precos:
        return []

    candidatos = sorted(set(round(preco, 2) for preco in precos if preco is not None))
    totais = set(candidatos)
    filtrados = []

    for preco in candidatos:
        eh_parcela = False
        for parcelas in range(2, 13):
            total_estimado = round(preco * parcelas, 2)
            if total_estimado in totais:
                eh_parcela = True
                break
        if not eh_parcela:
            filtrados.append(preco)

    filtrados = filtrados or candidatos

    # Fallback para parcelas arredondadas: remove o menor valor quando ele é um
    # outlier muito abaixo do próximo preço disponível.
    while len(filtrados) >= 2:
        menor = filtrados[0]
        proximo = filtrados[1]
        if menor < (proximo * 0.45):
            filtrados.pop(0)
            continue
        break

    return filtrados or candidatos


def fechar_popups(page):
    try:
        page.evaluate("""
        () => {
          const ids = [
            'dengage-blocked-push-info-container',
            'dengage-push-perm-slideup',
            'onesignal-slidedown-container'
          ];
          for (const id of ids) {
            const el = document.getElementById(id);
            if (el) el.remove();
          }
          const selectors = [
            '._dn_blocked_info-container',
            '._dn_blocked_info-background',
            '#dengage-blocked-push-info-container',
            '.modal-backdrop',
            '[data-testid="modal-overlay"]'
          ];
          for (const sel of selectors) {
            document.querySelectorAll(sel).forEach((el) => {
              el.style.display = 'none';
              el.style.pointerEvents = 'none';
              el.remove?.();
            });
          }
        }
        """)
    except Exception:
        pass

    seletores = [
        "button:has-text('Aceitar')",
        "button:has-text('Entendi')",
        "button:has-text('Fechar')",
        "button:has-text('Continuar')",
        "[aria-label='Fechar']",
        "[aria-label='Close']",
    ]
    for seletor in seletores:
        try:
            loc = page.locator(seletor).first
            if loc.is_visible(timeout=1200):
                log(f"Fechando popup: {seletor}")
                loc.click(timeout=3000)
                page.wait_for_timeout(1200)
                return True
        except Exception:
            pass
    return False


def log_duration(label: str, start: float):
    elapsed = time.perf_counter() - start
    log(f"{label}: {elapsed:.1f}s")


def aguardar_resultados(page, timeout: int = BUSCA_RESULT_TIMEOUT) -> bool:
    for seletor in RESULT_SELECTORS:
        try:
            page.wait_for_selector(seletor, timeout=timeout)
            log(f"Resultado detectado via seletor {seletor}")
            return True
        except Exception:
            continue

    try:
        page.wait_for_function(
            "() => /voos encontrados|resultados|ordenar|filtrar/i.test(document.body.innerText)",
            timeout=timeout,
        )
        log("Resultado detectado pelo texto da página")
        return True
    except Exception:
        log("Não encontrou resultados por texto")
        return False


def navegar_para_busca(page, origem: str, destino: str, data_ida_iso: str) -> bool:
    url_busca = construir_url_busca(origem, destino, data_ida_iso)
    log(f"Navegando direto para URL de busca: {url_busca}")
    inicio = time.perf_counter()
    page.goto(url_busca, wait_until="domcontentloaded", timeout=60000)
    log_duration("Carregamento da busca", inicio)
    try:
        page.wait_for_timeout(1500)
    except Exception:
        pass
    return aguardar_resultados(page)


def obter_inputs_texto_visiveis(page):
    resultado = []
    inputs = page.locator("input:visible")
    qtd = inputs.count()
    log(f"Inputs visíveis totais: {qtd}")

    for i in range(qtd):
        try:
            inp = inputs.nth(i)
            tipo = (inp.get_attribute("type") or "").lower()
            editable = inp.is_editable()
            placeholder = inp.get_attribute("placeholder")
            aria = inp.get_attribute("aria-label")
            nome = (inp.get_attribute("name") or "").lower()
            elem_id = (inp.get_attribute("id") or "").lower()

            info = {
                "indice_visivel": i,
                "type": tipo,
                "editable": editable,
                "placeholder": placeholder,
                "aria_label": aria,
                "name": nome,
                "id": elem_id,
            }
            log(f"Input visível {i}: {info}")

            eh_newsletter = any(token in f"{nome} {elem_id}" for token in ["newsletter", "email"])
            tipo_invalido = tipo in {"checkbox", "radio", "submit", "button", "hidden"}
            if editable and not tipo_invalido and not eh_newsletter:
                resultado.append((i, inp))
        except Exception as e:
            warn(f"Erro inspecionando input {i}: {e}")

    log(f"Inputs de texto candidatos: {len(resultado)}")
    return resultado


def preencher_input(inp, valor: str, nome: str, page):
    inp.click(timeout=5000)
    page.wait_for_timeout(300)
    try:
        inp.press("Control+A")
        page.wait_for_timeout(100)
        inp.press("Backspace")
    except Exception:
        pass
    page.wait_for_timeout(100)
    inp.fill(valor, timeout=5000)
    page.wait_for_timeout(1200)

    try:
        page.keyboard.press("ArrowDown")
        page.wait_for_timeout(250)
        page.keyboard.press("Enter")
        page.wait_for_timeout(1000)
    except Exception:
        pass

    log(f"{nome} preenchido com '{valor}'")


def preencher_origem_destino_por_id(page, seletor: str, valor: str, nome: str):
    inp = page.locator(f"{seletor}:visible").first
    inp.wait_for(state="visible", timeout=5000)
    preencher_input(inp, valor, nome, page)


def preencher_data_ida(page, data_iso: str):
    data_br = datetime.strptime(data_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    target = datetime.strptime(data_iso, "%Y-%m-%d")
    meses = {
        1: "janeiro", 2: "fevereiro", 3: "março", 4: "abril",
        5: "maio", 6: "junho", 7: "julho", 8: "agosto",
        9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro",
    }
    aria_data = f"{target.day} de {meses[target.month]} de {target.year}"

    try:
        gatilhos = [
            "input#outboundDate:visible",
            "input#outbounddate:visible",
            "label:has-text('Ida')",
            "label:has-text('Data da ida')",
        ]
        aberto = False
        for sel in gatilhos:
            try:
                page.locator(sel).first.click(timeout=3000, force=True)
                page.wait_for_timeout(600)
                aberto = True
                break
            except Exception:
                pass
        if not aberto:
            page.get_by_text("Ida", exact=False).first.click(timeout=3000)
            page.wait_for_timeout(600)

        for _ in range(12):
            dia = page.locator(f"abbr[aria-label='{aria_data}']").first
            if dia.count() > 0:
                dia.click(timeout=3000)
                page.wait_for_timeout(500)
                try:
                    page.get_by_role("button", name="Continuar").click(timeout=2000)
                except Exception:
                    pass
                log(f"data preenchido com '{data_br}' via calendario")
                return True

            avancar = page.get_by_role("button", name=re.compile("Próximo mês", re.I))
            if avancar.count() == 0:
                break
            avancar.first.click(timeout=3000)
            page.wait_for_timeout(400)
    except Exception as e:
        warn(f"Falha preenchendo data via calendario: {e}")

    try:
        ok = page.evaluate(
            """
            ({ value }) => {
              const selectors = ['input#outboundDate', 'input#outbounddate'];
              for (const selector of selectors) {
                const inputs = Array.from(document.querySelectorAll(selector));
                const input = inputs[inputs.length - 1];
                if (!input) continue;
                input.removeAttribute('readonly');
                input.removeAttribute('disabled');
                const nativeSetter = Object.getOwnPropertyDescriptor(
                  window.HTMLInputElement.prototype,
                  'value'
                )?.set;
                nativeSetter ? nativeSetter.call(input, value) : input.value = value;
                input.setAttribute('value', value);
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.dispatchEvent(new Event('change', { bubbles: true }));
                input.dispatchEvent(new Event('blur', { bubbles: true }));
                return true;
              }
              return false;
            }
            """,
            {"value": data_br},
        )
        if ok:
            log(f"data preenchido com '{data_br}' via JS fallback")
            return True
    except Exception as e:
        warn(f"Falha preenchendo data via JS fallback: {e}")

    return False


def preencher_campos(page, origem: str, destino: str, data_ida_iso: str):
    try:
        obter_inputs_texto_visiveis(page)

        preencher_origem_destino_por_id(page, "#from", origem, "origem")
        page.wait_for_timeout(800)

        preencher_origem_destino_por_id(page, "#to", destino, "destino")
        page.wait_for_timeout(800)

        if not preencher_data_ida(page, data_ida_iso):
            return {
                "ok": False,
                "motivo": "Não conseguiu preencher a data"
            }

        return {
            "ok": True,
            "motivo": None
        }

    except Exception as e:
        return {
            "ok": False,
            "motivo": f"Falha ao preencher campos: {e}"
        }


def pagina_tem_resultado(page, url_base: str = URL):
    try:
        texto = page.locator("body").inner_text(timeout=10000).lower()

        sinais_resultado = [
            "voos encontrados",
            "resultados",
            "companhia aérea",
            "sem escalas",
            "1 escala",
            "2 escalas",
            "operado por",
            "ordenar",
            "filtrar",
        ]

        url_mudou = page.url != url_base
        encontrou_sinal = any(s in texto for s in sinais_resultado)
        encontrou_preco = bool(re.search(r"R\$\s*[\d\.\,]+", texto))

        return url_mudou or (encontrou_sinal and encontrou_preco)
    except Exception:
        return False


def construir_url_busca(origem: str, destino: str, data_ida_iso: str):
    if MAXMILHAS_SEARCH_BASE_URL:
        base = MAXMILHAS_SEARCH_BASE_URL
    else:
        parsed = urlparse(URL)
        if not parsed.scheme or not parsed.netloc:
            raise RuntimeError("Defina MAXMILHAS_SEARCH_BASE_URL no .env com uma URL válida.")
        base = f"{parsed.scheme}://{parsed.netloc}"
    return f"{base}/busca-passagens-aereas/OW/{origem}/{destino}/{data_ida_iso}/1/0/0/EC"


def clicar_buscar(page, origem: str, destino: str, data_ida_iso: str, url_base: str = URL):
    log("Tentando disparar busca...")
    fechar_popups(page)
    page.wait_for_timeout(500)

    try:
        candidatos = obter_inputs_texto_visiveis(page)
        if candidatos:
            ultimo_input = candidatos[-1][1]
            ultimo_input.click(timeout=3000)
            page.wait_for_timeout(300)
            ultimo_input.press("Enter")
            page.wait_for_timeout(4000)

            if page.url != url_base:
                log("Busca disparada com Enter no último input.")
                return True

            if pagina_tem_resultado(page, url_base=url_base):
                log("Busca disparada com Enter no último input.")
                return True
    except Exception as e:
        warn(f"Falha no Enter do último input: {e}")

    try:
        page.keyboard.press("Enter")
        page.wait_for_timeout(4000)

        if page.url != url_base or pagina_tem_resultado(page, url_base=url_base):
            log("Busca disparada com Enter global.")
            return True
    except Exception as e:
        warn(f"Falha no Enter global: {e}")

    seletores = [
        "button[type='submit']",
        "button:has-text('Pesquisar')",
        "button:has-text('Buscar')",
        "[role='button']:has-text('Pesquisar')",
        "[role='button']:has-text('Buscar')",
        "input[type='submit']",
    ]

    for seletor in seletores:
        try:
            loc = page.locator(seletor).first
            if loc.is_visible(timeout=1500):
                log(f"Clicando botão/role: {seletor}")
                try:
                    loc.click(timeout=5000)
                except Exception:
                    fechar_popups(page)
                    loc.click(timeout=5000, force=True)
                page.wait_for_timeout(5000)

                if page.url != url_base or pagina_tem_resultado(page, url_base=url_base):
                    return True
        except Exception:
            pass

    textos = ["Pesquisar", "Buscar", "Buscar passagens", "Ver voos"]
    for txt in textos:
        try:
            loc = page.get_by_text(txt, exact=False).first
            if loc.is_visible(timeout=1500):
                log(f"Clicando texto visível: {txt}")
                try:
                    loc.click(timeout=5000)
                except Exception:
                    fechar_popups(page)
                    loc.click(timeout=5000, force=True)
                page.wait_for_timeout(5000)

                if page.url != url_base or pagina_tem_resultado(page, url_base=url_base):
                    return True
        except Exception:
            pass

    try:
        houve_submit = page.evaluate("""
        () => {
          const forms = Array.from(document.querySelectorAll('form')).filter((form) => {
            const text = (form.innerText || '').toLowerCase();
            const html = (form.outerHTML || '').toLowerCase();
            return text.includes('pesquisar') || text.includes('buscar') || html.includes('passagens');
          });
          if (!forms.length) return false;
          forms[0].requestSubmit ? forms[0].requestSubmit() : forms[0].submit();
          return true;
        }
        """)
        if houve_submit:
            log("Tentado submit via JS no primeiro form.")
            page.wait_for_timeout(5000)

            if page.url != URL or pagina_tem_resultado(page):
                return True
    except Exception as e:
        warn(f"Falha no submit JS: {e}")

    try:
        url_busca = construir_url_busca(origem, destino, data_ida_iso)
        log(f"Navegando direto para URL de busca: {url_busca}")
        page.goto(url_busca, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(8000)
        if page.url != url_base or pagina_tem_resultado(page, url_base=url_base):
            return True
    except Exception as e:
        warn(f"Falha navegando direto para a URL de busca: {e}")

    return False


def extrair_precos(page):
    try:
        texto = page.locator("body").inner_text(timeout=10000)
        encontrados = re.findall(r"R\$\s*[\d\.\,]+", texto)
        precos = [normalizar_preco(x) for x in encontrados]
        precos = [p for p in precos if p is not None and p > 200]
        precos = filtrar_precos_parcelados(precos)
        return sorted(set(precos))
    except Exception as e:
        warn(f"Falha ao extrair preços: {e}")
        return []


def criar_browser(p, headless: bool = HEADLESS, timeout_padrao: int = TIMEOUT_PADRAO):
    return p.chromium.launch(
        headless=headless,
        timeout=timeout_padrao,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
        ],
    )


def criar_context(browser):
    return browser.new_context(
        locale="pt-BR",
        timezone_id="America/Porto_Velho",
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1366, "height": 768},
    )


def _executar_uma_tentativa_com_playwright(
    p,
    tentativa: int,
    origem: str,
    destino: str,
    data_ida_iso: str,
    *,
    headless: bool = HEADLESS,
    timeout_padrao: int = TIMEOUT_PADRAO,
):
    browser = None
    context = None
    params = montar_parametros_consulta(origem, destino, data_ida_iso)

    try:
        log(f"=== Tentativa {tentativa}/{MAX_TENTATIVAS} ===")
        browser = criar_browser(p, headless=headless, timeout_padrao=timeout_padrao)
        context = criar_context(browser)
        page = context.new_page()

        # Caminho rápido: a URL de busca já contém todos os parâmetros necessários.
        # Isso evita abrir a home, preencher formulário e submeter a busca em toda rota.
        buscou = navegar_para_busca(
            page,
            params["origem"],
            params["destino"],
            params["data_ida"],
        )
        if not buscou:
            page.goto(params["url_base"], wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(1500)

            log(f"Título: {page.title()}")
            log(f"URL atual: {page.url}")

            fechar_popups(page)
            page.wait_for_timeout(600)

            salvar_debug(page, f"debug_inicio_t{tentativa}")

            preenchimento = preencher_campos(page, params["origem"], params["destino"], params["data_ida"])
            if not preenchimento["ok"]:
                salvar_debug(page, f"debug_campos_falha_t{tentativa}", salvar_pagina_html=True)
                return {
                    "ok": False,
                    "motivo": preenchimento["motivo"],
                    "origem": params["origem"],
                    "destino": params["destino"],
                    "data_ida": params["data_ida"],
                    "url_final": page.url,
                    "timestamp": now_local_iso(sep="T"),
                }

            salvar_debug(page, f"debug_campos_t{tentativa}")

            buscou = clicar_buscar(
                page,
                params["origem"],
                params["destino"],
                params["data_ida"],
            )
            if buscou:
                buscou = aguardar_resultados(page)

        if not buscou:
            salvar_debug(page, f"debug_sem_busca_t{tentativa}", salvar_pagina_html=True)
            return {
                "ok": False,
                "motivo": "Não encontrou resultados válidos",
                "origem": params["origem"],
                "destino": params["destino"],
                "data_ida": params["data_ida"],
                "url_final": page.url,
                "timestamp": now_local_iso(sep="T"),
            }

        salvar_debug(page, f"debug_resultados_t{tentativa}", salvar_pagina_html=True)

        precos = extrair_precos(page)

        return {
            "ok": len(precos) > 0,
            "motivo": None if precos else "Nenhum preço válido encontrado",
            "origem": params["origem"],
            "destino": params["destino"],
            "data_ida": params["data_ida"],
            "url_final": page.url,
            "precos_encontrados": precos,
            "menor_preco": min(precos) if precos else None,
            "timestamp": now_local_iso(sep="T"),
        }

    except PlaywrightTimeoutError as e:
        return {
            "ok": False,
            "motivo": f"Timeout do Playwright: {e}",
            "timestamp": now_local_iso(sep="T"),
        }
    except Exception as e:
        traceback.print_exc()
        return {
            "ok": False,
            "motivo": f"Erro inesperado: {e}",
            "timestamp": now_local_iso(sep="T"),
        }
    finally:
        try:
            if context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass


def executar_uma_tentativa(
    tentativa: int,
    origem: str = ORIGEM,
    destino: str = DESTINO,
    data_ida_iso: str = DATA_IDA_ISO,
    *,
    playwright=None,
    headless: bool = HEADLESS,
    timeout_padrao: int = TIMEOUT_PADRAO,
):
    if playwright is not None:
        return _executar_uma_tentativa_com_playwright(
            playwright,
            tentativa,
            origem,
            destino,
            data_ida_iso,
            headless=headless,
            timeout_padrao=timeout_padrao,
        )

    with sync_playwright() as p:
        return _executar_uma_tentativa_com_playwright(
            p,
            tentativa,
            origem,
            destino,
            data_ida_iso,
            headless=headless,
            timeout_padrao=timeout_padrao,
        )


def buscar_menor_preco(
    origem: str = ORIGEM,
    destino: str = DESTINO,
    data_ida_iso: str = DATA_IDA_ISO,
    *,
    playwright=None,
    salvar_arquivo_json: bool = True,
    max_tentativas: int = MAX_TENTATIVAS,
):
    ultimo_resultado = None

    limpar_arquivos_debug()

    total_tentativas = max(1, int(max_tentativas))
    inicio_rota = time.perf_counter()

    for tentativa in range(1, total_tentativas + 1):
        resultado = executar_uma_tentativa(
            tentativa,
            origem=origem,
            destino=destino,
            data_ida_iso=data_ida_iso,
            playwright=playwright,
        )
        ultimo_resultado = resultado

        if resultado.get("ok"):
            if salvar_arquivo_json:
                salvar_json(resultado)
            if playwright is None:
                print("\n========== RESULTADO ==========")
                print(f"Rota: {resultado['origem']} -> {resultado['destino']}")
                print(f"Data: {resultado['data_ida']}")
                print(f"Menor preço: R$ {resultado['menor_preco']:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
                print(f"Preços encontrados: {resultado['precos_encontrados']}")
                print(f"URL final: {resultado['url_final']}")
                print("===============================\n")
            return resultado

        warn(f"Tentativa {tentativa} falhou: {resultado.get('motivo', 'Sem detalhe')}")
        if tentativa < total_tentativas:
            time.sleep(3)
        if time.perf_counter() - inicio_rota > MAX_ROUTAS_SEGUNDOS:
            warn("Tempo máximo por rota excedido, abortando tentativas adicionais.")
            break

    if salvar_arquivo_json:
        salvar_json(ultimo_resultado or {"ok": False, "motivo": "Sem resultado"})
    if playwright is None:
        print("\nFalhou após todas as tentativas.\n")
    return ultimo_resultado


if __name__ == "__main__":
    buscar_menor_preco()
