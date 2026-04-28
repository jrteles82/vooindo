"""
Módulo de assistente IA para sugestões inteligentes de voos.
Usa Deepseek API para gerar dicas contextuais.

Gera uma mensagem completa no formato:
✈️ [Origem -> Destino (data)](url)
Companhia • R$ Preço
📌 Dica personalizada
"""
import os
import time
from datetime import datetime
from typing import Optional

import requests

from app_logging import get_logger

logger = get_logger('ai_assistant')

DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY', '')
DEEPSEEK_MODEL = os.getenv('DEEPSEEK_MODEL', 'deepseek-v4-flash')
DEEPSEEK_BASE_URL = 'https://api.deepseek.com/v1'

# Cache em memória
_cache = {}
_CACHE_TTL = 3600


def _city_name(code: str) -> str:
    """Retorna nome da cidade a partir do código do aeroporto."""
    cities = {
        'AEP': 'Buenos Aires', 'PVH': 'Porto Velho (RO)', 'MIA': 'Miami',
        'GRU': 'São Paulo (GRU)', 'CGH': 'São Paulo (CGH)', 'VCP': 'Campinas (VCP)',
        'GIG': 'Rio de Janeiro (GIG)', 'SDU': 'Rio de Janeiro (SDU)',
        'BSB': 'Brasília', 'FOR': 'Fortaleza', 'NAT': 'Natal',
        'REC': 'Recife', 'SSA': 'Salvador', 'VIX': 'Vitória',
        'THE': 'Teresina', 'SLZ': 'São Luís', 'BEL': 'Belém',
        'MAO': 'Manaus', 'CWB': 'Curitiba', 'FLN': 'Florianópolis',
        'POA': 'Porto Alegre', 'CNF': 'Belo Horizonte (CNF)',
        'USH': 'Ushuaia', 'BUE': 'Buenos Aires',
    }
    return cities.get(code.upper(), code)


def _build_ai_prompt(rows: list[dict]) -> str:
    """Monta prompt pra IA: só precisa gerar as dicas."""
    lines = []
    lines.append("Você é um especialista em passagens aéreas. Analise as rotas abaixo.")
    lines.append("")
    lines.append("REGRAS:")
    lines.append("- Para CADA rota, gere APENAS uma linha:")
    lines.append("  📌 Dica personalizada (baseada em preço, data, companhia)")
    lines.append("- Se o preço for ótimo (abaixo da média): 💰 Compre agora!")
    lines.append("- Se for razoável: 📌 Preço justo, pode monitorar")
    lines.append("- Se for caro (acima da média): 📌 Preço elevado, sugiro esperar")
    lines.append("- Dicas curtas e diretas, 1 linha cada")
    lines.append("- NÃO repita o nome da rota, companhia ou preço")
    lines.append("- NUNCA mencione agências, milhas ou programas de fidelidade")
    lines.append("- Responda APENAS com as linhas de dica, uma por rota, sem numeração")
    lines.append("")
    lines.append("ROTAS:")

    for i, row in enumerate(rows, 1):
        origin = row.get('origin', '???').upper()
        dest = row.get('destination', '???').upper()
        date = row.get('outbound_date', '')
        try:
            date_fmt = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%y')
        except (ValueError, TypeError):
            date_fmt = date
        vendor = row.get('best_vendor') or row.get('vendor') or '—'
        price = row.get('best_vendor_price') or row.get('price')
        price_str = f'R$ {price:,.0f}'.replace(',', '.') if price else 'N/D'
        lines.append(f"Rota {i}: {origin} -> {dest} em {date_fmt}")
        lines.append(f"  {vendor} • {price_str}")

    return '\n'.join(lines)


def _call_deepseek(prompt: str) -> Optional[str]:
    """Chama a API do Deepseek."""
    if not DEEPSEEK_API_KEY:
        logger.warning('DEEPSEEK_API_KEY não configurada')
        return None

    payload = {
        'model': DEEPSEEK_MODEL,
        'messages': [
            {'role': 'system', 'content': 'Você é um especialista em passagens aéreas. Seja conciso.'},
            {'role': 'user', 'content': prompt}
        ],
        'temperature': 0.5,
        'max_tokens': 500,
    }

    try:
        resp = requests.post(
            f'{DEEPSEEK_BASE_URL}/chat/completions',
            headers={
                'Authorization': f'Bearer {DEEPSEEK_API_KEY}',
                'Content-Type': 'application/json',
            },
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        content = data['choices'][0]['message']['content']
        logger.info('[ai] Deepseek OK | tokens=%s-%s | model=%s',
                     data['usage']['prompt_tokens'],
                     data['usage']['completion_tokens'],
                     data.get('model', DEEPSEEK_MODEL))
        return content.strip()
    except Exception as exc:
        logger.error('[ai] Erro ao chamar Deepseek: %s', exc)
        return None


def _parse_dicas(raw: str, count: int) -> list[str]:
    """Extrai as dicas do texto bruto da IA, uma por rota."""
    dicas = []
    for line in raw.split('\n'):
        line = line.strip()
        # Pula linhas vazias ou que não começam com 📌/💰/sugiro/aguarde/etc
        if not line or line.startswith(('Rota', 'ROTAS', '---')):
            continue
        dicas.append(line)
    # Se IA retornou menos linhas que o esperado, preenche com vazio
    while len(dicas) < count:
        dicas.append('📌 Preço dentro do esperado para a data.')
    return dicas[:count]


def _cache_key(rows: list[dict]) -> str:
    key_parts = []
    for row in sorted(rows, key=lambda r: (r.get('origin',''), r.get('destination',''))):
        key_parts.append(f"{row.get('origin','')}-{row.get('destination','')}-{row.get('outbound_date','')}-{row.get('best_vendor_price','')}")
    return '|'.join(key_parts)


def generate_ai_message(rows: list[dict], force: bool = False) -> Optional[str]:
    """
    Gera mensagem completa com links inline + dicas da IA.
    Substitui a mensagem de links de booking.

    Formato:
    🔗 Acesse os voos encontrados por companhia:

    ✈️ [Origem Cidade -> Destino Cidade em 15/06/26](url)
    Companhia • R$ Preço
    📌 Dica personalizada

    Args:
        rows: Lista de dicionários com resultados
        force: Se True, ignora cache

    Returns:
        String formatada completa, ou None
    """
    valid_rows = [r for r in rows if r.get('best_vendor_price') or r.get('price')]
    if not valid_rows:
        return None

    # Cache check
    ck = _cache_key(valid_rows)
    if not force and ck in _cache:
        cached_at, cached_val = _cache[ck]
        if time.time() - cached_at < _CACHE_TTL:
            logger.info('[ai] cache HIT | key=%s', ck[:60])
            return cached_val

    # Pede dicas pra IA
    prompt = _build_ai_prompt(valid_rows)
    logger.info('[ai] chamando Deepseek | rotas=%s', len(valid_rows))

    raw_dicas = _call_deepseek(prompt)
    dicas = _parse_dicas(raw_dicas or '', len(valid_rows)) if raw_dicas else ['📌 Preço dentro do esperado.'] * len(valid_rows)

    # Monta a mensagem final no formato que o Teles pediu
    lines = ['🔗 Acesse os voos encontrados por companhia:\n']
    for i, row in enumerate(valid_rows):
        origin = row.get('origin', '???').upper()
        dest = row.get('destination', '???').upper()
        date = row.get('outbound_date', '')
        try:
            date_fmt = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%y')
        except (ValueError, TypeError):
            date_fmt = date
        vendor = row.get('best_vendor') or row.get('vendor') or row.get('airline') or '—'
        # Remove sufixo "Companhia aérea" que o Google cola (inclusive se concatenado)
        import re as _re_vendor
        vendor = _re_vendor.sub(r'Companhia\s*a[ée]rea\s*', '', vendor, flags=_re_vendor.I).strip()
        vendor = _re_vendor.sub(r'\s*Companhia\s*a[ée]rea\s*', '', vendor, flags=_re_vendor.I).strip()
        price = row.get('best_vendor_price') or row.get('price')
        price_str = f'R$ {price:,.0f}'.replace(',', '.') if price else 'N/D'
        booking_url = row.get('booking_url', '') or row.get('url', '')

        # Nome da cidade para o link
        origin_city = _city_name(origin)
        dest_city = _city_name(dest)
        link_text = f'{origin_city} → {dest_city} em {date_fmt}'

        if booking_url:
            # Usa HTML <a> que é mais tolerante com URLs complexas
            safe_url = booking_url.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')
            lines.append(f'✈️ <a href="{safe_url}">{link_text}</a>')
        else:
            lines.append(f'✈️ {link_text}')

        lines.append(f'{vendor} • {price_str}')

        # Dica da IA
        dica = dicas[i] if i < len(dicas) else '📌 Preço dentro do esperado.'
        lines.append(dica)
        lines.append('')

    result = '\n'.join(lines).strip()

    # Cache
    _cache[ck] = (time.time(), result)
    return result
