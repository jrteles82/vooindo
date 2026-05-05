#!/usr/bin/env python3
"""
route_optimizer.py — Otimizador adaptativo de rotas do Vooindo.

Analisa o histórico de scans a cada ciclo e produz:
  1. Ordem de prioridade: rotas rápidas primeiro, lentas depois
  2. Timeout sugerido por user (rota mais demorada)
  3. Estatísticas de melhoria contínua

Uso:
    from route_optimizer import compute_priorities, get_optimizer_state
    priorities = compute_priorities(conn)
    priorities.ordered_users  # lista de (user_id, nome, timeout_sugerido)
"""

import json
import logging
import os
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger('route_optimizer')

STATE_FILE = Path('/opt/vooindo/.optimizer_state.json')

# Default timeout values per category
TIMEOUT_DOMESTIC = 180       # 3min para rotas domésticas
TIMEOUT_INTERNATIONAL = 350  # ~6min para rotas internacionais
TIMEOUT_MAX = 600            # 10min absoluto (evita worker preso) — aumentado por timeouts frequentes em PVH-FOR e PVH-MIA
TIMEOUT_DEFAULT = 300        # 5min fallback

# Quantas horas de histórico considerar
HISTORY_HOURS = 24

# Fração de workers para rotas rápidas vs lentas
FAST_WORKER_RATIO = 0.7


def _load_state() -> dict:
    """Carrega estado persistente do otimizador."""
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text())
    except Exception:
        pass
    return {
        'version': 1,
        'created_at': datetime.now().isoformat(),
        'cycles': [],         # histórico de ciclos analisados
        'user_stats': {},     # user_id -> {avg_dur, max_dur, scans, timeout, priority_score}
        'optimizations': [],  # log de otimizações aplicadas
    }


def _save_state(state: dict):
    """Salva estado persistente."""
    try:
        STATE_FILE.write_text(json.dumps(state, indent=2, default=str))
    except Exception as e:
        logger.warning('Erro ao salvar optimizer state: %s', e)


def _get_route_type(origin: str, destination: str) -> str:
    """Classifica rota como domestica ou internacional."""
    origin_br = origin and len(origin) == 3  # código IATA
    dest_br = destination and len(destination) == 3
    # Simples heurística: cidades do Brasil vs exterior
    # Na prática, seria bom ter uma lista de aeroportos BR
    br_airports = {
        'PVH', 'GRU', 'CGH', 'VCP', 'GIG', 'SDU', 'BSB', 'CNF', 'POA',
        'CWB', 'FLN', 'REC', 'SSA', 'FOR', 'NAT', 'MAO', 'BEL', 'VIX',
        'SLZ', 'THE', 'AJU', 'MCZ', 'CGB', 'CPV', 'JPA', 'CGR', 'IGU',
        'LDB', 'RBR', 'BVB', 'PMW', 'MCP', 'IOS', 'BPS', 'CXJ', 'JOI',
        'UDI', 'GYN', 'PLU', 'BHZ',
    }
    is_domestic = (origin in br_airports) and (destination in br_airports)
    return 'domestic' if is_domestic else 'international'


def compute_priorities(conn) -> dict:
    """
    Analisa histórico e retorna prioridades otimizadas para o próximo ciclo.
    
    Returns:
        dict com:
          - ordered_users: [(user_id, nome, timeout_sugerido), ...] (rápidos primeiro)
          - user_timeouts: {user_id: timeout_sugerido}
          - metrics: {avg_cycle_dur, total_scans, users_count}
          - optimizations: [str, ...]  # ações sugeridas
    """
    state = _load_state()
    cur = conn.cursor()
    
    # 1. Coletar dados históricos de duração por user
    cur.execute('''
        SELECT bu.user_id, bu.first_name,
               ROUND(AVG(TIMESTAMPDIFF(SECOND, j.started_at, j.finished_at))) as avg_dur,
               ROUND(MAX(TIMESTAMPDIFF(SECOND, j.started_at, j.finished_at))) as max_dur,
               COUNT(*) as scans
        FROM scan_jobs j
        JOIN bot_users bu ON bu.user_id = j.user_id
        WHERE j.created_at >= NOW() - INTERVAL %s HOUR
          AND j.status = 'done'
          AND j.started_at IS NOT NULL AND j.finished_at IS NOT NULL
        GROUP BY bu.user_id, bu.first_name
        ORDER BY avg_dur ASC
    ''', (HISTORY_HOURS,))
    
    user_rows = cur.fetchall()
    
    # 2. Coletar rotas de cada user pra classificar
    cur.execute('''
        SELECT ur.user_id, ur.origin, ur.destination
        FROM user_routes ur
        WHERE ur.active = 1
    ''')
    user_routes = {}
    for r in cur.fetchall():
        uid = r['user_id']
        if uid not in user_routes:
            user_routes[uid] = []
        user_routes[uid].append({
            'origin': r['origin'],
            'destination': r['destination'],
            'route_type': _get_route_type(r['origin'], r['destination']),
        })
    
    # 3. Calcular timeout e prioridade por user
    user_stats = {}
    ordered_users = []
    total_scans = 0
    
    for r in user_rows:
        uid = r['user_id']
        avg_dur = float(r["avg_dur"] or 0) or TIMEOUT_DEFAULT
        max_dur = r['max_dur'] or TIMEOUT_DEFAULT
        scans = r['scans'] or 0
        name = r['first_name']
        total_scans += scans
        
        # Classificar rota
        routes = user_routes.get(uid, [])
        has_international = any(rt['route_type'] == 'international' for rt in routes)
        
        # Timeout sugerido: baseado no histórico + tipo de rota
        if has_international:
            suggested_timeout = max(avg_dur * 1.8, TIMEOUT_INTERNATIONAL)
        else:
            suggested_timeout = max(avg_dur * 1.8, TIMEOUT_DOMESTIC)
        
        # Cap no máximo absoluto
        suggested_timeout = min(suggested_timeout, TIMEOUT_MAX)
        
        # Score de prioridade (menor = mais rápido = maior prioridade)
        # Rotas domésticas sempre na frente das internacionais
        priority_score = avg_dur
        if not has_international:
            priority_score = avg_dur * 0.5  # prioridade extra pra domésticas
        
        user_stats[str(uid)] = {
            'avg_dur': avg_dur,
            'max_dur': max_dur,
            'scans': scans,
            'timeout': suggested_timeout,
            'priority_score': priority_score,
            'has_international': has_international,
            'routes': routes,
        }
        
        ordered_users.append({
            'user_id': uid,
            'name': name,
            'avg_dur': avg_dur,
            'timeout': suggested_timeout,
            'priority_score': priority_score,
            'has_international': has_international,
        })
    
    # 4. Ordenar: rápido primeiro (menor priority_score)
    ordered_users.sort(key=lambda u: u['priority_score'])
    
    # 5. Atualizar estado
    state['user_stats'] = user_stats
    state['last_updated'] = datetime.now().isoformat()
    _save_state(state)
    
    # 6. Métricas
    avg_cycle_dur = 0
    if user_rows:
        avg_cycle_dur = sum(r['avg_dur'] or 0 for r in user_rows) / len(user_rows)
    
    # 7. Sugestões de otimização
    optimizations = []
    if user_rows:
        fastest = ordered_users[0]
        slowest = ordered_users[-1]
        ratio = slowest['avg_dur'] / max(fastest['avg_dur'], 1)
        if ratio > 5:
            optimizations.append(
                f"Disparidade alta: {slowest['name']} ({slowest['avg_dur']:.0f}s) "
                f"é {ratio:.0f}x mais lento que {fastest['name']} ({fastest['avg_dur']:.0f}s). "
                "Priorizar rápidos reduz tempo de espera dos demais."
            )
    
    result = {
        'ordered_users': [(u['user_id'], u['name'], u['timeout']) for u in ordered_users],
        'user_timeouts': {str(u['user_id']): u['timeout'] for u in ordered_users},
        'user_priorities': ordered_users,
        'metrics': {
            'avg_cycle_dur': avg_cycle_dur,
            'total_scans': total_scans,
            'users_count': len(ordered_users),
            'fastest_user': ordered_users[0]['name'] if ordered_users else None,
            'fastest_dur': ordered_users[0]['avg_dur'] if ordered_users else 0,
            'slowest_user': ordered_users[-1]['name'] if ordered_users else None,
            'slowest_dur': ordered_users[-1]['avg_dur'] if ordered_users else 0,
        },
        'optimizations': optimizations,
    }
    
    return result


def get_user_timeout(conn, user_id: int, default_timeout: int = TIMEOUT_DEFAULT) -> int:
    """Retorna timeout personalizado para um user."""
    priorities = compute_priorities(conn)
    return priorities['user_timeouts'].get(str(user_id), default_timeout)


def log_optimization(action: str, details: dict = None):
    """Registra uma otimização aplicada."""
    state = _load_state()
    state.setdefault('optimizations', []).append({
        'timestamp': datetime.now().isoformat(),
        'action': action,
        'details': details or {},
    })
    # Manter só últimas 100
    state['optimizations'] = state['optimizations'][-100:]
    _save_state(state)


def log_cycle_result(conn):
    """Registra resultado do ciclo para análise de melhoria."""
    cur = conn.cursor()
    cur.execute('''
        SELECT HOUR(created_at) as h, DATE(created_at) as d,
               COUNT(*) as jobs,
               SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) as done,
               SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as err,
               ROUND(AVG(TIMESTAMPDIFF(SECOND, COALESCE(started_at, created_at), COALESCE(finished_at, NOW())))) as avg_dur,
               ROUND(MAX(TIMESTAMPDIFF(SECOND, COALESCE(started_at, created_at), COALESCE(finished_at, NOW())))) as max_dur,
               SUM(CASE WHEN error_message = 'job_timeout_300s' THEN 1 ELSE 0 END) as timeouts
        FROM scan_jobs
        WHERE created_at >= NOW() - INTERVAL 2 HOUR
          AND HOUR(created_at) != HOUR(NOW())  -- ciclo completo
        GROUP BY d, h
        ORDER BY d DESC, h DESC
        LIMIT 1
    ''')
    row = cur.fetchone()
    if not row:
        return
    
    state = _load_state()
    state.setdefault('cycles', []).append({
        'timestamp': datetime.now().isoformat(),
        'hour': row['h'],
        'jobs': row['jobs'],
        'done': row['done'],
        'errors': row['err'],
        'avg_dur': row['avg_dur'],
        'max_dur': row['max_dur'],
        'timeouts': row['timeouts'],
    })
    # Manter só últimas 50
    state['cycles'] = state['cycles'][-50:]
    _save_state(state)


def get_optimizer_state() -> dict:
    """Retorna estado atual do otimizador para debug/monitoramento."""
    return _load_state()


if __name__ == '__main__':
    # Teste: mostrar prioridades atuais
    from db import connect
    conn = connect()
    result = compute_priorities(conn)
    m = result['metrics']
    print('=== PRIORIDADES ===')
    print(f"  Users: {m['users_count']}")
    print(f"  Medio ciclo: {m['avg_cycle_dur']:.0f}s")
    print(f"  Mais rapido: {m['fastest_user']} ({m['fastest_dur']:.0f}s)")
    print(f"  Mais lento: {m['slowest_user']} ({m['slowest_dur']:.0f}s)")
    print()
    for u in result['user_priorities']:
        ps = u['priority_score']
        nm = u['name']
