import json


def parse_job_payload(payload_raw) -> dict:
    raw = str(payload_raw or '').strip()
    if not raw or raw == 'dry_run':
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def is_dry_run_payload(payload_raw) -> bool:
    raw = str(payload_raw or '').strip()
    if raw == 'dry_run':
        return True
    data = parse_job_payload(raw)
    return bool(data.get('dry_run')) if isinstance(data, dict) else False


def build_route_job_payload(*, cycle_started_iso: str, route: dict, total_routes: int,
                            label: str, executor_timeout: int, retry: int | None = None,
                            dry_run: bool = False) -> str:
    payload = {
        'round_started_at': cycle_started_iso,
        'route': {
            'id': route.get('id', 0),
            'origin': route.get('origin', ''),
            'destination': route.get('destination', ''),
            'outbound_date': route.get('outbound_date', ''),
            'inbound_date': route.get('inbound_date') or '',
            'date_type': route.get('date_type') or 'fixed',
            'flexible_month': route.get('flexible_month') or '',
            'trip_type': route.get('trip_type') or 'one-way',
        },
        'group_info': {
            'total_routes': int(total_routes),
            'label': label,
        },
        'executor_timeout': int(executor_timeout),
    }
    if retry is not None:
        payload['retry'] = int(retry)
    if dry_run:
        payload['dry_run'] = True
    return json.dumps(payload, ensure_ascii=False)
