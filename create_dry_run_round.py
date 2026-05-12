#!/opt/vooindo/.venv/bin/python
import argparse
from config import now_local_iso
from db import connect as connect_db, sql
from dry_run_utils import build_route_job_payload


def main():
    ap = argparse.ArgumentParser(description='Cria jobs scheduled em dry-run sem enviar mensagens aos usuários.')
    ap.add_argument('--user-id', type=int, default=None, help='user_id do bot_users')
    ap.add_argument('--all', action='store_true', help='criar dry-run para TODOS os usuarios')
    ap.add_argument('--executor-timeout', type=int, default=300)
    ap.add_argument('--group-suffix', default='manualtest')
    args = ap.parse_args()

    if not args.user_id and not args.all:
        raise SystemExit('Informe --user-id ou --all')
    if args.user_id and args.all:
        raise SystemExit('Use --user-id OU --all, não ambos')

    conn = connect_db()
    try:
        user_ids = []
        if args.user_id:
            user_ids = [args.user_id]
        else:
            rows = conn.execute(sql('''
                SELECT bu.user_id, COALESCE(bu.blocked, 0) AS blocked
                FROM bot_users bu
                JOIN user_routes ur ON ur.user_id = bu.user_id AND ur.active = 1
                WHERE bu.confirmed = 1
                GROUP BY bu.user_id
            ''')).fetchall()
            user_ids = [r['user_id'] for r in rows if str(r.get('blocked', '0')) == '0']

        total_jobs = 0
        for uid in user_ids:
            user = conn.execute(sql('''
                SELECT bu.user_id, bu.chat_id, COALESCE(bu.first_name, '') AS first_name
                FROM bot_users bu
                WHERE bu.user_id = %s
                LIMIT 1
            '''), (uid,)).fetchone()
            if not user:
                continue

            routes = conn.execute(sql('''
                SELECT id, origin, destination, outbound_date, inbound_date
                FROM user_routes
                WHERE user_id = %s AND active = 1
                ORDER BY id
            '''), (uid,)).fetchall()
            if not routes:
                continue

        cycle_started_iso = now_local_iso(sep='T')
        group_key = f"dryrun_{args.user_id}_{cycle_started_iso}_{args.group_suffix}"
        created = []
        for route in routes:
            payload = build_route_job_payload(
                cycle_started_iso=cycle_started_iso,
                route=route,
                total_routes=len(routes),
                label=user.get('first_name') or f'user_{args.user_id}',
                executor_timeout=args.executor_timeout,
                dry_run=True,
            )
            ins = conn.execute(
                sql("INSERT INTO scan_jobs (user_id, chat_id, job_type, status, payload, cost_score, group_key) VALUES (%s, %s, 'scheduled', 'pending', %s, %s, %s)"),
                (int(user['user_id']), str(user['chat_id']), payload, 1, group_key),
            )
            conn.commit()
            created.append(int(getattr(ins, 'lastrowid', 0) or 0))

            total_jobs += len(created)

        if args.all:
            print(f'total_users={len(user_ids)} total_jobs={total_jobs}')
            print('dry_run=true (sem envio aos usuários)')
        else:
            print(f'group_key={group_key}')
            print(f'user_id={args.user_id} chat_id={user["chat_id"]} nome={user.get("first_name") or "?"}')
            print(f'rotas={len(routes)} jobs={created}')
            print('dry_run=true (sem envio ao usuário)')
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
