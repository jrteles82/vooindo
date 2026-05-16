"""Módulo de Comentários / Mural - Vooindo

Gerencia comentários dos usuários com moderação do admin.
Tabela: comments
"""

from db import connect as db_connect, sql

ITEMS_PER_PAGE = 5


def create_comment(user_id: int, chat_id: str, username: str, text: str) -> int:
    """Salva um novo comentário como pending."""
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            sql("INSERT INTO comments (user_id, chat_id, username, text, status) VALUES (%s, %s, %s, %s, 'pending')"),
            (user_id, chat_id, username, text),
        )
        conn.commit()
        return cur.lastrowid or 0
    finally:
        conn.close()


def get_approved_comments(page: int = 0) -> tuple[list[dict], int]:
    """Retorna comentários aprovados com paginação.
    Returns: (comments_list, total_pages)"""
    conn = db_connect()
    try:
        cur = conn.cursor()
        offset = page * ITEMS_PER_PAGE

        cur.execute(
            sql("SELECT COUNT(*) as total FROM comments WHERE status = 'approved'")
        )
        total = cur.fetchone()['total']
        total_pages = max(0, (total - 1) // ITEMS_PER_PAGE) if total > 0 else 0

        cur.execute(
            sql("""
                SELECT id, user_id, chat_id, username, text, created_at
                FROM comments
                WHERE status = 'approved'
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
            """),
            (ITEMS_PER_PAGE, offset),
        )
        rows = cur.fetchall()
        return rows, total_pages
    finally:
        conn.close()


def get_user_pending_comments(user_id: int) -> list[dict]:
    """Retorna comentários pendentes/del do usuário."""
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            sql("""
                SELECT id, text, status, created_at
                FROM comments
                WHERE user_id = %s AND status IN ('pending', 'approved')
                ORDER BY created_at DESC
                LIMIT 20
            """),
            (user_id,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def delete_own_comment(comment_id: int, user_id: int) -> bool:
    """Usuário apaga próprio comentário (só se pending)."""
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            sql("UPDATE comments SET status = 'deleted_by_user' WHERE id = %s AND user_id = %s AND status = 'pending'"),
            (comment_id, user_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def moderate_comment(comment_id: int, action: str, moderator_id: int) -> bool:
    """Admin modera comentário: approve, reject, delete."""
    valid = {'approve': 'approved', 'reject': 'rejected', 'delete': 'deleted_by_admin'}
    if action not in valid:
        return False
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            sql("UPDATE comments SET status = %s, moderated_at = NOW(), moderated_by = %s WHERE id = %s"),
            (valid[action], moderator_id, comment_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def get_pending_comments(page: int = 0) -> tuple[list[dict], int]:
    """Admin: lista comentários pendentes de moderação."""
    conn = db_connect()
    try:
        cur = conn.cursor()
        offset = page * ITEMS_PER_PAGE

        cur.execute(sql("SELECT COUNT(*) as total FROM comments WHERE status = 'pending'"))
        total = cur.fetchone()['total']
        total_pages = max(0, (total - 1) // ITEMS_PER_PAGE) if total > 0 else 0

        cur.execute(
            sql("""
                SELECT c.id, c.user_id, c.chat_id, c.username, c.text, c.created_at,
                       bu.first_name
                FROM comments c
                LEFT JOIN bot_users bu ON bu.user_id = c.user_id
                WHERE c.status = 'pending'
                ORDER BY c.created_at DESC
                LIMIT %s OFFSET %s
            """),
            (ITEMS_PER_PAGE, offset),
        )
        return cur.fetchall(), total_pages
    finally:
        conn.close()


def count_pending_comments() -> int:
    """Admin badge: quantos pendentes."""
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(sql("SELECT COUNT(*) as total FROM comments WHERE status = 'pending'"))
        return cur.fetchone()['total']
    finally:
        conn.close()


def get_comment_by_id(comment_id: int) -> dict | None:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(sql("SELECT * FROM comments WHERE id = %s"), (comment_id,))
        return cur.fetchone()
    finally:
        conn.close()
