import os
from typing import List, Dict, Any

import psycopg2
import psycopg2.extras
from openai import OpenAI

from .db_schema_index import refresh_schema as _refresh_schema, DIMENSION


def get_db_connection(dsn: str):
    """Return a new database connection using ``psycopg2``."""
    if not dsn:
        raise ValueError("Database URL is required")
    return psycopg2.connect(dsn)


def create_schema_index_table(conn) -> None:
    """Ensure the ``schema_index`` table exists."""
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS schema_index(
                table_name text PRIMARY KEY,
                embedding vector({DIMENSION})
            )
            """
        )
    conn.commit()


def load_schema_embeddings(conn) -> None:
    """Populate the ``schema_index`` table if empty."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM schema_index")
        count = cur.fetchone()[0]
    if count == 0:
        _refresh_schema()


def search_schema(conn, question: str, top_k: int = 5) -> str:
    """Return a short schema context for ``question`` using similarity search."""
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    query_emb = (
        client.embeddings.create(input=question, model="text-embedding-3-large")
        .data[0]
        .embedding
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM schema_index ORDER BY embedding <=> %s LIMIT %s",
            (query_emb, top_k),
        )
        names = [r[0] for r in cur.fetchall()]
    return ", ".join(names)


def execute_select(conn, sql: str) -> List[Dict[str, Any]]:
    """Execute a SELECT statement and return rows as dictionaries."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [dict(r) for r in rows]
