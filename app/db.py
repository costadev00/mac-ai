import os
import psycopg2
from typing import List, Dict
from openai import OpenAI

DIMENSION = 3072  # dimension for text-embedding-3-large


def get_db_connection(url: str):
    return psycopg2.connect(url)


def create_schema_index_table(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS schema_index (
                id SERIAL PRIMARY KEY,
                item_type TEXT,
                table_name TEXT,
                column_name TEXT,
                embedding vector({DIMENSION})
            )
            """
        )
    conn.commit()


def fetch_schema(conn) -> List[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema='public'
            ORDER BY table_name, ordinal_position
            """
        )
        return cur.fetchall()


def load_schema_embeddings(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM schema_index")
        if cur.fetchone()[0] > 0:
            return
    columns = fetch_schema(conn)
    texts = []
    items = []
    table_set = set()
    for table, column in columns:
        texts.append(f"Tabela {table} coluna {column}")
        items.append(("column", table, column))
        table_set.add(table)
    for table in sorted(table_set):
        texts.append(f"Tabela {table}")
        items.append(("table", table, None))
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    embeddings = []
    for i in range(0, len(texts), 100):
        response = client.embeddings.create(
            input=texts[i:i + 100],
            model="text-embedding-3-large",
        )
        embeddings.extend([d.embedding for d in response.data])
    with conn.cursor() as cur:
        for (item_type, table, column), emb in zip(items, embeddings):
            cur.execute(
                "INSERT INTO schema_index (item_type, table_name, column_name, embedding) VALUES (%s, %s, %s, %s)",
                (item_type, table, column, emb),
            )
    conn.commit()


def search_schema(conn, question: str, top_k: int = 5) -> str:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    query_emb = client.embeddings.create(
        input=question,
        model="text-embedding-3-large",
    ).data[0].embedding
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name, column_name, item_type
            FROM schema_index
            ORDER BY embedding <=> %s
            LIMIT %s
            """,
            (query_emb, top_k),
        )
        rows = cur.fetchall()
    schema = {}
    for table, column, item_type in rows:
        if item_type == "table":
            schema.setdefault(table, [])
        else:
            schema.setdefault(table, []).append(column)
    parts = []
    for table, cols in schema.items():
        if cols:
            parts.append(f"{table}({', '.join(cols)})")
        else:
            parts.append(f"{table}")
    return "; ".join(parts)


def execute_select(conn, sql: str) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute(sql)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
