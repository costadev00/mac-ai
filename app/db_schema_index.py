import os
from typing import Dict, List

# These imports are optional for tests where the modules may not exist
try:
    import fdb  # type: ignore
except ImportError:  # pragma: no cover - fdb might not be installed in test env
    fdb = None  # type: ignore

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - openai might not be installed in test env
    OpenAI = None  # type: ignore

DIMENSION = 3072  # dimension for text-embedding-3-large


def refresh_schema() -> None:
    """Refresh schema embeddings stored in the ``schema_index`` collection.

    This function connects to the database defined by ``DATABASE_URL`` using
    ``fdb``. It introspects ``information_schema`` to gather all tables and
    their columns, creates one text string per table describing its columns,
    embeds each string with ``text-embedding-3-large`` and upserts the
    resulting vectors into the ``schema_index`` table using PGVector.
    """

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL environment variable not set")

    if fdb is None:
        raise ImportError("fdb module not available")
    if OpenAI is None:
        raise ImportError("openai module not available")

    conn = fdb.connect(dsn=db_url)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
        """
    )
    rows = cur.fetchall()

    tables: Dict[str, List[str]] = {}
    for table_name, column_name in rows:
        tables.setdefault(table_name, []).append(column_name)

    table_names = list(tables.keys())
    texts = [f"{tbl}: {', '.join(cols)}" for tbl, cols in tables.items()]

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    embeddings: List[List[float]] = []
    for i in range(0, len(texts), 100):
        response = client.embeddings.create(
            input=texts[i:i + 100],
            model="text-embedding-3-large",
        )
        embeddings.extend([d.embedding for d in response.data])

    for table_name, emb in zip(table_names, embeddings):
        cur.execute(
            """
            INSERT INTO schema_index (table_name, embedding)
            VALUES (%s, %s)
            ON CONFLICT (table_name)
            DO UPDATE SET embedding = EXCLUDED.embedding
            """,
            (table_name, emb),
        )
    conn.commit()
    cur.close()
    conn.close()
