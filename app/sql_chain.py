import os
from typing import List, Dict, Tuple
from contextlib import contextmanager
import signal

from openai import OpenAI
from langchain.prompts import PromptTemplate
from langchain.llms import OpenAI as LCOpenAI

import fdb

from .db import get_db_connection, search_schema


class TimeoutException(Exception):
    pass


@contextmanager
def timeout(seconds: int):
    def _handle(signum, frame):
        raise TimeoutException("Query timed out")

    original = signal.signal(signal.SIGALRM, _handle)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original)


def search_examples(conn, question: str, top_k: int = 3) -> List[str]:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    query_emb = client.embeddings.create(
        input=question,
        model="text-embedding-3-large",
    ).data[0].embedding
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT example
            FROM sql_examples
            ORDER BY embedding <=> %s
            LIMIT %s
            """,
            (query_emb, top_k),
        )
        rows = cur.fetchall()
    return [r[0] for r in rows]


def run_user_query(pergunta_pt: str) -> Tuple[str, List[Dict]]:
    """Generate and run a SQL query based on a Portuguese question."""

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # 1. Translate question to English
    translation = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "Translate the following user question from Portuguese to English."},
            {"role": "user", "content": pergunta_pt},
        ],
        temperature=0,
    ).choices[0].message.content.strip()

    # 2. Retrieve schema snippets and canonical examples
    pg_url = os.getenv("READONLY_DATABASE_URL") or os.getenv("DATABASE_URL")
    pg_conn = get_db_connection(pg_url)
    schema_context = search_schema(pg_conn, translation)
    examples = search_examples(pg_conn, translation)
    pg_conn.close()

    context_lines = [f"Schema: {schema_context}"]
    if examples:
        context_lines.append("Examples:\n" + "\n".join(examples))
    context = "\n".join(context_lines)

    # 3. Prompt the model for SQL generation
    prompt = PromptTemplate(
        input_variables=["context", "question"],
        template=(
            "You are a helpful assistant that writes read-only SQL queries for Firebird.\n"
            "Always use SELECT statements and include LIMIT 100.\n"
            "Respond with exactly one parameterised query.\n"
            "{context}\nQuestion: {question}\nSQL:"),
    )
    llm = LCOpenAI(temperature=0, model_name="gpt-3.5-turbo")
    sql = llm(prompt.format(context=context, question=translation)).strip().splitlines()[0]
    if sql.startswith("```"):
        sql = sql.strip("`")
    if "limit" not in sql.lower():
        sql = sql.rstrip(";") + " LIMIT 100"

    # 4. Execute on Firebird with timeout
    fb_conn = fdb.connect(
        dsn=os.getenv("FIREBIRD_DSN"),
        user=os.getenv("FIREBIRD_USER"),
        password=os.getenv("FIREBIRD_PASSWORD"),
    )
    fb_conn.charset = "UTF8"
    with timeout(10):
        cur = fb_conn.cursor()
        cur.execute(sql)
        rows = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]
    fb_conn.close()

    # 5. Return generated SQL and rows
    return sql, rows
