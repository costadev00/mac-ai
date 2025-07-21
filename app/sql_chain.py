import os
import re
import signal
import time
from typing import Dict, List

import structlog
import fdb

from .agent import generate_sql
from .db import search_schema, execute_select

logger = structlog.get_logger()

class QueryTimeout(Exception):
    pass


def _timeout_handler(signum, frame):
    raise QueryTimeout()


def _connect_readonly() -> fdb.Connection:
    dsn = os.getenv("FIREBIRD_DSN")
    user = os.getenv("FIREBIRD_USER")
    password = os.getenv("FIREBIRD_PASSWORD")
    role = os.getenv("FIREBIRD_READONLY_ROLE", "READ_ONLY")
    return fdb.connect(dsn=dsn, user=user, password=password, role=role)


def run_user_query(user_prompt: str) -> Dict[str, List[Dict]]:
    start = time.monotonic()
    conn = _connect_readonly()
    try:
        context = search_schema(conn, user_prompt)
        sql = generate_sql(user_prompt, context)
        if re.search(r"\b(insert|update|delete)\b", sql, re.IGNORECASE):
            raise ValueError("Unsafe query")
        if "limit" not in sql.lower():
            sql = sql.rstrip(";") + " LIMIT 100"

        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(10)
        try:
            rows = execute_select(conn, sql)
        finally:
            signal.alarm(0)
        duration = time.monotonic() - start
        logger.info(
            "run_user_query",
            user_prompt=user_prompt,
            retrieved_context=context,
            sql=sql,
            rows_count=len(rows),
            duration=duration,
        )
        return {"sql": sql, "resultado": rows}
    finally:
        conn.close()
