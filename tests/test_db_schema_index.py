import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import json
import sqlite3
from typing import Any

from app import db_schema_index


class DummyEmbeddings:
    def __init__(self, values):
        self._values = list(values)

    def create(self, input, model):
        data = []
        for val in self._values[: len(input)]:
            data.append(type("Obj", (), {"embedding": val}))
        self._values = self._values[len(input) :]
        return type("Resp", (), {"data": data})


class DummyOpenAI:
    def __init__(self, return_values):
        self.embeddings = DummyEmbeddings(return_values)


def setup_sqlite_db(path: str) -> None:
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        'CREATE TABLE "information_schema.columns"(table_schema text, table_name text, column_name text, ordinal_position int)'
    )
    cur.executemany(
        'INSERT INTO "information_schema.columns"(table_schema, table_name, column_name, ordinal_position) VALUES ("public", ?, ?, ?)',
        [
            ("users", "id", 1),
            ("users", "name", 2),
            ("orders", "id", 1),
        ],
    )
    cur.execute(
        "CREATE TABLE schema_index(table_name text primary key, embedding text)"
    )
    conn.commit()
    conn.close()


def make_fdb() -> Any:
    sqlite3.register_adapter(list, lambda l: json.dumps(l))

    class CursorWrapper:
        def __init__(self, cursor):
            self._cursor = cursor

        def execute(self, sql: str, params: Any = None):
            sql = sql.replace("%s", "?")
            sql = sql.replace("information_schema.columns", '"information_schema.columns"')
            if params is None:
                return self._cursor.execute(sql)
            return self._cursor.execute(sql, params)

        def fetchall(self):
            return self._cursor.fetchall()

        def close(self):
            self._cursor.close()

        def __getattr__(self, name):
            return getattr(self._cursor, name)

    class ConnWrapper:
        def __init__(self, conn):
            self._conn = conn

        def cursor(self):
            return CursorWrapper(self._conn.cursor())

        def commit(self):
            self._conn.commit()

        def close(self):
            self._conn.close()

    class FDBModule:
        @staticmethod
        def connect(dsn: str):
            return ConnWrapper(sqlite3.connect(dsn))

    return FDBModule


def test_refresh_schema_upserts(monkeypatch, tmp_path):
    db_path = tmp_path / "test.db"
    setup_sqlite_db(str(db_path))

    monkeypatch.setenv("DATABASE_URL", str(db_path))
    monkeypatch.setenv("OPENAI_API_KEY", "dummy")
    monkeypatch.setattr(db_schema_index, "fdb", make_fdb())
    monkeypatch.setattr(db_schema_index, "OpenAI", lambda api_key=None: DummyOpenAI([[1.0], [2.0]]))

    db_schema_index.refresh_schema()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT table_name, embedding FROM schema_index ORDER BY table_name")
    rows = cur.fetchall()
    conn.close()

    assert len(rows) == 2
    assert rows[0][0] == "orders"
    assert json.loads(rows[0][1]) == [1.0]
    assert rows[1][0] == "users"
    assert json.loads(rows[1][1]) == [2.0]

    monkeypatch.setattr(db_schema_index, "OpenAI", lambda api_key=None: DummyOpenAI([[3.0], [4.0]]))
    db_schema_index.refresh_schema()
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT table_name, embedding FROM schema_index ORDER BY table_name")
    rows2 = cur.fetchall()
    conn.close()

    assert len(rows2) == 2
    assert json.loads(rows2[0][1]) == [3.0]
    assert json.loads(rows2[1][1]) == [4.0]
