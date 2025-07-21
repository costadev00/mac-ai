import os
import json
from fastapi import FastAPI, HTTPException
from .db import (
    get_db_connection,
    create_schema_index_table,
    load_schema_embeddings,
    search_schema,
    execute_select,
)
from .agent import generate_sql
from .models import QuestionRequest
from openai import OpenAI


def run_user_query(question: str):
    """Generate and execute a safe SELECT query for the given question."""
    ro_url = os.getenv("READONLY_DATABASE_URL")
    conn = get_db_connection(ro_url)
    schema_context = search_schema(conn, question)
    sql = generate_sql(question, schema_context)
    if not sql.lower().startswith("select"):
        conn.close()
        raise HTTPException(400, "Consulta insegura gerada")
    if "limit" not in sql.lower():
        sql = sql.rstrip(";") + " LIMIT 100"
    try:
        rows = execute_select(conn, sql)
    except Exception as e:
        conn.close()
        raise HTTPException(400, str(e))
    conn.close()
    return sql, rows


def make_human_answer(question: str, rows, sql: str) -> str:
    """Use an LLM to craft a human friendly answer in Portuguese."""
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    data_str = json.dumps(rows, ensure_ascii=False)
    prompt = (
        "Responda a pergunta em português utilizando os resultados abaixo."\
        "\nPergunta: {q}\nSQL: {s}\nResultados: {r}\nResposta:".format(
            q=question, s=sql, r=data_str
        )
    )
    completion = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    return completion.choices[0].message.content.strip()

app = FastAPI(title="Perguntas SQL")

@app.on_event("startup")
def startup_event():
    db_url = os.getenv("DATABASE_URL")
    conn = get_db_connection(db_url)
    create_schema_index_table(conn)
    load_schema_embeddings(conn)
    conn.close()

@app.post("/perguntar")
def perguntar(req: QuestionRequest):
    sql, rows = run_user_query(req.pergunta)
    resposta = make_human_answer(req.pergunta, rows, sql)
    return {"resposta": resposta, "sql": sql, "linhas": rows}
=======
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello, world!"}
