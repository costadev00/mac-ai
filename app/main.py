import os
from fastapi import FastAPI, HTTPException
from .db import (
    get_db_connection,
    create_schema_index_table,
    load_schema_embeddings,
    search_schema,
    execute_select,
)
from .agent import generate_sql
from .models import QuestionRequest, AnswerResponse

app = FastAPI(title="Perguntas SQL")

@app.on_event("startup")
def startup_event():
    db_url = os.getenv("DATABASE_URL")
    conn = get_db_connection(db_url)
    create_schema_index_table(conn)
    load_schema_embeddings(conn)
    conn.close()

@app.post("/perguntar", response_model=AnswerResponse)
def perguntar(req: QuestionRequest):
    question = req.pergunta
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
        result = execute_select(conn, sql)
    except Exception as e:
        conn.close()
        raise HTTPException(400, str(e))
    conn.close()
    return {"sql": sql, "resultado": result}
