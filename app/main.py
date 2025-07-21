import os
import logging
import structlog
from fastapi import FastAPI, HTTPException
from .db import (
    get_db_connection,
    create_schema_index_table,
    load_schema_embeddings,
)
from .sql_chain import run_user_query
from .models import QuestionRequest, AnswerResponse

os.makedirs("logs", exist_ok=True)
logging.basicConfig(filename="logs/app.log", level=logging.INFO, format="%(message)s")
structlog.configure(processors=[structlog.processors.JSONRenderer()])

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
    try:
        return run_user_query(req.pergunta)
    except Exception as e:
        raise HTTPException(400, str(e))
