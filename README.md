# mac-ai

A small FastAPI app that uses OpenAI to translate natural language questions in Portuguese into SQL queries for a PostgreSQL database.  The schema is embedded with pgvector so the LLM has context when generating SQL.

## Running with Docker Compose

1. Create an `.env` file with your OpenAI API key:

```bash
OPENAI_API_KEY=sk-...
```

2. Start the services:

```bash
docker compose up --build
```

The API will be available at `http://localhost:8000` and PostgreSQL at port `5432`.

## Refreshing the Schema Index

Whenever the database schema changes you should refresh the vector index used for lookup.

```bash
docker compose exec app python -m app.refresh_schema
```

This truncates and rebuilds the `schema_index` table with new embeddings.

## Example Usage

Ask a question via the `/perguntar` endpoint:

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"pergunta": "Liste todos os registros da tabela exemplo"}' \
  http://localhost:8000/perguntar
```

The response contains the generated SQL and the query result.

## Architecture Overview

```
+---------+        HTTP       +--------------+        SQL        +-----------+
| Client  |  <--------------> |   FastAPI    |  <------------->  | Postgres  |
| (curl)  |                   |  (mac-ai)    |                  |  +pgvector |
+---------+                   +--------------+                  +-----------+
                                 ^        |
                                 |        v
                              OpenAI  Embeddings
```

1. On startup the app reads the database schema and stores embeddings in the `schema_index` table.
2. A question is embedded and compared with `schema_index` to gather context.
3. The LLM generates a `SELECT` statement which is executed against the database using a readonly user.
4. Results are returned to the caller.

