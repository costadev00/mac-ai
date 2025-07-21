import os
from .db import get_db_connection, refresh_schema


def main():
    url = os.getenv("DATABASE_URL")
    conn = get_db_connection(url)
    refresh_schema(conn)
    conn.close()


if __name__ == "__main__":
    main()
