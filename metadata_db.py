import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

ENV_FILE = Path(__file__).resolve().parent / ".env"


def connect_db():
    settings = {}

    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                name, value = line.split("=", 1)
                settings[name.strip()] = value.strip().strip("\"'")

    password = os.getenv(
        "POSTGRES_PASSWORD",
        settings.get("POSTGRES_PASSWORD"),
    )

    if not password:
        raise RuntimeError("POSTGRES_PASSWORD is missing from .env")

    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname="storage",
        user="storage",
        password=password,
        row_factory=dict_row,
        connect_timeout=10,
    )


if __name__ == "__main__":
    with connect_db() as connection:
        database = connection.execute(
            "SELECT current_database() AS name"
        ).fetchone()

        tables = connection.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """).fetchall()

        expected = {
            "buckets",
            "object_versions",
            "chunks",
            "object_chunks",
            "chunk_locations",
        }
        found = {row["table_name"] for row in tables}

        assert expected <= found, f"Missing tables: {expected - found}"

        print("Connected database:", database["name"])
        for table in sorted(expected):
            print("FOUND:", table)

        print("PASS: Python connects to PostgreSQL and finds all metadata tables")