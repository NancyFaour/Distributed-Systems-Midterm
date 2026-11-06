"""Persistent heartbeat registry. Object placement and repair are later phases."""
import os
import sqlite3
import time
from contextlib import asynccontextmanager, closing
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from pydantic import BaseModel, Field

DATA_DIR = Path(os.getenv("METADATA_DATA_DIR", "data/metadata"))
DATABASE = DATA_DIR / "nodes.sqlite3"
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "15"))
POSTGRES = os.getenv('METADATA_BACKEND') == 'postgres'


def connect():
    connection = sqlite3.connect(DATABASE, timeout=5)
    connection.row_factory = sqlite3.Row
    return connection


@asynccontextmanager
async def lifespan(application):
    if POSTGRES:
        from metadata_db import connect_db
        with connect_db() as connection:
            connection.execute('SELECT 1 FROM node_heartbeats LIMIT 1')
        yield
        return
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with closing(connect()) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                storage_url TEXT NOT NULL,
                free_space_bytes INTEGER NOT NULL,
                chunk_count INTEGER NOT NULL,
                last_seen REAL NOT NULL
            )
        """)
        connection.commit()
    yield


app = FastAPI(title="Metadata Heartbeat Service", lifespan=lifespan)


class Heartbeat(BaseModel):
    node_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    storage_url: str = Field(min_length=1, max_length=250)
    free_space_bytes: int = Field(ge=0)
    chunk_count: int = Field(ge=0)


@app.get("/health")
def health():
    if POSTGRES:
        from metadata_db import connect_db
        with connect_db() as connection:
            connection.execute('SELECT 1 FROM node_heartbeats LIMIT 1')
        return {'status':'healthy','backend':'postgresql'}
    with closing(connect()) as connection:
        connection.execute("SELECT COUNT(*) FROM nodes").fetchone()
    return {"status": "healthy"}


@app.post("/heartbeats")
def receive_heartbeat(heartbeat: Heartbeat):
    received = time.time()
    if POSTGRES:
        from metadata_db import connect_db
        with connect_db() as connection:
            connection.execute('''INSERT INTO node_heartbeats VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT(node_id) DO UPDATE SET storage_url=excluded.storage_url,
                free_space_bytes=excluded.free_space_bytes,chunk_count=excluded.chunk_count,last_seen=excluded.last_seen''',
                (heartbeat.node_id,heartbeat.storage_url,heartbeat.free_space_bytes,heartbeat.chunk_count,received))
        return {'accepted':True,'node_id':heartbeat.node_id}
    with closing(connect()) as connection:
        connection.execute("""
            INSERT INTO nodes VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                storage_url=excluded.storage_url,
                free_space_bytes=excluded.free_space_bytes,
                chunk_count=excluded.chunk_count,
                last_seen=excluded.last_seen
        """, (heartbeat.node_id, heartbeat.storage_url,
              heartbeat.free_space_bytes, heartbeat.chunk_count, received))
        connection.commit()
    return {"accepted": True, "node_id": heartbeat.node_id}


@app.get("/nodes")
def list_nodes():
    now = time.time()
    if POSTGRES:
        from metadata_db import connect_db
        with connect_db() as connection:
            rows = connection.execute('SELECT * FROM node_heartbeats ORDER BY node_id').fetchall()
    else:
        with closing(connect()) as connection:
            rows = connection.execute("SELECT * FROM nodes ORDER BY node_id").fetchall()
    nodes = []
    for row in rows:
        entry = dict(row)
        age = max(0, now - entry.pop("last_seen"))
        entry["last_seen"] = datetime.fromtimestamp(
            now - age, timezone.utc).isoformat()
        entry["seconds_since_heartbeat"] = round(age, 2)
        entry["status"] = "online" if age <= HEARTBEAT_TIMEOUT else "offline"
        nodes.append(entry)
    return {"heartbeat_timeout_seconds": HEARTBEAT_TIMEOUT, "nodes": nodes}
