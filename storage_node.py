import hashlib
import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import threading
import urllib.error
import urllib.request
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse

NODE_ID = os.getenv("NODE_ID", "node-1")
PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(
    os.getenv("DATA_DIR", str(PROJECT_DIR / "data" / NODE_ID))
)
DATA_DIR.mkdir(parents=True, exist_ok=True)

MAX_CHUNK_BYTES = 16 * 1024 * 1024
LOCK = threading.RLock()
METADATA_URL = os.getenv("METADATA_URL", "http://127.0.0.1:8010").rstrip("/")
STORAGE_URL = os.getenv("STORAGE_URL", "http://127.0.0.1:8001")
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "5"))
LOGGER = logging.getLogger("uvicorn.error")


def node_statistics():
    with LOCK:
        chunk_count = sum(1 for path in DATA_DIR.glob("*.chunk") if path.is_file())
    return {
        "node_id": NODE_ID,
        "storage_url": STORAGE_URL,
        "free_space_bytes": shutil.disk_usage(DATA_DIR).free,
        "chunk_count": chunk_count,
    }


def heartbeat_loop(stop):
    while not stop.is_set():
        try:
            request = urllib.request.Request(
                f"{METADATA_URL}/heartbeats",
                data=json.dumps(node_statistics()).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=3) as response:
                response.read()
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            LOGGER.warning("Heartbeat for %s failed; retrying: %s", NODE_ID, error)
        stop.wait(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(application):
    if HEARTBEAT_INTERVAL <= 0:
        raise ValueError("HEARTBEAT_INTERVAL_SECONDS must be positive")
    stop = threading.Event()
    worker = threading.Thread(target=heartbeat_loop, args=(stop,), daemon=True)
    worker.start()
    try:
        yield
    finally:
        stop.set()
        await asyncio.to_thread(worker.join, 4)


app = FastAPI(title="Storage Node API", lifespan=lifespan)


def chunk_path(chunk_hash: str) -> Path:
    if not re.fullmatch(r"[0-9a-f]{64}", chunk_hash):
        raise HTTPException(400, "Expected a lowercase SHA-256 hash")
    return DATA_DIR / f"{chunk_hash}.chunk"


def verified_chunk(chunk_hash: str) -> bytes:
    path = chunk_path(chunk_hash)

    try:
        with path.open("rb") as file:
            content = file.read(MAX_CHUNK_BYTES + 1)
    except FileNotFoundError:
        raise HTTPException(404, "Chunk not found")

    if (
        len(content) > MAX_CHUNK_BYTES
        or hashlib.sha256(content).hexdigest() != chunk_hash
    ):
        raise HTTPException(
            500,
            "Chunk corrupted: SHA-256 mismatch",
            headers={"X-Chunk-Integrity": "failed"},
        )

    return content


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "data_directory": str(DATA_DIR.resolve()),
        "heartbeat_interval_seconds": HEARTBEAT_INTERVAL,
        **node_statistics(),
    }


@app.put("/chunks/{chunk_hash}")
async def put_chunk(chunk_hash: str, request: Request):
    destination = chunk_path(chunk_hash)
    digest = hashlib.sha256()
    size = 0

    temporary = tempfile.NamedTemporaryFile(
        dir=DATA_DIR, suffix=".tmp", delete=False
    )
    temporary_path = Path(temporary.name)

    try:
        with temporary:
            async for block in request.stream():
                size += len(block)
                if size > MAX_CHUNK_BYTES:
                    raise HTTPException(413, "Chunk exceeds 16 MiB")

                digest.update(block)
                temporary.write(block)

            temporary.flush()
            os.fsync(temporary.fileno())

        if digest.hexdigest() != chunk_hash:
            raise HTTPException(400, "Uploaded content does not match hash")

        with LOCK:
            existed = destination.exists()
            os.replace(temporary_path, destination)

        return JSONResponse(
            status_code=200 if existed else 201,
            content={
                "node_id": NODE_ID,
                "hash": chunk_hash,
                "size_bytes": size,
            },
        )
    finally:
        temporary_path.unlink(missing_ok=True)


@app.get("/chunks/{chunk_hash}")
def get_chunk(chunk_hash: str):
    with LOCK:
        content = verified_chunk(chunk_hash)

    return Response(
        content=content,
        media_type="application/octet-stream",
        headers={"ETag": f'"{chunk_hash}"', "X-Chunk-Integrity": "verified"},
    )


@app.head("/chunks/{chunk_hash}")
def head_chunk(chunk_hash: str):
    with LOCK:
        content = verified_chunk(chunk_hash)

    return Response(
        headers={
            "Content-Length": str(len(content)),
            "ETag": f'"{chunk_hash}"',
            "X-Chunk-Integrity": "verified",
        }
    )


@app.delete("/chunks/{chunk_hash}", status_code=204)
def delete_chunk(chunk_hash: str):
    with LOCK:
        try:
            chunk_path(chunk_hash).unlink()
        except FileNotFoundError:
            raise HTTPException(404, "Chunk not found")

    return Response(status_code=204)
