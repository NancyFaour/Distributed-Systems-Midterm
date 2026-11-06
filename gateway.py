"""Authenticated, versioned, S3-like HTTP gateway (JSON API, not AWS SigV4)."""

import asyncio
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import socket
import tempfile
import time
from uuid import UUID
from urllib.parse import quote

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, Security
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import APIKeyHeader
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field
import requests
from starlette.background import BackgroundTask

from metadata_db import connect_db
from redundancy import Policy
from redundant_store import RedundantStore, QuorumError
from settings import setting

STORE = RedundantStore()
INSTANCE = os.getenv("HOSTNAME", socket.gethostname())
ROOT = Path(__file__).parent
SPOOL = Path(os.getenv("SPOOL_DIR", str(ROOT / "data" / "gateway-spool")))
SPOOL.mkdir(parents=True, exist_ok=True)
MAX_UPLOAD = int(os.getenv("MAX_UPLOAD_BYTES", str(2 * 1024**3)))
REQUESTS = Counter(
    "gateway_requests_total", "Gateway HTTP requests", ["method", "route", "status"]
)
LATENCY = Histogram(
    "gateway_request_seconds", "Gateway response-start latency", ["method", "route"]
)
UPLOAD_BYTES = Counter(
    "gateway_uploaded_bytes_total", "Successfully committed upload bytes"
)
DOWNLOAD_BYTES = Counter(
    "gateway_downloaded_bytes_total", "Bytes yielded to download responses"
)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def authorize(request: Request, key: str | None = Security(api_key_header)):
    configured = [
        item.strip()
        for item in setting("API_KEYS", setting("API_KEY", "")).split(",")
        if item.strip()
    ]
    if not configured:
        raise HTTPException(503, "Configure API_KEY in .env before using the gateway")
    if not key or not any(
        hmac.compare_digest(key.encode(), candidate.encode())
        for candidate in configured
    ):
        raise HTTPException(401, "A valid X-API-Key header is required")
    maximum = int(os.getenv("RATE_LIMIT_PER_MINUTE", "300"))
    window = int(time.time() // 60)
    digest = hashlib.sha256(key.encode()).hexdigest()
    with connect_db() as connection:
        row = connection.execute(
            """INSERT INTO api_rate_limits VALUES (%s,%s,1)
            ON CONFLICT(key_hash,window_id) DO UPDATE SET requests=api_rate_limits.requests+1
            WHERE api_rate_limits.requests < %s RETURNING requests""",
            (digest, window, maximum),
        ).fetchone()
        connection.execute(
            "DELETE FROM api_rate_limits WHERE window_id < %s", (window - 2,)
        )
    if row is None:
        raise HTTPException(
            429,
            "API key rate limit reached",
            headers={"Retry-After": str(60 - int(time.time()) % 60)},
        )


app = FastAPI(
    title="Distributed Object Storage Gateway",
    version="1.0.0",
    description="Versioned JSON object API with SHA-256 storage, replication and Reed–Solomon coding.",
    root_path=os.getenv("ROOT_PATH", ""),
)


@app.middleware("http")
async def metrics(request, call_next):
    started = time.monotonic()
    try:
        response = await call_next(request)
    except Exception:
        route = getattr(request.scope.get("route"), "path", "unmatched")
        REQUESTS.labels(request.method, route, "500").inc()
        LATENCY.labels(request.method, route).observe(time.monotonic() - started)
        raise
    route = getattr(request.scope.get("route"), "path", "unmatched")
    REQUESTS.labels(request.method, route, str(response.status_code)).inc()
    LATENCY.labels(request.method, route).observe(time.monotonic() - started)
    response.headers["X-Gateway-Instance"] = INSTANCE
    return response


@app.exception_handler(QuorumError)
async def quorum_error(request, error):
    return JSONResponse(status_code=503, content={"detail": str(error)})


@app.exception_handler(KeyError)
async def missing_error(request, error):
    return JSONResponse(status_code=404, content={"detail": "Object not found"})


@app.get("/health")
def health():
    with connect_db() as connection:
        connection.execute("SELECT 1")
    return {"status": "healthy", "instance": INSTANCE}


class BucketPolicy(BaseModel):
    mode: str = "replication"
    n: int = Field(default=3, ge=1, le=64)
    w: int = Field(default=2, ge=1, le=64)
    r: int = Field(default=2, ge=1, le=64)
    k: int = Field(default=4, ge=1, le=255)
    m: int = Field(default=2, ge=1, le=255)


def validate_bucket(bucket):
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise HTTPException(
            400, "Bucket names require 3–63 lowercase letters, digits, dots or hyphens"
        )


def require_bucket(bucket):
    validate_bucket(bucket)
    with connect_db() as connection:
        if not connection.execute(
            "SELECT 1 FROM buckets WHERE bucket_name=%s", (bucket,)
        ).fetchone():
            raise HTTPException(404, "Bucket not found")


def validate_tags(tags):
    if (
        not isinstance(tags, dict)
        or len(tags) > 20
        or any(
            not isinstance(k, str)
            or not isinstance(v, str)
            or not 1 <= len(k) <= 64
            or len(v) > 256
            or "." in k
            for k, v in tags.items()
        )
    ):
        raise HTTPException(
            400,
            "Tags must be a JSON object with at most 20 string values; keys cannot contain dots",
        )
    return tags


def resolve(bucket, key, version=None):
    require_bucket(bucket)
    with connect_db() as connection:
        row = connection.execute(
            """SELECT * FROM object_versions WHERE bucket_name=%s AND object_key=%s
            AND (%s::bigint IS NULL OR version=%s) ORDER BY version DESC LIMIT 1""",
            (bucket, key, version, version),
        ).fetchone()
    if row is None:
        raise HTTPException(404, "Object/version not found")
    return row


@app.get("/buckets", dependencies=[Depends(authorize)])
def list_buckets():
    with connect_db() as connection:
        return {
            "buckets": connection.execute("""SELECT b.*,p.policy_key,COUNT(v.object_id) AS versions,
            COALESCE(SUM(v.size_bytes),0) AS logical_bytes FROM buckets b LEFT JOIN bucket_policies p
            USING(bucket_name) LEFT JOIN object_versions v USING(bucket_name)
            GROUP BY b.bucket_name,p.policy_key ORDER BY b.bucket_name""").fetchall()
        }


@app.put("/buckets/{bucket}", status_code=201, dependencies=[Depends(authorize)])
def create_bucket(bucket: str, policy: BucketPolicy = BucketPolicy()):
    validate_bucket(bucket)
    try:
        value = Policy(**policy.model_dump())
        if value.total > len(STORE.active_nodes()):
            raise ValueError("Policy exceeds the number of active storage nodes")
        STORE.configure_bucket(bucket, value)
    except ValueError as error:
        raise HTTPException(400, str(error))
    return {"bucket": bucket, "policy": value.to_dict(), "policy_key": value.key}


@app.delete("/buckets/{bucket}", status_code=204, dependencies=[Depends(authorize)])
def delete_bucket(bucket: str):
    require_bucket(bucket)
    with connect_db() as connection:
        connection.execute(
            "SELECT 1 FROM buckets WHERE bucket_name=%s FOR UPDATE", (bucket,)
        )
        if connection.execute(
            "SELECT 1 FROM object_versions WHERE bucket_name=%s LIMIT 1", (bucket,)
        ).fetchone():
            raise HTTPException(
                409, "Delete all object versions before deleting the bucket"
            )
        connection.execute(
            "DELETE FROM bucket_policies WHERE bucket_name=%s", (bucket,)
        )
        connection.execute("DELETE FROM buckets WHERE bucket_name=%s", (bucket,))


@app.get("/buckets/{bucket}/objects", dependencies=[Depends(authorize)])
def list_objects(
    bucket: str,
    prefix: str = "",
    versions: bool = False,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    require_bucket(bucket)
    with connect_db() as connection:
        rows = connection.execute(
            """SELECT * FROM (
            SELECT v.*,row_number() OVER(PARTITION BY object_key ORDER BY version DESC) AS position
            FROM object_versions v WHERE bucket_name=%s AND starts_with(object_key,%s)
        ) q WHERE (%s OR position=1) ORDER BY object_key,version DESC LIMIT %s OFFSET %s""",
            (bucket, prefix, versions, limit + 1, offset),
        ).fetchall()
    return {
        "objects": rows[:limit],
        "next_offset": offset + limit if len(rows) > limit else None,
    }


@app.put(
    "/buckets/{bucket}/objects/{key:path}",
    status_code=201,
    dependencies=[Depends(authorize)],
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/octet-stream": {
                    "schema": {"type": "string", "format": "binary"}
                }
            },
        }
    },
)
async def upload_object(
    bucket: str,
    key: str,
    request: Request,
    chunking: str = Query("fixed", pattern="^(fixed|cdc)$"),
):
    await asyncio.to_thread(require_bucket, bucket)
    if not key or len(key.encode()) > 1024 or any(ord(c) < 32 for c in key):
        raise HTTPException(
            400, "Object key requires 1–1024 UTF-8 bytes without control characters"
        )
    try:
        tags = validate_tags(json.loads(request.headers.get("X-Object-Tags", "{}")))
    except (json.JSONDecodeError, UnicodeError):
        raise HTTPException(400, "X-Object-Tags must contain a JSON object")
    content_type = request.headers.get(
        "Content-Type", "application/octet-stream"
    ).split(";")[0]
    if len(content_type) > 200 or any(ord(c) < 32 for c in content_type):
        raise HTTPException(400, "Invalid Content-Type")
    fd, name = tempfile.mkstemp(dir=SPOOL)
    path = Path(name)
    size, digest = 0, hashlib.sha256()
    try:
        with os.fdopen(fd, "wb") as destination:
            async for block in request.stream():
                size += len(block)
                if size > MAX_UPLOAD:
                    raise HTTPException(413, "Upload exceeds MAX_UPLOAD_BYTES")
                digest.update(block)
                await asyncio.to_thread(destination.write, block)
        obj = await asyncio.to_thread(
            STORE.put_file,
            bucket,
            key,
            path,
            chunking,
            {
                "tags": tags,
                "content_type": content_type,
                "content_sha256": digest.hexdigest(),
            },
        )
        UPLOAD_BYTES.inc(size)
        return obj
    finally:
        path.unlink(missing_ok=True)


def parse_range(value, size):
    if value is None:
        return 0, size - 1, False
    match = re.fullmatch(r"bytes=(\d*)-(\d*)", value) if len(value) <= 200 else None
    if not match or not any(match.groups()) or size == 0:
        raise HTTPException(
            416,
            "Only one satisfiable byte range is supported",
            headers={"Content-Range": f"bytes */{size}"},
        )
    first, last = match.groups()
    if not first:
        length = int(last)
        start, end = max(0, size - length), size - 1
        invalid = length == 0
    else:
        start, end = int(first), min(int(last), size - 1) if last else size - 1
        invalid = start >= size or end < start
    if invalid:
        raise HTTPException(
            416,
            "Byte range outside the object",
            headers={"Content-Range": f"bytes */{size}"},
        )
    return start, end, True


def object_headers(row):
    return {
        "ETag": '"' + str(row.get("content_sha256") or row["object_id"]) + '"',
        "X-Object-Version": str(row["version"]),
        "Accept-Ranges": "bytes",
        "Content-Type": row["content_type"],
    }


@app.head("/buckets/{bucket}/objects/{key:path}", dependencies=[Depends(authorize)])
def head_object(bucket: str, key: str, version: int | None = None):
    row = resolve(bucket, key, version)
    return Response(
        headers={**object_headers(row), "Content-Length": str(row["size_bytes"])}
    )


@app.get("/buckets/{bucket}/objects/{key:path}", dependencies=[Depends(authorize)])
def download_object(
    bucket: str, key: str, request: Request, version: int | None = None
):
    row = resolve(bucket, key, version)
    headers = object_headers(row)
    if request.headers.get("If-None-Match") == headers["ETag"]:
        return Response(status_code=304, headers=headers)
    value = request.headers.get("Range")
    if request.headers.get("If-Range") not in (None, headers["ETag"]):
        value = None
    start, end, partial = parse_range(value, row["size_bytes"])
    fd, name = tempfile.mkstemp(dir=SPOOL)
    os.close(fd)
    path = Path(name)
    try:
        # Verify/reconstruct before sending headers: corruption becomes an HTTP
        # failure rather than a truncated successful response. RAM stays bounded.
        STORE.download_file(row["object_id"], path)
    except Exception:
        path.unlink(missing_ok=True)
        raise

    def blocks():
        try:
            with path.open("rb") as source:
                source.seek(start)
                remaining = max(0, end - start + 1)
                while remaining:
                    block = source.read(min(65536, remaining))
                    if not block:
                        break
                    remaining -= len(block)
                    DOWNLOAD_BYTES.inc(len(block))
                    yield block
        finally:
            path.unlink(missing_ok=True)

    headers["Content-Length"] = str(max(0, end - start + 1))
    headers["Content-Disposition"] = "attachment; filename*=UTF-8''" + quote(
        key.rsplit("/", 1)[-1], safe=""
    )
    if partial:
        headers["Content-Range"] = f"bytes {start}-{end}/{row['size_bytes']}"
    return StreamingResponse(
        blocks(),
        status_code=206 if partial else 200,
        headers=headers,
        background=BackgroundTask(path.unlink, missing_ok=True),
    )


@app.delete(
    "/buckets/{bucket}/objects/{key:path}",
    status_code=204,
    dependencies=[Depends(authorize)],
)
def delete_object(bucket: str, key: str, version: int | None = None):
    row = resolve(bucket, key, version)
    STORE.delete_object(row["object_id"])


@app.get("/buckets/{bucket}/metadata/{key:path}", dependencies=[Depends(authorize)])
def metadata(bucket: str, key: str, version: int | None = None):
    row = resolve(bucket, key, version)
    return {
        **row,
        "chunk_hashes": STORE.get_metadata(row["object_id"])["chunk_hashes"],
        "fragments": STORE.fragments(row["object_id"]),
    }


@app.put("/buckets/{bucket}/tags/{key:path}", dependencies=[Depends(authorize)])
def update_tags(bucket: str, key: str, tags: dict, version: int | None = None):
    row = resolve(bucket, key, version)
    validate_tags(tags)
    with connect_db() as connection:
        return connection.execute(
            "UPDATE object_versions SET tags=%s WHERE object_id=%s RETURNING *",
            (Jsonb(tags), row["object_id"]),
        ).fetchone()


@app.get("/search", dependencies=[Depends(authorize)])
def search(
    bucket: str | None = None,
    prefix: str = "",
    q: str = "",
    min_size: int = Query(0, ge=0),
    max_size: int | None = Query(None, ge=0),
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    tags: str = "{}",
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0, le=9000),
):
    from search_index import search_objects

    try:
        wanted = validate_tags(json.loads(tags))
    except json.JSONDecodeError:
        raise HTTPException(400, "tags must be a JSON object")
    if max_size is not None and max_size < min_size:
        raise HTTPException(400, "max_size must be at least min_size")
    if created_after and created_after.tzinfo is None:
        created_after = created_after.replace(tzinfo=timezone.utc)
    if created_before and created_before.tzinfo is None:
        created_before = created_before.replace(tzinfo=timezone.utc)
    if created_before and created_after and created_after > created_before:
        raise HTTPException(400, "created_after must precede created_before")
    try:
        return search_objects(
            bucket,
            prefix,
            q,
            min_size,
            max_size,
            created_after,
            created_before,
            wanted,
            limit,
            offset,
        )
    except requests.RequestException:
        raise HTTPException(
            503,
            "Elasticsearch is unavailable; ordinary object listing remains available",
        )


@app.get("/cluster", dependencies=[Depends(authorize)])
def cluster():
    from cluster_status import cluster_status

    return cluster_status(STORE)


@app.get("/jobs", dependencies=[Depends(authorize)])
def jobs():
    with connect_db() as connection:
        return {
            "jobs": connection.execute(
                "SELECT * FROM maintenance_jobs ORDER BY created_at DESC LIMIT 50"
            ).fetchall()
        }


@app.post("/jobs/{kind}", status_code=202, dependencies=[Depends(authorize)])
def submit_job(kind: str):
    from job_queue import enqueue

    if kind not in ("repair", "scrub", "rebalance", "gc", "search"):
        raise HTTPException(400, "Unknown maintenance job type")
    return enqueue(kind)


@app.get("/jobs/{job_id}", dependencies=[Depends(authorize)])
def job_status(job_id: UUID):
    with connect_db() as connection:
        row = connection.execute(
            "SELECT * FROM maintenance_jobs WHERE job_id=%s", (job_id,)
        ).fetchone()
    if row is None:
        raise HTTPException(404, "Job not found")
    return row


@app.put("/cluster/members/{node_id}", dependencies=[Depends(authorize)])
def membership(node_id: str, active: bool = True):
    from job_queue import enqueue

    if node_id not in STORE.nodes:
        raise HTTPException(
            400, "Node must be configured in STORAGE_NODES on all gateways and workers"
        )
    with connect_db() as connection:
        connection.execute("SELECT pg_advisory_xact_lock(734983016)")
        members = {
            r["node_id"]
            for r in connection.execute(
                "SELECT * FROM storage_members WHERE active"
            ).fetchall()
        }
        wanted = members | {node_id} if active else members - {node_id}
        policies = connection.execute("""SELECT DISTINCT p.policy FROM object_policies o
            JOIN redundancy_policies p USING(policy_key)""").fetchall()
        if any(Policy(**r["policy"]).total > len(wanted) for r in policies):
            raise HTTPException(
                409,
                "Too few nodes would remain to satisfy an existing redundancy policy",
            )
        if active:
            registry = requests.get(STORE.metadata_url + "/nodes", timeout=5).json()[
                "nodes"
            ]
            if not any(
                n["node_id"] == node_id and n["status"] == "online" for n in registry
            ):
                raise HTTPException(
                    409, "Start the node and wait for its heartbeat before adding it"
                )
        connection.execute(
            """INSERT INTO storage_members(node_id,active) VALUES (%s,%s)
            ON CONFLICT(node_id) DO UPDATE SET active=excluded.active,changed_at=now()""",
            (node_id, active),
        )
    return {"node_id": node_id, "active": active, "job": enqueue("rebalance")}


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics():
    from cluster_status import cluster_metrics

    return Response(
        generate_latest() + cluster_metrics(STORE), media_type=CONTENT_TYPE_LATEST
    )
