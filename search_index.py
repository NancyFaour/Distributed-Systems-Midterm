"""Elasticsearch projection with a transactional PostgreSQL outbox."""

from urllib.parse import quote
import json
import requests
from fastapi.encoders import jsonable_encoder
from metadata_db import connect_db
from settings import setting

INDEX = setting("SEARCH_INDEX", "storage-objects-v1")


def base_url():
    return setting("ELASTICSEARCH_URL", "http://127.0.0.1:9200").rstrip("/")


def ensure_index():
    result = requests.put(
        base_url() + "/" + INDEX,
        json={
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "object_id": {"type": "keyword"},
                    "bucket_name": {"type": "keyword"},
                    "object_key": {
                        "type": "keyword",
                        "fields": {"text": {"type": "text"}},
                    },
                    "version": {"type": "long"},
                    "size_bytes": {"type": "long"},
                    "created_at": {"type": "date"},
                    "tags": {"type": "flattened"},
                    "content_type": {"type": "keyword"},
                }
            },
        },
        timeout=20,
    )
    if (
        result.status_code != 400
        or result.json().get("error", {}).get("type")
        != "resource_already_exists_exception"
    ):
        result.raise_for_status()


def sync_index(batch=200, object_id=None):
    ensure_index()
    done = 0
    with connect_db() as connection:
        pending = connection.execute(
            """SELECT object_id FROM search_outbox
            WHERE (%s::uuid IS NULL OR object_id=%s) ORDER BY updated_at
            LIMIT %s FOR UPDATE SKIP LOCKED""",
            (object_id, object_id, batch),
        ).fetchall()
        lines = []
        for item in pending:
            row = connection.execute(
                "SELECT * FROM object_versions WHERE object_id=%s", (item["object_id"],)
            ).fetchone()
            if row:
                lines.append(
                    json.dumps(
                        {"index": {"_index": INDEX, "_id": str(item["object_id"])}}
                    )
                )
                lines.append(json.dumps(jsonable_encoder(row)))
            else:
                lines.append(
                    json.dumps(
                        {"delete": {"_index": INDEX, "_id": str(item["object_id"])}}
                    )
                )
        if lines:
            response = requests.post(
                base_url() + "/_bulk?refresh=wait_for",
                data=("\n".join(lines) + "\n").encode(),
                headers={"Content-Type": "application/x-ndjson"},
                timeout=30,
            )
            response.raise_for_status()
            failures = [
                op
                for entry in response.json()["items"]
                for op in entry.values()
                if op["status"] not in (200, 201, 404)
            ]
            if failures:
                raise requests.RequestException(
                    "Elasticsearch rejected bulk metadata updates: "
                    + str(failures[0].get("error"))
                )
            for item in pending:
                connection.execute(
                    "DELETE FROM search_outbox WHERE object_id=%s", (item["object_id"],)
                )
            done = len(pending)
    return {"indexed_updates": done}


def search_objects(
    bucket, prefix, text, minimum, maximum, after, before, tags, limit, offset
):
    filters = []
    if bucket:
        filters.append({"term": {"bucket_name": bucket}})
    if prefix:
        filters.append({"prefix": {"object_key": prefix}})
    sizes = {"gte": minimum}
    if maximum is not None:
        sizes["lte"] = maximum
    filters.append({"range": {"size_bytes": sizes}})
    dates = {}
    if after:
        dates["gte"] = after.isoformat()
    if before:
        dates["lte"] = before.isoformat()
    if dates:
        filters.append({"range": {"created_at": dates}})
    for key, value in tags.items():
        filters.append({"term": {"tags." + key: value}})
    query = {"bool": {"filter": filters}}
    if text:
        query["bool"]["must"] = [{"match": {"object_key.text": text}}]
    response = requests.post(
        base_url() + "/" + INDEX + "/_search",
        json={
            "query": query,
            "size": limit,
            "from": offset,
            "track_total_hits": True,
            "sort": [{"created_at": "desc"}, {"object_id": "asc"}],
        },
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    documents = [hit["_source"] for hit in payload["hits"]["hits"]]
    # PostgreSQL is authoritative: stale index entries must never resurrect deleted data.
    ids = [doc["object_id"] for doc in documents]
    with connect_db() as connection:
        live = {
            str(row["object_id"])
            for row in connection.execute(
                "SELECT object_id FROM object_versions WHERE object_id=ANY(%s::uuid[])",
                (ids,),
            ).fetchall()
        }
        pending = connection.execute(
            "SELECT COUNT(*) AS n FROM search_outbox"
        ).fetchone()["n"]
    return {
        "engine": "elasticsearch",
        "objects": [doc for doc in documents if doc["object_id"] in live],
        "indexed_total": payload["hits"]["total"]["value"],
        "pending_updates": pending,
        "consistency": "eventual; results include object versions",
    }
