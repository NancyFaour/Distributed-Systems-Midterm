import hashlib
import json
import os
import time
import uuid

import pytest
from fastapi.testclient import TestClient
from gateway import app
from metadata_db import connect_db

pytestmark = pytest.mark.integration


@pytest.fixture
def client(monkeypatch):
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 with the Docker lab running")
    key = "pytest-" + uuid.uuid4().hex
    monkeypatch.setenv("API_KEY", key)
    monkeypatch.delenv("API_KEYS", raising=False)
    monkeypatch.setenv("RATE_LIMIT_PER_MINUTE", "10000")
    with TestClient(app) as session:
        session.headers["X-API-Key"] = key
        yield session


@pytest.fixture
def bucket(client):
    name = "pytest-" + uuid.uuid4().hex[:12]
    assert client.put("/buckets/" + name, json={}).status_code == 201
    yield name
    versions = client.get(
        "/buckets/" + name + "/objects?versions=true&limit=1000"
    ).json()["objects"]
    for row in versions:
        assert (
            client.delete(
                "/buckets/" + name + "/objects/" + row["object_key"],
                params={"version": row["version"]},
            ).status_code
            == 204
        )
    assert client.delete("/buckets/" + name).status_code == 204


def test_authentication_and_shared_rate_limit(client, monkeypatch):
    assert client.get("/buckets", headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get("/buckets").status_code == 200
    monkeypatch.setenv("RATE_LIMIT_PER_MINUTE", "1")
    assert client.get("/buckets").status_code == 429
    assert "Retry-After" in client.get("/buckets").headers


@pytest.mark.parametrize("mode", ["replication", "erasure"])
def test_returning_nodes_revalidate_missing_fragments(client, tmp_path, monkeypatch, mode):
    from redundant_store import RedundantStore
    from redundancy import Policy

    store = RedundantStore()
    name = "pytest-return-" + uuid.uuid4().hex[:12]
    store.configure_bucket(name, Policy(mode=mode))
    payload = uuid.uuid4().bytes * 1003
    source = tmp_path / "source.bin"
    source.write_bytes(payload)
    obj = store.put_file(name, "returning.bin", source)
    try:
        # Hold the same scan lease as workers to make the outage deterministic
        # without stopping public storage nodes or changing unrelated objects.
        with connect_db() as lease:
            lease.execute("SELECT pg_advisory_lock(734983015)")
            real_get = __import__("requests").get

            def offline_registry(url, *args, **kwargs):
                response = real_get(url, *args, **kwargs)
                if url == store.metadata_url + "/nodes":
                    document = response.json()
                    for node in document["nodes"]:
                        node["status"] = "offline"
                    response._content = json.dumps(document).encode()
                return response

            with monkeypatch.context() as outage:
                outage.setattr("redundant_store.requests.get", offline_registry)
                events = store.maintain(object_id=obj["object_id"])
            assert events and all(event["outcome"] == "unrecoverable" for event in events)
            assert all(f["status"] == "missing" for f in store.fragments(obj["object_id"]))

            # Real heartbeats are online again. Existing bytes must be fetched
            # and hash-checked before all stale missing labels become healthy.
            assert store.maintain(object_id=obj["object_id"]) == []
            assert all(f["status"] == "healthy" for f in store.fragments(obj["object_id"]))
            destination = tmp_path / "restored.bin"
            store.download_file(obj["object_id"], destination)
            assert destination.read_bytes() == payload
    finally:
        store.delete_object(obj["object_id"])
        assert client.delete("/buckets/" + name).status_code == 204


def test_versioned_upload_download_ranges_tags_and_delete(client, bucket):
    root = "/buckets/" + bucket
    content = b"0123456789" * 1000
    first = client.put(
        root + "/objects/reports/demo.txt",
        content=content,
        headers={
            "Content-Type": "text/plain",
            "X-Object-Tags": '{"course":"distributed"}',
        },
    )
    assert first.status_code == 201, first.text
    v1 = first.json()["version"]
    assert first.json()["content_sha256"] == hashlib.sha256(content).hexdigest()
    assert client.get(root + "/objects/reports/demo.txt").content == content
    response = client.get(
        root + "/objects/reports/demo.txt", headers={"Range": "bytes=10-29"}
    )
    assert response.status_code == 206 and response.content == content[10:30]
    assert response.headers["Content-Range"] == "bytes 10-29/10000"
    assert (
        client.get(
            root + "/objects/reports/demo.txt", headers={"Range": "bytes=-10"}
        ).content
        == content[-10:]
    )
    assert (
        client.get(
            root + "/objects/reports/demo.txt", headers={"Range": "bytes=10000-"}
        ).status_code
        == 416
    )
    head = client.head(root + "/objects/reports/demo.txt")
    assert (
        head.status_code == 200
        and head.content == b""
        and head.headers["Content-Length"] == "10000"
    )
    assert (
        client.get(
            root + "/objects/reports/demo.txt",
            headers={"If-None-Match": head.headers["ETag"]},
        ).status_code
        == 304
    )
    second = client.put(
        root + "/objects/reports/demo.txt?chunking=cdc", content=b"new version"
    )
    assert second.status_code == 201
    assert client.get(root + "/objects/reports/demo.txt").content == b"new version"
    assert (
        client.get(root + "/objects/reports/demo.txt", params={"version": v1}).content
        == content
    )
    assert len(client.get(root + "/objects?versions=true").json()["objects"]) == 2
    assert len(client.get(root + "/objects?prefix=reports/").json()["objects"]) == 1
    assert client.get(
        root + "/metadata/reports/demo.txt", params={"version": v1}
    ).json()["tags"] == {"course": "distributed"}
    assert (
        client.put(
            root + "/tags/reports/demo.txt", json={"reviewed": "yes"}
        ).status_code
        == 200
    )
    assert client.delete(root).status_code == 409
    assert client.delete(root + "/objects/reports/demo.txt").status_code == 204
    assert client.get(root + "/objects/reports/demo.txt").content == content


def test_empty_object_and_validation(client, bucket, monkeypatch):
    assert (
        client.put("/buckets/" + bucket + "/objects/empty", content=b"").status_code
        == 201
    )
    assert client.get("/buckets/" + bucket + "/objects/empty").content == b""
    assert (
        client.get(
            "/buckets/" + bucket + "/objects/empty", headers={"Range": "bytes=0-"}
        ).status_code
        == 416
    )
    assert (
        client.put(
            "/buckets/" + bucket + "/objects/bad",
            content=b"x",
            headers={"X-Object-Tags": "[]"},
        ).status_code
        == 400
    )
    assert client.put("/buckets/BAD_NAME", json={}).status_code == 400
    assert (
        client.put("/buckets/" + bucket + "-bad", json={"w": 1, "r": 2}).status_code
        == 400
    )
    import gateway

    monkeypatch.setattr(gateway, "MAX_UPLOAD", 10)
    assert (
        client.put(
            "/buckets/" + bucket + "/objects/too-big", content=b"x" * 11
        ).status_code
        == 413
    )


def test_erasure_bucket_multichunk_download_and_dedup(client):
    name = "pytest-ec-" + uuid.uuid4().hex[:10]
    assert client.put("/buckets/" + name, json={"mode": "erasure"}).status_code == 201
    payload = os.urandom(4 * 1024 * 1024 + 123)
    try:
        first = client.put(
            "/buckets/" + name + "/objects/large.bin", content=payload
        ).json()
        second = client.put(
            "/buckets/" + name + "/objects/copy.bin", content=payload
        ).json()
        assert second["uploaded_unique_chunks"] == 0
        assert client.get("/buckets/" + name + "/objects/large.bin").content == payload
        assert (
            client.delete("/buckets/" + name + "/objects/large.bin").status_code == 204
        )
        assert client.get("/buckets/" + name + "/objects/copy.bin").content == payload
        assert (
            client.delete("/buckets/" + name + "/objects/copy.bin").status_code == 204
        )
    finally:
        for row in client.get("/buckets/" + name + "/objects?versions=true").json()[
            "objects"
        ]:
            client.delete(
                "/buckets/" + name + "/objects/" + row["object_key"],
                params={"version": row["version"]},
            )
        client.delete("/buckets/" + name)


def test_elasticsearch_search_and_cluster(client, bucket):
    payload = b'{"test":"search metadata"}'
    uploaded = client.put(
        "/buckets/" + bucket + "/objects/reports/search.json",
        content=payload,
        headers={"X-Object-Tags": '{"topic":"storage"}'},
    )
    assert uploaded.status_code == 201
    from search_index import sync_index

    # Process this fixture rather than assuming the global indexing backlog is empty.
    sync_index(object_id=uploaded.json()["object_id"])
    response = client.get(
        "/search",
        params={
            "bucket": bucket,
            "prefix": "reports/",
            "min_size": 1,
            "max_size": 1000,
            "tags": json.dumps({"topic": "storage"}),
            "created_after": "2020-01-01T00:00:00Z",
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["engine"] == "elasticsearch"
    assert len(response.json()["objects"]) == 1
    assert (
        client.get(
            "/search", params={"bucket": bucket, "tags": '{"topic":"other"}'}
        ).json()["objects"]
        == []
    )
    assert (
        client.get("/search", params={"min_size": 100, "max_size": 1}).status_code
        == 400
    )
    state = client.get("/cluster")
    assert state.status_code == 200 and len(state.json()["nodes"]) >= 7
    assert "storage_degraded_objects" in client.get("/metrics").text
