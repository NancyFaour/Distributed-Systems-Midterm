import hashlib
import io
from itertools import combinations
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from chunking import fixed_chunks, content_defined_chunks
from consistent_hashing import ConsistentHashRing
from redundancy import Policy
from gateway import parse_range
import storage_node


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, (0, 99, False)),
        ("bytes=10-19", (10, 19, True)),
        ("bytes=95-", (95, 99, True)),
        ("bytes=-10", (90, 99, True)),
        ("bytes=0-999", (0, 99, True)),
    ],
)
def test_byte_ranges(value, expected):
    assert parse_range(value, 100) == expected


@pytest.mark.parametrize(
    "value",
    [
        "bytes=100-",
        "bytes=20-10",
        "bytes=-0",
        "bytes=0-1,3-4",
        "x=1-2",
        "bytes=-",
        "bytes=" + ("9" * 5000) + "-",
    ],
)
def test_invalid_ranges(value):
    with pytest.raises(HTTPException) as error:
        parse_range(value, 100)
    assert error.value.status_code == 416


def test_empty_object_range():
    assert parse_range(None, 0) == (0, -1, False)
    with pytest.raises(HTTPException):
        parse_range("bytes=0-", 0)


def test_erasure_any_four_and_quorum():
    data = bytes(range(256)) * 63 + b"odd"
    digest = hashlib.sha256(data).hexdigest()
    policy = Policy(mode="erasure")
    shards = policy.encode(data)
    for indices in combinations(range(6), 4):
        assert policy.decode({i: shards[i] for i in indices}, len(data), digest) == data
    with pytest.raises(RuntimeError):
        policy.decode({i: shards[i] for i in range(3)}, len(data), digest)
    with pytest.raises(ValueError):
        Policy(n=3, w=1, r=2)


def test_chunking_and_ring_stability():
    data = bytes(range(256)) * 1000
    for chunks in (
        list(fixed_chunks(io.BytesIO(data), 1024)),
        list(content_defined_chunks(io.BytesIO(data), 1024, 2048, 8192)),
    ):
        assert b"".join(chunks) == data
    ring = ConsistentHashRing(["a", "b", "c", "d"])
    hashes = [hashlib.sha256(str(i).encode()).hexdigest() for i in range(2000)]
    before = {h: ring.get_node(h) for h in hashes}
    ring.add_node("e")
    changed = [h for h in hashes if ring.get_node(h) != before[h]]
    assert 0 < len(changed) < len(hashes) // 2
    assert all(ring.get_node(h) == "e" for h in changed)
    ring.remove_node("e")
    assert all(ring.get_node(h) == before[h] for h in hashes)


def test_chunk_api_integrity_and_delete(tmp_path, monkeypatch):
    monkeypatch.setattr(storage_node, "DATA_DIR", tmp_path)
    client = TestClient(storage_node.app)
    data = b"content addressed chunk"
    digest = hashlib.sha256(data).hexdigest()
    url = "/chunks/" + digest
    assert client.put(url, content=data).status_code == 201
    assert client.get(url).content == data
    assert client.head(url).headers["Content-Length"] == str(len(data))
    assert client.head(url).content == b""
    assert client.put(url, content=b"wrong bytes").status_code == 400
    assert client.get(url).content == data
    (tmp_path / (digest + ".chunk")).write_bytes(b"bit rot")
    assert client.get(url).status_code == 500
    assert client.head(url).status_code == 500
    assert client.put(url, content=data).status_code == 200
    assert client.delete(url).status_code == 204
    assert client.get(url).status_code == 404
