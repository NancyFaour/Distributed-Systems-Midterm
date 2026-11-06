"""Verify a real 100 MiB streamed HTTP upload, download and byte range."""

import argparse
import hashlib
from pathlib import Path
import random
import sys
import tempfile
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import requests
from settings import setting


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default="http://127.0.0.1:8080/api")
    p.add_argument("--mib", type=int, default=100)
    a = p.parse_args()
    if not 1 <= a.mib <= 1024:
        raise SystemExit("Use between 1 and 1024 MiB")
    token = uuid.uuid4().hex[:10]
    with requests.Session() as client, tempfile.TemporaryDirectory() as directory:
        client.headers["X-API-Key"] = setting("API_KEY", "")
        root = a.base_url.rstrip("/") + "/buckets/large-gateway-" + token
        client.put(root, json={}, timeout=30).raise_for_status()
        path = Path(directory) / "large.bin"
        digest = hashlib.sha256()
        rng = random.Random(token)
        with path.open("wb") as source:
            for _ in range(a.mib):
                block = rng.randbytes(1024 * 1024)
                source.write(block)
                digest.update(block)
        url = root + "/objects/large.bin"
        try:
            with path.open("rb") as source:
                uploaded = client.put(
                    url,
                    data=source,
                    headers={"Content-Type": "application/octet-stream"},
                    timeout=600,
                )
            uploaded.raise_for_status()
            assert (
                uploaded.status_code == 201
                and uploaded.json()["size_bytes"] == a.mib * 1024 * 1024
            )
            print(
                "UPLOAD: HTTP 201;",
                uploaded.json()["size_bytes"],
                "bytes;",
                len(uploaded.json()["chunk_hashes"]),
                "ordered chunks",
                flush=True,
            )
            actual = hashlib.sha256()
            count = 0
            with client.get(url, stream=True, timeout=600) as response:
                response.raise_for_status()
                for block in response.iter_content(1024 * 1024):
                    actual.update(block)
                    count += len(block)
            assert (
                count == a.mib * 1024 * 1024
                and actual.hexdigest() == digest.hexdigest()
            )
            print(
                "DOWNLOAD: HTTP 200;",
                count,
                "bytes; exact SHA-256",
                actual.hexdigest(),
                flush=True,
            )
            with path.open("rb") as source:
                source.seek(-1024, 2)
                last = source.read()
            response = client.get(url, headers={"Range": "bytes=-1024"}, timeout=600)
            assert response.status_code == 206 and response.content == last
            print("RANGE: HTTP 206;", response.headers["Content-Range"], flush=True)
            print(
                "PASS: a real large file streamed through Nginx and FastAPI and reconstructed exactly",
                flush=True,
            )
        finally:
            client.delete(url, timeout=180)
            client.delete(root, timeout=30)


if __name__ == "__main__":
    main()
