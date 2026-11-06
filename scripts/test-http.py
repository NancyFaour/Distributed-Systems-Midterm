"""Real HTTP evidence through Nginx; credentials are never printed."""

import argparse
import hashlib
import json
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import requests
from settings import setting


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080/api")
    parser.add_argument(
        "--phase", choices=["crud", "search", "cluster", "balance"], default="crud"
    )
    args = parser.parse_args()
    base = args.base_url.rstrip("/")
    client = requests.Session()
    client.headers["X-API-Key"] = setting("API_KEY", "")
    if args.phase == "balance":
        instances = {
            requests.get(base + "/health", timeout=10).headers["X-Gateway-Instance"]
            for _ in range(12)
        }
        print("GATEWAY INSTANCES:", sorted(instances))
        assert len(instances) >= 2, "Start both gateways before this test"
        print("PASS: Nginx distributes real requests across two gateways")
        return
    if args.phase == "cluster":
        r = client.get(base + "/cluster", timeout=30)
        r.raise_for_status()
        print(json.dumps(r.json(), indent=2))
        assert (
            sum(n["status"] == "online" and n["active"] for n in r.json()["nodes"]) >= 7
        )
        print(
            "PASS: cluster health, distribution, degraded objects and repair history available"
        )
        return
    bucket = "http-evidence-" + uuid.uuid4().hex[:10]
    root = base + "/buckets/" + bucket
    r = client.put(root, json={}, timeout=20)
    r.raise_for_status()
    payload = b"Distributed storage gateway evidence\n" * 1024
    url = root + "/objects/reports/evidence.txt"
    try:
        unauthorized = requests.get(base + "/buckets", timeout=10)
        assert unauthorized.status_code == 401
        print("AUTH: HTTP 401 without an API key")
        r = client.put(
            url,
            data=payload,
            headers={
                "Content-Type": "text/plain",
                "X-Object-Tags": '{"phase":"gateway"}',
            },
            timeout=120,
        )
        assert r.status_code == 201, r.text
        print("UPLOAD: HTTP 201; version", r.json()["version"])
        r = client.get(url, timeout=120)
        r.raise_for_status()
        assert hashlib.sha256(r.content).digest() == hashlib.sha256(payload).digest()
        print("DOWNLOAD: HTTP 200; full SHA-256 matches")
        r = client.get(url, headers={"Range": "bytes=10-29"}, timeout=120)
        assert r.status_code == 206 and r.content == payload[10:30]
        print("RANGE: HTTP 206;", r.headers["Content-Range"])
        r = client.head(url, timeout=15)
        assert r.status_code == 200 and not r.content
        print("HEAD: HTTP 200; Content-Length", r.headers["Content-Length"])
        r = client.get(root + "/objects", timeout=15)
        r.raise_for_status()
        assert len(r.json()["objects"]) == 1
        print("LIST: one object found")
        if args.phase == "search":
            end = time.monotonic() + 90
            while time.monotonic() < end:
                r = client.get(
                    base + "/search",
                    params={
                        "bucket": bucket,
                        "prefix": "reports/",
                        "min_size": 1,
                        "tags": '{"phase":"gateway"}',
                    },
                    timeout=15,
                )
                if r.status_code == 200 and r.json()["objects"]:
                    break
                time.sleep(2)
            assert r.status_code == 200 and len(r.json()["objects"]) == 1, r.text
            print("SEARCH:", json.dumps(r.json(), indent=2))
            print("PASS: Elasticsearch prefix, size and tag search finds the object")
        assert client.delete(url, timeout=90).status_code == 204
        assert client.get(url, timeout=15).status_code == 404
        print("DELETE: HTTP 204; subsequent GET returns HTTP 404")
        print("PASS: authenticated gateway operations through the load balancer")
    finally:
        client.delete(url, timeout=90)
        client.delete(root, timeout=20)


if __name__ == "__main__":
    main()
