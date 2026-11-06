"""Verify real metadata availability after deletion of the current HA primary pod."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import requests
from settings import setting
from scripts.kubernetes_access import gateway_connection, compose_resource_window


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--context", default="kind-midterm")
    p.add_argument("--base-url", default="http://127.0.0.1:8081/api")
    p.add_argument("--pause-compose", action="store_true", help="Pause and automatically resume Compose containers during the Kubernetes test")
    a = p.parse_args()
    with compose_resource_window(a.pause_compose), gateway_connection(a.context, a.base_url):
        run_test(a)


def run_test(a):

    def kube(*args):
        return subprocess.run(
            ["kubectl", "--context", a.context, "-n", "storage-lab", *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    config = json.loads(kube("get", "configmap", "storage-config", "-o", "json"))
    if config["data"]["POSTGRES_HOST"] != "postgres-ha-rw":
        raise SystemExit(
            "Enable HA metadata first; a separate unused cluster is not application HA evidence"
        )
    cluster = json.loads(
        kube("get", "cluster.postgresql.cnpg.io", "postgres-ha", "-o", "json")
    )
    if cluster["status"].get("readyInstances") != 3:
        raise SystemExit("Wait for three Ready database instances")
    old = cluster["status"]["currentPrimary"]
    client = requests.Session()
    client.headers["X-API-Key"] = setting("API_KEY", "")
    bucket = "ha-evidence-" + uuid.uuid4().hex[:10]
    root = a.base_url.rstrip("/") + "/buckets/" + bucket
    data = b"Metadata replication and automatic primary failover " + uuid.uuid4().bytes
    client.put(root, json={}, timeout=30).raise_for_status()
    client.put(
        root + "/objects/evidence.bin", data=data, timeout=120
    ).raise_for_status()
    try:
        started = time.monotonic()
        kube("delete", "pod", old, "--wait=true")
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            cluster = json.loads(
                kube("get", "cluster.postgresql.cnpg.io", "postgres-ha", "-o", "json")
            )
            new = cluster.get("status", {}).get("currentPrimary")
            try:
                response = client.get(root + "/objects/evidence.bin", timeout=15)
                if (
                    new
                    and new != old
                    and response.status_code == 200
                    and response.content == data
                ):
                    break
            except requests.RequestException:
                pass
            time.sleep(2)
        else:
            raise RuntimeError(
                "Did not observe a different primary and successful reconstruction within 240 seconds"
            )
        print("PREVIOUS PRIMARY:", old)
        print("NEW PRIMARY:", new)
        print("OBSERVED RECOVERY SECONDS:", round(time.monotonic() - started, 2))
        print("OBJECT SHA-256:", hashlib.sha256(response.content).hexdigest())
        print(
            "PASS: a different database primary serves the same persisted object metadata and the downloaded bytes match"
        )
    finally:
        client.delete(root + "/objects/evidence.bin", timeout=120)
        client.delete(root, timeout=30)


if __name__ == "__main__":
    main()
