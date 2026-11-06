"""Real StatefulSet/PVC persistence evidence through the Kubernetes gateway."""

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
        r = subprocess.run(
            ["kubectl", "--context", a.context, "-n", "storage-lab", *args],
            check=True,
            capture_output=True,
            text=True,
        )
        return r.stdout.strip()

    def pause(value):
        code = (
            "import urllib.request; urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:8000/"
            + ("pause" if value else "resume")
            + "',method='POST'),timeout=60).read()"
        )
        kube("exec", "deployment/recovery-service", "--", "python", "-c", code)

    session = requests.Session()
    session.headers["X-API-Key"] = setting("API_KEY", "")
    base = a.base_url.rstrip("/")
    bucket = "k8s-evidence-" + uuid.uuid4().hex[:10]
    root = base + "/buckets/" + bucket
    url = root + "/objects/pvc.bin"
    data = uuid.uuid4().bytes * 65536
    r = session.put(root, json={}, timeout=30)
    r.raise_for_status()
    paused = False
    try:
        print("Uploading a test object to the Kubernetes storage cluster...", flush=True)
        r = session.put(url, data=data, timeout=180)
        r.raise_for_status()
        r = session.get(root + "/metadata/pvc.bin", timeout=30)
        r.raise_for_status()
        fragment = r.json()["fragments"][0]
        node = fragment["node_id"]
        old = json.loads(kube("get", "pod", node, "-o", "json"))["metadata"]["uid"]
        pause(True)
        paused = True
        print("Replacing storage pod", node, "and waiting for its PVC to be remounted...", flush=True)
        kube("delete", "pod", node, "--wait=true")
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            try:
                pod = json.loads(kube("get", "pod", node, "-o", "json"))
            except subprocess.CalledProcessError:
                time.sleep(1)
                continue
            ready = any(
                x["type"] == "Ready" and x["status"] == "True"
                for x in pod.get("status", {}).get("conditions", [])
            )
            if pod["metadata"]["uid"] != old and ready:
                break
            time.sleep(2)
        else:
            raise RuntimeError("Replacement storage pod did not become Ready")
        code = (
            "from pathlib import Path; import hashlib; print(hashlib.sha256(Path('/data/"
            + fragment["fragment_hash"]
            + ".chunk').read_bytes()).hexdigest())"
        )
        assert (
            kube("exec", node, "--", "python", "-c", code) == fragment["fragment_hash"]
        )
        r = session.get(url, timeout=180)
        r.raise_for_status()
        assert hashlib.sha256(r.content).digest() == hashlib.sha256(data).digest()
        print("STORAGE POD:", node)
        print("OLD POD UID:", old)
        print("NEW POD UID:", pod["metadata"]["uid"])
        print("PERSISTENT FRAGMENT SHA-256:", fragment["fragment_hash"])
        print(
            "PASS: StatefulSet replaced the pod, its original fragment survived on the PVC, and the object reconstructed exactly"
        )
        print(kube("get", "pvc"))
    finally:
        if paused:
            pause(False)
        session.delete(url, timeout=120)
        session.delete(root, timeout=30)


if __name__ == "__main__":
    main()
