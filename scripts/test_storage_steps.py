"""Run one storage-node test at a time and capture the terminal for your report."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import uuid

import requests

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / "data" / "storage-demo.json"
BASE = "http://127.0.0.1:8001"


def expect(condition, message):
    if not condition:
        raise RuntimeError(message)


def compose(*arguments):
    subprocess.run(["docker", "compose", *arguments], cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("step", choices=["put", "get", "head", "invalid-put",
                                        "isolation", "corruption", "persistence",
                                        "delete", "heartbeats"])
    step = parser.parse_args().step
    if step == "heartbeats":
        response = requests.get("http://127.0.0.1:8010/nodes", timeout=10)
        response.raise_for_status()
        nodes = {node["node_id"]: node for node in response.json()["nodes"]}
        for number in range(1, 5):
            node = nodes[f"node-{number}"]
            expect(node["status"] == "online", f"Node {number} is offline")
            expect(node["free_space_bytes"] >= 0 and node["chunk_count"] >= 0,
                   "Invalid heartbeat statistics")
            print(json.dumps(node, indent=2))
        print("PASS: all four nodes send valid heartbeats")
        return

    if not STATE.exists():
        expect(step == "put", "Start with: python scripts/test_storage_steps.py put")
        text = "Distributed storage screenshot test " + uuid.uuid4().hex
        fixture = {"content": text, "hash": hashlib.sha256(text.encode()).hexdigest()}
        STATE.parent.mkdir(parents=True, exist_ok=True)
        STATE.write_text(json.dumps(fixture), encoding="utf-8")
    fixture = json.loads(STATE.read_text(encoding="utf-8"))
    content = fixture["content"].encode("utf-8")
    digest = fixture["hash"]
    expect(re.fullmatch(r"[0-9a-f]{64}", digest) is not None and
           hashlib.sha256(content).hexdigest() == digest, "Invalid test fixture")
    url = f"{BASE}/chunks/{digest}"
    print(f"TEST: {step.upper()}\nNODE: node-1\nSHA-256: {digest}")

    if step == "put":
        response = requests.put(url, data=content, timeout=10)
        print("HTTP:", response.status_code)
        expect(response.status_code in (200, 201), response.text)
        print(json.dumps(response.json(), indent=2))
        print("PASS: content-addressed chunk stored on disk")
    elif step == "get":
        response = requests.get(url, timeout=10)
        print("HTTP:", response.status_code)
        expect(response.status_code == 200, response.text)
        expect(response.content == content, "Downloaded bytes differ")
        expect(hashlib.sha256(response.content).hexdigest() == digest, "Hash mismatch")
        print("CONTENT:", response.content.decode())
        print("PASS: exact content and SHA-256 verified")
    elif step == "head":
        response = requests.head(url, timeout=10)
        print("HTTP:", response.status_code)
        expect(response.status_code == 200, "Chunk is missing or corrupted")
        expect(int(response.headers["Content-Length"]) == len(content), "Wrong size")
        expect(response.headers["ETag"] == f'"{digest}"', "Wrong checksum")
        expect(not response.content, "HEAD returned a response body")
        print("Content-Length:", response.headers["Content-Length"])
        print("ETag:", response.headers["ETag"])
        print("PASS: correct metadata; response body is empty")
    elif step == "invalid-put":
        response = requests.put(url, data=b"incorrect content", timeout=10)
        print("HTTP:", response.status_code, response.text)
        expect(response.status_code == 400, "Incorrect hash was accepted")
        saved = requests.get(url, timeout=10)
        expect(saved.status_code == 200 and saved.content == content,
               "Original chunk was not preserved")
        print("PASS: incorrect upload rejected; original chunk preserved")
    elif step == "isolation":
        response = requests.get(url, timeout=10)
        expect(response.status_code == 200 and response.content == content, "Upload first")
        print("node-1: HTTP 200")
        for number in range(2, 5):
            response = requests.get(f"http://127.0.0.1:{8000 + number}/chunks/{digest}",
                                    timeout=10)
            print(f"node-{number}: HTTP {response.status_code}")
            expect(response.status_code == 404, "Chunk unexpectedly exists on another node")
        print("PASS: the nodes use separate data volumes")
    elif step == "corruption":
        response = requests.get(url, timeout=10)
        expect(response.status_code == 200 and response.content == content, "Upload first")
        # Modify ONLY this script's random test chunk; never modify user chunks.
        code = "from pathlib import Path; import sys; Path('/data', sys.argv[1]+'.chunk').write_bytes(b'CORRUPTED TEST CONTENT')"
        try:
            compose("exec", "-T", "storage-node-1", "python", "-c", code, digest)
            for method in ("GET", "HEAD"):
                response = requests.request(method, url, timeout=10)
                print(f"{method}: HTTP {response.status_code}; integrity={response.headers.get('X-Chunk-Integrity')}")
                expect(response.status_code == 500 and
                       response.headers.get("X-Chunk-Integrity") == "failed",
                       f"{method} did not reject corrupted data")
            print("PASS: GET and HEAD reject stored-data corruption")
        finally:
            response = requests.put(url, data=content, timeout=10)
            expect(response.status_code in (200, 201), "Could not restore test chunk")
            print("Test chunk restored using a valid PUT (automatic repair is a later phase).")
    elif step == "persistence":
        response = requests.get(url, timeout=10)
        expect(response.status_code == 200 and response.content == content, "Upload first")
        old_id = subprocess.check_output(["docker", "compose", "ps", "-q", "storage-node-1"],
                                         cwd=ROOT, text=True).strip()
        compose("up", "-d", "--no-build", "--force-recreate", "--no-deps", "--wait",
                "--wait-timeout", "90", "storage-node-1")
        new_id = subprocess.check_output(["docker", "compose", "ps", "-q", "storage-node-1"],
                                         cwd=ROOT, text=True).strip()
        expect(old_id != new_id, "Container was not replaced")
        response = requests.get(url, timeout=10)
        expect(response.status_code == 200 and response.content == content, "Chunk did not survive")
        print("OLD CONTAINER:", old_id[:12], "NEW CONTAINER:", new_id[:12])
        print("PASS: chunk survived container replacement in its persistent volume")
    elif step == "delete":
        response = requests.delete(url, timeout=10)
        print("DELETE HTTP:", response.status_code)
        expect(response.status_code == 204, "Upload the demo chunk before deleting it")
        for method in ("GET", "HEAD"):
            response = requests.request(method, url, timeout=10)
            print(f"{method} after deletion: HTTP {response.status_code}")
            expect(response.status_code == 404, "Deleted chunk is still accessible")
        print("PASS: chunk deleted; GET and HEAD now return 404")


if __name__ == "__main__":
    main()
