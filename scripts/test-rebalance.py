"""Measure actual fragment relocation after adding/removing the eighth node."""

import argparse
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import requests
from redundant_store import RedundantStore
from settings import setting


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8080/api")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")
    session = requests.Session()
    session.headers["X-API-Key"] = setting("API_KEY", "")
    store = RedundantStore(
        {f"node-{i}": f"http://127.0.0.1:{8000 + i}" for i in range(1, 9)}
    )
    if "node-8" in store.active_nodes():
        raise SystemExit(
            "This demonstration starts with node-8 outside the ring. Remove it through the membership API first."
        )
    bucket = "rebalance-evidence-" + uuid.uuid4().hex[:8]
    store.configure_bucket(bucket)
    objects = []
    added = False

    def snapshot():
        return {
            (f["chunk_hash"], f["policy_key"], f["fragment_index"]): f["node_id"]
            for obj in objects
            for f in store.fragments(obj["object_id"])
        }

    def wait(job):
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            response = session.get(base + "/jobs/" + job["job_id"], timeout=20)
            response.raise_for_status()
            row = response.json()
            if row and row["status"] == "done":
                return row
            if row and row["status"] == "failed":
                raise RuntimeError(row["error"])
            time.sleep(2)
        raise RuntimeError("Rebalance worker did not complete within 300 seconds")

    def change(active):
        response = session.put(
            base + "/cluster/members/node-8",
            params={"active": str(active).lower()},
            timeout=30,
        )
        response.raise_for_status()
        return wait(response.json()["job"])

    try:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.bin"
            for index in range(64):
                data = (f"{bucket}:{index}:".encode() * 500)[:8192]
                path.write_bytes(data)
                objects.append(store.put_file(bucket, str(index), path))
            baseline = snapshot()
            added = True
            job = change(True)
            after = snapshot()
            moved = [slot for slot in baseline if after[slot] != baseline[slot]]
            assert moved and all(after[slot] == "node-8" for slot in moved)
            print(
                f"ADD node-8: {len(moved)}/{len(baseline)} fixture fragments moved ({100 * len(moved) / len(baseline):.2f}%)"
            )
            print("WORKER RESULT:", json.dumps(job["result"], indent=2))
            for obj in objects:
                destination = Path(directory) / "download.bin"
                store.download_file(obj["object_id"], destination)
                assert (
                    hashlib.sha256(destination.read_bytes()).hexdigest()
                    == obj["chunk_hashes"][0]
                )
            print("PASS: real bytes migrated; all objects still reconstruct exactly")
            job = change(False)
            added = False
            restored = snapshot()
            removed = [slot for slot in after if restored[slot] != after[slot]]
            assert removed and all(after[slot] == "node-8" for slot in removed)
            assert all(owner != "node-8" for owner in restored.values())
            print(
                f"REMOVE node-8: {len(removed)}/{len(after)} fixture fragments moved ({100 * len(removed) / len(after):.2f}%)"
            )
            print(
                "PASS: only the removed node's assigned fragments moved; membership restored"
            )
    finally:
        if added:
            change(False)
        for obj in objects:
            store.delete_object(obj["object_id"])
    print("PASS: fixture objects cleaned up; existing user objects retained")


if __name__ == "__main__":
    main()
