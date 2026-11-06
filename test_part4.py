"""Run each Part 4 phase separately to capture genuine Docker test evidence."""
import argparse
import hashlib
import json
from pathlib import Path
import random
import subprocess
import time
import uuid

import requests

from metadata_db import connect_db
from redundancy import Policy
from redundant_store import RedundantStore, QuorumError

ROOT = Path(__file__).resolve().parent
DEMO = ROOT / "data" / "part4"
DEMO.mkdir(parents=True, exist_ok=True)
STATE = DEMO / "state.json"


def show(value):
    print(json.dumps(value, indent=2, default=str), flush=True)


def compose(*arguments):
    subprocess.run(["docker", "compose", *arguments], cwd=ROOT, check=True)


def hash_file(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def wait_for(check, timeout=90):
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(2)
    raise RuntimeError("Timed out waiting for recovery; inspect http://127.0.0.1:8020/status")


def worker_pause(paused):
    try:
        response = requests.post("http://127.0.0.1:8020/"+("pause" if paused else "resume"), timeout=30)
        response.raise_for_status()
    except requests.ConnectionError:
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["replication", "erasure", "quorum", "repair-replication", "repair-erasure", "scrub", "cleanup"])
    phase = parser.parse_args().phase
    store = RedundantStore()
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    source = DEMO / "original.bin"
    if phase in ("replication", "erasure"):
        if phase in state:
            print(f"Existing {phase} demo retained. Run cleanup before creating another.")
            obj = store.get_metadata(state[phase])
            show(obj)
            fragments = store.fragments(state[phase])
            show(fragments)
            policy = Policy() if phase == "replication" else Policy(mode="erasure", k=4, m=2)
            assert len(fragments) == policy.total
            assert len({row['node_id'] for row in fragments}) == policy.total
            target = DEMO / (phase+"-download.bin")
            store.download_file(state[phase], target)
            assert hash_file(target) == state["sha256"]
            physical = sum(row["size_bytes"] for row in fragments)
            print(f"LOGICAL BYTES: {source.stat().st_size}; ALLOCATED FRAGMENT PAYLOAD BYTES: {physical}; FACTOR: {physical/source.stat().st_size:.4f}x")
            print("ORIGINAL SHA-256:", state["sha256"])
            print("DOWNLOAD SHA-256:", hash_file(target))
            print(f"PASS: {phase} uses distinct Docker nodes and reconstructs the original file")
            return
        if not state:
            source.write_bytes(random.Random(uuid.uuid4().int).randbytes(256*1024+123))
            state["sha256"] = hash_file(source)
        bucket = "part4-"+phase+"-"+uuid.uuid4().hex[:10]
        policy = Policy() if phase == "replication" else Policy(mode="erasure", k=4, m=2)
        store.configure_bucket(bucket, policy)
        obj = store.put_file(bucket, "demo.bin", source)
        state[phase] = str(obj["object_id"])
        STATE.write_text(json.dumps(state))
        fragments = store.fragments(obj["object_id"])
        assert len(fragments) == policy.total and len({row["node_id"] for row in fragments}) == policy.total
        assert all(row["status"] == "healthy" for row in fragments)
        target = DEMO / (phase+"-download.bin")
        store.download_file(obj["object_id"], target)
        assert hash_file(target) == state["sha256"]
        show(obj)
        show([{key: row[key] for key in ("fragment_index", "node_id", "size_bytes", "status")} for row in fragments])
        physical = sum(row["size_bytes"] for row in fragments)
        print(f"LOGICAL BYTES: {source.stat().st_size}; ALLOCATED FRAGMENT PAYLOAD BYTES: {physical}; FACTOR: {physical/source.stat().st_size:.4f}x")
        print("ORIGINAL SHA-256:", state["sha256"])
        print("DOWNLOAD SHA-256:", hash_file(target))
        print(f"PASS: {'N=3, W=2, R=2 replication' if phase=='replication' else 'Reed-Solomon 4+2'} stored on distinct Docker nodes; download verified")
        return

    if phase == "quorum":
        obj_id = state["replication"]
        obj = store.get_metadata(obj_id)
        fragments = store.fragments(obj_id)
        failed = [row["node_id"] for row in fragments[:2]]
        extra = None
        worker_pause(True)
        try:
            compose("stop", "storage-"+failed[0])
            target = DEMO / "quorum-read.bin"
            store.download_file(obj_id, target)
            assert hash_file(target) == state["sha256"]
            print("PASS: read succeeds with one replica unavailable (R=2)", flush=True)
            extra = store.put_file(obj["bucket_name"], "quorum-write.bin", source)
            print("PASS: write succeeds with two verified replicas (W=2)", flush=True)
            compose("stop", "storage-"+failed[1])
            try:
                store.download_file(obj_id, target)
            except QuorumError as error:
                print("PASS: read below R=2 rejected:", error, flush=True)
            else:
                raise AssertionError("Read quorum not enforced")
            before = None
            with connect_db() as connection:
                before = connection.execute("SELECT COUNT(*) AS n FROM object_versions WHERE bucket_name=%s", (obj["bucket_name"],)).fetchone()["n"]
            try:
                store.put_file(obj["bucket_name"], "must-not-commit.bin", source)
            except QuorumError as error:
                print("PASS: write below W=2 rejected:", error, flush=True)
            else:
                raise AssertionError("Write quorum not enforced")
            with connect_db() as connection:
                after = connection.execute("SELECT COUNT(*) AS n FROM object_versions WHERE bucket_name=%s", (obj["bucket_name"],)).fetchone()["n"]
            assert before == after
            print("PASS: rejected write did not commit object metadata", flush=True)
        finally:
            compose("up", "-d", "--no-build", "--no-deps", "--wait", *["storage-"+node for node in failed])
            if extra:
                store.delete_object(extra["object_id"])
            worker_pause(False)
        return

    if phase.startswith("repair-") or phase == "scrub":
        def recovery_ready():
            try:
                return requests.get("http://127.0.0.1:8020/status", timeout=3).ok
            except requests.ConnectionError:
                return False
        wait_for(recovery_ready, timeout=45)
        worker_pause(False)
        modes = ("replication", "erasure") if phase == "scrub" else (phase.removeprefix("repair-"),)
        for mode in modes:
            obj_id = state[mode]
            fragment = store.fragments(obj_id)[0]
            with connect_db() as connection:
                last = connection.execute("SELECT COALESCE(MAX(event_id),0) n FROM recovery_events").fetchone()["n"]
            stopped = None
            try:
                if phase == "scrub":
                    code = "from pathlib import Path; import sys; Path('/data',sys.argv[1]+'.chunk').write_bytes(b'CORRUPTED PART4 TEST FRAGMENT')"
                    compose("exec", "-T", "storage-"+fragment["node_id"], "python", "-c", code, fragment["fragment_hash"])
                else:
                    stopped = "storage-"+fragment["node_id"]
                    compose("stop", stopped)
                def repaired_event():
                    with connect_db() as connection:
                        return connection.execute("""SELECT * FROM recovery_events WHERE event_id>%s
                            AND chunk_hash=%s AND policy_key=%s AND outcome='repaired'
                            AND cause=%s ORDER BY event_id DESC LIMIT 1""", (last, fragment["chunk_hash"], fragment["policy_key"],
                                "scrub" if phase=="scrub" else "node_failure")).fetchone()
                print("Waiting for heartbeat detection/background repair (up to 180 seconds)...", flush=True)
                try:
                    event = wait_for(repaired_event, timeout=180)
                except RuntimeError:
                    with connect_db() as connection:
                        recent = connection.execute("""SELECT cause,outcome,detail,created_at
                            FROM recovery_events WHERE event_id>%s AND chunk_hash=%s
                            AND policy_key=%s ORDER BY event_id DESC LIMIT 3""",
                            (last, fragment["chunk_hash"], fragment["policy_key"])).fetchall()
                    print("Latest recovery diagnostics:", flush=True)
                    show(recent)
                    raise
                show(event)
                current = store.fragments(obj_id)
                assert len({row["node_id"] for row in current}) == len(current)
                assert all(store._check(row) for row in current)
                target = DEMO / (mode+"-repaired.bin")
                store.download_file(obj_id, target)
                assert hash_file(target) == state["sha256"]
                print(f"PASS: {mode} {'background scrub detected bit rot and repaired it' if phase=='scrub' else 'missed heartbeat triggered repair onto a healthy node'}; full redundancy and file SHA-256 verified", flush=True)
            finally:
                if stopped:
                    compose("up", "-d", "--no-build", "--no-deps", "--wait", stopped)
        return

    if phase == "cleanup":
        for mode in ("replication", "erasure"):
            if mode in state:
                show(store.delete_object(state[mode]))
        STATE.unlink(missing_ok=True)
        print("PASS: demo object references removed; unrelated user objects preserved")


if __name__ == "__main__":
    main()
