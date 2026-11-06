"""Exercise automatic queued repair/scrubbing using the inactive eighth spare.

Uses its own database schema, RabbitMQ queue, search index and temporary worker
processes. The seven-node public placement ring remains unchanged.
"""

import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import requests
from psycopg import sql
from metadata_db import connect_db
from redundant_store import RedundantStore
from redundancy import Policy
from consistent_hashing import ConsistentHashRing
from migrate import migrate

ROOT = Path(__file__).resolve().parents[1]


def main():
    with connect_db() as connection:
        active = connection.execute(
            "SELECT active FROM storage_members WHERE node_id='node-8'"
        ).fetchone()
    if active and active["active"]:
        raise SystemExit(
            "Node-8 must be outside the PUBLIC ring for this isolated failure test"
        )
    token = uuid.uuid4().hex[:12]
    schema = "validation_recovery_" + token
    with connect_db() as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    nodes = {f"node-{i}": f"http://127.0.0.1:{8000 + i}" for i in range(1, 9)}
    os.environ.update(
        PGOPTIONS="-c search_path=" + schema,
        MAINTENANCE_QUEUE="storage-recovery-" + token,
        STORAGE_NODES=json.dumps(nodes),
        INITIAL_STORAGE_NODES=json.dumps(nodes),
        SEARCH_INDEX="validation-" + token,
        JOB_QUEUE_ENABLED="true",
        SCRUB_INTERVAL_SECONDS="20",
        HEARTBEAT_REGISTRY_URL="http://127.0.0.1:8010",
    )
    migrate()
    from job_queue import broker, QUEUE

    store = RedundantStore()
    children = []
    logs = []
    objects = []
    stopped = False
    output = ROOT / "test-results"
    output.mkdir(exist_ok=True)

    def compose(*args):
        subprocess.run(
            ["docker", "compose", *args], cwd=ROOT, check=True, capture_output=True
        )

    def wait(check, seconds=150):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            value = check()
            if value:
                return value
            time.sleep(2)
        raise RuntimeError(
            "Automatic recovery condition did not complete within timeout"
        )

    def events(cause):
        with connect_db() as connection:
            return connection.execute(
                "SELECT * FROM recovery_events WHERE chunk_hash=%s AND cause=%s AND outcome='repaired' ORDER BY event_id",
                (digest, cause),
            ).fetchall()

    def stop_child(child):
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(child.pid), "/T", "/F"], capture_output=True
            )
        else:
            child.terminate()
            child.wait(timeout=10)

    try:
        for index in range(2):
            log = (output / f"recovery-validation-worker-{index}.log").open(
                "w", encoding="utf-8"
            )
            logs.append(log)
            children.append(
                subprocess.Popen(
                    [sys.executable, str(ROOT / "job_queue.py")],
                    cwd=ROOT,
                    env={**os.environ, "HOSTNAME": f"recovery-validation-{index}"},
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                )
            )
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "fixture.bin"
            ring = ConsistentHashRing(nodes)
            while True:
                content = (uuid.uuid4().bytes * 4096) + b"isolated recovery"
                digest = hashlib.sha256(content).hexdigest()
                if "node-8" in ring.get_nodes(digest, 3):
                    break
            source.write_bytes(content)
            for mode, policy in [
                ("replication", Policy()),
                ("erasure", Policy(mode="erasure")),
            ]:
                bucket = "recovery-" + mode + "-" + token
                store.configure_bucket(bucket, policy)
                obj = store.put_file(bucket, "fixture.bin", source)
                objects.append(obj)
                assert any(
                    f["node_id"] == "node-8" for f in store.fragments(obj["object_id"])
                )
            with socket.socket() as free:
                free.bind(("127.0.0.1", 0))
                port = free.getsockname()[1]
            log = (output / "recovery-validation-scheduler.log").open(
                "w", encoding="utf-8"
            )
            logs.append(log)
            children.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "uvicorn",
                        "recovery_service:app",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(port),
                    ],
                    cwd=ROOT,
                    env=os.environ.copy(),
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                )
            )

            def healthy():
                try:
                    return (
                        requests.get(
                            f"http://127.0.0.1:{port}/health", timeout=2
                        ).status_code
                        == 200
                    )
                except requests.RequestException:
                    return False

            wait(healthy, 60)
            compose("stop", "-t", "3", "storage-node-8")
            stopped = True
            for obj in objects:
                destination = Path(directory) / "read-while-offline.bin"
                store.download_file(obj["object_id"], destination)
                assert destination.read_bytes() == content
            print(
                "PASS: replication and RS reads succeed while the eighth spare is stopped",
                flush=True,
            )
            repaired = wait(
                lambda: (
                    events("node_failure")
                    if len({r["policy_key"] for r in events("node_failure")}) == 2
                    else None
                )
            )
            for obj in objects:
                assert all(
                    f["node_id"] != "node-8" and f["status"] == "healthy"
                    for f in store.fragments(obj["object_id"])
                )
            print(
                "PASS: missed heartbeats automatically triggered queued repair for BOTH policies",
                flush=True,
            )
            print(json.dumps(repaired, default=str, indent=2), flush=True)
            while True:
                fresh_content = uuid.uuid4().bytes * 2048
                fresh_hash = hashlib.sha256(fresh_content).hexdigest()
                if "node-8" in ring.get_nodes(fresh_hash, 6):
                    break
            source.write_bytes(fresh_content)
            extra = store.put_file(
                "recovery-erasure-" + token, "new-during-failure.bin", source
            )
            try:
                assert all(
                    f["node_id"] != "node-8"
                    for f in store.fragments(extra["object_id"])
                )
                target = Path(directory) / "new-during-failure.bin"
                store.download_file(extra["object_id"], target)
                assert target.read_bytes() == fresh_content
                print(
                    "PASS: a NEW erasure-coded object uses healthy spare targets while node-8 remains offline",
                    flush=True,
                )
            finally:
                store.delete_object(extra["object_id"])
            compose("up", "-d", "--no-build", "--no-deps", "--wait", "storage-node-8")
            stopped = False
            for obj in objects:
                f = store.fragments(obj["object_id"])[0]
                code = (
                    "from pathlib import Path; Path('/data/"
                    + f["fragment_hash"]
                    + ".chunk').write_bytes(b'controlled fixture corruption')"
                )
                compose("exec", "-T", "storage-" + f["node_id"], "python", "-c", code)
            scrubbed = wait(
                lambda: (
                    events("scrub")
                    if len({r["policy_key"] for r in events("scrub")}) == 2
                    else None
                )
            )
            for obj in objects:
                destination = Path(directory) / "verified.bin"
                store.download_file(obj["object_id"], destination)
                assert hashlib.sha256(destination.read_bytes()).hexdigest() == digest
            print(
                "PASS: background scrubber repaired corrupted replicas AND RS fragments",
                flush=True,
            )
            print(json.dumps(scrubbed, default=str, indent=2), flush=True)
    finally:
        if stopped:
            compose("up", "-d", "--no-build", "--no-deps", "--wait", "storage-node-8")
        for child in reversed(children):
            stop_child(child)
        for log in logs:
            log.close()
        for obj in objects:
            store.delete_object(obj["object_id"])
        store.drain_obsolete()
        session = broker()
        try:
            session.channel().queue_delete(queue=QUEUE)
        finally:
            session.close()
        requests.delete("http://127.0.0.1:9200/validation-" + token, timeout=15)
        os.environ.pop("PGOPTIONS", None)
        with connect_db() as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
    print(
        "PASS: temporary recovery infrastructure cleaned up; public metadata and placement preserved",
        flush=True,
    )


if __name__ == "__main__":
    main()
