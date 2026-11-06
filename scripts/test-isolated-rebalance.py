"""Two real RabbitMQ workers and physical rebalancing in a disposable DB schema.

The public bucket metadata and active ring are untouched. Start storage-node-8
first. Unique fixture hashes prevent sharing physical bytes with other workloads.
"""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from psycopg import sql
from metadata_db import connect_db
from redundant_store import RedundantStore
from migrate import migrate

ROOT = Path(__file__).resolve().parents[1]


def main():
    token = uuid.uuid4().hex[:12]
    schema = "validation_rebalance_" + token
    with connect_db() as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    os.environ["PGOPTIONS"] = "-c search_path=" + schema
    os.environ["MAINTENANCE_QUEUE"] = "storage-validation-" + token
    os.environ["STORAGE_NODES"] = json.dumps(
        {f"node-{i}": f"http://127.0.0.1:{8000 + i}" for i in range(1, 9)}
    )
    os.environ["INITIAL_STORAGE_NODES"] = json.dumps(
        {f"node-{i}": "" for i in range(1, 8)}
    )
    migrate()
    from job_queue import broker, enqueue, publish_pending, QUEUE

    store = RedundantStore()
    objects = []
    workers = []
    logs = []
    output = ROOT / "test-results"
    output.mkdir(exist_ok=True)

    def snapshot():
        return {
            (f["chunk_hash"], f["fragment_index"]): f["node_id"]
            for obj in objects
            for f in store.fragments(obj["object_id"])
        }

    def change(active):
        with connect_db() as connection:
            connection.execute(
                """INSERT INTO storage_members(node_id,active) VALUES ('node-8',%s)
                ON CONFLICT(node_id) DO UPDATE SET active=excluded.active""",
                (active,),
            )
        job = enqueue("rebalance")
        publish_pending()
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            with connect_db() as connection:
                row = connection.execute(
                    "SELECT * FROM maintenance_jobs WHERE job_id=%s", (job["job_id"],)
                ).fetchone()
            if row["status"] == "done":
                return row
            if row["status"] == "failed":
                raise RuntimeError(row["error"])
            time.sleep(1)
        raise RuntimeError("Timed out waiting for the disposable validation workers")

    try:
        for index in range(2):
            log = (output / f"isolated-worker-{index}.log").open("w", encoding="utf-8")
            logs.append(log)
            env = {**os.environ, "HOSTNAME": f"validation-worker-{index}"}
            workers.append(
                subprocess.Popen(
                    [sys.executable, str(ROOT / "job_queue.py")],
                    cwd=ROOT,
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                )
            )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            session = broker()
            try:
                result = session.channel().queue_declare(queue=QUEUE, durable=True)
                if result.method.consumer_count == 2:
                    break
            finally:
                session.close()
            time.sleep(1)
        else:
            raise RuntimeError("Two validation workers did not connect")
        store.configure_bucket("validation-" + token)
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "fixture.bin"
            for index in range(40):
                source.write_bytes((f"{token}:{index}:".encode() * 600)[:8192])
                objects.append(
                    store.put_file("validation-" + token, str(index), source)
                )
            before = snapshot()
            added = change(True)
            after = snapshot()
            moved = [slot for slot in before if before[slot] != after[slot]]
            assert moved and all(after[slot] == "node-8" for slot in moved)
            print(
                f"ADD node-8: {len(moved)}/{len(before)} fixture fragments moved ({100 * len(moved) / len(before):.2f}%)",
                flush=True,
            )
            print(
                "ADD WORKER:",
                added["worker_id"],
                json.dumps(added["result"]),
                flush=True,
            )
            for obj in objects:
                target = Path(directory) / "download.bin"
                store.download_file(obj["object_id"], target)
                assert (
                    hashlib.sha256(target.read_bytes()).hexdigest()
                    == obj["chunk_hashes"][0]
                )
            print(
                "PASS: actual migrated bytes reconstruct with exact SHA-256", flush=True
            )
            removed = change(False)
            restored = snapshot()
            changed = [slot for slot in after if after[slot] != restored[slot]]
            assert changed and all(after[slot] == "node-8" for slot in changed)
            print(
                f"REMOVE node-8: {len(changed)}/{len(after)} fixture fragments moved ({100 * len(changed) / len(after):.2f}%)",
                flush=True,
            )
            print(
                "REMOVE WORKER:",
                removed["worker_id"],
                json.dumps(removed["result"]),
                flush=True,
            )
            assert added["worker_id"] != removed["worker_id"], (
                "Both worker processes must receive a real job"
            )
            print(
                "PASS: RabbitMQ distributed real rebalancing jobs across two workers",
                flush=True,
            )
    finally:
        for obj in objects:
            store.delete_object(obj["object_id"])
        for worker in workers:
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/PID", str(worker.pid), "/T", "/F"],
                    capture_output=True,
                )
            else:
                worker.terminate()
                worker.wait(timeout=10)
        for log in logs:
            log.close()
        session = broker()
        try:
            session.channel().queue_delete(queue=QUEUE)
        finally:
            session.close()
        os.environ.pop("PGOPTIONS", None)
        with connect_db() as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
    print(
        "PASS: disposable schema, queue and fixture objects removed; public metadata preserved",
        flush=True,
    )


if __name__ == "__main__":
    main()
