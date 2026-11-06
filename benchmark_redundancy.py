"""Measured storage and repair payload traffic for small- and large-file workloads."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import random
import subprocess
import time
import uuid
import threading

import requests

from metadata_db import connect_db
from redundancy import Policy
from redundant_store import RedundantStore

ROOT = Path(__file__).resolve().parent


def sha256(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True)


def pause(value):
    response = requests.post("http://127.0.0.1:8020/"+("pause" if value else "resume"), timeout=120)
    response.raise_for_status()


def allocation(bucket, node=None):
    with connect_db() as connection:
        return int(connection.execute("""SELECT COALESCE(SUM(size_bytes),0) AS n FROM (
            SELECT DISTINCT f.node_id,f.fragment_hash,f.size_bytes FROM fragment_locations f
            JOIN object_chunks o ON o.chunk_hash=f.chunk_hash
            JOIN object_policies p ON p.object_id=o.object_id AND p.policy_key=f.policy_key
            JOIN object_versions v ON v.object_id=o.object_id WHERE v.bucket_name=%s
            AND (%s::text IS NULL OR f.node_id=%s)
        ) q""", (bucket,node,node)).fetchone()["n"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=["smoke", "full"], default="smoke")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--output", default="data/benchmarks/redundancy-results.json")
    args = parser.parse_args()
    counts = {"small": 40, "large": 2} if args.profile == "smoke" else {"small": 5000, "large": 10}
    large_mib = 8 if args.profile == "smoke" else 100
    run_id = uuid.uuid4().hex[:10]
    directory = ROOT / "data" / "benchmarks" / run_id
    directory.mkdir(parents=True, exist_ok=True)
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    store = RedundantStore()
    results = {"profile": args.profile, "run_id": run_id,
               "traffic_definition": "application fragment payload bytes read and acknowledged written, including fragment headers; excludes HTTP/TCP headers, heartbeat/control traffic, and filesystem allocation metadata",
               "failure": "one real Docker node stopped; missed heartbeat detected; repairs placed on remaining distinct healthy nodes",
               "repair_execution": "background scanner paused for controlled measurement; repair invoked explicitly after heartbeat failure detection",
               "rows": []}
    pause(True)
    stopped = False
    failed_node = 'node-1'
    tracked = []
    tracking_lock = threading.Lock()
    try:
        for workload in ("small", "large"):
            paths = []
            for index in range(counts[workload]):
                rng = random.Random(f"{run_id}:{workload}:{index}")
                path = directory / f"{workload}-{index}.{'json' if workload=='small' else 'bin'}"
                if workload == "small":
                    size = rng.randint(1024, 50*1024)
                    prefix = ('{"id":'+str(index)+',"payload":"').encode()
                    payload = base64.b64encode(rng.randbytes(size))[:size-len(prefix)-2]
                    path.write_bytes(prefix+payload+b'"}')
                else:
                    with path.open("wb") as stream:
                        for _ in range(large_mib):
                            stream.write(rng.randbytes(1024*1024))
                paths.append(path)
            logical = sum(path.stat().st_size for path in paths)
            digests = {path.name: sha256(path) for path in paths}
            groups = []
            for mode, policy in (("replication", Policy()), ("erasure", Policy(mode="erasure", k=4, m=2))):
                bucket = f"bench-{run_id}-{workload}-{mode}"
                store.configure_bucket(bucket, policy)
                print(f"RUNNING {args.profile}: {workload}, {mode}, {len(paths)} files, {logical} logical bytes", flush=True)
                started = time.monotonic()
                def upload(path):
                    obj = RedundantStore().put_file(bucket, path.name, path)
                    with tracking_lock:
                        tracked.append(obj['object_id'])
                    return (path, obj)
                uploaded = []
                with ThreadPoolExecutor(max_workers=args.workers) as pool:
                    for path, obj in pool.map(upload, paths):
                        uploaded.append((path, obj))
                        if len(uploaded) % 100 == 0:
                            print(f"  uploaded {len(uploaded)}/{len(paths)}", flush=True)
                row = {"workload": workload, "policy": policy.key, "file_count": len(paths),
                       "logical_bytes": logical, "stored_payload_bytes": allocation(bucket),
                       "upload_seconds": round(time.monotonic()-started, 3)}
                row["storage_factor"] = round(row["stored_payload_bytes"]/logical, 6)
                row["storage_overhead_percent"] = round(100*(row["storage_factor"]-1), 4)
                started = time.monotonic()
                verified_count = 0
                def verify(pair):
                    nonlocal verified_count
                    path, obj = pair
                    destination = directory / (mode+"-"+path.name+".download")
                    RedundantStore().download_file(obj["object_id"], destination)
                    if sha256(destination) != digests[path.name]:
                        raise RuntimeError("Benchmark download integrity failure")
                    destination.unlink()
                    with tracking_lock:
                        verified_count += 1
                        if verified_count % 100 == 0 or verified_count == len(uploaded):
                            print(f"  verified {row['policy']}: {verified_count}/{len(uploaded)} downloads", flush=True)
                with ThreadPoolExecutor(max_workers=args.workers) as pool:
                    list(pool.map(verify, uploaded))
                row["verified_download_seconds"] = round(time.monotonic()-started, 3)
                row["all_downloads_verified"] = True
                groups.append((bucket, uploaded, row))

            # Choose a node holding a replication fragment, ensuring this failure
            # actually exercises both policies, including the small smoke profile.
            failed_node = store.fragments(groups[0][1][0][1]['object_id'])[0]['node_id']
            service = 'storage-'+failed_node
            for bucket, uploaded, row in groups:
                row['failed_node'] = failed_node
                row['lost_fragment_payload_bytes'] = allocation(bucket, failed_node)
            compose("stop", "-t", "3", service)
            stopped = True
            deadline = time.monotonic()+40
            while time.monotonic() < deadline:
                nodes = requests.get("http://127.0.0.1:8010/nodes", timeout=5).json()["nodes"]
                if any(node["node_id"] == failed_node and node["status"] == "offline" for node in nodes):
                    break
                time.sleep(1)
            else:
                raise RuntimeError("Heartbeat failure detection timed out")
            for bucket, uploaded, row in groups:
                started = time.monotonic()
                print(f"REPAIRING {workload}: {row['policy']} after failure of {failed_node}", flush=True)
                events = store.maintain(bucket=bucket)
                if any(event["outcome"] != "repaired" for event in events):
                    raise RuntimeError("Benchmark failed to restore redundancy")
                row["repaired_chunk_count"] = len(events)
                row["repair_read_bytes"] = sum(event["read_bytes"] for event in events)
                row["repair_written_bytes"] = sum(event["written_bytes"] for event in events)
                row["repair_total_bytes"] = row["repair_read_bytes"]+row["repair_written_bytes"]
                row['repair_bytes_per_lost_byte'] = round(row['repair_total_bytes']/row['lost_fragment_payload_bytes'],6) if row['lost_fragment_payload_bytes'] else 0
                row["repair_seconds"] = round(time.monotonic()-started, 3)
                verified_count = 0
                with ThreadPoolExecutor(max_workers=args.workers) as pool:
                    list(pool.map(verify, uploaded))
                row['post_repair_downloads_verified'] = True
                results["rows"].append(row)
                output.write_text(json.dumps(results, indent=2), encoding="utf-8")
                print(json.dumps(row, indent=2), flush=True)
            compose("up", "-d", "--no-build", "--no-deps", "--wait", service)
            stopped = False
            for bucket, uploaded, row in groups:
                for deleted_count, (_, obj) in enumerate(uploaded, 1):
                    store.delete_object(obj["object_id"])
                    tracked.remove(obj["object_id"])
                    if deleted_count % 100 == 0 or deleted_count == len(uploaded):
                        print(f"  cleaned {row['policy']}: {deleted_count}/{len(uploaded)} objects", flush=True)
            for path in paths:
                path.unlink()
            time.sleep(5)
            store.drain_obsolete()
        print("WORKLOAD | POLICY | FILES | STORAGE FACTOR | REPAIR READ | REPAIR WRITE", flush=True)
        for row in results["rows"]:
            print(f"{row['workload']} | {row['policy']} | {row['file_count']} | {row['storage_factor']:.4f}x | {row['repair_read_bytes']} | {row['repair_written_bytes']}", flush=True)
        print(f"PASS: both workloads compared using measured payload bytes. Results: {output}", flush=True)
    finally:
        if stopped:
            compose("up", "-d", "--no-build", "--no-deps", "--wait", 'storage-'+failed_node)
        for object_id in tracked:
            try:
                store.delete_object(object_id)
            except Exception as error:
                print(f"Cleanup pending for {object_id}: {error}", flush=True)
        store.drain_obsolete()
        pause(False)


if __name__ == "__main__":
    main()
