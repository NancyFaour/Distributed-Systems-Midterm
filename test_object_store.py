"""Screenshot-friendly integration test; run each phase in the displayed order."""
import argparse
import hashlib
import json
from pathlib import Path
import uuid

import requests

from object_store import ObjectStore

ROOT = Path(__file__).resolve().parent / "data" / "object-store-demo"
ROOT.mkdir(parents=True, exist_ok=True)
STATE = ROOT / "state.json"
store = ObjectStore()


def file_hash(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def show(value):
    print(json.dumps(value, indent=2, default=str), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["upload", "dedup", "read", "delete-first", "delete-last"])
    phase = parser.parse_args().phase
    if phase == "upload":
        bucket = "demo-" + uuid.uuid4().hex[:12]
        # Two identical 4 MiB chunks plus one different final chunk.
        block = ("demo-" + bucket).encode()
        repeated = (block * (4*1024*1024 // len(block) + 1))[:4*1024*1024]
        source = ROOT / "original.bin"
        with source.open("wb") as output:
            output.write(repeated)
            output.write(repeated)
            output.write(b"B" * (1024*1024 + 123))
        first = store.put_file(bucket, "first.bin", source)
        state = {"bucket": bucket, "first": str(first["object_id"]),
                 "hashes": first["chunk_hashes"], "original_sha256": file_hash(source)}
        STATE.write_text(json.dumps(state), encoding="utf-8")
        assert len(first["chunk_hashes"]) == 3 and len(set(first["chunk_hashes"])) == 2
        show(first)
        show(store.chunk_stats(first["chunk_hashes"]))
        print("PASS: ordered object metadata saved in PostgreSQL; 3 references use 2 unique chunks")
        return

    if not STATE.exists():
        raise RuntimeError("Run the upload phase first")
    state = json.loads(STATE.read_text(encoding="utf-8"))
    hashes = state["hashes"]
    if phase == "dedup":
        if "second" in state:
            raise RuntimeError("Second object already created; continue with read")
        second = store.put_file(state["bucket"], "second.bin", ROOT / "original.bin")
        state["second"] = str(second["object_id"])
        STATE.write_text(json.dumps(state), encoding="utf-8")
        stats = store.chunk_stats(hashes)
        references = {row["chunk_hash"]: row["ref_count"] for row in stats}
        assert second["uploaded_unique_chunks"] == 0
        assert references[hashes[0]] == 4 and references[hashes[2]] == 2
        show(second)
        show(stats)
        print("PASS: second object uploaded zero new chunks; reference counts are 4 and 2")
    elif phase == "read":
        target = ROOT / "downloaded.bin"
        obj = store.download_file(state["second"], target)
        digest = file_hash(target)
        assert digest == state["original_sha256"]
        show(obj)
        print("ORIGINAL SHA-256:", state["original_sha256"])
        print("DOWNLOAD SHA-256:", digest)
        print("PASS: object reconstructed from ordered, verified chunks")
    elif phase == "delete-first":
        show(store.delete_object(state["first"]))
        stats = store.chunk_stats(hashes)
        references = {row["chunk_hash"]: row["ref_count"] for row in stats}
        assert references[hashes[0]] == 2 and references[hashes[2]] == 1
        store.download_file(state["second"], ROOT / "after-delete.bin")
        assert file_hash(ROOT / "after-delete.bin") == state["original_sha256"]
        show(stats)
        print("PASS: shared chunks retained; second object still downloads correctly")
    elif phase == "delete-last":
        show(store.delete_object(state["second"]))
        assert not store.chunk_stats(hashes)
        for digest in set(hashes):
            for node, address in store.nodes.items():
                response = requests.head(f"{address}/chunks/{digest}", timeout=10)
                assert response.status_code == 404, f"Chunk still exists on {node}"
        print("PASS: last references removed; database rows and physical chunks deleted")


if __name__ == "__main__":
    main()
