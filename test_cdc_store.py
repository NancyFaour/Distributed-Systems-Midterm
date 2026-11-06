"""CDC integration with real PostgreSQL metadata and Docker chunk storage."""
import hashlib
from pathlib import Path
import random
import tempfile
import uuid

from object_store import ObjectStore


def sha256(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def main():
    store = ObjectStore()
    bucket = "cdc-test-" + uuid.uuid4().hex[:12]
    remaining = []
    with tempfile.TemporaryDirectory(prefix="cdc-integration-") as directory:
        directory = Path(directory)
        source = directory / "source.bin"
        random_source = random.Random(12345)
        with source.open("wb") as file:
            for _ in range(3):
                file.write(random_source.randbytes(1024*1024))
            file.write(b"final bytes")
        try:
            first = store.put_file(bucket, "first.bin", source, chunking_method="cdc")
            remaining.append(first["object_id"])
            metadata = store.get_metadata(first["object_id"])
            assert metadata["chunking_method"] == "cdc"
            assert metadata["size_bytes"] == source.stat().st_size
            assert metadata["chunk_hashes"] == first["chunk_hashes"]
            print(f"CDC file size: {metadata['size_bytes']} bytes; chunks: {len(metadata['chunk_hashes'])}", flush=True)
            print("PASS: bucket, key, size, version, creation time and ordered CDC hashes saved", flush=True)

            second = store.put_file(bucket, "second.bin", source, chunking_method="cdc")
            remaining.append(second["object_id"])
            assert second["uploaded_unique_chunks"] == 0
            print("PASS: identical CDC upload stores zero new chunks", flush=True)

            target = directory / "rebuilt.bin"
            store.download_file(second["object_id"], target)
            assert sha256(source) == sha256(target)
            print("SOURCE SHA-256:", sha256(source), flush=True)
            print("REBUILT SHA-256:", sha256(target), flush=True)
            print("PASS: reconstruction from CDC chunks matches the original", flush=True)

            expected = {}
            for digest in first["chunk_hashes"]:
                expected[digest] = expected.get(digest, 0) + 1
            stats = store.chunk_stats(expected)
            assert all(row["ref_count"] == 2*expected[row["chunk_hash"]] for row in stats)
            print("PASS: reference counts include both objects and repeated chunk occurrences", flush=True)

            result = store.delete_object(first["object_id"])
            remaining.remove(first["object_id"])
            assert not result["pending_cleanup"]
            store.download_file(second["object_id"], target)
            assert sha256(source) == sha256(target)
            stats = store.chunk_stats(expected)
            assert all(row["ref_count"] == expected[row["chunk_hash"]] for row in stats)
            print("PASS: first deletion retains shared chunks and the second object", flush=True)

            result = store.delete_object(second["object_id"])
            remaining.remove(second["object_id"])
            assert not result["pending_cleanup"] and not store.chunk_stats(expected)
            print("PASS: final deletion removes unreferenced chunks", flush=True)
            print("ALL CDC INTEGRATION TESTS PASSED", flush=True)
        finally:
            for object_id in remaining:
                store.delete_object(object_id)


if __name__ == "__main__":
    main()
