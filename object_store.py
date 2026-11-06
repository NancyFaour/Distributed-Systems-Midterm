"""Object metadata and placement coordinator for the local four-node cluster."""
from collections import Counter
import hashlib
import os
from pathlib import Path
import tempfile

import requests

from chunking import fixed_chunks, content_defined_chunks
from consistent_hashing import ConsistentHashRing
from metadata_db import connect_db

DEFAULT_NODES = {f"node-{i}": f"http://127.0.0.1:{8000+i}" for i in range(1, 5)}


class ObjectStore:
    def __init__(self, nodes=None):
        self.nodes = dict(DEFAULT_NODES if nodes is None else nodes)
        self.ring = ConsistentHashRing(self.nodes, virtual_nodes=128)

    @staticmethod
    def _lock_chunks(connection, hashes):
        # A common ordering prevents overlapping uploads from deadlocking.
        keys = sorted({int(h[:16], 16) for h in hashes})
        for key in keys:
            signed_key = key if key < 2**63 else key - 2**64
            connection.execute("SELECT pg_advisory_xact_lock(%s)", (signed_key,))

    def _url(self, node, digest):
        return f"{self.nodes[node]}/chunks/{digest}"

    def put_file(self, bucket, key, filename, chunking_method="fixed"):
        if chunking_method not in ("fixed", "cdc"):
            raise ValueError("chunking_method must be fixed or cdc")
        manifest = []
        offset = 0
        # File-based two-pass processing retains only a small manifest in RAM.
        with Path(filename).open("rb") as source:
            chunker = fixed_chunks if chunking_method == "fixed" else content_defined_chunks
            for index, content in enumerate(chunker(source)):
                digest = hashlib.sha256(content).hexdigest()
                manifest.append((index, digest, len(content), offset))
                offset += len(content)
            counts = Counter(item[1] for item in manifest)
            representatives = {item[1]: item for item in manifest}
            uploaded = 0
            with connect_db() as connection:
                self._lock_chunks(connection, counts)
                connection.execute(
                    "INSERT INTO buckets(bucket_name) VALUES (%s) ON CONFLICT DO NOTHING",
                    (bucket,))
                obj = connection.execute("""
                    INSERT INTO object_versions(bucket_name, object_key, size_bytes, chunking_method)
                    VALUES (%s, %s, %s, %s) RETURNING *
                """, (bucket, key, offset, chunking_method)).fetchone()
                for digest in sorted(counts):
                    _, _, size, start = representatives[digest]
                    row = connection.execute(
                        "SELECT * FROM chunks WHERE chunk_hash=%s FOR UPDATE", (digest,)).fetchone()
                    reusable = False
                    if row:
                        if row["size_bytes"] != size:
                            raise RuntimeError("Existing chunk size conflicts with its hash")
                        locations = connection.execute(
                            "SELECT node_id FROM chunk_locations WHERE chunk_hash=%s ORDER BY node_id",
                            (digest,)).fetchall()
                        for location in locations:
                            if location["node_id"] not in self.nodes:
                                continue
                            try:
                                check = requests.head(self._url(location["node_id"], digest), timeout=10)
                                reusable = (check.status_code == 200 and
                                            check.headers.get("ETag") == f'"{digest}"' and
                                            int(check.headers.get("Content-Length", -1)) == size)
                            except requests.RequestException:
                                reusable = False
                            if reusable:
                                break
                    if not reusable:
                        source.seek(start)
                        content = source.read(size)
                        if hashlib.sha256(content).hexdigest() != digest:
                            raise RuntimeError("Source file changed during upload")
                        node = self.ring.get_node(digest)
                        response = requests.put(self._url(node, digest), data=content, timeout=60)
                        response.raise_for_status()
                        connection.execute("""
                            INSERT INTO chunks(chunk_hash, size_bytes) VALUES (%s, %s)
                            ON CONFLICT (chunk_hash) DO NOTHING
                        """, (digest, size))
                        connection.execute("""
                            INSERT INTO chunk_locations(chunk_hash, node_id) VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                        """, (digest, node))
                        uploaded += 1
                    connection.execute(
                        "UPDATE chunks SET ref_count=ref_count+%s WHERE chunk_hash=%s",
                        (counts[digest], digest))
                with connection.cursor() as cursor:
                    cursor.executemany("""
                        INSERT INTO object_chunks(object_id, chunk_index, chunk_hash)
                        VALUES (%s, %s, %s)
                    """, [(obj["object_id"], index, digest) for index, digest, _, _ in manifest])
        return {**obj, "chunk_hashes": [m[1] for m in manifest],
                "uploaded_unique_chunks": uploaded,
                "reused_chunk_references": len(manifest) - uploaded}

    @staticmethod
    def _metadata(connection, object_id, lock="FOR SHARE"):
        obj = connection.execute(
            f"SELECT * FROM object_versions WHERE object_id=%s {lock}", (object_id,)).fetchone()
        if obj is None:
            raise KeyError(f"Object version not found: {object_id}")
        obj["chunk_hashes"] = [row["chunk_hash"] for row in connection.execute("""
            SELECT chunk_hash FROM object_chunks WHERE object_id=%s ORDER BY chunk_index
        """, (object_id,)).fetchall()]
        return obj

    def get_metadata(self, object_id):
        with connect_db() as connection:
            return self._metadata(connection, object_id)

    def download_file(self, object_id, destination):
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.NamedTemporaryFile(dir=destination.parent, delete=False)
        temporary_path = Path(temporary.name)
        try:
            with temporary, connect_db() as connection:
                # Deleting this object must wait until its active read finishes.
                obj = self._metadata(connection, object_id)
                written = 0
                for digest in obj["chunk_hashes"]:
                    row = connection.execute("SELECT size_bytes FROM chunks WHERE chunk_hash=%s",
                                             (digest,)).fetchone()
                    locations = connection.execute(
                        "SELECT node_id FROM chunk_locations WHERE chunk_hash=%s ORDER BY node_id",
                        (digest,)).fetchall()
                    verified = None
                    for location in locations:
                        if location["node_id"] not in self.nodes:
                            continue
                        try:
                            with requests.get(self._url(location["node_id"], digest),
                                              timeout=60, stream=True) as response:
                                if response.status_code != 200:
                                    continue
                                content = bytearray()
                                for block in response.iter_content(65536):
                                    content.extend(block)
                                    if len(content) > row["size_bytes"]:
                                        break
                            if (len(content) == row["size_bytes"] and
                                    hashlib.sha256(content).hexdigest() == digest):
                                verified = content
                                break
                        except requests.RequestException:
                            continue
                    if verified is None:
                        raise RuntimeError(f"No valid copy of chunk {digest}")
                    temporary.write(verified)
                    written += len(verified)
                if written != obj["size_bytes"]:
                    raise RuntimeError("Reconstructed file size does not match metadata")
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_path, destination)
            return obj
        finally:
            temporary_path.unlink(missing_ok=True)

    def chunk_stats(self, hashes):
        with connect_db() as connection:
            return connection.execute("""
                SELECT chunk_hash, size_bytes, ref_count FROM chunks
                WHERE chunk_hash=ANY(%s) ORDER BY chunk_hash
            """, (list(hashes),)).fetchall()

    def delete_object(self, object_id):
        with connect_db() as connection:
            obj = self._metadata(connection, object_id, lock="FOR UPDATE")
            counts = Counter(obj["chunk_hashes"])
            self._lock_chunks(connection, counts)
            connection.execute("DELETE FROM object_versions WHERE object_id=%s", (object_id,))
            for digest, count in counts.items():
                connection.execute("UPDATE chunks SET ref_count=ref_count-%s WHERE chunk_hash=%s",
                                   (count, digest))
        # Logical deletion commits first. Failed physical deletes leave ref_count=0
        # rows, so they can be retried without losing a live object's metadata.
        pending = self.collect_unreferenced(counts)
        return {"deleted_object": str(object_id), "pending_cleanup": pending}

    def collect_unreferenced(self, hashes):
        pending = []
        for digest in sorted(set(hashes)):
            with connect_db() as connection:
                self._lock_chunks(connection, [digest])
                row = connection.execute(
                    "SELECT ref_count FROM chunks WHERE chunk_hash=%s FOR UPDATE", (digest,)).fetchone()
                if row is None or row["ref_count"] != 0:
                    continue
                locations = connection.execute(
                    "SELECT node_id FROM chunk_locations WHERE chunk_hash=%s", (digest,)).fetchall()
                removed = True
                for location in locations:
                    node = location["node_id"]
                    try:
                        response = requests.delete(self._url(node, digest), timeout=10)
                        removed = removed and response.status_code in (204, 404)
                    except (KeyError, requests.RequestException):
                        removed = False
                if removed:
                    connection.execute("DELETE FROM chunks WHERE chunk_hash=%s", (digest,))
                else:
                    pending.append(digest)
        return pending
