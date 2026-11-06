"""Bucket-selectable redundant object storage, recovery, and payload traffic accounting."""

from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import struct
import tempfile

import requests
from psycopg.types.json import Jsonb

from chunking import fixed_chunks, content_defined_chunks
from consistent_hashing import ConsistentHashRing
from metadata_db import connect_db
from object_store import ObjectStore
from redundancy import Policy


class QuorumError(RuntimeError):
    pass


class RedundantStore(ObjectStore):
    def __init__(self, nodes=None):
        if nodes is None:
            nodes = (
                json.loads(os.environ["STORAGE_NODES"])
                if os.getenv("STORAGE_NODES")
                else {f"node-{i}": f"http://127.0.0.1:{8000 + i}" for i in range(1, 8)}
            )
        super().__init__(nodes)
        self.metadata_url = os.getenv("HEARTBEAT_REGISTRY_URL", "http://127.0.0.1:8010")

    @staticmethod
    def _header(digest, policy, index):
        label = policy.key.encode()
        return (
            b"DSF1"
            + bytes.fromhex(digest)
            + struct.pack("!HH", index, len(label))
            + label
        )

    def configure_bucket(self, bucket, policy=None):
        policy = policy or Policy()
        if policy.total > len(self.nodes):
            raise ValueError("Policy requires more distinct nodes than configured")
        with connect_db() as connection:
            connection.execute(
                "INSERT INTO buckets(bucket_name) VALUES (%s) ON CONFLICT DO NOTHING",
                (bucket,),
            )
            connection.execute(
                "SELECT * FROM buckets WHERE bucket_name=%s FOR UPDATE", (bucket,)
            )
            existing = connection.execute(
                "SELECT policy_key FROM bucket_policies WHERE bucket_name=%s", (bucket,)
            ).fetchone()
            if existing and existing["policy_key"] != policy.key:
                if connection.execute(
                    "SELECT 1 FROM object_versions WHERE bucket_name=%s LIMIT 1",
                    (bucket,),
                ).fetchone():
                    raise ValueError(
                        "Cannot change a bucket policy while it contains object versions"
                    )
            connection.execute(
                "INSERT INTO redundancy_policies VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (policy.key, Jsonb(policy.to_dict())),
            )
            connection.execute(
                """INSERT INTO bucket_policies VALUES (%s,%s)
                ON CONFLICT(bucket_name) DO UPDATE SET policy_key=excluded.policy_key""",
                (bucket, policy.key),
            )
        return policy

    def bucket_policy(self, bucket):
        with connect_db() as connection:
            row = connection.execute(
                """SELECT p.policy FROM bucket_policies b JOIN redundancy_policies p
                USING(policy_key) WHERE b.bucket_name=%s""",
                (bucket,),
            ).fetchone()
        return Policy(**row["policy"]) if row else self.configure_bucket(bucket)

    def active_nodes(self):
        # Membership is shared across gateway/worker processes. Before the gateway
        # migration, retain compatibility with the earlier storage-node exercises.
        import psycopg

        try:
            with connect_db() as connection:
                rows = connection.execute(
                    "SELECT node_id FROM storage_members WHERE active"
                ).fetchall()
            return {row["node_id"] for row in rows} & self.nodes.keys()
        except psycopg.errors.UndefinedTable:
            return set(self.nodes)

    @staticmethod
    def _fragments(connection, digest, key):
        return connection.execute(
            """SELECT * FROM fragment_locations WHERE chunk_hash=%s
            AND policy_key=%s ORDER BY fragment_index""",
            (digest, key),
        ).fetchall()

    def _send(self, node, payload, traffic):
        digest = hashlib.sha256(payload).hexdigest()
        try:
            response = requests.put(
                self._url(node, digest), data=payload, timeout=(1, 30)
            )
            if response.status_code in (200, 201):
                traffic["written_bytes"] += len(payload)
                return True
        except requests.RequestException:
            pass
        return False

    def _check(self, fragment):
        try:
            response = requests.head(
                self._url(fragment["node_id"], fragment["fragment_hash"]),
                timeout=(1, 10),
            )
            return (
                response.status_code == 200
                and int(response.headers.get("Content-Length", -1))
                == fragment["size_bytes"]
                and response.headers.get("ETag")
                == '"' + fragment["fragment_hash"] + '"'
            )
        except (KeyError, requests.RequestException):
            return False

    def _fetch(self, fragment, digest, policy, traffic):
        try:
            with requests.get(
                self._url(fragment["node_id"], fragment["fragment_hash"]),
                timeout=(1, 30),
                stream=True,
            ) as response:
                if response.status_code != 200:
                    return None
                content = bytearray()
                for block in response.iter_content(65536):
                    traffic["read_bytes"] += len(block)
                    content.extend(block)
                    if len(content) > fragment["size_bytes"]:
                        return None
            if (
                len(content) != fragment["size_bytes"]
                or hashlib.sha256(content).hexdigest() != fragment["fragment_hash"]
            ):
                return None
            header = self._header(digest, policy, fragment["fragment_index"])
            if not content.startswith(header):
                return None
            return bytes(content[len(header) :])
        except (KeyError, requests.RequestException):
            return None

    def _read_data(
        self, connection, digest, policy, traffic, repair=False, candidates=None
    ):
        size = connection.execute(
            "SELECT size_bytes FROM chunks WHERE chunk_hash=%s", (digest,)
        ).fetchone()["size_bytes"]
        rows = (
            self._fragments(connection, digest, policy.key)
            if candidates is None
            else candidates
        )
        available = {}
        required = (
            1 if repair and policy.mode == "replication" else policy.read_required
        )
        for fragment in rows:
            value = self._fetch(fragment, digest, policy, traffic)
            if value is not None:
                available[fragment["fragment_index"]] = value
                if len(available) >= required:
                    break
        if len(available) < required:
            raise QuorumError(
                f"Need {required} verified fragments/copies; got {len(available)}"
            )
        if repair and policy.mode == "replication":
            result = next(iter(available.values()))
            if len(result) != size or hashlib.sha256(result).hexdigest() != digest:
                raise RuntimeError("Repair source failed chunk integrity")
            return result
        return policy.decode(available, size, digest)

    def put_file(self, bucket, key, filename, chunking_method="fixed", metadata=None):
        attempted = []
        try:
            return self._put_file(
                bucket, key, filename, chunking_method, attempted, metadata
            )
        except Exception:
            if attempted:
                with connect_db() as connection:
                    self._lock_chunks(connection, [item[2] for item in attempted])
                    with connection.cursor() as cursor:
                        cursor.executemany(
                            "INSERT INTO obsolete_fragments VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                            attempted,
                        )
            raise

    def _put_file(
        self, bucket, key, filename, chunking_method, attempted, metadata=None
    ):
        if chunking_method not in ("fixed", "cdc"):
            raise ValueError("Unknown chunking method")
        policy = self.bucket_policy(bucket)
        active = self.active_nodes()
        placement_nodes = active
        try:
            registry = requests.get(self.metadata_url + "/nodes", timeout=(1, 2))
            registry.raise_for_status()
            online = {
                node["node_id"]
                for node in registry.json()["nodes"]
                if node["status"] == "online"
            } & active
            # New representations can use healthy spares without waiting for an
            # administrator to remove a failed member. Existing replica groups
            # still require their configured quorum until the repair job moves them.
            if len(online) >= policy.total:
                placement_nodes = online
        except (requests.RequestException, ValueError, KeyError):
            pass  # Actual storage acknowledgements still enforce W (or all EC shards).
        ring = ConsistentHashRing(placement_nodes)
        if policy.total > len(ring.nodes):
            raise QuorumError("Not enough active distinct nodes for this bucket policy")
        manifest, offset = [], 0
        traffic = {"read_bytes": 0, "written_bytes": 0}
        uploaded = 0
        with Path(filename).open("rb") as source:
            chunker = (
                fixed_chunks if chunking_method == "fixed" else content_defined_chunks
            )
            for index, content in enumerate(chunker(source)):
                manifest.append(
                    (index, hashlib.sha256(content).hexdigest(), len(content), offset)
                )
                offset += len(content)
            counts = Counter(row[1] for row in manifest)
            representatives = {row[1]: row for row in manifest}
            with connect_db() as connection:
                self._lock_chunks(connection, counts)
                obj = connection.execute(
                    """INSERT INTO object_versions
                    (bucket_name,object_key,size_bytes,chunking_method) VALUES (%s,%s,%s,%s) RETURNING *""",
                    (bucket, key, offset, chunking_method),
                ).fetchone()
                if metadata is not None:
                    obj = connection.execute(
                        """UPDATE object_versions SET tags=%s,
                        content_type=%s,content_sha256=%s WHERE object_id=%s RETURNING *""",
                        (
                            Jsonb(metadata.get("tags", {})),
                            metadata.get("content_type", "application/octet-stream"),
                            metadata.get("content_sha256"),
                            obj["object_id"],
                        ),
                    ).fetchone()
                connection.execute(
                    "INSERT INTO object_policies VALUES (%s,%s)",
                    (obj["object_id"], policy.key),
                )
                for digest in sorted(counts):
                    _, _, size, position = representatives[digest]
                    connection.execute(
                        "INSERT INTO chunks(chunk_hash,size_bytes) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                        (digest, size),
                    )
                    stored = connection.execute(
                        "SELECT size_bytes FROM chunks WHERE chunk_hash=%s", (digest,)
                    ).fetchone()
                    if stored["size_bytes"] != size:
                        raise RuntimeError("Chunk size/hash conflict")
                    connection.execute(
                        """INSERT INTO chunk_representations VALUES (%s,%s,0)
                        ON CONFLICT DO NOTHING""",
                        (digest, policy.key),
                    )
                    existing = self._fragments(connection, digest, policy.key)
                    good = sum(self._check(fragment) for fragment in existing)
                    if good < policy.write_required:
                        source.seek(position)
                        data = source.read(size)
                        if hashlib.sha256(data).hexdigest() != digest:
                            raise RuntimeError("Source file changed during upload")
                        targets = (
                            [fragment["node_id"] for fragment in existing]
                            if len(existing) == policy.total
                            else ring.get_nodes(digest, policy.total)
                        )
                        acknowledgements = 0
                        for index, shard in enumerate(policy.encode(data)):
                            payload = self._header(digest, policy, index) + shard
                            node = targets[index]
                            attempted.append(
                                (node, hashlib.sha256(payload).hexdigest(), digest)
                            )
                            acknowledged = self._send(node, payload, traffic)
                            acknowledgements += acknowledged
                            connection.execute(
                                """INSERT INTO fragment_locations VALUES (%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT(chunk_hash,policy_key,fragment_index) DO UPDATE SET
                                fragment_hash=excluded.fragment_hash,node_id=excluded.node_id,
                                size_bytes=excluded.size_bytes,status=excluded.status""",
                                (
                                    digest,
                                    policy.key,
                                    index,
                                    hashlib.sha256(payload).hexdigest(),
                                    node,
                                    len(payload),
                                    "healthy" if acknowledged else "missing",
                                ),
                            )
                        if acknowledgements < policy.write_required:
                            raise QuorumError(
                                f"Write quorum failed: {acknowledgements}/{policy.write_required}"
                            )
                        uploaded += 1
                    connection.execute(
                        "UPDATE chunks SET ref_count=ref_count+%s WHERE chunk_hash=%s",
                        (counts[digest], digest),
                    )
                    connection.execute(
                        """UPDATE chunk_representations SET ref_count=ref_count+%s
                        WHERE chunk_hash=%s AND policy_key=%s""",
                        (counts[digest], digest, policy.key),
                    )
                with connection.cursor() as cursor:
                    cursor.executemany(
                        "INSERT INTO object_chunks VALUES (%s,%s,%s)",
                        [
                            (obj["object_id"], index, digest)
                            for index, digest, _, _ in manifest
                        ],
                    )
        return {
            **obj,
            "policy": policy.to_dict(),
            "policy_key": policy.key,
            "chunk_hashes": [row[1] for row in manifest],
            "uploaded_unique_chunks": uploaded,
            "traffic": traffic,
        }

    def download_file(self, object_id, destination):
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.NamedTemporaryFile(dir=destination.parent, delete=False)
        path = Path(temporary.name)
        traffic = {"read_bytes": 0, "written_bytes": 0}
        try:
            with temporary, connect_db() as connection:
                obj = self._metadata(connection, object_id)
                row = connection.execute(
                    """SELECT p.policy FROM object_policies o JOIN redundancy_policies p
                    USING(policy_key) WHERE o.object_id=%s""",
                    (object_id,),
                ).fetchone()
                if row is None:
                    raise ValueError("Use ObjectStore for legacy single-copy objects")
                policy = Policy(**row["policy"])
                size = 0
                for digest in obj["chunk_hashes"]:
                    data = self._read_data(connection, digest, policy, traffic)
                    temporary.write(data)
                    size += len(data)
                if size != obj["size_bytes"]:
                    raise RuntimeError("Wrong reconstructed file size")
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(path, destination)
            return {**obj, "traffic": traffic}
        finally:
            path.unlink(missing_ok=True)

    def delete_object(self, object_id):
        with connect_db() as connection:
            obj = self._metadata(connection, object_id, lock="FOR UPDATE")
            row = connection.execute(
                "SELECT policy_key FROM object_policies WHERE object_id=%s",
                (object_id,),
            ).fetchone()
            if row is None:
                raise ValueError("Use ObjectStore for legacy objects")
            key = row["policy_key"]
            counts = Counter(obj["chunk_hashes"])
            self._lock_chunks(connection, counts)
            connection.execute(
                "DELETE FROM object_versions WHERE object_id=%s", (object_id,)
            )
            for digest, count in counts.items():
                connection.execute(
                    "UPDATE chunks SET ref_count=ref_count-%s WHERE chunk_hash=%s",
                    (count, digest),
                )
                connection.execute(
                    """UPDATE chunk_representations SET ref_count=ref_count-%s
                    WHERE chunk_hash=%s AND policy_key=%s""",
                    (count, digest, key),
                )
        return self.collect_representations(counts, key)

    def collect_representations(self, hashes, policy_key):
        pending = []
        for digest in sorted(set(hashes)):
            with connect_db() as connection:
                self._lock_chunks(connection, [digest])
                row = connection.execute(
                    """SELECT ref_count FROM chunk_representations
                    WHERE chunk_hash=%s AND policy_key=%s FOR UPDATE""",
                    (digest, policy_key),
                ).fetchone()
                if row is None or row["ref_count"]:
                    continue
                complete = True
                for fragment in self._fragments(connection, digest, policy_key):
                    try:
                        response = requests.delete(
                            self._url(fragment["node_id"], fragment["fragment_hash"]),
                            timeout=(1, 10),
                        )
                        complete = complete and response.status_code in (204, 404)
                    except requests.RequestException:
                        complete = False
                if complete:
                    connection.execute(
                        "DELETE FROM chunk_representations WHERE chunk_hash=%s AND policy_key=%s",
                        (digest, policy_key),
                    )
                    connection.execute(
                        """DELETE FROM chunks c WHERE c.chunk_hash=%s AND c.ref_count=0
                        AND NOT EXISTS(SELECT 1 FROM chunk_representations r WHERE r.chunk_hash=c.chunk_hash)""",
                        (digest,),
                    )
                else:
                    pending.append(digest)
        return {"pending_cleanup": pending}

    def fragments(self, object_id):
        with connect_db() as connection:
            row = connection.execute(
                "SELECT policy_key FROM object_policies WHERE object_id=%s",
                (object_id,),
            ).fetchone()
            if row is None:
                raise KeyError(object_id)
            return connection.execute(
                """SELECT DISTINCT f.* FROM object_chunks o JOIN fragment_locations f
                ON o.chunk_hash=f.chunk_hash WHERE o.object_id=%s AND f.policy_key=%s
                ORDER BY f.chunk_hash,f.fragment_index""",
                (object_id, row["policy_key"]),
            ).fetchall()

    def maintain(self, scrub=False, object_id=None, bucket=None):
        registry = requests.get(self.metadata_url + "/nodes", timeout=5)
        registry.raise_for_status()
        online = {
            row["node_id"]
            for row in registry.json()["nodes"]
            if row["status"] == "online"
        } & self.active_nodes()
        with connect_db() as connection:
            representations = connection.execute(
                """SELECT r.*,p.policy FROM chunk_representations r
                JOIN redundancy_policies p USING(policy_key) WHERE r.ref_count>0
                AND (%s::uuid IS NULL OR EXISTS(SELECT 1 FROM object_chunks o JOIN object_policies op
                    USING(object_id) WHERE o.object_id=%s AND o.chunk_hash=r.chunk_hash AND op.policy_key=r.policy_key))
                AND (%s::text IS NULL OR EXISTS(SELECT 1 FROM object_chunks o JOIN object_policies op
                    USING(object_id) JOIN object_versions v USING(object_id) WHERE v.bucket_name=%s
                    AND o.chunk_hash=r.chunk_hash AND op.policy_key=r.policy_key))
                ORDER BY r.chunk_hash,r.policy_key""",
                (object_id, object_id, bucket, bucket),
            ).fetchall()
        events = []
        for item in representations:
            digest, key, policy = (
                item["chunk_hash"],
                item["policy_key"],
                Policy(**item["policy"]),
            )
            traffic = {"read_bytes": 0, "written_bytes": 0}
            with connect_db() as connection:
                self._lock_chunks(connection, [digest])
                if not connection.execute(
                    "SELECT 1 FROM chunk_representations WHERE chunk_hash=%s AND policy_key=%s AND ref_count>0",
                    (digest, key),
                ).fetchone():
                    continue
                rows = self._fragments(connection, digest, key)
                good, bad = [], []
                for fragment in rows:
                    if fragment["node_id"] not in online:
                        connection.execute(
                            """UPDATE fragment_locations SET status='missing'
                            WHERE chunk_hash=%s AND policy_key=%s AND fragment_index=%s""",
                            (digest, key, fragment["fragment_index"]),
                        )
                        bad.append(fragment)
                    elif (
                        (scrub or fragment["status"] != "healthy")
                        and self._fetch(fragment, digest, policy, traffic) is None
                    ):
                        connection.execute(
                            """UPDATE fragment_locations SET status='corrupt'
                            WHERE chunk_hash=%s AND policy_key=%s AND fragment_index=%s""",
                            (digest, key, fragment["fragment_index"]),
                        )
                        bad.append(fragment)
                    else:
                        # A missed heartbeat does not prove its on-disk copy was
                        # lost. Revalidate returning copies before trusting them
                        # as repair sources; never keep a stale missing/corrupt
                        # label permanently after the node becomes reachable.
                        if fragment["status"] != "healthy":
                            connection.execute(
                                """UPDATE fragment_locations SET status='healthy'
                                WHERE chunk_hash=%s AND policy_key=%s AND fragment_index=%s""",
                                (digest, key, fragment["fragment_index"]),
                            )
                            fragment["status"] = "healthy"
                        good.append(fragment)
                if not bad:
                    continue
                cause = (
                    "scrub"
                    if scrub and all(f["node_id"] in online for f in bad)
                    else "node_failure"
                )
                outcome, detail = "repaired", ""
                try:
                    data = self._read_data(
                        connection,
                        digest,
                        policy,
                        traffic,
                        repair=True,
                        candidates=good,
                    )
                    encoded = policy.encode(data)
                    used = {row["node_id"] for row in rows}
                    for fragment in bad:
                        index, old_node = (
                            fragment["fragment_index"],
                            fragment["node_id"],
                        )
                        if old_node in online:
                            target = old_node
                        else:
                            candidates = (
                                ConsistentHashRing(online).get_nodes(
                                    digest, len(online)
                                )
                                if online
                                else []
                            )
                            target = next(
                                (node for node in candidates if node not in used), None
                            )
                        if target is None:
                            outcome, detail = (
                                "degraded",
                                "Not enough distinct healthy nodes to restore full redundancy",
                            )
                            continue
                        payload = self._header(digest, policy, index) + encoded[index]
                        if not self._send(target, payload, traffic):
                            outcome, detail = "degraded", "Replacement write failed"
                            continue
                        if (
                            hashlib.sha256(payload).hexdigest()
                            != fragment["fragment_hash"]
                        ):
                            raise RuntimeError(
                                "Re-encoded fragment differs from expected hash"
                            )
                        connection.execute(
                            """UPDATE fragment_locations SET node_id=%s,status='healthy'
                            WHERE chunk_hash=%s AND policy_key=%s AND fragment_index=%s""",
                            (target, digest, key, index),
                        )
                        used.add(target)
                        if target != old_node:
                            connection.execute(
                                "INSERT INTO obsolete_fragments VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                                (old_node, fragment["fragment_hash"], digest),
                            )
                except (RuntimeError, ValueError) as error:
                    outcome, detail = "unrecoverable", str(error)
                event = {
                    "chunk_hash": digest,
                    "policy_key": key,
                    "cause": cause,
                    "outcome": outcome,
                    **traffic,
                    "detail": detail,
                }
                connection.execute(
                    """INSERT INTO recovery_events
                    (chunk_hash,policy_key,cause,outcome,read_bytes,written_bytes,detail)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    tuple(event.values()),
                )
                events.append(event)
        self.drain_obsolete(online)
        with connect_db() as connection:
            unused = connection.execute(
                "SELECT chunk_hash,policy_key FROM chunk_representations WHERE ref_count=0"
            ).fetchall()
        for unused_row in unused:
            self.collect_representations(
                [unused_row["chunk_hash"]], unused_row["policy_key"]
            )
        return events

    def drain_obsolete(self, online=None):
        with connect_db() as connection:
            queued = connection.execute("SELECT * FROM obsolete_fragments").fetchall()
        for item in queued:
            if online is not None and item["node_id"] not in online:
                continue
            with connect_db() as connection:
                self._lock_chunks(connection, [item["chunk_hash"]])
                live = connection.execute(
                    "SELECT 1 FROM fragment_locations WHERE node_id=%s AND fragment_hash=%s LIMIT 1",
                    (item["node_id"], item["fragment_hash"]),
                ).fetchone()
                if not live:
                    try:
                        response = requests.delete(
                            self._url(item["node_id"], item["fragment_hash"]),
                            timeout=(1, 10),
                        )
                        if response.status_code not in (204, 404):
                            continue
                    except requests.RequestException:
                        continue
                connection.execute(
                    "DELETE FROM obsolete_fragments WHERE node_id=%s AND fragment_hash=%s",
                    (item["node_id"], item["fragment_hash"]),
                )
