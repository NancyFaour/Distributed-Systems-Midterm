"""Copy before switching placement; remove obsolete fragments after commit."""

import hashlib
from consistent_hashing import ConsistentHashRing
from metadata_db import connect_db
from redundant_store import RedundantStore
from redundancy import Policy


def rebalance(store=None):
    store = store or RedundantStore()
    ring = ConsistentHashRing(store.active_nodes())
    moved = total = affected = 0
    traffic = {"read_bytes": 0, "written_bytes": 0}
    with connect_db() as connection:
        representations = connection.execute("""SELECT r.*,p.policy FROM chunk_representations r
            JOIN redundancy_policies p USING(policy_key) WHERE ref_count>0 ORDER BY chunk_hash,policy_key""").fetchall()
    for item in representations:
        with connect_db() as connection:
            digest, key = item["chunk_hash"], item["policy_key"]
            store._lock_chunks(connection, [digest])
            if not connection.execute(
                "SELECT 1 FROM chunk_representations WHERE chunk_hash=%s AND policy_key=%s AND ref_count>0",
                (digest, key),
            ).fetchone():
                continue
            policy = Policy(**item["policy"])
            wanted = set(ring.get_nodes(digest, policy.total))
            rows = store._fragments(connection, digest, key)
            current = {row["node_id"] for row in rows}
            targets = sorted(wanted - current)
            changes = [row for row in rows if row["node_id"] not in wanted]
            total += len(rows)
            if changes:
                affected += 1
            for fragment, target in zip(changes, targets):
                data = store._fetch(fragment, digest, policy, traffic)
                if data is None:
                    # Reconstruct the source shard when the departing owner is unavailable.
                    original = store._read_data(
                        connection, digest, policy, traffic, repair=True
                    )
                    data = policy.encode(original)[fragment["fragment_index"]]
                payload = (
                    store._header(digest, policy, fragment["fragment_index"]) + data
                )
                if hashlib.sha256(payload).hexdigest() != fragment["fragment_hash"]:
                    raise RuntimeError("Rebalance source failed integrity verification")
                if not store._send(target, payload, traffic):
                    raise RuntimeError(
                        "Target write failed; original placement retained"
                    )
                connection.execute(
                    """UPDATE fragment_locations SET node_id=%s,status='healthy'
                    WHERE chunk_hash=%s AND policy_key=%s AND fragment_index=%s""",
                    (target, digest, key, fragment["fragment_index"]),
                )
                connection.execute(
                    "INSERT INTO obsolete_fragments VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                    (fragment["node_id"], fragment["fragment_hash"], digest),
                )
                moved += 1
    store.drain_obsolete()
    return {
        "representations_scanned": len(representations),
        "representations_changed": affected,
        "total_fragments": total,
        "moved_fragments": moved,
        "moved_percent": round(100 * moved / total, 3) if total else 0,
        **traffic,
    }
