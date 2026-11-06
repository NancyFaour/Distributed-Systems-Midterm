"""Cluster observations; free space is per logical node on a shared Docker disk."""

import requests
from prometheus_client import CollectorRegistry, Gauge, generate_latest
from metadata_db import connect_db


def cluster_status(store):
    registry = requests.get(store.metadata_url + "/nodes", timeout=5)
    registry.raise_for_status()
    nodes = registry.json()["nodes"]
    active = store.active_nodes()
    online = [
        n["node_id"]
        for n in nodes
        if n["status"] == "online" and n["node_id"] in active
    ]
    with connect_db() as connection:
        distribution = connection.execute("""SELECT node_id,COUNT(*) AS fragments,SUM(size_bytes) AS stored_bytes
            FROM fragment_locations GROUP BY node_id""").fetchall()
        degraded = connection.execute(
            """SELECT COUNT(DISTINCT o.object_id) AS count
            FROM object_chunks o JOIN object_policies p USING(object_id)
            JOIN fragment_locations f ON f.chunk_hash=o.chunk_hash AND f.policy_key=p.policy_key
            WHERE f.status!='healthy' OR NOT(f.node_id=ANY(%s::text[]))""",
            (online,),
        ).fetchone()["count"]
        totals = connection.execute("""SELECT COUNT(*) AS object_versions,COALESCE(SUM(size_bytes),0) AS logical_bytes
            FROM object_versions""").fetchone()
        chunks = connection.execute("""SELECT COUNT(*) AS unique_chunks,COALESCE(SUM(size_bytes),0) AS unique_chunk_bytes,
            COALESCE(SUM(ref_count),0) AS chunk_references FROM chunks WHERE ref_count>0""").fetchone()
        events = connection.execute(
            "SELECT * FROM recovery_events ORDER BY event_id DESC LIMIT 20"
        ).fetchall()
        jobs = connection.execute(
            "SELECT * FROM maintenance_jobs ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
        repairs = connection.execute(
            "SELECT COUNT(*) AS count FROM recovery_events WHERE outcome='repaired'"
        ).fetchone()["count"]
        traffic = connection.execute(
            "SELECT COALESCE(SUM(read_bytes+written_bytes),0) AS bytes FROM recovery_events"
        ).fetchone()["bytes"]
        pending = connection.execute(
            "SELECT COUNT(*) AS count FROM search_outbox"
        ).fetchone()["count"]
    by_id = {d["node_id"]: d for d in distribution}
    return {
        **totals,
        **chunks,
        "degraded_objects": degraded,
        "repairs_completed": repairs,
        "repair_payload_bytes": traffic,
        "search_updates_pending": pending,
        "nodes": [
            {
                **n,
                **by_id.get(n["node_id"], {"fragments": 0, "stored_bytes": 0}),
                "active": n["node_id"] in active,
            }
            for n in nodes
        ],
        "events": events,
        "jobs": jobs,
        "free_space_note": "Logical nodes share the Docker host disk; free-space values must not be summed.",
    }


def cluster_metrics(store):
    state = cluster_status(store)
    registry = CollectorRegistry()
    for name, field in [
        ("storage_degraded_objects", "degraded_objects"),
        ("storage_object_versions", "object_versions"),
        ("storage_logical_bytes", "logical_bytes"),
        ("storage_repairs_completed", "repairs_completed"),
        ("storage_repair_payload_bytes", "repair_payload_bytes"),
    ]:
        Gauge(name, name.replace("_", " "), registry=registry).set(state[field])
    health = Gauge(
        "storage_node_online", "Node heartbeat health", ["node"], registry=registry
    )
    free = Gauge(
        "storage_node_free_bytes",
        "Node filesystem free bytes (shared host)",
        ["node"],
        registry=registry,
    )
    count = Gauge(
        "storage_node_chunks", "Node reported chunk count", ["node"], registry=registry
    )
    used = Gauge(
        "storage_node_assigned_bytes",
        "Assigned fragment payload bytes",
        ["node"],
        registry=registry,
    )
    for node in state["nodes"]:
        health.labels(node["node_id"]).set(node["status"] == "online")
        free.labels(node["node_id"]).set(node["free_space_bytes"])
        count.labels(node["node_id"]).set(node["chunk_count"])
        used.labels(node["node_id"]).set(node["stored_bytes"])
    return generate_latest(registry)
