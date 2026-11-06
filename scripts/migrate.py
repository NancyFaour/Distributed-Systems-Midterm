"""Idempotent schema initialization, preserving existing objects and volumes."""

import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from metadata_db import connect_db


def migrate():
    for name in ("schema.sql", "redundancy.sql", "gateway.sql"):
        with connect_db() as connection:
            connection.execute(
                (ROOT / "infra" / "postgres" / name).read_text(), prepare=False
            )
        print("APPLIED:", name)
    nodes = json.loads(
        os.getenv("INITIAL_STORAGE_NODES", os.getenv("STORAGE_NODES", "{}"))
    )
    if not nodes:
        nodes = {f"node-{i}": "" for i in range(1, 8)}
    with connect_db() as connection:
        if not connection.execute("SELECT 1 FROM storage_members LIMIT 1").fetchone():
            for node in nodes:
                connection.execute(
                    "INSERT INTO storage_members(node_id) VALUES (%s)", (node,)
                )
    print("PASS: metadata schema ready; existing data preserved")


if __name__ == "__main__":
    migrate()
