"""Move the local Kubernetes lab metadata to its three-instance HA cluster.

Requires CloudNativePG and a Ready postgres-ha cluster. Uses a maintenance
window, copies metadata through pipes, and retains the original PostgreSQL PVC.
"""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import yaml


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--context", default="kind-midterm")
    a = p.parse_args()

    def kube(*args, check=True):
        r = subprocess.run(
            ["kubectl", "--context", a.context, "-n", "storage-lab", *args],
            text=True,
            capture_output=True,
            check=check,
        )
        if check and r.stderr:
            print(r.stderr.strip())
        return r.stdout.strip()

    cluster = json.loads(
        kube("get", "cluster.postgresql.cnpg.io", "postgres-ha", "-o", "json")
    )
    if cluster.get("status", {}).get("readyInstances") != 3:
        raise SystemExit(
            "Wait for all three postgres-ha instances to become Ready before migration"
        )
    config = json.loads(kube("get", "configmap", "storage-config", "-o", "json"))
    if config["data"]["POSTGRES_HOST"] == "postgres-ha-rw":
        print("Metadata already uses postgres-ha-rw. Run the HA failover test.")
        return
    primary = cluster["status"]["currentPrimary"]
    exists = kube(
        "exec",
        primary,
        "--",
        "psql",
        "-U",
        "postgres",
        "-d",
        "storage",
        "-Atc",
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'",
    )
    if int(exists):
        raise SystemExit("HA target already contains tables; refusing to overwrite it")
    deployments = ["gateway", "metadata-service", "worker", "recovery-service"]
    counts = {
        name: json.loads(kube("get", "deployment", name, "-o", "json"))["spec"][
            "replicas"
        ]
        for name in deployments
    }
    switched = False
    try:
        for name in deployments:
            kube("scale", "deployment/" + name, "--replicas=0")
        selector = "app in (gateway,metadata-service,worker,recovery-service)"
        remaining = json.loads(kube("get", "pods", "-l", selector, "-o", "json"))["items"]
        if remaining:
            kube("wait", "--for=delete", "pod", "-l", selector, "--timeout=180s")
        producer = subprocess.Popen(
            [
                "kubectl",
                "--context",
                a.context,
                "-n",
                "storage-lab",
                "exec",
                "postgres-0",
                "--",
                "pg_dump",
                "-U",
                "storage",
                "-d",
                "storage",
                "--no-owner",
                "--no-acl",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        consumer = subprocess.Popen(
            [
                "kubectl",
                "--context",
                a.context,
                "-n",
                "storage-lab",
                "exec",
                "-i",
                primary,
                "--",
                "psql",
                "-U",
                "postgres",
                "-d",
                "storage",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "SET ROLE storage",
                "-f",
                "-",
            ],
            stdin=producer.stdout,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        producer.stdout.close()
        _, restore_error = consumer.communicate(timeout=300)
        dump_error = producer.stderr.read()
        producer.wait(timeout=30)
        if producer.returncode or consumer.returncode:
            raise RuntimeError(
                "Metadata copy failed; original database retained. "
                + (restore_error + dump_error).decode(errors="replace")
            )
        query = "SELECT (SELECT count(*) FROM object_versions),(SELECT count(*) FROM chunks)"
        old = kube(
            "exec",
            "postgres-0",
            "--",
            "psql",
            "-U",
            "storage",
            "-d",
            "storage",
            "-Atc",
            query,
        )
        new = kube(
            "exec",
            primary,
            "--",
            "psql",
            "-U",
            "postgres",
            "-d",
            "storage",
            "-Atc",
            query,
        )
        if old != new:
            raise RuntimeError("Metadata row counts differ; original database retained")
        kube(
            "patch",
            "configmap",
            "storage-config",
            "--type=merge",
            "-p",
            json.dumps({"data": {"POSTGRES_HOST": "postgres-ha-rw"}}),
        )
        switched = True
        for name, count in counts.items():
            kube("scale", "deployment/" + name, "--replicas=" + str(count))
        for name in deployments:
            kube("rollout", "status", "deployment/" + name, "--timeout=300s")
        kube("scale", "statefulset/postgres", "--replicas=0")
        print(
            "PASS: metadata copied with matching object/chunk counts; gateways, heartbeat registry and workers now use postgres-ha-rw"
        )
        print(
            "Original PostgreSQL PVC retained as a pre-migration backup. New writes belong to the HA database."
        )
    finally:
        for name, count in counts.items():
            kube("scale", "deployment/" + name, "--replicas=" + str(count), check=False)


if __name__ == "__main__":
    main()
