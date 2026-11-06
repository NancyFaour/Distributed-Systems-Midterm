"""Preserve an enabled HA backend when regenerating/redeploying the local lab."""

import argparse
import json
from pathlib import Path
import subprocess
import yaml

ROOT = Path(__file__).resolve().parents[1]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--context", default="kind-midterm")
    a = p.parse_args()
    result = subprocess.run(
        [
            "kubectl",
            "--context",
            a.context,
            "-n",
            "storage-lab",
            "get",
            "configmap",
            "storage-config",
            "--ignore-not-found",
            "-o",
            "json",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    if not result.stdout.strip():
        return
    path = ROOT / "infra/kubernetes/lab.yaml"
    docs = list(yaml.safe_load_all(path.read_text()))
    deployment = subprocess.run([
        'kubectl', '--context', a.context, '-n', 'storage-lab', 'get', 'deployment',
        'gateway', '--ignore-not-found', '-o', 'json'], capture_output=True, text=True, check=True)
    if deployment.stdout.strip():
        replicas = json.loads(deployment.stdout)['spec']['replicas']
        for item in docs:
            if item['kind'] == 'Deployment' and item['metadata']['name'] == 'gateway':
                item['spec']['replicas'] = replicas
    if json.loads(result.stdout)['data'].get('POSTGRES_HOST') != 'postgres-ha-rw':
        path.write_text(yaml.safe_dump_all(docs, sort_keys=False), encoding='utf-8')
        return
    docs = [
        d
        for d in docs
        if d["metadata"]["name"] not in ("postgres", "postgres-headless")
    ]
    for d in docs:
        if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "storage-config":
            d["data"]["POSTGRES_HOST"] = "postgres-ha-rw"
    for name in ("postgres-ha.yaml", "cnpg-access.yaml"):
        docs.append(yaml.safe_load((ROOT / "infra/kubernetes" / name).read_text()))
    path.write_text(yaml.safe_dump_all(docs, sort_keys=False), encoding="utf-8")
    print(
        "Preserved existing HA metadata backend; original single-instance database is not restarted."
    )


if __name__ == "__main__":
    main()
