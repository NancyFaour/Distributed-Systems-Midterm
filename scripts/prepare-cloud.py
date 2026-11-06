"""Prepare a GKE/AKS/EKS-ready HA deployment; no provisioning or publishing."""

import argparse
from pathlib import Path
import subprocess
import sys
import yaml

ROOT = Path(__file__).resolve().parents[1]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--app-image", required=True)
    p.add_argument("--ui-image", required=True)
    p.add_argument("--storage-class", default="")
    p.add_argument("--output", default="infra/kubernetes/cloud.generated.yaml")
    a = p.parse_args()
    if ":local" in a.app_image or ":local" in a.ui_image:
        raise SystemExit("Supply pushed registry image tags for a cloud deployment")
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/generate-kubernetes.py"),
            "--app-image",
            a.app_image,
            "--ui-image",
            a.ui_image,
        ],
        check=True,
    )
    docs = list(yaml.safe_load_all((ROOT / "infra/kubernetes/lab.yaml").read_text()))
    docs = [
        d
        for d in docs
        if d["metadata"]["name"] not in ("postgres", "postgres-headless")
    ]
    for d in docs:
        if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "storage-config":
            d["data"]["POSTGRES_HOST"] = "postgres-ha-rw"
        if d["kind"] == "Service" and d["metadata"]["name"] == "ui":
            d["spec"]["type"] = "LoadBalancer"
        if d["kind"] == "StatefulSet":
            d["spec"]["template"]["spec"]["affinity"] = {
                "podAntiAffinity": {
                    "preferredDuringSchedulingIgnoredDuringExecution": [
                        {
                            "weight": 100,
                            "podAffinityTerm": {
                                "labelSelector": {
                                    "matchLabels": d["spec"]["selector"]["matchLabels"]
                                },
                                "topologyKey": "kubernetes.io/hostname",
                            },
                        }
                    ]
                }
            }
            if a.storage_class:
                for pvc in d["spec"]["volumeClaimTemplates"]:
                    pvc["spec"]["storageClassName"] = a.storage_class
    ha = yaml.safe_load((ROOT / "infra/kubernetes/postgres-ha.yaml").read_text())
    if a.storage_class:
        ha["spec"]["storage"]["storageClass"] = a.storage_class
    docs.append(ha)
    docs.append(
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": "postgres-operator", "namespace": "storage-lab"},
            "spec": {
                "podSelector": {"matchLabels": {"cnpg.io/cluster": "postgres-ha"}},
                "policyTypes": ["Ingress"],
                "ingress": [
                    {
                        "from": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {
                                        "kubernetes.io/metadata.name": "cnpg-system"
                                    }
                                }
                            }
                        ],
                        "ports": [
                            {"protocol": "TCP", "port": 8000},
                            {"protocol": "TCP", "port": 5432},
                        ],
                    }
                ],
            },
        }
    )
    # Public entry is limited to the UI/reverse proxy. Keep internal unauthenticated
    # Elasticsearch, storage chunks, broker, and operational endpoints private.
    docs.append(
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": "ui-entry", "namespace": "storage-lab"},
            "spec": {
                "podSelector": {"matchLabels": {"app": "ui"}},
                "policyTypes": ["Ingress"],
                "ingress": [{"ports": [{"protocol": "TCP", "port": 80}]}],
            },
        }
    )
    target = ROOT / a.output
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(yaml.safe_dump_all(docs, sort_keys=False), encoding="utf-8")
    print("PREPARED:", target)
    print(
        "Not deployed. Install CloudNativePG, configure cloud kubecontext and secrets, then apply. Add HTTPS before sending a real API key over a public connection."
    )


if __name__ == "__main__":
    main()
