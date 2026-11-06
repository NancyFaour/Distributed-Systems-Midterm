"""Emit Kubernetes Secret JSON to kubectl stdin. Never save or log the output."""

import base64
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from settings import setting


def secret(name, values):
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": "storage-lab"},
        "type": "Opaque",
        "data": {k: base64.b64encode(v.encode()).decode() for k, v in values.items()},
    }


if __name__ == "__main__":
    values = {
        k: setting(k, "")
        for k in (
            "POSTGRES_PASSWORD",
            "RABBITMQ_PASSWORD",
            "API_KEY",
            "GRAFANA_PASSWORD",
        )
    }
    if not all(values.values()):
        raise SystemExit("Run scripts/init-env.ps1 first")
    print(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "List",
                "items": [
                    secret("storage-secrets", values),
                    secret(
                        "storage-db-auth",
                        {
                            "username": "storage",
                            "password": values["POSTGRES_PASSWORD"],
                        },
                    ),
                ],
            }
        )
    )
