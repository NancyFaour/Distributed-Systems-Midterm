"""Generate portable Kubernetes manifests; no cloud resources are created."""

import argparse
import json
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "infra" / "kubernetes"
NS = "storage-lab"


def resource(kind, name, spec=None, **extra):
    api = {
        "Deployment": "apps/v1",
        "StatefulSet": "apps/v1",
        "Job": "batch/v1",
        "HorizontalPodAutoscaler": "autoscaling/v2",
        "PodDisruptionBudget": "policy/v1",
    }.get(kind, "v1")
    result = {
        "apiVersion": api,
        "kind": kind,
        "metadata": {"name": name, "namespace": NS},
        **extra,
    }
    if spec is not None:
        result["spec"] = spec
    return result


def service(name, port=8000, target=None, headless=False, selector=None):
    spec = {
        "selector": {"app": selector or name},
        "ports": [{"name": "http", "port": port, "targetPort": target or port}],
    }
    if headless:
        spec["clusterIP"] = "None"
    return resource("Service", name, spec)


def probe(path="/health", port=8000):
    return {
        "httpGet": {"path": path, "port": port},
        "periodSeconds": 10,
        "timeoutSeconds": 5,
        "failureThreshold": 6,
    }


def container(name, image, command=None, env=True, port=8000):
    result = {
        "name": name,
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "resources": {
            "requests": {"cpu": "50m", "memory": "96Mi"},
            "limits": {"cpu": "1000m", "memory": "512Mi"},
        },
    }
    if env:
        result["envFrom"] = [
            {"configMapRef": {"name": "storage-config"}},
            {"secretRef": {"name": "storage-secrets"}},
        ]
    if command:
        result["command"] = command
    if port:
        result.update(
            ports=[{"containerPort": port}],
            readinessProbe=probe(port=port),
            livenessProbe=probe(port=port),
            startupProbe={**probe(port=port), "failureThreshold": 30},
        )
    return result


def deployment(name, cont, replicas=1, volumes=None):
    spec = {"containers": [cont]}
    if name in {"gateway", "metadata-service", "worker", "recovery-service"}:
        wait_code = """import time
from metadata_db import connect_db
while True:
    try:
        with connect_db() as connection:
            connection.execute('SELECT content_type,tags FROM object_versions LIMIT 0')
            connection.execute('SELECT 1 FROM node_heartbeats LIMIT 0')
            connection.execute('SELECT 1 FROM maintenance_jobs LIMIT 0')
        break
    except Exception:
        print('Waiting for metadata schema', flush=True)
        time.sleep(3)
"""
        spec["initContainers"] = [
            {
                "name": "wait-for-schema",
                "image": cont["image"],
                "imagePullPolicy": "IfNotPresent",
                "envFrom": cont["envFrom"],
                "command": ["python", "-c", wait_code],
            }
        ]
    if volumes:
        spec["volumes"] = volumes
    return resource(
        "Deployment",
        name,
        {
            "replicas": replicas,
            "selector": {"matchLabels": {"app": name}},
            "template": {"metadata": {"labels": {"app": name}}, "spec": spec},
        },
    )


def stateful(name, cont, size, replicas=1, mount="/data"):
    cont["volumeMounts"] = [{"name": "data", "mountPath": mount}]
    return resource(
        "StatefulSet",
        name,
        {
            "serviceName": name + "-headless",
            "replicas": replicas,
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [cont], "securityContext": {"fsGroup": 1000}},
            },
            "volumeClaimTemplates": [
                {
                    "metadata": {"name": "data"},
                    "spec": {
                        "accessModes": ["ReadWriteOnce"],
                        "resources": {"requests": {"storage": size}},
                    },
                }
            ],
        },
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--app-image", default="distributed-storage-node:local")
    p.add_argument("--ui-image", default="distributed-storage-ui:local")
    args = p.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    images = {
        name: value["image"]
        for name, value in yaml.safe_load((ROOT / "compose.yaml").read_text())[
            "services"
        ].items()
        if "image" in value
    }
    nodes = {
        f"storage-{i}": f"http://storage-{i}.storage-headless:8000" for i in range(8)
    }
    config = {
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PORT": "5432",
        "METADATA_BACKEND": "postgres",
        "HEARTBEAT_REGISTRY_URL": "http://metadata-service:8000",
        "METADATA_URL": "http://metadata-service:8000",
        "STORAGE_NODES": json.dumps(nodes),
        "INITIAL_STORAGE_NODES": json.dumps(
            {k: v for k, v in nodes.items() if k != "storage-7"}
        ),
        "ELASTICSEARCH_URL": "http://elasticsearch:9200",
        "RABBITMQ_HOST": "rabbitmq",
        "RABBITMQ_USER": "midterm",
        "JOB_QUEUE_ENABLED": "true",
        "SCRUB_INTERVAL_SECONDS": "60",
        "ROOT_PATH": "/api",
        "RATE_LIMIT_PER_MINUTE": "300",
        "HEARTBEAT_TIMEOUT_SECONDS": "15",
    }
    docs = [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}},
        resource("ConfigMap", "storage-config", data=config),
    ]
    pg = container("postgres", "postgres:17-alpine", env=False, port=None)
    pg["env"] = [
        {"name": "POSTGRES_USER", "value": "storage"},
        {"name": "POSTGRES_DB", "value": "storage"},
        {"name": "PGDATA", "value": "/var/lib/postgresql/data/pgdata"},
        {
            "name": "POSTGRES_PASSWORD",
            "valueFrom": {
                "secretKeyRef": {"name": "storage-secrets", "key": "POSTGRES_PASSWORD"}
            },
        },
    ]
    pg["readinessProbe"] = {
        "exec": {"command": ["pg_isready", "-U", "storage", "-d", "storage"]},
        "periodSeconds": 5,
    }
    pg["livenessProbe"] = pg["readinessProbe"]
    pg["startupProbe"] = {**pg["readinessProbe"], "failureThreshold": 60}
    docs += [
        service("postgres", 5432),
        service("postgres-headless", 5432, headless=True, selector="postgres"),
        stateful("postgres", pg, "10Gi", mount="/var/lib/postgresql/data"),
    ]
    mig = container(
        "migrate", args.app_image, ["python", "scripts/migrate.py"], port=None
    )
    docs.append(
        resource(
            "Job",
            "storage-migrate",
            {
                "backoffLimit": 12,
                "template": {
                    "spec": {"restartPolicy": "OnFailure", "containers": [mig]}
                },
            },
        )
    )
    metadata = container(
        "metadata-service",
        args.app_image,
        [
            "python",
            "-m",
            "uvicorn",
            "metadata_service:app",
            "--host",
            "0.0.0.0",
            "--port",
            "8000",
        ],
    )
    docs += [service("metadata-service"), deployment("metadata-service", metadata, 2)]
    node = container(
        "storage",
        args.app_image,
        [
            "python",
            "-m",
            "uvicorn",
            "storage_node:app",
            "--host",
            "0.0.0.0",
            "--port",
            "8000",
        ],
    )
    node["env"] = [
        {"name": "NODE_ID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
        {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
        {"name": "STORAGE_URL", "value": "http://$(POD_NAME).storage-headless:8000"},
        {"name": "DATA_DIR", "value": "/data"},
    ]
    docs += [
        service("storage-headless", headless=True, selector="storage"),
        stateful("storage", node, "20Gi", 7),
    ]
    gateway = container(
        "gateway",
        args.app_image,
        [
            "python",
            "-m",
            "uvicorn",
            "gateway:app",
            "--host",
            "0.0.0.0",
            "--port",
            "8000",
        ],
    )
    docs += [
        service("gateway"),
        deployment("gateway", gateway, 2),
        service("gateway-1", selector="gateway"),
        service("gateway-2", selector="gateway"),
    ]
    worker = container("worker", args.app_image, ["python", "job_queue.py"], port=None)
    docs.append(deployment("worker", worker, 2))
    recovery = container(
        "recovery",
        args.app_image,
        [
            "python",
            "-m",
            "uvicorn",
            "recovery_service:app",
            "--host",
            "0.0.0.0",
            "--port",
            "8000",
        ],
    )
    docs += [service("recovery-service"), deployment("recovery-service", recovery)]
    rabbit = container("rabbitmq", "rabbitmq:4-management", env=False, port=None)
    rabbit["env"] = [
        {"name": "RABBITMQ_DEFAULT_USER", "value": "midterm"},
        {"name": "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS", "value": "+S 2:2 +A 4"},
        {
            "name": "RABBITMQ_DEFAULT_PASS",
            "valueFrom": {
                "secretKeyRef": {"name": "storage-secrets", "key": "RABBITMQ_PASSWORD"}
            },
        },
    ]
    # Match RabbitMQ's recommended lightweight AMQP readiness check. Repeated
    # CLI launches can starve a CPU-limited broker and cause restart loops.
    rabbit["readinessProbe"] = {
        "tcpSocket": {"port": 5672},
        "periodSeconds": 10,
        "timeoutSeconds": 5,
    }
    rabbit["startupProbe"] = {**rabbit["readinessProbe"], "failureThreshold": 60}
    rabbit["resources"]["limits"]["memory"] = "768Mi"
    docs += [
        service("rabbitmq", 5672),
        service("rabbitmq-headless", 5672, headless=True, selector="rabbitmq"),
        stateful("rabbitmq", rabbit, "5Gi", mount="/var/lib/rabbitmq"),
    ]
    es = container(
        "elasticsearch",
        "docker.elastic.co/elasticsearch/elasticsearch:9.5.6",
        env=False,
        port=None,
    )
    es["env"] = [
        {"name": k, "value": v}
        for k, v in {
            "discovery.type": "single-node",
            "xpack.security.enabled": "false",
            "ES_JAVA_OPTS": "-Xms384m -Xmx384m",
        }.items()
    ]
    es["resources"] = {
        "requests": {"cpu": "200m", "memory": "768Mi"},
        "limits": {"cpu": "1500m", "memory": "1536Mi"},
    }
    es["readinessProbe"] = probe("/_cluster/health", 9200)
    es["startupProbe"] = {**probe("/", 9200), "failureThreshold": 60}
    es["livenessProbe"] = probe("/", 9200)
    docs += [
        service("elasticsearch", 9200),
        service(
            "elasticsearch-headless", 9200, headless=True, selector="elasticsearch"
        ),
        stateful("elasticsearch", es, "10Gi", mount="/usr/share/elasticsearch/data"),
    ]
    ui = container("ui", args.ui_image, env=False, port=None)
    ui["env"] = [
        {
            "name": "GATEWAY_1_ADDRESS",
            "value": "gateway-1.storage-lab.svc.cluster.local:8000",
        },
        {
            "name": "GATEWAY_2_ADDRESS",
            "value": "gateway-2.storage-lab.svc.cluster.local:8000",
        },
    ]
    ui["ports"] = [{"containerPort": 80}]
    ui["readinessProbe"] = probe("/", 80)
    ui["livenessProbe"] = probe("/", 80)
    docs += [service("ui", 80), deployment("ui", ui)]
    docs.append(
        resource(
            "HorizontalPodAutoscaler",
            "gateway",
            {
                "scaleTargetRef": {
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "gateway",
                },
                "minReplicas": 2,
                "maxReplicas": 6,
                "metrics": [
                    {
                        "type": "Resource",
                        "resource": {
                            "name": "cpu",
                            "target": {"type": "Utilization", "averageUtilization": 60},
                        },
                    }
                ],
            },
        )
    )
    docs.append(
        resource(
            "PodDisruptionBudget",
            "gateway",
            {"minAvailable": 1, "selector": {"matchLabels": {"app": "gateway"}}},
        )
    )
    prom = {
        "global": {"scrape_interval": "15s"},
        "scrape_configs": [
            {
                "job_name": "gateway",
                "kubernetes_sd_configs": [
                    {"role": "pod", "namespaces": {"names": [NS]}}
                ],
                "relabel_configs": [
                    {
                        "source_labels": ["__meta_kubernetes_pod_label_app"],
                        "regex": "gateway",
                        "action": "keep",
                    },
                    {
                        "source_labels": ["__meta_kubernetes_pod_ip"],
                        "target_label": "__address__",
                        "replacement": "$1:8000",
                    },
                ],
            }
        ],
    }
    docs.append(
        resource(
            "ConfigMap",
            "prometheus-config",
            data={"prometheus.yml": yaml.safe_dump(prom)},
        )
    )
    docs.append(
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": "prometheus", "namespace": NS},
        }
    )
    docs.append(
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": {"name": "prometheus", "namespace": NS},
            "rules": [
                {
                    "apiGroups": [""],
                    "resources": ["pods"],
                    "verbs": ["get", "list", "watch"],
                }
            ],
        }
    )
    docs.append(
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {"name": "prometheus", "namespace": NS},
            "subjects": [
                {"kind": "ServiceAccount", "name": "prometheus", "namespace": NS}
            ],
            "roleRef": {
                "kind": "Role",
                "name": "prometheus",
                "apiGroup": "rbac.authorization.k8s.io",
            },
        }
    )
    pc = container("prometheus", images["prometheus"], env=False, port=None)
    pc["volumeMounts"] = [{"name": "config", "mountPath": "/etc/prometheus"}]
    pd = deployment(
        "prometheus",
        pc,
        volumes=[{"name": "config", "configMap": {"name": "prometheus-config"}}],
    )
    pd["spec"]["template"]["spec"]["serviceAccountName"] = "prometheus"
    docs += [service("prometheus", 9090), pd]
    datasource = {
        "apiVersion": 1,
        "datasources": [
            {
                "name": "Prometheus",
                "uid": "prometheus",
                "type": "prometheus",
                "access": "proxy",
                "url": "http://prometheus:9090",
                "isDefault": True,
            }
        ],
    }
    dashboard = (
        (ROOT / "infra/grafana/dashboards/storage.json").read_text()
        if (ROOT / "infra/grafana/dashboards/storage.json").exists()
        else "{}"
    )
    docs.append(
        resource(
            "ConfigMap",
            "grafana-config",
            data={
                "datasource.yaml": yaml.safe_dump(datasource),
                "provider.yaml": yaml.safe_dump(
                    {
                        "apiVersion": 1,
                        "providers": [
                            {
                                "name": "Storage",
                                "type": "file",
                                "options": {"path": "/var/lib/grafana/dashboards"},
                            }
                        ],
                    }
                ),
                "storage.json": dashboard,
            },
        )
    )
    gc = container("grafana", images["grafana"], env=False, port=None)
    gc["env"] = [
        {
            "name": "GF_SECURITY_ADMIN_PASSWORD",
            "valueFrom": {
                "secretKeyRef": {"name": "storage-secrets", "key": "GRAFANA_PASSWORD"}
            },
        },
        {"name": "GF_PLUGINS_PREINSTALL_DISABLED", "value": "true"},
    ]
    gc["volumeMounts"] = [
        {
            "name": "config",
            "mountPath": "/etc/grafana/provisioning/datasources/datasource.yaml",
            "subPath": "datasource.yaml",
        },
        {
            "name": "config",
            "mountPath": "/etc/grafana/provisioning/dashboards/provider.yaml",
            "subPath": "provider.yaml",
        },
        {
            "name": "config",
            "mountPath": "/var/lib/grafana/dashboards/storage.json",
            "subPath": "storage.json",
        },
    ]
    docs += [
        service("grafana", 3000),
        deployment(
            "grafana",
            gc,
            volumes=[{"name": "config", "configMap": {"name": "grafana-config"}}],
        ),
    ]
    docs.append(
        resource(
            "NetworkPolicy",
            "internal-only",
            {
                "podSelector": {},
                "policyTypes": ["Ingress"],
                "ingress": [{"from": [{"podSelector": {}}]}],
            },
        )
    )
    # NetworkPolicy uses networking.k8s.io, not core/v1.
    docs[-1]["apiVersion"] = "networking.k8s.io/v1"
    (OUT / "lab.yaml").write_text(
        yaml.safe_dump_all(docs, sort_keys=False), encoding="utf-8"
    )
    (OUT / "kustomization.yaml").write_text(
        "resources:\n  - lab.yaml\n", encoding="utf-8"
    )
    # A separate optional HA cluster. Operator installation is a deployment prerequisite.
    ha = {
        "apiVersion": "postgresql.cnpg.io/v1",
        "kind": "Cluster",
        "metadata": {"name": "postgres-ha", "namespace": NS},
        "spec": {
            "instances": 3,
            "imageName": "ghcr.io/cloudnative-pg/postgresql:17.11",
            "bootstrap": {
                "initdb": {
                    "database": "storage",
                    "owner": "storage",
                    "secret": {"name": "storage-db-auth"},
                }
            },
            "storage": {"size": "20Gi"},
            "postgresql": {
                "synchronous": {
                    "method": "any",
                    "number": 1,
                    "dataDurability": "required",
                }
            },
            "resources": {
                "requests": {"cpu": "100m", "memory": "256Mi"},
                "limits": {"memory": "1Gi"},
            },
        },
    }
    (OUT / "postgres-ha.yaml").write_text(
        yaml.safe_dump(ha, sort_keys=False), encoding="utf-8"
    )
    print(
        f"Generated {len(docs)} Kubernetes resources and optional three-instance PostgreSQL HA cluster."
    )


if __name__ == "__main__":
    main()
