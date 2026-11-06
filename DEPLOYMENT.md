# Deployment and bonus features

Local Docker Compose is the primary runnable lab. Cloud resources have not been provisioned by these scripts.

Nginx uses a 2-second upstream connection timeout for gateway failover and separate 600-second read/send timeouts for transfers. This avoids waiting for a stopped container beyond the client's authentication timeout. See the [Nginx connection-timeout directive](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_connect_timeout). Run `scripts/test-gateway-failover.py` using the project virtual environment to verify stopped-gateway routing and restore the service automatically.

## Docker services

The shared Python Dockerfile packages storage nodes, the PostgreSQL-backed heartbeat registry, gateway, recovery scheduler and maintenance workers. Each service runs its own process in a separate container. `ui/Dockerfile` builds React and serves it through Nginx. Nginx balances traffic across two gateways. Two RabbitMQ workers process durable repair, scrub, rebalance, garbage-collection and search-index jobs. PostgreSQL holds job state and transactional outboxes; messages use publisher confirms and manual acknowledgements.

Seven storage nodes support RS 4+2 with a spare for repair. An eighth node is available for the membership/rebalancing demonstration:

```powershell
docker compose --profile scale up -d --no-build --wait storage-node-8
python scripts/test-rebalance.py
```

The script restores the original ring membership after measuring actual fragment movement. The node-8 container and volume are kept for repeatable evidence.

For isolated regression evidence without changing public bucket metadata, run `python scripts/test-isolated-rebalance.py` after starting node-8. It uses a temporary PostgreSQL schema, a dedicated RabbitMQ queue and two disposable workers; it verifies physical migration and removes only its own fixtures.

## Local Kubernetes

Do this separately from full benchmarks. Running Compose and Kubernetes together uses substantially more memory.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/deploy-kind.ps1
kubectl --context kind-midterm -n storage-lab get pods,pvc
kubectl --context kind-midterm -n storage-lab rollout status statefulset/storage --timeout=600s
kubectl --context kind-midterm -n storage-lab port-forward service/ui 8081:80
```

Open http://localhost:8081 and use the existing local API key. The Kubernetes deployment is a separate, initially empty storage cluster. Compose data is not migrated. The storage StatefulSet has seven independent PVCs and startup, readiness and liveness probes. The base deployment has one PostgreSQL instance; it demonstrates local deployment, not metadata high availability.

On Docker Desktop's containerd image store, kind can encounter a [missing-content-digest import issue](https://kind.sigs.k8s.io/docs/user/known-issues). The deployment helper uses `scripts/load-kind-images.py` to export/import only linux/amd64 and preserves binary tar streams. Supporting images may also be preloaded with this helper to avoid duplicate downloads inside kind.

For real PVC evidence after the cluster becomes Ready, run `.\.venv\Scripts\python.exe -u scripts/test-kubernetes.py --pause-compose`. This replaces one storage pod, verifies that its original fragment remains on the PVC and downloads the object again. Both this script and `test-postgres-ha.py` reuse a healthy connection or automatically open a localhost-only port-forward on 8081 and close their own tunnel on exit. A second terminal is only needed when you want to keep the Kubernetes web interface open separately. On this development laptop, `--pause-compose` temporarily pauses running Compose containers, retains all volumes, and automatically resumes those containers on exit. This avoids competition between the two local deployments while measuring Kubernetes behavior. The PostgreSQL failover script accepts the same option. Run these experiments sequentially, with no concurrent Compose benchmark or failure test.

Install Metrics Server using its [official installation instructions](https://github.com/kubernetes-sigs/metrics-server#installation). For kind, the documented local testing option `--kubelet-insecure-tls` may be needed; this is not required on a correctly configured cloud cluster. Verify metrics before claiming autoscaling:

```powershell
kubectl --context kind-midterm top pods -n storage-lab
kubectl --context kind-midterm get hpa -n storage-lab
```

An HPA targets gateway CPU at 60%, with two to six replicas. Generate requests with `python scripts/load-gateway.py --base-url http://localhost:8081/api --seconds 120`, then capture changing replica counts. Merely having an HPA manifest does not demonstrate a successful scale event.

RabbitMQ uses lightweight AMQP TCP startup/readiness probes, following the [RabbitMQ monitoring guidance](https://www.rabbitmq.com/docs/monitoring). Repeated CLI probes caused timeouts and broker restarts under this laptop's CPU limits; the TCP probe avoids launching an Erlang CLI for each check.

`scripts/install-kind-extras.ps1` installs the official CloudNativePG operator and Metrics Server for the local cluster. Its insecure kubelet certificate option is restricted to kind contexts. Once the operator is Ready, apply `infra/kubernetes/cnpg-access.yaml` and `infra/kubernetes/postgres-ha.yaml`, wait for three Ready database instances, then run `python scripts/enable-postgres-ha.py`. This uses a maintenance window to copy metadata through pipes, verifies row counts, changes the application backend and retains the original database PVC. It refuses to overwrite a nonempty HA target. Run `python scripts/test-postgres-ha.py` to delete the active primary pod and verify that a different primary serves the same object metadata and bytes. The deploy helper preserves an already enabled HA backend on later redeployments.

To demonstrate a PVC surviving pod replacement, upload an object, delete only its storage pod, wait for the StatefulSet to recreate it, then download and compare hashes. Do not delete PVCs.

## Cloud deployment with replicated PostgreSQL

Use an existing GKE, EKS or AKS Kubernetes cluster with a default persistent storage class, network-policy support, Metrics Server and enough capacity for the workloads. A cloud account, billing/project selection, registry permissions and credentials are prerequisites. Choose the provider required by your course; the application manifests are portable.

1. Build and push the Python and UI images to your private registry using your own registry/project names. Configure image-pull credentials where required. The scripts do not push images automatically.
2. Install the CloudNativePG operator using the [official Helm chart](https://github.com/cloudnative-pg/charts/tree/main/charts/cloudnative-pg):

```powershell
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm repo update
helm upgrade --install cnpg cnpg/cloudnative-pg --namespace cnpg-system --create-namespace
```

3. Generate a fresh-cloud deployment with your pushed image tags:

```powershell
python scripts/prepare-cloud.py --app-image YOUR_REGISTRY/storage:lab --ui-image YOUR_REGISTRY/storage-ui:lab
```

4. Select the intended cloud kubecontext explicitly. Create namespace `storage-lab`, pipe `scripts/kubernetes-secrets.py` to `kubectl apply -f -`, then apply `infra/kubernetes/cloud.generated.yaml` to that context. Never run the secret generator by itself in a terminal or save its output to source control.
5. Wait for the three-member `postgres-ha` cluster, schema migration Job, storage PVCs, gateways and UI. The application connects to `postgres-ha-rw`. PostgreSQL uses one synchronous standby; observe failover before claiming metadata HA. Existing Compose or base-Kubernetes data is not copied into this new database.
6. Configure a DNS name and HTTPS ingress/load balancer certificate before sending API keys across the public network. The generated Service makes the UI reachable on HTTP for initial provisioning; use a local port-forward for authenticated tests until HTTPS is configured.
7. Capture the provider console, running pods/PVCs, `kubectl get cluster.postgresql.cnpg.io`, an actual database-primary failover, HPA scale-out and storage rebalancing. These are deployment evidence, not screenshots of YAML files.

To add the eighth Kubernetes storage node, scale StatefulSet `storage` to eight replicas, wait for `storage-7` to become Ready and send a heartbeat, then use the authenticated `PUT /cluster/members/storage-7?active=true` endpoint. It queues real copy-before-switch rebalancing. Compare the completed job's `moved_fragments`, `total_fragments` and `moved_percent`. Scale-out does not happen merely by starting a pod; membership activation triggers the migration.

Prometheus and Grafana are included. Dashboard metrics cover gateway request throughput and latency, upload bytes, node free space and chunks, degraded objects, repair completion rate and payload traffic. Gateway latency measures time until response headers; large-download completion time is measured by the benchmark separately. Node free-space values share a disk in local Docker and must not be summed.

References: [StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/), [HPA](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/), [CloudNativePG replication](https://cloudnative-pg.io/docs/current/replication/), [RabbitMQ delivery guarantees](https://www.rabbitmq.com/docs/confirms).
