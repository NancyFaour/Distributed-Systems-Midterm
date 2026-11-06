# Distributed object storage lab

FastAPI storage nodes, PostgreSQL metadata, consistent hashing, SHA-256 chunk addressing, reference-counted deduplication, replication and Reed–Solomon 4+2 coding.

## Start tomorrow

Start Docker Desktop, open this project in PyCharm and run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/start-lab.ps1
```

This creates the virtual environment and missing local credentials, builds images, applies idempotent schema migrations and starts services while preserving volumes. Select .venv/Scripts/python.exe as the PyCharm interpreter.

Open **http://localhost:8080**. Use API_KEY from the ignored .env file. API documentation: **http://localhost:8080/api/docs**. Keep credentials out of screenshots and Git.

Persistent PostgreSQL, RabbitMQ and Grafana accounts are initialized when their volumes are first created. Editing .env alone does not change an existing account password.

Follow **[TESTING.md](TESTING.md)** for every test and screenshot checkpoint. Keep the existing single Word report. Use docs/framework-flowchart.mmd for the implementation-aligned write/read/failure-repair diagram, comparing it with your previously prepared flowchart.

## Modules

- Seven independent storage containers and volumes; five-second heartbeats, PUT/GET/HEAD/DELETE, upload/read SHA-256 verification and atomic writes.
- PostgreSQL buckets, object versions, size/date/tags, ordered chunk hashes, locations and reference counts.
- Fixed 4 MiB chunking or content-defined Rabin–Karp rolling-hash chunking; SHA-256 remains the content identifier.
- Consistent hashing with 128 virtual nodes per physical node; queued copy-before-switch rebalancing on membership changes.
- Deduplication shares chunk representations within the same redundancy policy. Different policies retain separate representations. Reference counts protect live objects during deletion.
- Replication defaults to N=3, W=2, R=2, requiring W+R>N. Bucket-selectable zfec Reed–Solomon defaults to four data plus two parity shards. Initial EC writes require all six shards.
- Automatic missed-heartbeat repair and periodic integrity scrubbing. Repairs require enough verified source data and distinct healthy targets.
- Authenticated FastAPI JSON gateway: streaming uploads, full/range downloads, HEAD, versions, listing/deletion, metadata/tags, search, cluster status and shared PostgreSQL rate limiting.
- React console: upload, download, browsing, metadata inspection, Elasticsearch search, node distribution and maintenance-job status.
- Nginx balances two gateways. RabbitMQ distributes durable maintenance jobs across two workers, using publisher confirms, manual acknowledgements and PostgreSQL job/outbox state.
- Prometheus application metrics and a provisioned Grafana storage dashboard.
- Kubernetes manifests: StatefulSet/PVCs/probes, gateway replicas/HPA, workers, monitoring and optional three-instance CloudNativePG. Manifests are deployment assets; actual deployment and failover need runtime evidence.

Version identifiers are generated globally in PostgreSQL. Versions of a particular object key need not have consecutive numbers.

## Tests and workloads

```powershell
python -m pytest tests/test_unit.py -q
$env:RUN_INTEGRATION='1'
python -m pytest tests/test_gateway.py -q
python scripts/test-http.py --phase crud
python scripts/test-http.py --phase search
python scripts/test-http.py --phase balance
python benchmark_redundancy.py --profile smoke --output test-results/benchmark-smoke.json
python benchmark_redundancy.py --profile full --output test-results/benchmark-full.json
```

The full profile compares 5,000 JSON files of 1–50 KiB and 10 binary files of 100 MiB under both policies. It measures payload storage and repair reads/writes and verifies downloaded SHA-256 before and after repair. Automatic scanning is temporarily paused for controlled per-policy measurement; separate failure tests demonstrate automatic repair. Run failure, corruption, persistence, rebalance and benchmark experiments sequentially.

The original step-by-step scripts remain available. An optional eighth node is used for actual movement tests:

```powershell
docker compose --profile scale up -d --no-build --wait storage-node-8
python scripts/test-rebalance.py
```

## Addresses

| Service | Local address |
|---|---|
| React + Nginx | http://localhost:8080 |
| Gateway documentation | http://localhost:8080/api/docs |
| Node documentation | http://localhost:8001/docs through port 8007 |
| Heartbeats | http://localhost:8010/nodes |
| Recovery | http://localhost:8020/status and /events |
| PostgreSQL | localhost:5433; DB/user storage; password in .env |
| Elasticsearch | http://localhost:9200 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000; credentials in .env |
| RabbitMQ management | http://localhost:15672; credentials in .env |
| Kafka | localhost:9092; container address kafka:19092 |

Development ports bind to loopback. Internal services trust the isolated lab network. The API key grants shared administrator access, not tenant isolation.

## Deployment and limits

Read **[DEPLOYMENT.md](DEPLOYMENT.md)** for kind, cloud prerequisites, PostgreSQL HA, autoscaling and evidence. Cloud provisioning, image publishing and HTTPS require a selected provider and configured account.

This is an educational S3-like JSON API, without AWS SigV4/XML compatibility. Deleting the latest version exposes an older version when one remains; there are no S3 delete markers. Uploads/downloads spool to temporary disk to bound Python RAM. Byte-range downloads currently reconstruct the complete object first. Elasticsearch is an eventually consistent metadata projection and search includes object versions.

Local containers share one host failure domain and disk free-space pool. Free-space values must not be summed. Insufficient source fragments or replacement capacity leave objects degraded. Hard host/process crashes between physical writes and metadata commit can leave orphan bytes; normal failed writes are queued for cleanup. The local database is a single instance; metadata HA is only established after the optional replicated deployment and failover tests.

Use docker compose stop to preserve data. Avoid volume deletion when retaining stored objects and report fixtures. The original main.py is a PyCharm greeting, not the gateway.

References: [FastAPI requests](https://fastapi.tiangolo.com/advanced/using-request-directly/), [Elasticsearch Docker](https://www.elastic.co/docs/deploy-manage/deploy/self-managed/install-elasticsearch-docker-basic), [RabbitMQ acknowledgements](https://www.rabbitmq.com/docs/confirms), [Kubernetes StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/).
