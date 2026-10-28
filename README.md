# Distributed Systems Midterm: local environment

Use Python 3.12, Git, Docker Desktop with its Linux engine, and kubectl on Windows.
The existing Python requirements are installed in `.venv`. The GitHub repository
is already initialized; a clone retains its history and `origin` remote.

## Start

Open PowerShell in this repository:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\finish-setup.ps1
```

This installs the Python dependencies, starts the four services, creates or checks
the Kubernetes cluster, and verifies real Kafka/RabbitMQ messages and monitoring.
Progress and the final `COMPLETE` or `FAILED` result are saved in `.setup-status.log`.
The first run downloads large container images and may take a while.

To run just the supporting services and Python checks:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1
.\.venv\Scripts\python.exe main.py
.\.venv\Scripts\python.exe scripts\verify.py
```

The setup script creates `.venv` and a local `.env` if absent, installs Python
dependencies, and starts the services. Docker Desktop must be running first.
Activation is optional; to activate, use `.\.venv\Scripts\Activate.ps1`.
Set PyCharm's project interpreter to `.venv\Scripts\python.exe` in this clone.
`main.py` currently prints a greeting; it is not an HTTP application.

| Service | Local address | Login |
| --- | --- | --- |
| Prometheus | http://localhost:9090 | None |
| Grafana | http://localhost:3000 | `GRAFANA_USER` / `GRAFANA_PASSWORD` in `.env` |
| RabbitMQ management | http://localhost:15672 | `RABBITMQ_USER` / `RABBITMQ_PASSWORD` in `.env` |
| RabbitMQ AMQP | localhost:5672 | Same RabbitMQ credentials |
| Kafka | 127.0.0.1:9092 | Local development, plaintext |

All published ports bind to loopback. Credentials are randomly generated in
the ignored `.env` file. Keep this file local. To initialize another checkout,
run the setup script or copy `.env.example` to `.env` and choose passwords.
Credentials are initialized on the first creation of the corresponding data
volume; editing `.env` alone does not change an existing account password.
Container images are pinned by digest to preserve the selected builds.

Containers reach Kafka at `kafka:19092`; host Python clients use `127.0.0.1:9092`.
Grafana's Prometheus data source is provisioned automatically. Prometheus scrapes
itself and RabbitMQ every 15 seconds. Application and Kafka metrics exporters
can be added when implementing the distributed application.

## Storage-node volumes

Each of the four planned storage nodes has a dedicated Docker named volume.
The storage-node application will mount its assigned volume at `/data`.

| Node | Docker volume | Container mount |
| --- | --- | --- |
| storage-node-1 | distributed-systems-midterm_storage-node-1-data | /data |
| storage-node-2 | distributed-systems-midterm_storage-node-2-data | /data |
| storage-node-3 | distributed-systems-midterm_storage-node-3-data | /data |
| storage-node-4 | distributed-systems-midterm_storage-node-4-data | /data |

These volumes are declared in `compose.yaml`. Until node services are implemented,
create them on another checkout with:

```powershell
1..4 | ForEach-Object {
    docker volume create --label com.docker.compose.project=distributed-systems-midterm --label "com.docker.compose.volume=storage-node-$_-data" "distributed-systems-midterm_storage-node-$_-data"
}
docker volume ls --filter name=distributed-systems-midterm_storage-node-
```

Volumes persist when a container is replaced. They are separate Docker volumes
on one development computer; they do not provide separate physical disks or
protection against loss of that computer. Backups and distributed redundancy
will be addressed in later phases. Do not use `docker compose down -v` when
you need to keep stored data.

For this phase's screenshot, open Docker Desktop's **Volumes** page, filter for
`storage-node-`, and capture all four volumes and their names.

## Kubernetes cluster

This setup uses a dedicated kind cluster named `midterm`, running in Docker.
The four supporting services run through Compose, separately from Kubernetes.
The kind binary is stored locally under ignored `.tools`.

To install kind on a fresh checkout, follow the official Windows instructions:
https://kind.sigs.k8s.io/docs/user/quick-start/#installing-from-release-binaries
or run `winget install Kubernetes.kind` and use `kind` in place of `.\.tools\kind.exe`.

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup-kubernetes.ps1
kubectl --context kind-midterm get nodes
kubectl --context kind-midterm get pods -A
```

Explicitly use `--context kind-midterm` for this project's Kubernetes commands.
The cluster provides a development target for later application manifests.

## Manage the environment

```powershell
docker compose ps
docker compose logs --tail 100
docker compose stop
docker compose start
```

Named volumes preserve broker and monitoring data. `docker compose down` removes
the project's containers and network while keeping these volumes. Adding `-v`
also deletes the stored data. The smoke check uses a temporary RabbitMQ queue
and writes a small message to Kafka topic `environment-smoke-check`.

## Object metadata and placement tests

`object_store.py` connects the fixed-size chunker, consistent hashing ring,
PostgreSQL metadata, and the four running storage-node HTTP APIs. Run this
coordinator module from the project virtual environment; PostgreSQL is exposed
on loopback port 5433 and the nodes on ports 8001 through 8004.

The database schema is in `infra/postgres/schema.sql`. `metadata_db.py` reads the
local database password from the ignored `.env`. Object versions retain the
bucket, key, byte size, generated version identifier, creation timestamp, and
ordered chunk references. Version identifiers are globally generated database
identifiers; consecutive versions of a particular key need not be consecutive
integers.

Run these phases in order, taking a screenshot after each PASS result:

```powershell
python test_object_store.py upload
python test_object_store.py dedup
python test_object_store.py read
python test_object_store.py delete-first
python test_object_store.py delete-last
```

The demo file contains three logical chunks, including a repeated chunk, so
only two unique chunks need physical storage. Reference counts measure ordered
chunk occurrences: after two identical objects, counts are 4 and 2. Deleting
the first object leaves counts 2 and 1 and the second object remains readable.
Deleting the final object removes the unreferenced chunk rows and physical data.
PostgreSQL transactions and consistently ordered advisory locks protect updates
from overlapping writers. Chunk deletion runs after logical object deletion
commits; unavailable nodes leave zero-reference rows for explicit retry through
`collect_unreferenced`, rather than deleting a live object's metadata.

This stage uses fixed-size chunking and one stored copy per unique chunk.
Content-defined chunking, automatic migration, replication, erasure coding,
and repair are subsequent phases. Placement tests measure logical reassignment
of 10,000 deterministic sample hashes; they do not measure physical migration.
An upload interrupted before its database commit can leave unreferenced bytes
on a node; a complete orphan reconciliation job is not implemented yet.

## Technical references

- [Docker Desktop Kubernetes](https://docs.docker.com/desktop/use-desktop/kubernetes/)
- [kind installation and clusters](https://kind.sigs.k8s.io/docs/user/quick-start/)
- [Kafka Docker image](https://kafka.apache.org/41/getting-started/docker/)
- [Prometheus Docker installation](https://prometheus.io/docs/prometheus/latest/installation/)
- [Grafana Docker installation](https://grafana.com/docs/grafana/latest/setup-grafana/installation/docker/)
