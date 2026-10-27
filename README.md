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

## Kubernetes

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

## References

- [Docker Desktop Kubernetes](https://docs.docker.com/desktop/use-desktop/kubernetes/)
- [kind installation and clusters](https://kind.sigs.k8s.io/docs/user/quick-start/)
- [Kafka Docker image](https://kafka.apache.org/41/getting-started/docker/)
- [Prometheus Docker installation](https://prometheus.io/docs/prometheus/latest/installation/)
- [Grafana Docker installation](https://grafana.com/docs/grafana/latest/setup-grafana/installation/docker/)
