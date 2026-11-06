"""Open a scoped local kubectl tunnel for the Kubernetes evidence scripts."""

from contextlib import contextmanager
import os
from pathlib import Path
import subprocess
import tempfile
import time
from urllib.parse import urlsplit

import requests


@contextmanager
def compose_resource_window(enabled):
    if not enabled:
        yield
        return
    result = subprocess.run(
        ["docker", "ps", "--filter", "label=com.docker.compose.project=distributed-systems-midterm",
         "--filter", "status=running", "--format", "{{.ID}}"],
        check=True, capture_output=True, text=True,
    )
    containers = result.stdout.split()
    paused = []
    try:
        if containers:
            print("Temporarily pausing the Compose lab to free resources for Kubernetes; data volumes are retained", flush=True)
            result = subprocess.run(["docker", "pause", *containers], capture_output=True, text=True)
            paused = result.stdout.split()
            result.check_returncode()
        yield
    finally:
        if paused:
            print("Resuming the Compose lab...", flush=True)
            subprocess.run(["docker", "unpause", *paused], check=True, capture_output=True, text=True)
            print("Compose containers resumed", flush=True)


def gateway_ready(base_url):
    try:
        response = requests.get(base_url.rstrip("/") + "/health", timeout=3)
        data = response.json()
        return response.status_code == 200 and data.get("status") == "healthy" and bool(data.get("instance"))
    except (requests.RequestException, ValueError, AttributeError):
        return False


@contextmanager
def gateway_connection(context, base_url):
    if gateway_ready(base_url):
        yield
        return
    parsed = urlsplit(base_url)
    if parsed.scheme != "http" or parsed.hostname not in ("localhost", "127.0.0.1") or parsed.port != 8081:
        raise RuntimeError(f"Kubernetes gateway is not healthy at {base_url}; check its URL and connection")

    output = Path(__file__).resolve().parents[1] / "test-results"
    output.mkdir(exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", prefix="kubectl-forward-", suffix=".log", dir=output,
                                     encoding="utf-8", delete=False) as log:
        command = ["kubectl", "--context", context, "-n", "storage-lab", "port-forward",
                   "--address", "127.0.0.1", "service/ui", "8081:80"]
        print("Opening Kubernetes gateway connection on 127.0.0.1:8081...", flush=True)
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                   creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        try:
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                if gateway_ready(base_url):
                    print("Kubernetes gateway connection ready", flush=True)
                    yield
                    return
                if process.poll() is not None:
                    raise RuntimeError(f"kubectl port-forward exited; inspect {log.name}")
                time.sleep(1)
            raise RuntimeError(f"Kubernetes gateway did not become healthy; inspect {log.name}")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            print("Automatic Kubernetes gateway connection closed", flush=True)
