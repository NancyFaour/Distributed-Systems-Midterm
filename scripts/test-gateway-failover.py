"""Stop a real gateway, check Nginx failover, and always restore the service."""

from pathlib import Path
import subprocess
import sys
import time

import requests

ROOT = Path(__file__).resolve().parents[1]
BASE = "http://127.0.0.1:8080/api"


def compose(*args, capture=False):
    return subprocess.run(
        ["docker", "compose", *args], cwd=ROOT, check=True,
        capture_output=capture, text=True,
    )


def main():
    compose("up", "-d", "--no-build", "--no-deps", "--wait", "--wait-timeout", "120",
            "gateway-1", "gateway-2")
    expected = compose(
        "exec", "-T", "gateway-2", "python", "-c",
        "import socket; print(socket.gethostname())", capture=True,
    ).stdout.strip()

    # Populate Nginx's DNS/connection caches while both gateways are live. The
    # first requests after stop must work even with the old address cached.
    instances = set()
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline and len(instances) < 2:
        response = requests.get(BASE + "/health", timeout=10)
        response.raise_for_status()
        instances.add(response.json()["instance"])
        if len(instances) < 2:
            time.sleep(0.5)
    assert len(instances) == 2, "Both gateways must serve requests before failure"
    print("BEFORE FAILURE: two gateway instances:", sorted(instances), flush=True)
    slowest = 0
    try:
        compose("stop", "gateway-1")
        compose("ps", "--all", "gateway-1", "gateway-2")
        # Separate connections exercise routing of unauthenticated responses,
        # including the exact request that previously exceeded its timeout.
        for _ in range(8):
            started = time.monotonic()
            unauthorized = requests.get(BASE + "/buckets", timeout=10)
            assert unauthorized.status_code == 401, unauthorized.text
            response = requests.get(BASE + "/health", timeout=10)
            response.raise_for_status()
            assert response.json()["instance"] == expected
            slowest = max(slowest, time.monotonic() - started)
        print("PASS: 8 authentication checks returned HTTP 401; all 8 health checks reached gateway-2",
              flush=True)
        print(f"Slowest authentication/health request pair: {slowest:.3f} seconds", flush=True)
        subprocess.run(
            [sys.executable, "-u", "scripts/test-http.py", "--phase", "crud"],
            cwd=ROOT, check=True,
        )
        print("PASS: authenticated CRUD, ranges and SHA-256 checks with gateway-1 stopped", flush=True)
    finally:
        compose("up", "-d", "--no-build", "--no-deps", "--wait", "--wait-timeout", "120", "gateway-1")
    compose("ps", "gateway-1", "gateway-2")
    print("PASS: gateway failover verified; both gateways restored and healthy", flush=True)


if __name__ == "__main__":
    main()
