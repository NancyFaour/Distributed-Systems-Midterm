"""Bounded, public health-request load for an explicit HPA demonstration."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import time
import requests


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default="http://127.0.0.1:8080/api")
    p.add_argument("--seconds", type=int, default=60)
    p.add_argument("--workers", type=int, default=12)
    a = p.parse_args()
    deadline = time.monotonic() + min(max(a.seconds, 1), 600)

    def worker(_):
        good = bad = 0
        with requests.Session() as client:
            while time.monotonic() < deadline:
                try:
                    r = client.get(a.base_url.rstrip("/") + "/health", timeout=10)
                    good += r.status_code == 200
                    bad += r.status_code != 200
                except requests.RequestException:
                    bad += 1
        return good, bad

    with ThreadPoolExecutor(max_workers=min(max(a.workers, 1), 64)) as pool:
        results = list(pool.map(worker, range(a.workers)))
    print(
        "SUCCESSFUL REQUESTS:",
        sum(r[0] for r in results),
        "FAILED:",
        sum(r[1] for r in results),
    )
    print(
        "Capture actual HPA replica changes; request count alone does not establish autoscaling."
    )


if __name__ == "__main__":
    main()
