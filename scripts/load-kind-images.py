"""Single-platform image transfer, avoiding kind/containerd missing-digest imports.

Use binary subprocess pipes rather than PowerShell's text pipeline for tar data.
Requires Docker's image-save --platform option (available in this lab's Docker).
"""

import argparse
import json
import re
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", default="midterm")
    parser.add_argument("images", nargs="+")
    args = parser.parse_args()
    nodes = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            "label=io.x-k8s.kind.cluster=" + args.cluster,
            "--format",
            "{{.Names}}",
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    if not nodes:
        raise SystemExit("No running kind nodes found for " + args.cluster)
    for node in nodes:
        for image in args.images:
            if re.search(r":lab-[0-9a-f]{12}$", image):
                current = json.loads(
                    subprocess.run(
                        ["docker", "exec", node, "crictl", "images", "-o", "json"],
                        capture_output=True,
                        text=True,
                        check=True,
                    ).stdout
                )
                expected = "docker.io/library/" + image if "/" not in image else image
                if any(
                    expected in item.get("repoTags", [])
                    for item in current.get("images", [])
                ):
                    print("CACHED:", image, "on", node, flush=True)
                    continue
            print("IMPORT linux/amd64:", image, "->", node, flush=True)
            source = subprocess.Popen(
                ["docker", "image", "save", "--platform=linux/amd64", image],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            target = subprocess.Popen(
                [
                    "docker",
                    "exec",
                    "--privileged",
                    "-i",
                    node,
                    "ctr",
                    "--namespace=k8s.io",
                    "images",
                    "import",
                    "--platform",
                    "linux/amd64",
                    "--digests",
                    "--snapshotter=overlayfs",
                    "-",
                ],
                stdin=source.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            source.stdout.close()
            try:
                output, error = target.communicate(timeout=1800)
                source_error = source.stderr.read()
                source.wait(timeout=30)
                if target.returncode or source.returncode:
                    raise RuntimeError((error + source_error).decode(errors="replace"))
                print(output.decode(errors="replace").strip(), flush=True)
            finally:
                for process in (source, target):
                    if process.poll() is None:
                        process.kill()
                        process.wait()
    print("PASS: requested images imported for linux/amd64", flush=True)


if __name__ == "__main__":
    main()
