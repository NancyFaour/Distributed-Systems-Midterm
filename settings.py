"""Local .env configuration without printing credentials."""

import os
from pathlib import Path


def setting(name, default=None):
    if name in os.environ:
        return os.environ[name]
    path = Path(__file__).parent / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            if line.strip() and not line.lstrip().startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                if key.strip() == name:
                    return value.strip().strip("\"'")
    return default
