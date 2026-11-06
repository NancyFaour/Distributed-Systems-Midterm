"""Immutable chunk replication with quorums, or systematic Reed-Solomon coding."""
from dataclasses import asdict, dataclass
import hashlib
import json

from zfec import Encoder, Decoder


@dataclass(frozen=True)
class Policy:
    mode: str = "replication"
    n: int = 3
    w: int = 2
    r: int = 2
    k: int = 4
    m: int = 2  # parity count; zfec's second argument is TOTAL k+m

    def __post_init__(self):
        if self.mode == "replication":
            if not (1 <= self.w <= self.n and 1 <= self.r <= self.n and self.w+self.r > self.n):
                raise ValueError("Replication requires 1<=W,R<=N and W+R>N")
        elif self.mode == "erasure":
            if not (1 <= self.k and 1 <= self.m and self.k+self.m <= 256):
                raise ValueError("Erasure coding requires k>=1, m>=1, k+m<=256")
        else:
            raise ValueError("mode must be replication or erasure")

    @property
    def total(self):
        return self.n if self.mode == "replication" else self.k+self.m

    @property
    def write_required(self):
        # Initial EC writes require ALL shards, preserving the declared m tolerance.
        return self.w if self.mode == "replication" else self.total

    @property
    def read_required(self):
        return self.r if self.mode == "replication" else self.k

    @property
    def key(self):
        value = f"rep:{self.n}:{self.w}:{self.r}" if self.mode == "replication" else f"rs:{self.k}:{self.m}"
        return value

    def to_dict(self):
        return asdict(self)

    def encode(self, data):
        if self.mode == "replication":
            return [bytes(data)] * self.n
        width = max(1, (len(data)+self.k-1)//self.k)
        padded = bytes(data) + b"\0" * (width*self.k-len(data))
        primary = [padded[i*width:(i+1)*width] for i in range(self.k)]
        return Encoder(self.k, self.total).encode(primary)

    def decode(self, available, original_size, expected_hash):
        if len(available) < self.read_required:
            raise RuntimeError(f"Read quorum unavailable: need {self.read_required}, got {len(available)}")
        if self.mode == "replication":
            data = next(iter(available.values()))
        else:
            indices = sorted(available)[:self.k]
            data = b"".join(Decoder(self.k, self.total).decode(
                [available[i] for i in indices], indices))[:original_size]
        if len(data) != original_size or hashlib.sha256(data).hexdigest() != expected_hash:
            raise RuntimeError("Reconstructed chunk failed SHA-256 integrity verification")
        return bytes(data)
