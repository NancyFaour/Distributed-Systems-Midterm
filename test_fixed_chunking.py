import hashlib
import random
from pathlib import Path

from chunking import DEFAULT_CHUNK_SIZE, fixed_chunks

TEST_DIR = Path(__file__).resolve().parent / "data" / "fixed-chunk-test"
TEST_DIR.mkdir(parents=True, exist_ok=True)

original = TEST_DIR / "original.bin"
rebuilt = TEST_DIR / "rebuilt.bin"

# Reproducible test file: 10 MiB plus 123 bytes.
random_source = random.Random(42)

with original.open("wb") as file:
    for _ in range(10):
        file.write(random_source.randbytes(1024 * 1024))
    file.write(random_source.randbytes(123))

chunk_hashes = []
chunk_sizes = []

with original.open("rb") as source:
    for index, content in enumerate(fixed_chunks(source)):
        digest = hashlib.sha256(content).hexdigest()
        (TEST_DIR / f"{digest}.chunk").write_bytes(content)

        chunk_hashes.append(digest)
        chunk_sizes.append(len(content))

        print(f"Chunk {index}: {len(content)} bytes")
        print(f"SHA-256: {digest}")

# Reconstruct in the original chunk order.
with rebuilt.open("wb") as destination:
    for digest in chunk_hashes:
        content = (TEST_DIR / f"{digest}.chunk").read_bytes()
        assert hashlib.sha256(content).hexdigest() == digest
        destination.write(content)

with original.open("rb") as file:
    original_hash = hashlib.file_digest(file, "sha256").hexdigest()

with rebuilt.open("rb") as file:
    rebuilt_hash = hashlib.file_digest(file, "sha256").hexdigest()

assert len(chunk_hashes) == 3
assert chunk_sizes == [
    DEFAULT_CHUNK_SIZE,
    DEFAULT_CHUNK_SIZE,
    2 * 1024 * 1024 + 123,
]
assert original.stat().st_size == rebuilt.stat().st_size
assert original_hash == rebuilt_hash

print("\nOriginal size:", original.stat().st_size)
print("Rebuilt size: ", rebuilt.stat().st_size)
print("Original SHA-256:", original_hash)
print("Rebuilt SHA-256: ", rebuilt_hash)
print("PASS: fixed-size chunking and exact reconstruction")