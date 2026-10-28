from typing import BinaryIO, Iterator
from collections import deque

DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB


def fixed_chunks(
    source: BinaryIO,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> Iterator[bytes]:
    """Read a file in fixed-size chunks without loading it all into memory."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    while True:
        chunk = source.read(chunk_size)
        if not chunk:
            break
        yield chunk


def content_defined_chunks(source: BinaryIO, minimum=256*1024,
                           average=1024*1024, maximum=4*1024*1024,
                           window_size=64) -> Iterator[bytes]:
    """Rabin-Karp rolling fingerprint; SHA-256 remains the chunk identifier."""
    if not (0 < minimum <= average <= maximum) or average & (average - 1):
        raise ValueError("Require min <= average <= max and a power-of-two average")
    if window_size < 1:
        raise ValueError("window_size must be positive")
    mask64 = (1 << 64) - 1
    outgoing_factor = pow(257, window_size - 1, 1 << 64)
    window = deque()
    fingerprint = 0
    chunk = bytearray()
    while block := source.read(65536):
        for value in block:
            if len(window) == window_size:
                fingerprint = (fingerprint - (window.popleft() + 1)*outgoing_factor) & mask64
            fingerprint = (fingerprint*257 + value + 1) & mask64
            window.append(value)
            chunk.append(value)
            if len(chunk) >= minimum and (
                    fingerprint & (average - 1) == 0 or len(chunk) >= maximum):
                yield bytes(chunk)
                chunk.clear()
    if chunk:
        yield bytes(chunk)
