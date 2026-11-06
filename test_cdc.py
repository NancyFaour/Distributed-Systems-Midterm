import hashlib
import io
import random
from chunking import fixed_chunks, content_defined_chunks

original = random.Random(42).randbytes(2*1024*1024)
modified = original[:100000] + b"INSERTED CONTENT!" + original[100000:]
rates = {}
for name, chunker in (
    ("fixed", lambda stream: fixed_chunks(stream, 32768)),
    ("cdc", lambda stream: content_defined_chunks(stream, 8192, 32768, 131072)),
):
    before = list(chunker(io.BytesIO(original)))
    after = list(chunker(io.BytesIO(modified)))
    assert b"".join(before) == original and b"".join(after) == modified
    old_hashes = {hashlib.sha256(part).hexdigest() for part in before}
    reusable = sum(len(part) for part in after
                   if hashlib.sha256(part).hexdigest() in old_hashes)
    rates[name] = reusable / len(modified)
    print(f"{name.upper()}: before={len(before)} chunks, after={len(after)} chunks")
    print(f"Reusable bytes after insertion: {reusable}/{len(modified)} ({rates[name]:.2%})")
    if name == "cdc":
        assert all(8192 <= len(part) <= 131072 for part in after[:-1])
        assert len(after[-1]) <= 131072
    print("PASS: exact reconstruction and SHA-256 content addressing")
assert rates["cdc"] > rates["fixed"] and rates["cdc"] > 0.8
print("PASS: rolling-hash chunking preserves more reusable data after insertion")
