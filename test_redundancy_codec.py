import hashlib
from itertools import combinations

from redundancy import Policy

data = b"Distributed storage redundancy test." * 1000
digest = hashlib.sha256(data).hexdigest()

# Replication: N=3, W=2, R=2.
replication = Policy(mode="replication", n=3, w=2, r=2)
copies = replication.encode(data)

assert len(copies) == 3
assert all(copy == data for copy in copies)
assert replication.w + replication.r > replication.n
print("PASS: replication creates three copies; W+R>N")

restored = replication.decode(
    {0: copies[0], 2: copies[2]}, len(data), digest
)
assert restored == data
print("PASS: replication read succeeds with two valid copies")

try:
    replication.decode({0: copies[0]}, len(data), digest)
except RuntimeError:
    print("PASS: replication rejects a read below R=2")
else:
    raise AssertionError("Read quorum was not enforced")

try:
    Policy(mode="replication", n=3, w=1, r=2)
except ValueError:
    print("PASS: invalid quorum configuration rejected")
else:
    raise AssertionError("Invalid quorum accepted")

# Reed–Solomon: four data shards plus two parity shards.
erasure = Policy(mode="erasure", k=4, m=2)
shards = erasure.encode(data)
assert len(shards) == 6

tested = 0
for selected in combinations(range(6), 4):
    available = {index: shards[index] for index in selected}
    restored = erasure.decode(available, len(data), digest)
    assert restored == data
    tested += 1

print(f"PASS: Reed-Solomon reconstructs from all {tested} combinations of 4/6 shards")

try:
    erasure.decode(
        {index: shards[index] for index in range(3)},
        len(data),
        digest,
    )
except RuntimeError:
    print("PASS: reconstruction rejected with fewer than four shards")
else:
    raise AssertionError("Insufficient shards accepted")

print("ALL CODEC AND POLICY TESTS PASSED")