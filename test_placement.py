import hashlib
from collections import Counter

from consistent_hashing import ConsistentHashRing

nodes = ["node-1", "node-2", "node-3", "node-4"]
ring = ConsistentHashRing(nodes, virtual_nodes=128)

# Reproducible sample of 10,000 chunk identifiers.
hashes = [
    hashlib.sha256(f"placement-test-{i}".encode()).hexdigest()
    for i in range(10_000)
]

before = {digest: ring.get_node(digest) for digest in hashes}

print("INITIAL DISTRIBUTION:", dict(sorted(Counter(before.values()).items())))

# Addition: four nodes become five.
ring.add_node("node-5")
after_addition = {digest: ring.get_node(digest) for digest in hashes}

moved_on_addition = [
    digest for digest in hashes
    if before[digest] != after_addition[digest]
]

assert all(
    after_addition[digest] == "node-5"
    for digest in moved_on_addition
)

print(
    f"ADD node-5: {len(moved_on_addition)}/{len(hashes)} "
    f"chunks reassigned ({len(moved_on_addition) / len(hashes):.2%})"
)
print("PASS: changed assignments move only to the added node")

# Removing the added node must restore the original assignments.
ring.remove_node("node-5")
assert all(ring.get_node(digest) == before[digest] for digest in hashes)
print("PASS: original placement restored after removing node-5")

# Removal: original four nodes become three.
ring.remove_node("node-4")
after_removal = {digest: ring.get_node(digest) for digest in hashes}

moved_on_removal = [
    digest for digest in hashes
    if before[digest] != after_removal[digest]
]

assert all(before[digest] == "node-4" for digest in moved_on_removal)

print(
    f"REMOVE node-4: {len(moved_on_removal)}/{len(hashes)} "
    f"chunks reassigned ({len(moved_on_removal) / len(hashes):.2%})"
)
print("PASS: only chunks assigned to the removed node are reassigned")