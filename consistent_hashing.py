import hashlib
import re
from bisect import bisect_left


class ConsistentHashRing:
    def __init__(self, nodes, virtual_nodes=128):
        if virtual_nodes < 1:
            raise ValueError("virtual_nodes must be positive")

        self.virtual_nodes = virtual_nodes
        self.nodes = set(nodes)
        self._rebuild()

    def _rebuild(self):
        points = []

        for node in sorted(self.nodes):
            for number in range(self.virtual_nodes):
                label = f"{node}:{number}".encode()
                position = int(hashlib.sha256(label).hexdigest(), 16)
                points.append((position, node))

        points.sort()
        self.positions = [position for position, _ in points]
        self.owners = [node for _, node in points]

    def add_node(self, node):
        self.nodes.add(node)
        self._rebuild()

    def remove_node(self, node):
        self.nodes.remove(node)
        self._rebuild()

    def get_node(self, chunk_hash):
        if not self.positions:
            raise ValueError("No storage nodes available")

        if not re.fullmatch(r"[0-9a-f]{64}", chunk_hash):
            raise ValueError("Expected a lowercase SHA-256 hash")

        position = int(chunk_hash, 16)
        index = bisect_left(self.positions, position)

        # Wrap around when the hash is beyond the final ring position.
        index %= len(self.positions)
        return self.owners[index]

    def get_nodes(self, chunk_hash, count):
        """Select distinct physical owners clockwise, never repeated virtual nodes."""
        if count < 1 or count > len(self.nodes):
            raise ValueError("Not enough distinct storage nodes for this policy")
        self.get_node(chunk_hash)  # validate hash and nonempty ring
        start = bisect_left(self.positions, int(chunk_hash, 16)) % len(self.positions)
        selected = []
        for offset in range(len(self.positions)):
            owner = self.owners[(start + offset) % len(self.owners)]
            if owner not in selected:
                selected.append(owner)
                if len(selected) == count:
                    return selected
        raise RuntimeError("Could not select enough distinct owners")
