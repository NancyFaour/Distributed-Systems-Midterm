BEGIN;
CREATE TABLE IF NOT EXISTS redundancy_policies (
    policy_key TEXT PRIMARY KEY,
    policy JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS bucket_policies (
    bucket_name TEXT PRIMARY KEY REFERENCES buckets(bucket_name),
    policy_key TEXT NOT NULL REFERENCES redundancy_policies(policy_key)
);
CREATE TABLE IF NOT EXISTS object_policies (
    object_id UUID PRIMARY KEY REFERENCES object_versions(object_id) ON DELETE CASCADE,
    policy_key TEXT NOT NULL REFERENCES redundancy_policies(policy_key)
);
CREATE TABLE IF NOT EXISTS chunk_representations (
    chunk_hash TEXT NOT NULL REFERENCES chunks(chunk_hash) ON DELETE CASCADE,
    policy_key TEXT NOT NULL REFERENCES redundancy_policies(policy_key),
    ref_count BIGINT NOT NULL CHECK (ref_count >= 0),
    PRIMARY KEY (chunk_hash, policy_key)
);
CREATE TABLE IF NOT EXISTS fragment_locations (
    chunk_hash TEXT NOT NULL,
    policy_key TEXT NOT NULL,
    fragment_index INTEGER NOT NULL CHECK (fragment_index >= 0),
    fragment_hash TEXT NOT NULL CHECK (fragment_hash ~ '^[0-9a-f]{64}$'),
    node_id TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    status TEXT NOT NULL CHECK (status IN ('healthy','missing','corrupt')),
    PRIMARY KEY (chunk_hash, policy_key, fragment_index),
    UNIQUE (chunk_hash, policy_key, node_id),
    FOREIGN KEY (chunk_hash, policy_key)
        REFERENCES chunk_representations(chunk_hash, policy_key) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS fragment_physical_lookup ON fragment_locations(node_id, fragment_hash);
CREATE TABLE IF NOT EXISTS recovery_events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    chunk_hash TEXT,
    policy_key TEXT,
    cause TEXT NOT NULL,
    outcome TEXT NOT NULL,
    read_bytes BIGINT NOT NULL DEFAULT 0,
    written_bytes BIGINT NOT NULL DEFAULT 0,
    detail TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS obsolete_fragments (
    node_id TEXT NOT NULL,
    fragment_hash TEXT NOT NULL,
    chunk_hash TEXT NOT NULL,
    PRIMARY KEY (node_id, fragment_hash)
);
COMMIT;
