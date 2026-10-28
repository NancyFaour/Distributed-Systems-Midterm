BEGIN;

CREATE TABLE IF NOT EXISTS buckets (
    bucket_name TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS object_versions (
    object_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bucket_name TEXT NOT NULL REFERENCES buckets(bucket_name),
    object_key TEXT NOT NULL,
    version BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    chunking_method TEXT NOT NULL DEFAULT 'fixed'
        CHECK (chunking_method IN ('fixed', 'cdc')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (bucket_name, object_key, version)
);

CREATE INDEX IF NOT EXISTS object_lookup
    ON object_versions (bucket_name, object_key, version DESC);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_hash TEXT PRIMARY KEY
        CHECK (chunk_hash ~ '^[0-9a-f]{64}$'),
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    ref_count BIGINT NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS object_chunks (
    object_id UUID NOT NULL
        REFERENCES object_versions(object_id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
    chunk_hash TEXT NOT NULL REFERENCES chunks(chunk_hash),
    PRIMARY KEY (object_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS chunk_reference_lookup
    ON object_chunks (chunk_hash);

CREATE TABLE IF NOT EXISTS chunk_locations (
    chunk_hash TEXT NOT NULL
        REFERENCES chunks(chunk_hash) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    PRIMARY KEY (chunk_hash, node_id)
);

COMMIT;