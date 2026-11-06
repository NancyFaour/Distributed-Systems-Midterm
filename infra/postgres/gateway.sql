BEGIN;
CREATE TABLE IF NOT EXISTS node_heartbeats (
    node_id TEXT PRIMARY KEY,
    storage_url TEXT NOT NULL,
    free_space_bytes BIGINT NOT NULL,
    chunk_count BIGINT NOT NULL,
    last_seen DOUBLE PRECISION NOT NULL
);
ALTER TABLE object_versions ADD COLUMN IF NOT EXISTS tags JSONB NOT NULL DEFAULT '{}';
ALTER TABLE object_versions ADD COLUMN IF NOT EXISTS content_type TEXT NOT NULL DEFAULT 'application/octet-stream';
ALTER TABLE object_versions ADD COLUMN IF NOT EXISTS content_sha256 TEXT;
CREATE INDEX IF NOT EXISTS object_tags ON object_versions USING gin(tags);
CREATE TABLE IF NOT EXISTS api_rate_limits (
    key_hash TEXT NOT NULL,
    window_id BIGINT NOT NULL,
    requests INTEGER NOT NULL,
    PRIMARY KEY(key_hash,window_id)
);
CREATE TABLE IF NOT EXISTS search_outbox (
    object_id UUID PRIMARY KEY,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE OR REPLACE FUNCTION queue_search_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO search_outbox(object_id) VALUES (COALESCE(NEW.object_id,OLD.object_id))
    ON CONFLICT(object_id) DO UPDATE SET updated_at=now();
    RETURN COALESCE(NEW,OLD);
END $$;
DROP TRIGGER IF EXISTS object_search_update ON object_versions;
CREATE TRIGGER object_search_update AFTER INSERT OR UPDATE OR DELETE ON object_versions
    FOR EACH ROW EXECUTE FUNCTION queue_search_update();
INSERT INTO search_outbox(object_id) SELECT object_id FROM object_versions ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS storage_members (
    node_id TEXT PRIMARY KEY,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS maintenance_control (
    id INTEGER PRIMARY KEY CHECK(id=1),
    paused BOOLEAN NOT NULL DEFAULT FALSE
);
INSERT INTO maintenance_control(id) VALUES (1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS maintenance_jobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind TEXT NOT NULL CHECK(kind IN ('repair','scrub','rebalance','gc','search')),
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','queued','running','done','failed')),
    automatic BOOLEAN NOT NULL DEFAULT FALSE,
    attempts INTEGER NOT NULL DEFAULT 0,
    worker_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    result JSONB,
    error TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_job_kind ON maintenance_jobs(kind)
    WHERE status IN ('pending','queued','running');
COMMIT;
