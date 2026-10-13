CREATE TABLE distribution_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    segment_id BIGINT NOT NULL,
    segment_code VARCHAR(64) NOT NULL,
    percent NUMERIC NOT NULL CHECK (percent >= 0 AND percent <= 100),
    snapshot_users BIGINT NOT NULL CHECK (snapshot_users >= 0),
    target_users BIGINT NOT NULL CHECK (target_users >= 0 AND target_users <= snapshot_users),
    existing_users BIGINT NOT NULL CHECK (existing_users >= 0 AND existing_users <= target_users),
    planned_users BIGINT NOT NULL CHECK (planned_users = target_users - existing_users),
    assigned_users BIGINT NOT NULL DEFAULT 0 CHECK (assigned_users >= 0),
    batch_count INT NOT NULL CHECK (batch_count >= 0),
    processed_batches INT NOT NULL DEFAULT 0 CHECK (processed_batches >= 0 AND processed_batches <= batch_count),
    status TEXT NOT NULL CHECK (status IN ('queued','running','retrying','completed','failed')),
    last_error TEXT NOT NULL DEFAULT '',
    retry_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX distribution_one_active_segment ON distribution_jobs(segment_id)
WHERE status IN ('queued','running','retrying');
CREATE INDEX distribution_segment_history ON distribution_jobs(segment_id,created_at);
CREATE TABLE outbox (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES distribution_jobs(id),
    batch_number INT NOT NULL CHECK (batch_number >= 0),
    payload JSONB NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    UNIQUE(job_id,batch_number)
);
CREATE INDEX outbox_pending ON outbox(available_at,id) WHERE published_at IS NULL;
CREATE TABLE processed_batches (
    job_id UUID NOT NULL,
    batch_number INT NOT NULL,
    inserted_users INT NOT NULL DEFAULT 0,
    consumer_id TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(job_id,batch_number),
    FOREIGN KEY(job_id,batch_number) REFERENCES outbox(job_id,batch_number)
);
CREATE TABLE kafka_dead_letters (
    topic TEXT NOT NULL,
    partition INT NOT NULL,
    message_offset BIGINT NOT NULL,
    payload BYTEA NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(topic,partition,message_offset)
);
