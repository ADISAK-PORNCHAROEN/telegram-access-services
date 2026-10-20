CREATE TABLE IF NOT EXISTS copy_jobs (
    job_id text PRIMARY KEY,
    receiving_bot_id bigint NOT NULL,
    requester_id bigint NOT NULL,
    destination_chat bigint NOT NULL,
    request_message_id bigint NOT NULL,
    link_index integer NOT NULL CHECK (link_index >= 0),
    source_chat text NOT NULL,
    source_message_id bigint NOT NULL,
    source_is_private boolean NOT NULL,
    filter_kind text CHECK (filter_kind IS NULL OR filter_kind IN ('photo', 'video', 'file')),
    state text NOT NULL CHECK (state IN
        ('queued', 'running', 'completed', 'partial', 'failed', 'delivery_unknown')),
    stage text NOT NULL CHECK (stage IN
        ('queued', 'resolve', 'download', 'upload', 'send', 'terminal')),
    send_intent_at timestamptz,
    result jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (receiving_bot_id, destination_chat, request_message_id, link_index)
);

CREATE INDEX IF NOT EXISTS copy_jobs_queue_order
    ON copy_jobs (created_at, job_id) WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS copy_jobs_pending_requester
    ON copy_jobs (requester_id) WHERE state = 'queued';
