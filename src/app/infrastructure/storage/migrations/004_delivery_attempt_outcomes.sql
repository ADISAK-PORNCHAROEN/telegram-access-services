CREATE TABLE IF NOT EXISTS copy_delivery_attempts (
    attempt_id text PRIMARY KEY,
    job_id text NOT NULL REFERENCES copy_jobs(job_id) ON DELETE CASCADE,
    intent jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('intent', 'confirmed', 'rejected')),
    outcomes jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz
);

CREATE INDEX IF NOT EXISTS copy_delivery_attempts_job
    ON copy_delivery_attempts (job_id, created_at, attempt_id);
