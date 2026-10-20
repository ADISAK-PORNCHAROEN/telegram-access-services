ALTER TABLE copy_jobs
    ADD COLUMN IF NOT EXISTS cancel_requested_at timestamptz;

ALTER TABLE copy_jobs
    ADD COLUMN IF NOT EXISTS next_eligible_at timestamptz;

ALTER TABLE copy_jobs
    DROP CONSTRAINT IF EXISTS copy_jobs_state_check;

ALTER TABLE copy_jobs
    ADD CONSTRAINT copy_jobs_state_check CHECK (state IN
        ('queued', 'running', 'waiting_rate_limit', 'completed', 'partial',
         'failed', 'expired', 'cancelled', 'delivery_unknown'));
