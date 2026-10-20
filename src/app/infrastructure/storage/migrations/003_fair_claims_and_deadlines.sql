ALTER TABLE copy_jobs
    ADD COLUMN IF NOT EXISTS deadline_at timestamptz;

UPDATE copy_jobs
SET deadline_at = created_at + interval '30 minutes'
WHERE deadline_at IS NULL;

ALTER TABLE copy_jobs
    ALTER COLUMN deadline_at SET DEFAULT (now() + interval '30 minutes');

ALTER TABLE copy_jobs
    ALTER COLUMN deadline_at SET NOT NULL;

ALTER TABLE copy_jobs
    ADD COLUMN IF NOT EXISTS claimed_at timestamptz;

CREATE INDEX IF NOT EXISTS copy_jobs_pending_fairness
    ON copy_jobs (requester_id, created_at, job_id) WHERE state = 'queued';

CREATE INDEX IF NOT EXISTS copy_jobs_claim_history
    ON copy_jobs (requester_id, claimed_at DESC) WHERE claimed_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS copy_jobs_active_requester
    ON copy_jobs (requester_id) WHERE state IN ('running', 'waiting_rate_limit');

CREATE INDEX IF NOT EXISTS copy_jobs_deadline_queue
    ON copy_jobs (deadline_at, created_at) WHERE state = 'queued';
