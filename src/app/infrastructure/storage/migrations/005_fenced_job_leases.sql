ALTER TABLE copy_jobs
    ADD COLUMN IF NOT EXISTS lease_generation bigint NOT NULL DEFAULT 0;

ALTER TABLE copy_jobs
    ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz;

-- Give jobs running before this migration one bounded legacy ownership window.
-- Subsequent claims always advance the fenced generation.
UPDATE copy_jobs
SET lease_expires_at = clock_timestamp() + interval '30 seconds'
WHERE state IN ('running', 'waiting_rate_limit')
  AND lease_expires_at IS NULL;

CREATE INDEX IF NOT EXISTS copy_jobs_expired_lease
    ON copy_jobs (lease_expires_at)
    WHERE state IN ('running', 'waiting_rate_limit');
