ALTER TABLE copy_jobs
    ADD COLUMN IF NOT EXISTS rate_limit_kind text;

ALTER TABLE copy_jobs
    ADD COLUMN IF NOT EXISTS rate_limit_account text;

ALTER TABLE copy_jobs
    ADD COLUMN IF NOT EXISTS rate_limit_required_seconds double precision;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'copy_jobs_rate_limit_kind_check'
          AND conrelid = 'copy_jobs'::regclass
    ) THEN
        ALTER TABLE copy_jobs
            ADD CONSTRAINT copy_jobs_rate_limit_kind_check
            CHECK (rate_limit_kind IS NULL OR rate_limit_kind IN ('flood', 'premium'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'copy_jobs_rate_limit_account_check'
          AND conrelid = 'copy_jobs'::regclass
    ) THEN
        ALTER TABLE copy_jobs
            ADD CONSTRAINT copy_jobs_rate_limit_account_check
            CHECK (rate_limit_account IS NULL OR rate_limit_account IN ('owner', 'bot', 'unknown'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'copy_jobs_rate_limit_required_seconds_check'
          AND conrelid = 'copy_jobs'::regclass
    ) THEN
        ALTER TABLE copy_jobs
            ADD CONSTRAINT copy_jobs_rate_limit_required_seconds_check
            CHECK (rate_limit_required_seconds IS NULL OR rate_limit_required_seconds > 0);
    END IF;
END $$;
